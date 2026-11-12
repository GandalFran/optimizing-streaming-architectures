# E7 — executable validation of generated configurations

`deploy_check.py` deploys **one** model answer (Kafka topics + Flink configuration
+ Flink SQL tables) on a fresh, local Apache Kafka 4.1.2 + Apache Flink 1.20.5
testbed, drives it with load, injects a TaskManager failure, and measures five
objective runtime criteria. `run_e7.sbatch` runs a list of deployments one after
another inside one SLURM CPU job.

Nothing is simulated or hand-entered. A quantity that could not be measured is
`null` and `null_reasons` says why; a deployment this testbed cannot judge gets
`untestable_reason` and no criteria.

## Files

| file | role |
|---|---|
| `deploy_check.py` | driver: one deployment per invocation, result JSON + artefact directory |
| `e7_artefacts.py` | answer → deployed artefacts (topic commands, Flink config, job SQL, record templates); records every substitution |
| `e7_gen.py` | load generator process (pre-serialised records, shared start time, idempotent acks=all producer) |
| `e7_sink_reader.py` | `read_committed` consumer on the sink topic (seq, sent time, receive time per record) |
| `run_e7.sbatch` | SLURM job: a JSONL list of (id, model, seed) deployed sequentially, resumable |
| `e7_smoke_answers.py` | writes the smoke-test answers (GOLD answers of `tests/test_validator.py` and variants) |
| `e7_harness_check.py` | calibration of the measurement chain without Flink (generator → topic → reader) |

It imports repository-local primitives in `testbed/vendor/`
(`topology.allocate`, `KafkaCluster`, `FlinkCluster`, `SysMon`) and the local
port allocator in `testbed/ports.py`, plus the static validator's parsers in
`src/kfbench` (block classification, `parse_topic_commands`, `load_yaml_docs`,
`flatten`, CREATE TABLE parsing, effective-value helpers), so the runtime check
reads an answer exactly as the static validator does.

## How to run

```bash
# one deployment (inside an allocation of >= 44 CPUs)
python testbed/deploy_check.py --id CASE-banking-native \
    --answer-file runs/e7/smoke_answers/a_gold_native.txt --model gold --out runs/e7/x.json
python testbed/deploy_check.py --id ID-001 \
    --answers runs/main/qwen2.5-14b.greedy.jsonl --model qwen2.5-14b --seed 0 --out runs/e7/ID-001__qwen.json

# a list (one line per deployment: {"id":..., "model":..., "seed":...}; see the header of run_e7.sbatch)
LIST=runs/e7/sample.jsonl sbatch testbed/run_e7.sbatch          # 180 s load, ~6 min per deployment
LIST=... DURATION=60 OUTDIR=runs/e7/smoke sbatch testbed/run_e7.sbatch
```

Result: `<out>.json`; artefacts in `<out>/` (answer, deployed `config.yaml` and
`job.sql`, sql-client output, generator and reader results, per-record sink data
`sink_records.npz`, `sysmon.jsonl`, `topology.json`, tails of every broker /
JobManager / TaskManager log).

SLURM shape: `--cpus-per-task=44 --mem=128G`, no GPU. The 22 physical cores are
carved by the bundled `topology.allocate` into role-dedicated, NUMA-local nodes
(whole cores, both SMT siblings; `numactl --physcpubind --membind`):

| role | nodes | physical cores (logical CPUs) each | runs |
|---|---|---|---|
| kafka | 3 | 2 (4) | one KRaft broker+controller each |
| flink | 3 | 4 (8) | one TaskManager each ("each TaskManager has 4 CPU cores") |
| jm | 1 | 1 (2) | the JobManager |
| control | 1 | 2 (4) | load generators, sql-client |
| meas | 1 | 1 (2) | the read_committed sink reader (never shares cores with load generation) |

`--kafka-cores/--tm-cores/--jm-cores/--ctl-cores/--meas-cores` change the shape;
the layout used is saved in `topology.json` and `topology` (incl. whether a node
had to span NUMA domains). The CPU use of every component during the steady
window is reported (`cpu_cores_steady`, `cpu_capacity_logical`) so a saturated
testbed component can be told apart from an under-provisioned answer.

Why 2 cores per broker (44 rather than 36 CPUs): with 1 core per broker the
brokers, not the answer, limited the GOLD exactly-once pipeline at 50 000 ev/s ×
1 KB. Same answer, same 180 s protocol: 1-core brokers → steady p50/p99 latency
3.6 s / 12.7 s, 575 completed checkpoints, sink 49.4k ev/s, generator clients
backlogged (≈0.8 cores each); 2-core brokers → 0.50 s / 2.45 s, 1 320
checkpoints, sink 50.2k ev/s, generators 0.13 cores each
(`runs/e7/smoke/a_gold_native_180s_S{1_k1,2_k2}meas1.json`).

Measurement-chain floor (`e7_harness_check.py`: generator → topic with 12
partitions, RF 3 → read_committed reader, no Flink, 50 000 ev/s × 1 KB, 1-core
brokers, reader on the control node): p50 5.8 ms, p95 8.7 ms, p99 10.6 ms,
p99.9 19.3 ms, all 2 999 799 records received (`runs/e7/smoke/harness_check.json`).
Runtime latencies include this floor.

## What one deployment does

1. **Kafka**: fresh 3-broker KRaft cluster with **Kafka's default broker
   settings** (`DefaultKafka` overrides the bundled `_props`). Only `process.roles`,
   `node.id`, `controller.quorum.voters`, listeners/advertised listeners,
   `controller.listener.names`, the listener→protocol map and `log.dirs` are set;
   heap 4 GiB. Not set (defaults apply): `num.partitions` (1),
   `default.replication.factor` (1), `min.insync.replicas` (1),
   `transaction.max.timeout.ms` (15 min), retention (7 d),
   `auto.create.topics.enable` (true), `compression.type` (producer).
   The properties actually used are in `kafka_broker_properties`.
2. **Topics**. Native track: every `kafka-topics` command the validator
   recognises is executed as written (argv, no shell), with exactly two
   substitutions: the binary path (`kafka-topics`, `kafka-topics.sh`,
   `bin/kafka-topics.sh`, … → the testbed's `kafka-topics.sh`) and the value of
   `--bootstrap-server`. A command prefix such as `docker exec kafka` and shell
   suffixes (`&&`, `||`, `;`, `|`, redirections) are dropped and recorded.
   Commands with removed flags (e.g. `--zookeeper`) run as written and fail;
   rc and stderr are recorded. Other shell lines (e.g. `kafka-configs.sh`) are not
   run and are listed in `ignored.commands`. k8s track: each Strimzi `KafkaTopic`
   is translated to `kafka-topics.sh --create --topic <spec.topicName or
   metadata.name> [--partitions spec.partitions] [--replication-factor
   spec.replicas] [--config k=v …]`; absent fields are omitted so broker
   defaults apply. A resource the Topic Operator would not reconcile (apiVersion
   not served by Strimzi 0.48, or a `strimzi.io/cluster` label that does not name
   the scenario's cluster) creates no topic. Every translation is in
   `translations`.
3. **Flink session cluster**: JobManager on its own core, TaskManagers each on a
   4-core node (`-Dtaskmanager.resource-id=tmN`). Native: 3 TaskManagers. k8s:
   `spec.taskManager.replicas` if set, else `ceil(parallelism / slots)` as the
   Flink Kubernetes Operator would request, capped at 3 (the prompt's pod
   limit), so a job needing more slots fails to schedule. Configuration
   (`config.yaml`, written flat):
   * base = Flink 1.20.5's **shipped** `conf/config.yaml` (JM 1600m, TM 1728m,
     1 slot, parallelism 1, region failover); the shipped total-memory default of
     the JM or TM is dropped when the answer sizes that process's memory itself
     (any of `*.memory.process.size`, `*.memory.flink.size`, `*.memory.heap.size`,
     `taskmanager.memory.task.heap.size`, `taskmanager.memory.managed.size`, legacy
     `*.heap.size`), so the shipped value cannot create a conflict the answer did
     not write;
   * + the answer's keys verbatim (native: first YAML mapping; k8s:
     `spec.flinkConfiguration`, plus `spec.job.parallelism → parallelism.default`,
     `spec.taskManager.resource.memory → taskmanager.memory.process.size`,
     `spec.jobManager.resource.memory → jobmanager.memory.process.size`, Gi/Mi
     converted, all recorded as translations);
   * substitutions (each recorded in `substitutions` with key, old and new value):
     object-store URIs `s3://, s3a://, s3p://, s3n://, hdfs://, gs://, wasb(s)://,
     abfs(s)://, oss://, viewfs://` → `file://<run_dir>/objstore/<bucket>/<path>`;
     absolute local paths in path-valued keys (`*dir`, `*dirs`, `*path`,
     `*localdir`, `*storageDir`) → under `<run_dir>/localfs/`; testbed-owned keys
     (all `env.*`, metric reporters, `io.tmp.dirs`, and every `*.port(s)`,
     `*.bind-port`, `*.address`, `*.bind-address`, `*.host`, `*.bind-host` of
     `jobmanager/taskmanager/rest/web/blob/query/metrics/high-availability`) are
     dropped and replaced by the testbed's values; k8s only: `execution.target`
     (set by the operator). High availability (`high-availability[.type]` other
     than NONE) is dropped with all `high-availability.*` keys: there is no
     ZooKeeper or Kubernetes HA service and the JobManager is never killed;
   * everything else — slots, parallelism, checkpointing, state backend, restart
     strategy, memory — is the answer's (or Flink's default).
   Memory: if the answer's TaskManager memory (validator semantics) exceeds the
   scenario's per-TaskManager budget (8 GiB), the deployment is **untestable**:
   there is no per-node cgroup limit, so an OOM kill / unschedulable pod cannot
   be reproduced.
4. **SQL job**. All of the answer's `CREATE TABLE` statements are submitted with
   the `properties.bootstrap.servers` value substituted. Source and sink are the
   tables whose `topic` option equals `spec.src` / `spec.sink` (validator
   matching). Augmentation, and nothing else: columns `` `__sent_ms` BIGINT `` and
   `` `__seq` BIGINT `` are added to the source and sink tables (before the first
   WATERMARK / PRIMARY KEY clause), and
   `INSERT INTO <sink> (<physical and writable metadata columns>, __sent_ms, __seq)
   SELECT …, __sent_ms, __seq FROM <source>` is generated: a sink column takes the
   same-named source column (wrapped in `CAST` when the declared types differ,
   `TRY_CAST` across incompatible type families), else `CAST(NULL AS <type>)`; a
   NOT NULL / PRIMARY KEY sink column without a source column gets a typed
   constant (`''`, `0`, `FALSE`, `CURRENT_TIMESTAMP`) instead, recorded in
   `augmentations`, because a NULL would fail Flink's not-null enforcer through
   the harness, not the answer. The answer's other SQL statements (its own
   INSERT, SET, views) are not executed and are listed in `ignored.sql_statements`.
   Submission: `sql-client.sh -f job.sql` against the session cluster
   (streaming, Flink defaults). A source/sink format whose jar is not on the
   testbed (avro, avro-confluent, protobuf, parquet, orc) → untestable. The
   generator writes plain JSON and the reader parses JSON-family records, so a
   csv/raw source or sink is also untestable (reason recorded).
5. **Load** (starts 5 s after every vertex is RUNNING): records with the source
   table's physical columns, random values per SQL type (timestamps = current
   time, in the table's `json.timestamp-format.standard`), one STRING column
   padded so the value incl. the measurement fields is ≈ `spec.event_bytes` (a
   non-key STRING column if there is one, else the column the prompt names as the
   key), plus `__sent_ms` (wall clock just before `produce()`) and `__seq`
   (0, 1, 2, … interleaved over generator processes). Kafka record keys are
   written only when the source table declares key fields. Rate
   `min(spec.throughput_eps, 50 000)` ev/s (cap recorded), acks=all, idempotent
   producer, `ceil(rate/30 000)` processes (the bundled design: pre-serialised
   records, shared start time), for `--duration` (180 s) seconds. Failed
   deliveries are recorded per sequence number.
6. **Measurement**: `read_committed` consumer on the sink topic, on its own
   node, subscribed before submission (it never creates the topic). Latency per record =
   receive wall clock − `__sent_ms` (one host clock; receive time taken when the
   poll batch returns, ≤ 2 ms). Flink REST is polled every 2 s (job state,
   checkpoint counts); final state, checkpoint counts, `numRestarts` and the
   exception history (`/jobs/:id/exceptions`) are recorded.
7. **Failure** (delivery exactly-once or at-least-once): at t = duration/2
   (90 s) the TaskManager holding the most busy slots is SIGKILLed and a new
   TaskManager is started on the same node 15 s later. After the
   load stops the sink may drain for up to 60 s (stops early once every
   acknowledged record was seen).
8. **Teardown** in `finally` (also on SIGTERM / SLURM timeout): generators,
   reader, job cancel, TaskManagers, JobManager, brokers; logs are copied out,
   node state deleted, the port block released; `teardown_leftovers` lists any
   process that survived (the sbatch additionally kills anything under the run
   root before the next deployment).

## Criteria

Steady-state window W = [t0 + warm-up (duration/6 = 30 s), kill time − guard
(duration/18 = 10 s)], or until the load end when no failure is injected.

| criterion | definition |
|---|---|
| C1_deploy | source and sink topics exist after the answer's commands **and** every job vertex is RUNNING ≤ 60 s after `sql-client` starts |
| C2_checkpoints | ≥ 3 completed checkpoints (only for at-least-once / exactly-once) |
| C3_throughput | sink rate ≥ 0.95 × offered rate in W. Offered = acknowledged sends per second in W; sink = least-squares slope of the cumulative count of records the read_committed reader received per second in W (robust to the per-checkpoint bursts of a transactional sink) |
| C4_latency | p99 latency of the first visible copy of every record sent in W ≤ `spec.latency_slo_ms`; no visible record ⇒ false |
| C5_delivery | exactly-once: lost = 0 and duplicates = 0; at-least-once: lost = 0. lost = acknowledged sequence numbers never seen by the reader by the end of the drain; duplicates = sequence numbers seen more than once |

`runtime_pass` = all applicable criteria true (false if any is false, null if
one could not be judged). If no job can be built from the answer, or it never
runs, C1 is false and C2–C5 are false with the reason in `criteria_reasons`.
Also reported: `offered_eps_whole_load`, `sink_eps_count`,
`latency_ms_all_records` (including the failure episode), `duplicate_extra_copies`,
per-second offered/sink counts, the REST timeline and every event.

## What is reproduced and what is not

Reproduced: Kafka 4.1.2 KRaft with replication across three broker processes,
ISR semantics, transactions and acks=all; the answer's topic layout and topic
configs; Flink 1.20.5 slots, parallelism, checkpointing, state backends,
restart strategies, the Kafka connector's transactional sink and source offset
handling; TaskManager loss and replacement; CPU capacity per node (dedicated,
pinned cores); NUMA-local memory.

Not reproduced:
* **Network**: loopback TCP; no NIC, switch, packet loss or bandwidth limit.
* **Kernel isolation**: one kernel and page cache for all "nodes".
* **Per-node memory/disk limits**: no per-node cgroup; memory is bounded only by
  JVM/Flink settings. TaskManager memory above the budget is reported untestable.
* **Kubernetes**: no API server, Strimzi or Flink operator. KafkaTopic and
  FlinkDeployment are translated as described above (CRD schema validation
  beyond apiVersion/label is not reproduced); application mode is emulated by a
  session cluster with the operator's TaskManager count.
* **Object storage / HA services**: replaced by local files; HA disabled.
* **Independent failure domains**: a node failure is a SIGKILL of one
  TaskManager process; brokers and the JobManager are not failed.
* **Shared host**: other users' jobs share the RAID and memory bandwidth;
  `sysmon.jsonl` records host CPU, iowait and RAID utilisation.

## Smoke tests (2026-11-10, `runs/e7/smoke/`)

| run | answer | outcome |
|---|---|---|
| `a_gold_native_180s_S2_k2meas1` | GOLD_NATIVE, CASE-banking-native, final shape, 180 s | RUNNING 5 s after submit; 1 320 checkpoints; offered 50.0k, sink 50.2k ev/s; steady p50/p95/p99 502/1805/2447 ms (SLO 100 ms → C4 false); TM kill at 90 s: lost 0, duplicates 0 |
| `b_no_txn_timeout` | GOLD_NATIVE without `properties.transaction.timeout.ms` | never RUNNING (restart loop): `KafkaException: Unexpected error in InitProducerIdResponse; The transaction timeout is larger than the maximum value allowed by the broker (as configured by transaction.max.timeout.ms).` |
| `c_no_ckpt_interval` | GOLD_NATIVE without the checkpoint interval | RUNNING; 0 checkpoints; 0 of 2 999 268 records visible to read_committed; at the TM kill `JobException: Recovery is suppressed by NoRestartBackoffTimeStrategy` → FAILED |
| `d_zookeeper_v2` | GOLD_NATIVE with `--zookeeper localhost:2181` | both commands rc=1 `zookeeper is not a recognized option`; no topics; job restart loop `UnknownTopicOrPartitionException` (source topic missing) |
| `e_gold_k8s` | GOLD_K8S (translation path) | topics translated and created; sql-client: `IllegalStateException: Property group.id is required when using committed offset for offsets initializer` (the source table sets neither group.id nor scan.startup.mode; the connector default is group-offsets) |
| `e2_gold_k8s_with_group_id` | GOLD_K8S + source group.id/auto.offset.reset (harness-path check, not a gold answer) | 1 TaskManager (ceil(3/4)); RUNNING; 226 checkpoints; lost 0, duplicates 0 after killing the only TaskManager |
| `ID-001__qwen2.5-14b__s0` | real native answer (5k ev/s, ALO) | RUNNING; Flink keys nested under `config:` are ignored by Flink → no checkpointing; p99 10.6 ms; TM kill → FAILED (no restart strategy), lost 149 999 |
| `ID-063__qwen2.5-14b__s0` | real native answer | sql-client: `ValidationException: Unsupported options found for 'kafka'. Unsupported options: scan.subtasks` |
| `ID-042__qwen2.5-14b__s0` | real k8s answer | KafkaTopics carry no `strimzi.io/cluster` label → no topics; sql-client: `ValidationException: The rowtime attribute field 'ts' is not defined in the table schema` |

Runs b–e, ID-* used 60 s of load (kill at 30 s) and 1-core brokers; `a_gold_native*`
at other shapes are the broker-sizing comparison above.
