#!/bin/bash
# Builds the versioned configuration catalogues used by the validator (L2/L3).
set -euo pipefail
OPT="${OPT:?Set OPT to the directory containing the JDK, Kafka and Flink installations}"
OUT="$(cd "$(dirname "$0")" && pwd)"
export JAVA_HOME=$OPT/jdk17 PATH=$OPT/jdk17/bin:$PATH
cd $OUT
# --- Kafka 4.1.2 -----------------------------------------------------------
rm -rf site-docs && tar -xzf $OPT/kafka/site-docs/kafka_2.13-4.1.2-site-docs.tgz site-docs/generated
python3 parse_kafka_docs.py site-docs/generated kafka-4.1.2-configs.json
for t in kafka-topics kafka-configs kafka-console-producer kafka-console-consumer; do
  $OPT/kafka/bin/$t.sh --help > cli-$t-4.1.2.txt 2>&1 || true
done
# --- Flink 1.20.5 ----------------------------------------------------------
CP=$(ls $OPT/flink/lib/*.jar $OPT/flink/opt/*.jar 2>/dev/null | tr '\n' ':')
javac -cp "$CP" -d . DumpFlinkOptions.java
java -cp ".:$CP" DumpFlinkOptions flink-1.20.5-options.jsonl $OPT/flink/lib/*.jar $(ls $OPT/flink/opt/*.jar 2>/dev/null | grep -E "rocksdb|s3|azure|gs|python" || true)
wc -l flink-1.20.5-options.jsonl
# --- Flink Kubernetes Operator 1.13.0 (v1.3): every ConfigOption by reflection, with deprecated and fallback keys --
mkdir -p operator-jars
for a in flink-kubernetes-operator flink-autoscaler flink-kubernetes-operator-api; do
  curl -sSfL -o operator-jars/$a-1.13.0.jar https://repo1.maven.org/maven2/org/apache/flink/$a/1.13.0/$a-1.13.0.jar
done
# runtime dependencies needed to initialise KubernetesOperatorConfigOptions (versions from the operator's parent POM)
curl -sSfL -o operator-jars/operator-framework-core-5.1.2.jar https://repo1.maven.org/maven2/io/javaoperatorsdk/operator-framework-core/5.1.2/operator-framework-core-5.1.2.jar
for a in kubernetes-client-api kubernetes-model-core kubernetes-model-common kubernetes-model-coordination; do
  curl -sSfL -o operator-jars/$a-7.3.1.jar https://repo1.maven.org/maven2/io/fabric8/$a/7.3.1/$a-7.3.1.jar
done
OCP="$CP$(ls operator-jars/*.jar | tr '\n' ':')"
java -cp ".:$OCP" DumpFlinkOptions flink-kubernetes-operator-1.13.0-options.jsonl operator-jars/flink-kubernetes-operator-1.13.0.jar \
  operator-jars/flink-autoscaler-1.13.0.jar operator-jars/flink-kubernetes-operator-api-1.13.0.jar
wc -l flink-kubernetes-operator-1.13.0-options.jsonl
# --- Kubernetes CRDs ------------------------------------------------------
mkdir -p crds
curl -sSfL -o crds/strimzi-crds-0.48.0.yaml https://github.com/strimzi/strimzi-kafka-operator/releases/download/0.48.0/strimzi-crds-0.48.0.yaml
for f in flinkdeployments.flink.apache.org-v1.yml flinksessionjobs.flink.apache.org-v1.yml; do
  curl -sSfL -o crds/$f https://raw.githubusercontent.com/apache/flink-kubernetes-operator/release-1.13/helm/flink-kubernetes-operator/crds/$f
done
ls -la crds
sha256sum kafka-4.1.2-configs.json flink-1.20.5-options.jsonl flink-kubernetes-operator-1.13.0-options.jsonl operator-jars/*.jar crds/* cli-*.txt > SHA256SUMS
