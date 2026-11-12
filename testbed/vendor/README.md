# Local testbed primitives

This directory contains the small runtime primitives required by the E7
executable-validation harness: topology allocation, Kafka and Flink process
wrappers, and process-tree monitoring. They are versioned with this repository
so the harness has no runtime dependency on another research project.

Set `TUNING_OPT` to a directory containing the Kafka, Flink, and JDK 17
installations. If it is not set, the harness looks for a repository-local
`testbed/runtime/` directory.
