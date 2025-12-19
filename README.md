# Crystal ⚙️

**Crystal** is a modular stream processing framework written fully in Go (Golang). It currently supports Apache Kafka as both a data source (reader) and sink (writer). 

# Overview
Crystal aims to provide an efficient foundation for stream processing workloads with a focus on:
- ⚙️ Modular design to support pluggable components
- 🖥️ Http integration for metric scrapping
- 📥 Kafka integration for stream input and output
- 🖥️ Currently non-distributed and single-node scale

# Features
- 📦 Kafka package for consume and producing operations
- ♻️ Is getting Designed for stream aggregation and join in an optimized manner
- 🧩 Modular architecture facilitating extensibility (e.g., add other sources/sinks)
- 🛠️ Lightweight core for efficient stream processing pipelines

# Limitations
- 🚧 No distributed processing support (single-node only)
- 📈 Does not yet scale horizontally
- 🔎 Focus on pipeline correctness and performance before scaling features
