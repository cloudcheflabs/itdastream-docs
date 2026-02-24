# High-Performance NIO Network Layer

ItdaStream implements a custom multi-reactor NIO network layer inspired by Apache Kafka's architecture, optimized for high-throughput, low-latency message processing.

- **Multi-reactor pattern**: Acceptor (connection distribution) → Processor threads (NIO I/O) → Handler threads (business logic)
- **Response ordering**: TreeMap-based sequence buffering ensures strict FIFO ordering despite concurrent handlers
- **Connection management**: TCP_NODELAY, KEEP_ALIVE, configurable max connections and backpressure via request queue
