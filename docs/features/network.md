# High-Performance NIO Network Layer

ItdaStream implements a custom multi-reactor NIO network layer inspired by Apache Kafka's network architecture, optimized for high-throughput, low-latency message processing.

## Multi-reactor pattern

- Acceptor thread: Single ServerSocketChannel with non-blocking accept, round-robin distributes connections to Processors
- Processor threads (default 3, configurable): Each has an independent NIO Selector; handles all I/O for assigned connections
- Handler threads (default 8, configurable): Execute request business logic from a shared ArrayBlockingQueue (capacity 500)

## Response ordering guarantee

- TreeMap buffers responses by sequence number
- Only sends responses matching nextSendSequence — ensures strict FIFO ordering despite concurrent handler threads
- Atomic sequence increment after each send

## Connection management

- TCP_NODELAY and KEEP_ALIVE enabled on all sockets
- Per-connection state in KafkaChannel: current receive, current send, transport layer
- Configurable max connections and backpressure via request queue