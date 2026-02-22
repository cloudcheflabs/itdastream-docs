# SASL Authentication and TLS Encryption

ItdaStream supports both SASL/PLAIN authentication and SSL/TLS encryption, enabling SASL_PLAINTEXT and SASL_SSL security protocols for Kafka clients.

## SASL/PLAIN Authentication

- Username = Access Key, Password = Secret Key (from IAM system)
- Standard Kafka SASL handshake protocol (SaslHandshake + SaslAuthenticate)
- Credential validation against RocksDB-backed AuthManager with expiration checking
- User identity propagated through KafkaChannel for per-request authorization

## SSL/TLS Encryption (SASL_SSL)

- Non-blocking TLS implementation using Java SSLEngine with TransportLayer abstraction
- TLSv1.3 by default (configurable)
- JKS keystore/truststore support
- SSLEngine handshake state machine: NEED_WRAP → NEED_UNWRAP → NEED_TASK → FINISHED
- Separate encrypted network buffers and decrypted application buffers
- Graceful close_notify handling

## TransportLayer abstraction (Apache Kafka pattern)

- PlaintextTransportLayer: Direct socket I/O (no encryption overhead)
- SslTransportLayer: Full TLS encryption via SSLEngine
- Transparent to upper layers — KafkaChannel, NetworkReceive, NetworkSend work identically with both