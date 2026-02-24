# SASL Authentication and TLS Encryption

ItdaStream supports SASL/PLAIN authentication and SSL/TLS encryption, enabling SASL_PLAINTEXT and SASL_SSL security protocols for Kafka clients.

- **SASL/PLAIN**: Username = Access Key, Password = Secret Key (from IAM system); standard Kafka SASL handshake protocol
- **SSL/TLS**: Non-blocking TLS via Java SSLEngine, TLSv1.3 by default, JKS keystore/truststore support
- **TransportLayer abstraction**: PlaintextTransportLayer (direct I/O) and SslTransportLayer (full TLS) — transparent to upper layers
