# Test Certificates

This directory contains self-signed TLS certificates for integration testing only.

**WARNING**: These certificates are for testing purposes only and should NEVER be used in production.

## Certificate Files

- `ca.crt` - Self-signed CA certificate
- `ca.key` - CA private key
- `server.crt` - Server certificate signed by the CA (for Mosquitto)
- `server.key` - Server private key
- `client.crt` - Client certificate signed by the CA (for mutual TLS)
- `client.key` - Client private key

## Regenerating Certificates

If you need to regenerate the certificates, you can use the following commands:

```bash
# Generate CA private key
openssl genrsa -out ca.key 2048

# Generate CA certificate
openssl req -new -x509 -days 3650 -key ca.key -out ca.crt -subj "/CN=Test CA/O=Test/C=US"

# Generate server private key
openssl genrsa -out server.key 2048

# Generate server CSR
openssl req -new -key server.key -out server.csr -subj "/CN=localhost/O=Test/C=US"

# Generate server certificate with SAN for localhost
cat > server.ext << EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
subjectAltName = @alt_names
[alt_names]
DNS.1 = localhost
DNS.2 = mosquitto-tls
IP.1 = 127.0.0.1
EOF
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt -days 3650 -extfile server.ext

# Generate client private key
openssl genrsa -out client.key 2048

# Generate client CSR
openssl req -new -key client.key -out client.csr -subj "/CN=mqtt-bridge-client/O=Test/C=US"

# Generate client certificate
cat > client.ext << EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
extendedKeyUsage = clientAuth
EOF
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out client.crt -days 3650 -extfile client.ext

# Clean up CSR and ext files
rm -f server.csr server.ext client.csr client.ext ca.srl
```

## Security Note

These certificates contain private keys that are committed to the repository for testing convenience.
They are generated specifically for localhost testing and have no value outside this test context.
Never use these certificates in any production or staging environment.
