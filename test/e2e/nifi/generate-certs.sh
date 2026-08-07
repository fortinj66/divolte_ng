#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
OUT_DIR="$ROOT_DIR/test/e2e/tmp/nifi-certs"

mkdir -p "$OUT_DIR"
rm -f "$OUT_DIR"/*

openssl genrsa -out "$OUT_DIR/ca.key.pem" 4096
openssl req -x509 -new -nodes -key "$OUT_DIR/ca.key.pem" -sha256 -days 3650 \
  -subj "/C=US/ST=State/L=City/O=Divolte E2E/CN=Divolte E2E Root CA" \
  -out "$OUT_DIR/ca.cert.pem"

openssl genrsa -out "$OUT_DIR/nifi.key.pem" 2048
openssl req -new -key "$OUT_DIR/nifi.key.pem" \
  -subj "/C=US/ST=State/L=City/O=Divolte E2E/CN=localhost" \
  -out "$OUT_DIR/nifi.csr.pem"
cat > "$OUT_DIR/nifi.ext" <<'EXT'
subjectAltName = DNS:localhost,DNS:nifi,IP:127.0.0.1
extendedKeyUsage = serverAuth
EXT
openssl x509 -req -in "$OUT_DIR/nifi.csr.pem" -CA "$OUT_DIR/ca.cert.pem" -CAkey "$OUT_DIR/ca.key.pem" \
  -CAcreateserial -out "$OUT_DIR/nifi.cert.pem" -days 825 -sha256 -extfile "$OUT_DIR/nifi.ext"

openssl pkcs12 -export -out "$OUT_DIR/nifi.keystore.p12" \
  -inkey "$OUT_DIR/nifi.key.pem" -in "$OUT_DIR/nifi.cert.pem" -certfile "$OUT_DIR/ca.cert.pem" \
  -passout pass:changeit -name nifi

if command -v keytool >/dev/null 2>&1; then
  keytool -importcert -noprompt -alias divolte-e2e-ca \
    -file "$OUT_DIR/ca.cert.pem" -keystore "$OUT_DIR/nifi.truststore.p12" \
    -storetype PKCS12 -storepass changeit
else
  podman run --rm --user 0 --entrypoint keytool -v "$OUT_DIR:/certs:Z" docker.io/apache/nifi:1.19.1 \
    -importcert -noprompt -alias divolte-e2e-ca \
    -file /certs/ca.cert.pem -keystore /certs/nifi.truststore.p12 \
    -storetype PKCS12 -storepass changeit
fi

openssl genrsa -out "$OUT_DIR/client.key.pem" 2048
openssl req -new -key "$OUT_DIR/client.key.pem" \
  -subj "/C=US/ST=State/L=City/O=Divolte E2E/CN=divolte-e2e-client" \
  -out "$OUT_DIR/client.csr.pem"
cat > "$OUT_DIR/client.ext" <<'EXT'
extendedKeyUsage = clientAuth
EXT
openssl x509 -req -in "$OUT_DIR/client.csr.pem" -CA "$OUT_DIR/ca.cert.pem" -CAkey "$OUT_DIR/ca.key.pem" \
  -CAcreateserial -out "$OUT_DIR/client.cert.pem" -days 825 -sha256 -extfile "$OUT_DIR/client.ext"

chmod 600 "$OUT_DIR"/*.key.pem
chmod 644 "$OUT_DIR"/*.p12
echo "Generated NiFi e2e certs in $OUT_DIR"
