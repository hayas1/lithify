#!/bin/bash
set -euo pipefail

if [[ -n "${TRUSTED_CA_CERT:-}" && -f "${TRUSTED_CA_CERT}" ]]; then
    truststore=/tmp/truststore.p12
    truststore_password=changeit
    cp /etc/pki/java/cacerts "${truststore}"
    chmod u+w "${truststore}"
    keytool -importcert -noprompt -alias trusted-ca -file "${TRUSTED_CA_CERT}" \
        -keystore "${truststore}" -storepass "${truststore_password}" >/dev/null
    export KAFKA_OPTS="${KAFKA_OPTS:-} -Djavax.net.ssl.trustStore=${truststore} -Djavax.net.ssl.trustStorePassword=${truststore_password}"
fi

exec /docker-entrypoint.sh "$@"
