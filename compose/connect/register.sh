#!/bin/sh
set -eu

for spec in "$@"; do
    template="${spec%%:*}"
    source="${spec#"${template}"}"
    source="${source#:}"
    name="${template}${source:+-${source}}"
    source_upper=$(echo "${source}" | tr '[:lower:]' '[:upper:]')

    sed "s/@SOURCE@/${source_upper}/g" "${CONNECTORS_DIR}/${template}.json" \
        | curl -fsS -X PUT -H 'Content-Type: application/json' --data-binary @- \
            "${CONNECT_URL}/connectors/${name}/config"
    echo
done
