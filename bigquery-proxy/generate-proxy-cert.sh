#!/bin/sh
set -eu

if [ -f /certs/proxy.crt ] && [ -f /certs/proxy.key ]; then
    exit 0
fi

openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
    -subj "/CN=bigquery-proxy" \
    -addext "subjectAltName=DNS:bigquery.googleapis.com,DNS:oauth2.googleapis.com" \
    -keyout /certs/proxy.key -out /certs/proxy.crt
chmod 644 /certs/proxy.crt
