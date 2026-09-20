#!/bin/sh
set -eu

datasets_url="${BIGQUERY_EMULATOR_URL}/bigquery/v2/projects/${BIGQUERY_PROJECT}/datasets"

attempts=0
until curl -fs -o /dev/null "${datasets_url}"; do
    attempts=$((attempts + 1))
    if [ "${attempts}" -ge 60 ]; then
        echo "bigquery-emulator is not reachable at ${BIGQUERY_EMULATOR_URL}" >&2
        exit 1
    fi
    sleep 1
done

for dataset in $(echo "${BIGQUERY_DATASETS}" | tr ',' ' '); do
    status=$(curl -s -o /dev/null -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
        -d "{\"datasetReference\": {\"projectId\": \"${BIGQUERY_PROJECT}\", \"datasetId\": \"${dataset}\"}}" \
        "${datasets_url}")
    case "${status}" in
        200 | 409) echo "${dataset}: ${status}" ;;
        *) echo "${dataset}: ${status}" >&2; exit 1 ;;
    esac
done
