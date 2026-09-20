#!/bin/sh
set -eu

here=$(cd "$(dirname "$0")" && pwd)
workdir=$(mktemp -d)
trap 'rm -rf "${workdir}"' EXIT

mkdir -p "${workdir}/bin" "${workdir}/connectors"
cat > "${workdir}/bin/curl" <<'STUB'
#!/bin/sh
for arg in "$@"; do last="${arg}"; done
echo "${last}" >> "${CURL_LOG}"
cat >> "${CURL_LOG}"
STUB
chmod +x "${workdir}/bin/curl"

printf '{"topics.regex": "${env:CDC_@SOURCE@_TOPICS_REGEX}"}\n' > "${workdir}/connectors/sink-example.json"
printf '{"topic.prefix": "mysql"}\n' > "${workdir}/connectors/source-mysql.json"

assert_registered() {
    expected="$1"
    shift
    : > "${workdir}/curl.log"
    PATH="${workdir}/bin:${PATH}" CURL_LOG="${workdir}/curl.log" CONNECT_URL=http://connect:8083 \
        CONNECTORS_DIR="${workdir}/connectors" "${here}/register.sh" "$@" < /dev/null > /dev/null
    if [ "$(cat "${workdir}/curl.log")" != "${expected}" ]; then
        echo "FAIL: register.sh $*" >&2
        diff "${workdir}/curl.log" - <<DIFF >&2 || true
${expected}
DIFF
        exit 1
    fi
    echo "ok: register.sh $*"
}

assert_registered 'http://connect:8083/connectors/source-mysql/config
{"topic.prefix": "mysql"}' source-mysql

assert_registered 'http://connect:8083/connectors/sink-example-postgres/config
{"topics.regex": "${env:CDC_POSTGRES_TOPICS_REGEX}"}' sink-example:postgres

assert_registered 'http://connect:8083/connectors/sink-example-mysql/config
{"topics.regex": "${env:CDC_MYSQL_TOPICS_REGEX}"}
http://connect:8083/connectors/sink-example-postgres/config
{"topics.regex": "${env:CDC_POSTGRES_TOPICS_REGEX}"}' sink-example:mysql sink-example:postgres
