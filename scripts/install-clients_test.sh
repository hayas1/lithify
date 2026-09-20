#!/bin/sh
set -eu

here=$(cd "$(dirname "$0")" && pwd)
workdir=$(mktemp -d)
trap 'rm -rf "${workdir}"' EXIT

mkdir -p "${workdir}/bin"
for utility in cat dirname dpkg grep mktemp sed; do
    utility_path=$(command -v "${utility}") || {
        echo "FAIL: ${utility} is required to run this test" >&2
        exit 1
    }
    ln -s "${utility_path}" "${workdir}/bin/${utility}"
done
for installer in apt-get curl sudo unzip; do
    cat > "${workdir}/bin/${installer}" <<STUB
#!/bin/sh
echo "${installer} \$*" >> "\${INSTALL_LOG}"
STUB
    chmod +x "${workdir}/bin/${installer}"
done

stub_installed() {
    for client in "$@"; do
        printf '#!/bin/sh\n' > "${workdir}/bin/${client}"
        chmod +x "${workdir}/bin/${client}"
    done
}

stub_missing() {
    for client in "$@"; do
        rm -f "${workdir}/bin/${client}"
    done
}

only_missing() {
    stub_installed mysql psql duckdb bq
    stub_missing "$@"
}

install_clients() {
    : > "${workdir}/log"
    PATH="${workdir}/bin" INSTALL_LOG="${workdir}/log" "${here}/install-clients.sh" "$@"
}

assert_nothing_installed() {
    what="$1"
    if [ -s "${workdir}/log" ]; then
        echo "FAIL: ${what} ran installers:" >&2
        cat "${workdir}/log" >&2
        exit 1
    fi
    echo "ok: ${what}"
}

assert_installed() {
    what="$1"
    pattern="$2"
    if ! grep -q "${pattern}" "${workdir}/log"; then
        echo "FAIL: ${what} did not run ${pattern}:" >&2
        cat "${workdir}/log" >&2
        exit 1
    fi
    echo "ok: ${what}"
}

assert_exit_code() {
    what="$1"
    expected="$2"
    actual="$3"
    if [ "${actual}" != "${expected}" ]; then
        echo "FAIL: ${what} exited ${actual}, want ${expected}" >&2
        exit 1
    fi
    echo "ok: ${what}"
}

only_missing
install_clients -y < /dev/null > /dev/null
assert_nothing_installed "installed clients are left alone"

only_missing duckdb
echo n | install_clients > /dev/null
assert_nothing_installed "a declined client is not installed"

only_missing duckdb
duckdb_version=$(grep '^FROM duckdb/duckdb:' "${here}/../external/duckdb/Dockerfile" | cut -d: -f2)
install_clients -y < /dev/null > /dev/null
assert_installed "duckdb is downloaded at the version the image pins" \
    "curl .*duckdb/releases/download/v${duckdb_version}/duckdb_cli-linux-"
assert_installed "duckdb is unpacked onto PATH" "sudo unzip"

only_missing mysql
install_clients -y < /dev/null > /dev/null
assert_installed "the MySQL client is installed with apt" "sudo apt-get install .*default-mysql-client"

only_missing

status=0
install_clients --nope > /dev/null 2>&1 || status=$?
assert_exit_code "an unknown flag is rejected" 2 "${status}"

status=0
install_clients --help > /dev/null || status=$?
assert_exit_code "--help succeeds" 0 "${status}"
