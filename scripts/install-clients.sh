#!/bin/sh
set -eu

root=$(cd "$(dirname "$0")/.." && pwd)
assume_yes=false

usage() {
    cat <<USAGE
Usage: install-clients.sh [-y] [-h]

Installs the command line clients for the stack: mysql, psql, duckdb and bq.
Clients that are already on PATH are left alone.

  -y, --yes   install without asking
  -h, --help  show this message
USAGE
}

for argument in "$@"; do
    case "${argument}" in
        -y | --yes) assume_yes=true ;;
        -h | --help)
            usage
            exit 0
            ;;
        *)
            usage >&2
            exit 2
            ;;
    esac
done

confirmed() {
    if ${assume_yes}; then
        return 0
    fi
    printf 'Install %s? [y/N] ' "$1"
    read -r answer || return 1
    case "${answer}" in
        y | Y | yes | Yes) return 0 ;;
        *) return 1 ;;
    esac
}

install_missing() {
    command_name="$1"
    installer="$2"
    if command -v "${command_name}" > /dev/null; then
        echo "${command_name}: already installed"
        return 0
    fi
    if ! confirmed "${command_name}"; then
        echo "${command_name}: skipped"
        return 0
    fi
    "${installer}"
    echo "${command_name}: installed"
}

apt_install() {
    sudo apt-get update -qq
    sudo apt-get install -y -qq "$@"
}

install_mysql() {
    apt_install default-mysql-client
}

install_psql() {
    apt_install postgresql-client
}

install_duckdb() {
    version=$(sed -n 's|^FROM duckdb/duckdb:||p' "${root}/compose/duckdb/Dockerfile")
    archive=$(mktemp -d)/duckdb.zip
    curl -fsSL -o "${archive}" \
        "https://github.com/duckdb/duckdb/releases/download/v${version}/duckdb_cli-linux-$(dpkg --print-architecture).zip"
    sudo unzip -o -q "${archive}" duckdb -d /usr/local/bin
    sudo chmod 755 /usr/local/bin/duckdb
}

install_bq() {
    curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg \
        | sudo gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" \
        | sudo tee /etc/apt/sources.list.d/google-cloud-sdk.list > /dev/null
    apt_install google-cloud-cli
}

install_missing mysql install_mysql
install_missing psql install_psql
install_missing duckdb install_duckdb
install_missing bq install_bq
