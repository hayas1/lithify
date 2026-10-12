#!/bin/sh
set -eu

root=$(cd "$(dirname "$0")/.." && pwd)
assume_yes=false
clients="mysql psql duckdb bq libduckdb"
named=""

usage() {
    cat <<USAGE
Usage: install-clients.sh [-y] [-h] [client...]

Installs the command line clients for the stack: mysql, psql, duckdb and bq,
and libduckdb, which the Rust workspace links against. Name some of them to
install those alone.
Clients that are already on PATH and libraries the linker already finds are
left alone.

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
        -*)
            usage >&2
            exit 2
            ;;
        *)
            case " ${clients} " in
                *" ${argument} "*) named="${named} ${argument}" ;;
                *)
                    usage >&2
                    exit 2
                    ;;
            esac
            ;;
    esac
done

requested() {
    case " ${named:-${clients}} " in
        *" $1 "*) return 0 ;;
        *) return 1 ;;
    esac
}

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

install_missing_command() {
    if command -v "$1" > /dev/null; then
        echo "$1: already installed"
        return 0
    fi
    install_confirmed "$1" "$2"
}

install_missing_library() {
    if ldconfig -p | grep -q "$1\.so"; then
        echo "$1: already installed"
        return 0
    fi
    install_confirmed "$1" "$2"
}

install_confirmed() {
    if ! confirmed "$1"; then
        echo "$1: skipped"
        return 0
    fi
    "$2"
    echo "$1: installed"
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

duckdb_version() {
    sed -n 's|^FROM duckdb/duckdb:||p' "${root}/compose/duckdb/Dockerfile"
}

duckdb_release() {
    echo "https://github.com/duckdb/duckdb/releases/download/v$(duckdb_version)/$1-linux-$(dpkg --print-architecture).zip"
}

install_duckdb() {
    unpacked=$(mktemp -d)
    curl -fsSL -o "${unpacked}/duckdb.zip" "$(duckdb_release duckdb_cli)"
    unzip -o -q "${unpacked}/duckdb.zip" duckdb -d "${unpacked}"
    sudo install -m 755 "${unpacked}/duckdb" /usr/local/bin
}

install_libduckdb() {
    unpacked=$(mktemp -d)
    curl -fsSL -o "${unpacked}/libduckdb.zip" "$(duckdb_release libduckdb)"
    unzip -o -q "${unpacked}/libduckdb.zip" -d "${unpacked}"
    sudo install -m 644 "${unpacked}/libduckdb.so" /usr/local/lib
    sudo install -m 644 "${unpacked}/duckdb.h" "${unpacked}/duckdb.hpp" /usr/local/include
    duckdb_pkg_config > "${unpacked}/duckdb.pc"
    sudo install -D -m 644 "${unpacked}/duckdb.pc" /usr/local/lib/pkgconfig/duckdb.pc
    sudo ldconfig
}

duckdb_pkg_config() {
    cat <<PKG_CONFIG
prefix=/usr/local
libdir=\${prefix}/lib
includedir=\${prefix}/include

Name: duckdb
Description: DuckDB, the library the Rust workspace links against
Version: $(duckdb_version)
Libs: -L\${libdir} -lduckdb
Cflags: -I\${includedir}
PKG_CONFIG
}

install_bq() {
    curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg \
        | sudo gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" \
        | sudo tee /etc/apt/sources.list.d/google-cloud-sdk.list > /dev/null
    apt_install google-cloud-cli
}

if requested mysql; then install_missing_command mysql install_mysql; fi
if requested psql; then install_missing_command psql install_psql; fi
if requested duckdb; then install_missing_command duckdb install_duckdb; fi
if requested bq; then install_missing_command bq install_bq; fi
if requested libduckdb; then install_missing_library libduckdb install_libduckdb; fi
