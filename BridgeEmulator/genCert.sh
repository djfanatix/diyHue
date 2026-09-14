#!/bin/bash
set -e

MAC=$1
CONFIG="${2:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/config}"
OPENSSL_CONF="${3:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/openssl.conf}"

mkdir -p "$CONFIG"

dec_serial=$(python3 -c "print(int(\"$MAC\".strip('\\u200e'), 16))")

if command -v faketime >/dev/null 2>&1; then
	faketime '2017-01-01 00:00:00' openssl req -new -days 7670 -config "$OPENSSL_CONF" -nodes -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -pkeyopt ec_param_enc:named_curve -subj "/C=NL/O=Philips Hue/CN=$MAC" -keyout private.key -out public.crt -set_serial "$dec_serial"
else
	openssl req -new -days 7670 -config "$OPENSSL_CONF" -nodes -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -pkeyopt ec_param_enc:named_curve -subj "/C=NL/O=Philips Hue/CN=$MAC" -keyout private.key -out public.crt -set_serial "$dec_serial"
fi

touch "$CONFIG/cert.pem"
cat private.key > "$CONFIG/cert.pem"
cat public.crt >> "$CONFIG/cert.pem"

rm -f private.key public.crt
