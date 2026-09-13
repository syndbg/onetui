#!/bin/sh
set -eu
umask 077
fixture_krb=/tmp/onetui-kafka-tls/krb5
mkdir -p "$fixture_krb"
export KRB5_CONFIG="$fixture_krb/krb5.conf"
export KRB5_KDC_PROFILE="$fixture_krb/kdc.conf"
cat > "$KRB5_CONFIG" <<'EOF'
[libdefaults]
 default_realm = ONETUI.TEST
 dns_lookup_kdc = false
 dns_lookup_realm = false
 rdns = false
 dns_canonicalize_hostname = false
[realms]
 ONETUI.TEST = {
  kdc = 127.0.0.1:8888
 }
EOF
cat > "$KRB5_KDC_PROFILE" <<EOF
[kdcdefaults]
 kdc_ports = 8888
 kdc_tcp_ports = 8888
[realms]
 ONETUI.TEST = {
  database_name = $fixture_krb/principal
  key_stash_file = $fixture_krb/stash
  max_life = 1h
  max_renewable_life = 2h
 }
EOF
if [ ! -f "$fixture_krb/principal" ]; then
    kdb5_util create -s -P fixture-kdc-only
    for principal in kafka/localhost fixture-reader fixture-denied; do
        kadmin.local -q "addprinc -randkey $principal@ONETUI.TEST"
        keytab=$(printf '%s' "$principal" | tr / -)
        kadmin.local -q "ktadd -k $fixture_krb/$keytab.keytab $principal@ONETUI.TEST"
    done
fi
krb5kdc -n &
kdc_pid=$!
attempt=0
until kinit -k -t "$fixture_krb/fixture-reader.keytab" \
    -c "FILE:$fixture_krb/reader.ccache" -l 1h fixture-reader@ONETUI.TEST; do
    attempt=$((attempt + 1))
    kill -0 "$kdc_pid" || exit 1
    [ "$attempt" -lt 20 ] || exit 1
    sleep 0.1
done
kinit -k -t "$fixture_krb/fixture-denied.keytab" \
    -c "FILE:$fixture_krb/denied.ccache" -l 1h fixture-denied@ONETUI.TEST
kinit -k -t "$fixture_krb/fixture-reader.keytab" \
    -c "FILE:$fixture_krb/expired.ccache" -l 1s fixture-reader@ONETUI.TEST
