#!/bin/sh
set -eu
cd "$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)"
if [ -e .env ]; then
  printf '%s\n' '.env already exists; preserved without replacing credentials.'
  exit 0
fi
command -v openssl >/dev/null 2>&1 || { printf '%s\n' 'openssl is required.' >&2; exit 1; }
umask 077
# noclobber prevents two invocations from replacing each other's credentials.
set -C
{
  printf 'HMDP_WEB_PORT=8080\n'
  printf 'HMDP_MYSQL_ROOT_PASSWORD=%s\n' "$(openssl rand -hex 24)"
  printf 'HMDP_MYSQL_PASSWORD=%s\n' "$(openssl rand -hex 24)"
  printf 'HMDP_REDIS_PASSWORD=%s\n' "$(openssl rand -hex 24)"
  printf 'HMDP_MONITOR_TOKEN=%s\n' "$(openssl rand -hex 24)"
  printf 'HMDP_ADMIN_IDS=\nHMDP_PAYMENT_MOCK_ENABLED=false\n'
} > .env
printf '%s\n' 'Created private .env. Keep it with the persistent database volumes.'
