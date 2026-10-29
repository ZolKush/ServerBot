#!/bin/sh
set -eu
umask 077

if [ "$#" -eq 3 ] && [ "$1" = python ] && [ "$2" = -m ] && [ "$3" = app.launcher ]; then
    python -m app.persistence.bootstrap --data-dir /var/lib/maintbot
    python -m app.config_check
fi

exec "$@"
