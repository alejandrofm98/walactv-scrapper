#!/bin/sh
set -eu

prefix="${SCRAPPER_CONTAINER_PREFIX:-walactv}"
sed "s/__SCRAPPER_PREFIX__/${prefix}/g" /ofelia.config.ini.template > /ofelia.config.ini
exec ofelia daemon --config /ofelia.config.ini
