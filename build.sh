#!/usr/bin/env bash

CMD="$1"
shift

set -euo pipefail
IFS=$'\n\t'


case $CMD in
 lint )
   go vet ./...
   ;;
 * )
   echo "Undefined command $CMD try (lint, build)"
esac