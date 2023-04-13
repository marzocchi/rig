#!/usr/bin/env bash

set -euo pipefail

if [[ $# -lt 3 ]]; then
  echo usage: "$(basename "$0")" NAMESPACE POD_NAME_PATTERN CMD \[ARGS...\] >&2
  exit 1
fi

namespace=$1
shift
pattern=$1
shift

kubectl get pods -n "$namespace" | grep -v NAME | grep "$pattern" | while read -r pod rest; do
  echo "$pod" kubectl exec -i -n "$namespace" "$pod" -- $@
done