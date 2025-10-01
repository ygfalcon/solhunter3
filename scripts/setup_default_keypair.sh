#!/usr/bin/env bash
set -e

MNEMONIC="${MNEMONIC:-}"
PASSPHRASE="${PASSPHRASE:-}"

GENERATED=false
if [ -z "$MNEMONIC" ]; then
  MNEMONIC=$(solhunter-wallet new | tail -n 1 | tr -d '\r\n')
  GENERATED=true
fi

solhunter-wallet derive default "$MNEMONIC" --passphrase "$PASSPHRASE"
solhunter-wallet select default

if [ "$GENERATED" = true ]; then
  echo "$MNEMONIC"
fi
