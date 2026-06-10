#!/usr/bin/env bash
# Install + run mafold-cli (Mafold terminal client + Claude Code agent).
#   bash <(curl -fsSL https://raw.githubusercontent.com/mafold-com/mafold-cli/main/install.sh) \
#     agent --token mb_xxxx --workdir ~/your-repo
# Any args after the script are forwarded to mafold-cli.
set -euo pipefail
REPO="mafold-com/mafold-cli"

os="$(uname -s)"; arch="$(uname -m)"
case "$os-$arch" in
  Darwin-arm64)   target="aarch64-apple-darwin";;
  Darwin-x86_64)  target="x86_64-apple-darwin";;
  Linux-x86_64)   target="x86_64-unknown-linux-gnu";;
  *) echo "unsupported platform: $os-$arch — build from source: https://github.com/$REPO" >&2; exit 1;;
esac

command -v claude >/dev/null 2>&1 || \
  echo "⚠️  Claude Code (claude) not found on PATH — needed for 'agent'. https://claude.com/claude-code" >&2

dir="${HOME}/.mafold"; mkdir -p "$dir"; bin="$dir/mafold-cli"
url="https://github.com/$REPO/releases/latest/download/mafold-cli-$target"
echo "↓ downloading mafold-cli ($target)…"
curl -fsSL "$url" -o "$bin"
chmod +x "$bin"
echo "✓ installed → $bin"

if [ "$#" -gt 0 ]; then
  exec "$bin" "$@"
else
  echo "run:  $bin agent --token mb_xxx --workdir ~/your-repo"
fi
