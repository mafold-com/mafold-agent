#!/usr/bin/env bash
# Install + run mafold-agent (Claude Code <-> Mafold bridge).
#   bash <(curl -fsSL https://raw.githubusercontent.com/mafold-com/mafold-agent/main/install.sh) \
#     --token mb_xxxx --workdir ~/your-repo
set -euo pipefail
REPO="mafold-com/mafold-agent"
TOKEN=""; WORKDIR="."; BASE="https://api.mafold.com"
while [ $# -gt 0 ]; do
  case "$1" in
    --token)   TOKEN="${2:-}"; shift 2;;
    --workdir) WORKDIR="${2:-}"; shift 2;;
    --base)    BASE="${2:-}"; shift 2;;
    *) echo "unknown arg: $1" >&2; shift;;
  esac
done

os="$(uname -s)"; arch="$(uname -m)"
case "$os-$arch" in
  Darwin-arm64)   target="aarch64-apple-darwin";;
  Darwin-x86_64)  target="x86_64-apple-darwin";;
  Linux-x86_64)   target="x86_64-unknown-linux-gnu";;
  *) echo "unsupported platform: $os-$arch — build from source: https://github.com/$REPO" >&2; exit 1;;
esac

command -v claude >/dev/null 2>&1 || {
  echo "⚠️  Claude Code (claude) not found on PATH. Install it first: https://claude.com/claude-code" >&2
}

dir="${HOME}/.mafold"; mkdir -p "$dir"; bin="$dir/mafold-agent"
url="https://github.com/$REPO/releases/latest/download/mafold-agent-$target"
echo "↓ downloading mafold-agent ($target)…"
curl -fsSL "$url" -o "$bin"
chmod +x "$bin"
echo "✓ installed → $bin"

if [ -n "$TOKEN" ]; then
  echo "→ starting (workdir: $WORKDIR)…"
  exec env MAFOLD_BOT_TOKEN="$TOKEN" MAFOLD_WORKDIR="$WORKDIR" MAFOLD_BASE="$BASE" "$bin"
else
  echo "run it with:  MAFOLD_BOT_TOKEN=mb_xxx MAFOLD_WORKDIR=~/your-repo $bin"
fi
