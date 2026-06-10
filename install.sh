#!/usr/bin/env bash
# Install + run mafold (Mafold terminal client + Claude Code agent).
#   bash <(curl -fsSL https://raw.githubusercontent.com/mafold-com/mafold-cli/main/install.sh) \
#     agent --detach --token mb_xxxx --workdir ~/your-repo
# Any args after the script are forwarded to mafold.
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

dir="${HOME}/.mafold"; mkdir -p "$dir"; bin="$dir/mafold"
url="https://github.com/$REPO/releases/latest/download/mafold-$target"
echo "↓ downloading mafold ($target)…"
curl -fsSL "$url" -o "$bin"
chmod +x "$bin"

# Best-effort: put `mafold` on PATH so you can run it directly later.
linked=""
for d in "/usr/local/bin" "${HOME}/.local/bin"; do
  if [ -d "$d" ] && [ -w "$d" ]; then ln -sf "$bin" "$d/mafold" && linked="$d/mafold" && break; fi
done
if [ -z "$linked" ]; then
  mkdir -p "${HOME}/.local/bin" && ln -sf "$bin" "${HOME}/.local/bin/mafold" && linked="${HOME}/.local/bin/mafold" || true
fi
echo "✓ installed → $bin${linked:+  (linked: $linked)}"

if [ "$#" -gt 0 ]; then
  exec "$bin" "$@"
else
  echo "run:  mafold agent --detach --token mb_xxx --workdir ~/your-repo"
fi
