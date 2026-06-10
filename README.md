# mafold-agent

Run **Claude Code** as your **Mafold** bot — on **your own machine, on your own files**.

You create a bot in the Mafold app, run this with its token, and then chatting
with your bot from Mafold drives Claude Code locally and sends the reply back.
Everything (files, commands, tools) runs on your machine.

```
You (Mafold app)  ──►  Mafold  ──WS (your mb_ token)──►  mafold-agent  ──►  claude (Claude Code) + your repo
```

## Setup

1. **Install [Claude Code](https://claude.com/claude-code)** and sign in (`claude` must be on your PATH).
2. **Create a bot** in the Mafold app and copy its token (`mb_…`).
3. **Run it:**

```sh
cargo build --release      # or download a release binary
MAFOLD_BOT_TOKEN=mb_xxxxxxxx \
MAFOLD_WORKDIR=~/code/my-repo \
./target/release/mafold-agent
```

Now message your bot from Mafold — it runs Claude Code in `MAFOLD_WORKDIR` and replies.

### Config (env)

| var | default | meaning |
|---|---|---|
| `MAFOLD_BOT_TOKEN` | — (required) | your bot's `mb_` token |
| `MAFOLD_WORKDIR` | `.` | directory Claude Code works in |
| `MAFOLD_BASE` | `https://api.mafold.com` | Mafold API base |

## Status

**P0** — headless one-shot (`claude -p`) + a single reply. Roadmap: streaming
token output, per-conversation sessions (resume), tool-use visualization
(file diffs / command output in chat), permission policy, multi-conversation
concurrency.

## Security

It runs on **your** machine as **your** bot, operating on **your** files in
`MAFOLD_WORKDIR`. Claude Code's tools (edits, shell) execute for real there —
point `MAFOLD_WORKDIR` at a repo you're comfortable letting it work in.

MIT licensed.
