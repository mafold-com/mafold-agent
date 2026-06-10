# mafold-cli

Mafold from your terminal — a small CLI client **and** a Claude Code agent daemon.

```
mafold-cli agent --token mb_xxxx --workdir ~/your-repo   # run Claude Code as your bot
mafold-cli --token mb_xxxx chats                          # list your conversations
mafold-cli --token mb_xxxx send @alice "hi there"         # send a message
```

Auth is a **bot token** (`mb_…`) — create a bot in the Mafold app — via
`--token` or `$MAFOLD_BOT_TOKEN`. Base URL defaults to `https://api.mafold.com`
(override with `--base` / `$MAFOLD_BASE`).

## Install

```sh
bash <(curl -fsSL https://raw.githubusercontent.com/mafold-com/mafold-cli/main/install.sh) \
  agent --token mb_xxxx --workdir ~/your-repo
```

The installer downloads the official release binary for your platform and runs
it. Or build from source: `cargo build --release`.

## `agent`

Drives the local **Claude Code** (`claude` must be installed + on PATH) in
`--workdir`: receives messages to your bot, runs Claude Code, and streams the
reply back. It always finalizes — a failure surfaces as a message, never a
stuck "typing…". The bot shows **online** only while the agent is running.

Everything runs on **your** machine, on **your** files in `--workdir`.

MIT licensed.
