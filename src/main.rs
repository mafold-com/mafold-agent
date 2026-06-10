//! mafold-agent — run Claude Code as YOUR Mafold bot, on YOUR machine.
//!
//! You create a bot in the Mafold app (get its `mb_…` token), then run this
//! with that token. Whenever someone messages your bot, it drives the local
//! `claude` (Claude Code) in your working directory and STREAMS the reply back.
//! Everything runs on your machine, on your files.
//!
//!   MAFOLD_BOT_TOKEN=mb_xxx MAFOLD_WORKDIR=~/code/myrepo mafold-agent

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, BufReader};

#[derive(Deserialize)]
struct Sender {
    username: String,
}
#[derive(Deserialize)]
struct IncomingMessage {
    conversation_id: String,
    sender: Sender,
    #[serde(default)]
    content: String,
}

struct Config {
    base: String,
    token: String,
    workdir: String,
    me: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let token = std::env::var("MAFOLD_BOT_TOKEN")
        .context("set MAFOLD_BOT_TOKEN to your bot's mb_ token (create a bot in the Mafold app)")?;
    let base = std::env::var("MAFOLD_BASE").unwrap_or_else(|_| "https://api.mafold.com".into());
    let workdir = std::env::var("MAFOLD_WORKDIR").unwrap_or_else(|_| ".".into());
    let http = reqwest::Client::new();

    let me: serde_json::Value = http
        .post(format!("{base}/api/getMe"))
        .bearer_auth(&token)
        .json(&serde_json::json!({}))
        .send()
        .await
        .context("getMe failed — check the token / MAFOLD_BASE")?
        .json()
        .await?;
    let my_username = me["result"]["username"].as_str().unwrap_or_default().to_string();
    anyhow::ensure!(!my_username.is_empty(), "could not resolve bot identity (bad token?)");
    println!("mafold-agent ✓ connected as @{my_username}  ·  workdir={workdir}");

    let cfg = Arc::new(Config {
        base: base.clone(),
        token: token.clone(),
        workdir,
        me: my_username.clone(),
    });

    let ws_base = base.replacen("https://", "wss://", 1).replacen("http://", "ws://", 1);
    let ws_url = format!("{ws_base}/api/ws?token={token}");
    let (ws, _) = tokio_tungstenite::connect_async(&ws_url)
        .await
        .context("WebSocket connect failed")?;
    let (mut write, mut read) = ws.split();
    println!("listening for messages to @{my_username} …");

    // Keepalive: ping every 25s so the server's heartbeat keeps the bot marked
    // online (it's idle otherwise — we only receive).
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(25));
        loop {
            tick.tick().await;
            if write
                .send(tokio_tungstenite::tungstenite::Message::Ping(Vec::new().into()))
                .await
                .is_err()
            {
                break;
            }
        }
    });

    while let Some(frame) = read.next().await {
        let frame = match frame {
            Ok(f) => f,
            Err(e) => {
                eprintln!("ws error: {e}");
                break;
            }
        };
        let text = match frame.into_text() {
            Ok(t) => t,
            Err(_) => continue,
        };
        let env: serde_json::Value = match serde_json::from_str(&text) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if env.get("method").and_then(|m| m.as_str()) != Some("events.messageNew") {
            continue;
        }
        let m: IncomingMessage = match serde_json::from_value(env["params"].clone()) {
            Ok(m) => m,
            Err(_) => continue,
        };
        if m.sender.username.eq_ignore_ascii_case(&cfg.me) || m.content.trim().is_empty() {
            continue;
        }

        println!("← @{}: {}", m.sender.username, m.content);
        let cfg = cfg.clone();
        let http = http.clone();
        // Handle concurrently so the socket keeps reading. (Single shared
        // workdir — fine for one user; per-conversation isolation is later.)
        tokio::spawn(async move {
            if let Err(e) = handle(&http, &cfg, &m.conversation_id, &m.content).await {
                eprintln!("handle error: {e}");
            }
        });
    }
    println!("disconnected.");
    Ok(())
}

/// Open a draft, stream Claude Code's reply into it, finalize.
async fn handle(http: &reqwest::Client, cfg: &Config, chat_id: &str, prompt: &str) -> Result<()> {
    // 1) open a streaming draft message from the bot (this flips "typing" on).
    let draft: serde_json::Value = http
        .post(format!("{}/api/botCreateDraft", cfg.base))
        .bearer_auth(&cfg.token)
        .json(&serde_json::json!({ "chat_id": chat_id }))
        .send()
        .await?
        .json()
        .await?;
    let msg_id = draft["result"]["id"].as_str().context("no draft id")?.to_string();

    // 2) run claude. On ANY failure, surface it in the chat — we must NEVER
    //    leave the draft unfinalized, or the conversation hangs on "typing…".
    match stream_claude(http, cfg, prompt, &msg_id).await {
        Ok(true) => {}
        Ok(false) => flush(http, cfg, &msg_id, "_(the agent produced no output)_").await,
        Err(e) => {
            eprintln!("claude failed: {e}");
            flush(http, cfg, &msg_id, &format!("⚠️ Agent error: {e}")).await;
        }
    }

    // 3) ALWAYS finalize.
    let _ = http
        .post(format!("{}/api/botFinalize", cfg.base))
        .bearer_auth(&cfg.token)
        .json(&serde_json::json!({ "message_id": msg_id }))
        .send()
        .await;
    println!("→ finalized reply for chat {chat_id}");
    Ok(())
}

/// Drive claude in stream-json, flushing text deltas into the draft. Returns
/// whether any text was produced. Errors bubble up so `handle` can finalize.
async fn stream_claude(http: &reqwest::Client, cfg: &Config, prompt: &str, msg_id: &str) -> Result<bool> {
    if !std::path::Path::new(&cfg.workdir).is_dir() {
        anyhow::bail!("working directory does not exist: {} — check --workdir", cfg.workdir);
    }
    let mut child = tokio::process::Command::new("claude")
        .arg("-p")
        .arg(prompt)
        .arg("--output-format")
        .arg("stream-json")
        .arg("--verbose")
        .arg("--include-partial-messages")
        .arg("--dangerously-skip-permissions") // autonomous; P2 adds a policy
        .current_dir(&cfg.workdir)
        // Avoid the "already inside Claude Code" nested-session guard, and use
        // the user's subscription auth rather than an inherited API key.
        .env_remove("CLAUDECODE")
        .env_remove("ANTHROPIC_API_KEY")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("couldn't run `claude` in {} — is Claude Code installed and on PATH?", cfg.workdir))?;

    let stdout = child.stdout.take().context("no stdout")?;
    let mut lines = BufReader::new(stdout).lines();
    let mut buf = String::new();
    let mut produced = false;
    let mut last_flush = Instant::now();

    while let Some(line) = lines.next_line().await? {
        let line = line.trim();
        if line.is_empty() { continue; }
        let v: serde_json::Value = match serde_json::from_str(line) { Ok(v) => v, Err(_) => continue };
        if v["type"] == "stream_event"
            && v["event"]["type"] == "content_block_delta"
            && v["event"]["delta"]["type"] == "text_delta"
        {
            if let Some(t) = v["event"]["delta"]["text"].as_str() {
                buf.push_str(t);
                produced = true;
                if buf.len() >= 120 || last_flush.elapsed() >= Duration::from_millis(120) {
                    flush(http, cfg, msg_id, &buf).await;
                    buf.clear();
                    last_flush = Instant::now();
                }
            }
        }
        if v["type"] == "result" { break; }
    }
    if !buf.is_empty() { flush(http, cfg, msg_id, &buf).await; }

    // If claude exited non-zero, surface its stderr (e.g. auth/rate errors).
    let status = child.wait().await?;
    if !status.success() {
        let mut err = String::new();
        if let Some(mut se) = child.stderr.take() {
            use tokio::io::AsyncReadExt;
            let _ = se.read_to_string(&mut err).await;
        }
        let err = err.trim();
        anyhow::bail!("claude exited unsuccessfully{}", if err.is_empty() { String::new() } else { format!(": {err}") });
    }
    Ok(produced)
}

async fn flush(http: &reqwest::Client, cfg: &Config, msg_id: &str, delta: &str) {
    let _ = http
        .post(format!("{}/api/botAppendDelta", cfg.base))
        .bearer_auth(&cfg.token)
        .json(&serde_json::json!({ "message_id": msg_id, "delta": delta }))
        .send()
        .await;
}
