//! mafold-agent — run Claude Code as YOUR Mafold bot, on YOUR machine.
//!
//! You create a bot in the Mafold app (get its `mb_…` token), then run this
//! with that token. It connects to Mafold, and whenever someone messages your
//! bot it drives the local `claude` (Claude Code) in your working directory and
//! sends the reply back. Everything runs on your machine, on your files.
//!
//!   MAFOLD_BOT_TOKEN=mb_xxx MAFOLD_WORKDIR=~/code/myrepo mafold-agent
//!
//! P0: headless one-shot (`claude -p`) + a single reply. Streaming + tool
//! events + per-conversation sessions come next (see memory: claude-code-daemon).

use anyhow::{Context, Result};
use futures_util::StreamExt;
use serde::Deserialize;
use std::process::Command;

#[derive(Deserialize)]
struct Sender { username: String }

#[derive(Deserialize)]
struct IncomingMessage {
    conversation_id: String,
    sender: Sender,
    #[serde(default)]
    content: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let token = std::env::var("MAFOLD_BOT_TOKEN")
        .context("set MAFOLD_BOT_TOKEN to your bot's mb_ token (create a bot in the Mafold app)")?;
    let base = std::env::var("MAFOLD_BASE").unwrap_or_else(|_| "https://api.mafold.com".into());
    let workdir = std::env::var("MAFOLD_WORKDIR").unwrap_or_else(|_| ".".into());
    let http = reqwest::Client::new();

    // Resolve our own identity so we don't reply to our own messages.
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

    let ws_base = base.replacen("https://", "wss://", 1).replacen("http://", "ws://", 1);
    let ws_url = format!("{ws_base}/api/ws?token={token}");
    let (mut ws, _) = tokio_tungstenite::connect_async(&ws_url)
        .await
        .context("WebSocket connect failed")?;
    println!("listening for messages to @{my_username} …");

    while let Some(frame) = ws.next().await {
        let frame = match frame { Ok(f) => f, Err(e) => { eprintln!("ws error: {e}"); break; } };
        let text = match frame.into_text() { Ok(t) => t, Err(_) => continue };
        let env: serde_json::Value = match serde_json::from_str(&text) { Ok(v) => v, Err(_) => continue };
        if env.get("method").and_then(|m| m.as_str()) != Some("events.messageNew") { continue; }
        let m: IncomingMessage = match serde_json::from_value(env["params"].clone()) { Ok(m) => m, Err(_) => continue };
        if m.sender.username.eq_ignore_ascii_case(&my_username) || m.content.trim().is_empty() { continue; }

        println!("← @{}: {}", m.sender.username, m.content);
        let reply = run_claude(&m.content, &workdir).unwrap_or_else(|e| format!("⚠️ {e}"));
        if let Err(e) = http
            .post(format!("{base}/api/sendMessage"))
            .bearer_auth(&token)
            .json(&serde_json::json!({ "chat_id": m.conversation_id, "text": reply }))
            .send()
            .await
        {
            eprintln!("send failed: {e}");
        } else {
            println!("→ replied ({} chars)", reply.len());
        }
    }
    println!("disconnected.");
    Ok(())
}

/// Drive Claude Code headlessly in the working directory and return its reply.
fn run_claude(prompt: &str, workdir: &str) -> Result<String> {
    let out = Command::new("claude")
        .arg("-p")
        .arg(prompt)
        .current_dir(workdir)
        .output()
        .context("couldn't run `claude` — is Claude Code installed and on PATH?")?;
    if !out.status.success() {
        anyhow::bail!("claude failed: {}", String::from_utf8_lossy(&out.stderr).trim());
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}
