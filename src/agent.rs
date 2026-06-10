//! Agent daemon — run Claude Code as your Mafold bot. Connects over WS, and for
//! each incoming message drives the local `claude` in the working directory and
//! streams the reply back. Always finalizes (never leaves the chat on "typing…").

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::process::Stdio;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, BufReader};

use crate::client::Client;

#[derive(Deserialize)]
struct Sender { username: String }
#[derive(Deserialize)]
struct IncomingMessage {
    conversation_id: String,
    sender: Sender,
    #[serde(default)]
    content: String,
}

pub async fn run(client: Client, workdir: String) -> Result<()> {
    let me = client.me().await.context("getMe failed — check the token / --base")?;
    let my_username = me["username"].as_str().unwrap_or_default().to_string();
    anyhow::ensure!(!my_username.is_empty(), "could not resolve bot identity (bad token?)");
    if !std::path::Path::new(&workdir).is_dir() {
        eprintln!("⚠️  working directory does not exist: {workdir} — claude will fail. Check --workdir.");
    }
    println!("mafold agent ✓ connected as @{my_username}  ·  workdir={workdir}");

    let (ws, _) = tokio_tungstenite::connect_async(&client.ws_url())
        .await
        .context("WebSocket connect failed")?;
    let (mut write, mut read) = ws.split();
    println!("listening for messages to @{my_username} …");

    // Keepalive so the heartbeat keeps the bot marked online.
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(25));
        loop {
            tick.tick().await;
            if write.send(tokio_tungstenite::tungstenite::Message::Ping(Vec::new().into())).await.is_err() {
                break;
            }
        }
    });

    while let Some(frame) = read.next().await {
        let frame = match frame { Ok(f) => f, Err(e) => { eprintln!("ws error: {e}"); break; } };
        let text = match frame.into_text() { Ok(t) => t, Err(_) => continue };
        let env: serde_json::Value = match serde_json::from_str(&text) { Ok(v) => v, Err(_) => continue };
        if env.get("method").and_then(|m| m.as_str()) != Some("events.messageNew") { continue; }
        let m: IncomingMessage = match serde_json::from_value(env["params"].clone()) { Ok(m) => m, Err(_) => continue };
        if m.sender.username.eq_ignore_ascii_case(&my_username) || m.content.trim().is_empty() { continue; }

        println!("← @{}: {}", m.sender.username, m.content);
        let client = client.clone();
        let workdir = workdir.clone();
        tokio::spawn(async move {
            if let Err(e) = handle(&client, &workdir, &m.conversation_id, &m.content).await {
                eprintln!("handle error: {e}");
            }
        });
    }
    println!("disconnected.");
    Ok(())
}

/// Open a draft, stream claude's reply, ALWAYS finalize (surfacing any error).
async fn handle(client: &Client, workdir: &str, chat_id: &str, prompt: &str) -> Result<()> {
    let msg_id = client.create_draft(chat_id).await?;
    match stream_claude(client, workdir, prompt, &msg_id).await {
        Ok(true) => {}
        Ok(false) => { let _ = client.append_delta(&msg_id, "_(the agent produced no output)_").await; }
        Err(e) => {
            eprintln!("claude failed: {e}");
            let _ = client.append_delta(&msg_id, &format!("⚠️ Agent error: {e}")).await;
        }
    }
    let _ = client.finalize(&msg_id).await;
    println!("→ finalized reply for chat {chat_id}");
    Ok(())
}

async fn stream_claude(client: &Client, workdir: &str, prompt: &str, msg_id: &str) -> Result<bool> {
    if !std::path::Path::new(workdir).is_dir() {
        anyhow::bail!("working directory does not exist: {workdir} — check --workdir");
    }
    let mut child = tokio::process::Command::new("claude")
        .arg("-p").arg(prompt)
        .arg("--output-format").arg("stream-json")
        .arg("--verbose")
        .arg("--include-partial-messages")
        .arg("--dangerously-skip-permissions")
        .current_dir(workdir)
        .env_remove("CLAUDECODE")
        .env_remove("ANTHROPIC_API_KEY")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("couldn't run `claude` in {workdir} — is Claude Code installed and on PATH?"))?;

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
                    let _ = client.append_delta(msg_id, &buf).await;
                    buf.clear();
                    last_flush = Instant::now();
                }
            }
        }
        if v["type"] == "result" { break; }
    }
    if !buf.is_empty() { let _ = client.append_delta(msg_id, &buf).await; }

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
