//! Agent daemon — run Claude Code as your Mafold bot. Connects over WS, and for
//! each incoming message drives the local `claude` in the working directory and
//! streams the reply back. Always finalizes (never leaves the chat on "typing…").
//!
//! Context: each Mafold conversation maps to one persistent Claude Code session
//! (`--resume <session_id>`), so follow-ups keep the full prior context + tool
//! work. The map is persisted to ~/.mafold/sessions.json so context survives a
//! daemon restart (Claude Code stores the sessions on disk).

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::Mutex;

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

// ── per-conversation Claude session map (persisted) ──
type Sessions = Arc<Mutex<HashMap<String, String>>>;

fn sessions_path() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
    PathBuf::from(home).join(".mafold").join("sessions.json")
}
fn load_sessions() -> HashMap<String, String> {
    std::fs::read_to_string(sessions_path())
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default()
}
fn save_sessions(map: &HashMap<String, String>) {
    if let Ok(s) = serde_json::to_string(map) {
        let _ = std::fs::write(sessions_path(), s);
    }
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

    let sessions: Sessions = Arc::new(Mutex::new(load_sessions()));
    // Serialize claude runs: same workdir → concurrent edits/sessions would
    // clash. One at a time (queued); each message still shows "typing" while it
    // waits because we open the draft before taking the lock.
    let exec_lock = Arc::new(Mutex::new(()));

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
        let sessions = sessions.clone();
        let exec_lock = exec_lock.clone();
        tokio::spawn(async move {
            if let Err(e) = handle(&client, &workdir, &m.conversation_id, &m.content, &sessions, &exec_lock).await {
                eprintln!("handle error: {e}");
            }
        });
    }
    println!("disconnected.");
    Ok(())
}

/// Open a draft, run claude (resuming this conversation's session), ALWAYS
/// finalize (surfacing any error).
async fn handle(
    client: &Client,
    workdir: &str,
    chat_id: &str,
    prompt: &str,
    sessions: &Sessions,
    exec_lock: &Arc<Mutex<()>>,
) -> Result<()> {
    let msg_id = client.create_draft(chat_id).await?;

    // Serialize: only one claude runs at a time in this workdir.
    let _guard = exec_lock.lock().await;
    let prior = sessions.lock().await.get(chat_id).cloned();

    match stream_claude(client, workdir, prompt, &msg_id, prior.as_deref(), sessions, chat_id).await {
        Ok(true) => {}
        Ok(false) => { let _ = client.append_delta(&msg_id, "_(the agent produced no output)_").await; }
        Err(e) => {
            eprintln!("claude failed: {e}");
            // A stale/expired resumed session → drop it so the next message
            // starts fresh instead of failing again.
            if prior.is_some() {
                let mut s = sessions.lock().await;
                if s.remove(chat_id).is_some() { save_sessions(&s); }
            }
            let _ = client.append_delta(&msg_id, &format!("⚠️ Agent error: {e}")).await;
        }
    }
    let _ = client.finalize(&msg_id).await;
    println!("→ finalized reply for chat {chat_id}");
    Ok(())
}

async fn stream_claude(
    client: &Client,
    workdir: &str,
    prompt: &str,
    msg_id: &str,
    prior_session: Option<&str>,
    sessions: &Sessions,
    chat_id: &str,
) -> Result<bool> {
    if !std::path::Path::new(workdir).is_dir() {
        anyhow::bail!("working directory does not exist: {workdir} — check --workdir");
    }
    let mut cmd = tokio::process::Command::new("claude");
    cmd.arg("-p").arg(prompt)
        .arg("--output-format").arg("stream-json")
        .arg("--verbose")
        .arg("--include-partial-messages")
        .arg("--dangerously-skip-permissions");
    // Resume this conversation's session → full prior context.
    if let Some(sid) = prior_session {
        cmd.arg("--resume").arg(sid);
    }
    let mut child = cmd
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
    let mut session_id: Option<String> = None;
    let mut last_flush = Instant::now();

    while let Some(line) = lines.next_line().await? {
        let line = line.trim();
        if line.is_empty() { continue; }
        let v: serde_json::Value = match serde_json::from_str(line) { Ok(v) => v, Err(_) => continue };
        // Capture the session id (present on every event; first wins).
        if session_id.is_none() {
            if let Some(sid) = v["session_id"].as_str() {
                session_id = Some(sid.to_string());
            }
        }
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

    // Remember the session for this conversation so the next message resumes it.
    if let Some(sid) = session_id {
        let mut s = sessions.lock().await;
        if s.get(chat_id).map(String::as_str) != Some(sid.as_str()) {
            s.insert(chat_id.to_string(), sid);
            save_sessions(&s);
        }
    }

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
