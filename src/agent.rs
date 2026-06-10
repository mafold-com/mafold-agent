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
use std::time::Duration;
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

pub async fn run(client: Client, workdir: String, auto_update: bool) -> Result<()> {
    // Self-update on startup (before connecting) so a (re)started agent is
    // always current; if it updates, re-exec into the new binary.
    if auto_update {
        if let Ok(Some((v, url))) = crate::update::check(&client.http).await {
            println!("↻ updating to v{v}…");
            if crate::update::apply(&client.http, &url).await.is_ok() {
                let _ = crate::update::reexec(); // replaces this process
            }
        }
    }

    let me = client.me().await.context("getMe failed — check the token / --base")?;
    let my_username = me["username"].as_str().unwrap_or_default().to_string();
    anyhow::ensure!(!my_username.is_empty(), "could not resolve bot identity (bad token?)");
    if !std::path::Path::new(&workdir).is_dir() {
        eprintln!("⚠️  working directory does not exist: {workdir} — claude will fail. Check --workdir.");
    }
    println!("mafold agent ✓ connected as @{my_username}  ·  workdir={workdir}");

    // Publish the command panel for this agent (Telegram-style).
    let _ = client.set_commands(serde_json::json!([
        { "command": "clear", "description": "Start a fresh conversation (clear context)" },
        { "command": "help",  "description": "What this agent can do" },
    ])).await;

    let sessions: Sessions = Arc::new(Mutex::new(load_sessions()));
    // Serialize claude runs: same workdir → concurrent edits/sessions would
    // clash. One at a time (queued); each message still shows "typing" while it
    // waits because we open the draft before taking the lock.
    let exec_lock = Arc::new(Mutex::new(()));

    // Hourly auto-update: check; if a newer release exists, apply + re-exec —
    // but only when IDLE (try_lock succeeds → no claude running/queued) so we
    // never kill an in-flight reply. Busy → retry next hour.
    if auto_update {
        let client = client.clone();
        let exec_lock = exec_lock.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(3600));
            tick.tick().await; // consume the immediate first tick
            loop {
                tick.tick().await;
                match crate::update::check(&client.http).await {
                    Ok(Some((v, url))) => {
                        if let Ok(_g) = exec_lock.try_lock() {
                            println!("↻ updating to v{v} — restarting…");
                            if crate::update::apply(&client.http, &url).await.is_ok() {
                                let _ = crate::update::reexec();
                            }
                        } else {
                            println!("update v{v} available — will apply when idle");
                        }
                    }
                    Ok(None) => {}
                    Err(e) => eprintln!("auto-update check failed: {e}"),
                }
            }
        });
    }

    // Reconnect loop: a dropped WS (network blip, server restart) must NOT kill
    // the daemon. Reconnect with backoff; sessions/exec_lock persist across it.
    let mut backoff = 1u64;
    loop {
        if let Err(e) = connect_and_run(&client, &workdir, &my_username, &sessions, &exec_lock).await {
            eprintln!("connection error: {e}");
        }
        eprintln!("reconnecting in {backoff}s…");
        tokio::time::sleep(Duration::from_secs(backoff)).await;
        backoff = (backoff * 2).min(30);
    }
}

/// One WS session: connect, keepalive-ping, dispatch incoming messages. Returns
/// when the socket drops (so the caller reconnects).
async fn connect_and_run(
    client: &Client,
    workdir: &str,
    my_username: &str,
    sessions: &Sessions,
    exec_lock: &Arc<Mutex<()>>,
) -> Result<()> {
    let (ws, _) = tokio_tungstenite::connect_async(&client.ws_url())
        .await
        .context("WebSocket connect failed")?;
    let (mut write, mut read) = ws.split();
    println!("listening for messages to @{my_username} …");

    // Keepalive so the heartbeat keeps the bot marked online.
    let ping = tokio::spawn(async move {
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
        if m.sender.username.eq_ignore_ascii_case(my_username) || m.content.trim().is_empty() { continue; }

        println!("← @{}: {}", m.sender.username, m.content);

        // Slash commands handled by the agent itself (no claude run).
        let cmd = m.content.trim();
        if cmd == "/clear" {
            {
                let mut s = sessions.lock().await;
                if s.remove(&m.conversation_id).is_some() { save_sessions(&s); }
            }
            let _ = client.send(&m.conversation_id, "🧹 Context cleared — starting fresh.").await;
            continue;
        }
        if cmd == "/help" {
            let _ = client.send(&m.conversation_id,
                "I'm a Claude Code agent running on this machine. Just message me — I keep context across the conversation.\n\n• /clear — start fresh (drop context)\n• /help — this message").await;
            continue;
        }

        let client = client.clone();
        let workdir = workdir.to_string();
        let sessions = sessions.clone();
        let exec_lock = exec_lock.clone();
        tokio::spawn(async move {
            if let Err(e) = handle(&client, &workdir, &m.conversation_id, &m.content, &sessions, &exec_lock).await {
                eprintln!("handle error: {e}");
            }
        });
    }
    ping.abort();
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

    // Decouple network sends from reading claude's stdout: a sender task drains
    // a channel and POSTs batched, ORDERED deltas. So a slow append never stalls
    // reading — long replies stream smoothly even on a slow link.
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    let sender = {
        let client = client.clone();
        let msg_id = msg_id.to_string();
        tokio::spawn(async move {
            let mut buf = String::new();
            loop {
                match tokio::time::timeout(Duration::from_millis(120), rx.recv()).await {
                    Ok(Some(chunk)) => {
                        buf.push_str(&chunk);
                        if buf.len() >= 240 {
                            let _ = client.append_delta(&msg_id, &buf).await;
                            buf.clear();
                        }
                    }
                    Ok(None) => break,           // channel closed → claude done
                    Err(_) => {                  // idle ~120ms → flush what we have
                        if !buf.is_empty() {
                            let _ = client.append_delta(&msg_id, &buf).await;
                            buf.clear();
                        }
                    }
                }
            }
            if !buf.is_empty() { let _ = client.append_delta(&msg_id, &buf).await; }
        })
    };

    let mut produced = false;
    let mut session_id: Option<String> = None;

    while let Some(line) = lines.next_line().await? {
        let line = line.trim();
        if line.is_empty() { continue; }
        let v: serde_json::Value = match serde_json::from_str(line) { Ok(v) => v, Err(_) => continue };
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
                let _ = tx.send(t.to_string());
                produced = true;
            }
        }
        // Tool use → a compact {% tool %} card (shows the work as it happens, so
        // a long agentic turn doesn't look frozen on "typing").
        if v["type"] == "assistant" {
            if let Some(blocks) = v["message"]["content"].as_array() {
                for b in blocks {
                    if b["type"] == "tool_use" {
                        let name = b["name"].as_str().unwrap_or("tool");
                        let detail = tool_detail(name, &b["input"]);
                        let tag = format!(
                            "\n{{% tool name=\"{}\" detail=\"{}\" /%}}\n",
                            attr_esc(name), attr_esc(&detail)
                        );
                        let _ = tx.send(tag);
                        produced = true;
                    }
                }
            }
        }
        if v["type"] == "result" { break; }
    }
    drop(tx);             // close the channel → sender flushes the tail + exits
    let _ = sender.await;

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

/// A short, human-readable detail for a tool-use card (the file, command, …).
fn tool_detail(name: &str, input: &serde_json::Value) -> String {
    let raw = match name.to_lowercase().as_str() {
        "bash" => input["command"].as_str(),
        "edit" | "write" | "multiedit" | "read" | "notebookedit" => input["file_path"].as_str(),
        "glob" | "grep" => input["pattern"].as_str(),
        "webfetch" => input["url"].as_str(),
        "task" => input["description"].as_str(),
        "todowrite" => Some("updating plan"),
        _ => None,
    };
    raw.unwrap_or("").to_string()
}

/// Sanitize a value for a markdoc attribute: no quotes/newlines, capped length.
fn attr_esc(s: &str) -> String {
    let cleaned: String = s
        .chars()
        .map(|c| match c { '"' => '\'', '\n' | '\r' | '\t' => ' ', _ => c })
        .collect();
    let cleaned = cleaned.trim();
    if cleaned.chars().count() > 80 {
        format!("{}…", cleaned.chars().take(80).collect::<String>())
    } else {
        cleaned.to_string()
    }
}
