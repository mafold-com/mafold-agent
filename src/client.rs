//! Shared Mafold client — HTTP API + WebSocket URL. Account-symmetric, so the
//! same client serves both the human-facing commands and the agent daemon.

use anyhow::{Context, Result};
use serde_json::{json, Value};

#[derive(Clone)]
pub struct Client {
    pub http: reqwest::Client,
    pub base: String,
    pub token: String,
}

impl Client {
    pub fn new(base: String, token: String) -> Self {
        Self { http: reqwest::Client::new(), base, token }
    }

    /// POST /api/<method>, unwrap the `{ok, result}` envelope or bail.
    async fn post(&self, method: &str, body: Value) -> Result<Value> {
        let v: Value = self
            .http
            .post(format!("{}/api/{method}", self.base))
            .bearer_auth(&self.token)
            .json(&body)
            .send()
            .await
            .with_context(|| format!("{method} request failed"))?
            .json()
            .await
            .with_context(|| format!("{method} returned non-JSON"))?;
        if v.get("ok").and_then(Value::as_bool) == Some(false) {
            anyhow::bail!("{method}: {}", v["description"].as_str().unwrap_or("error"));
        }
        Ok(v["result"].clone())
    }

    pub async fn me(&self) -> Result<Value> { self.post("getMe", json!({})).await }
    pub async fn chats(&self) -> Result<Value> { self.post("getChats", json!({})).await }

    pub async fn send(&self, chat_id: &str, text: &str) -> Result<Value> {
        self.post("sendMessage", json!({ "chat_id": chat_id, "text": text })).await
    }

    /// Resolve a chat argument: a conversation UUID is used as-is; anything else
    /// is treated as a username (`@name` or `name`) → open/find the DM.
    pub async fn resolve_chat(&self, arg: &str) -> Result<String> {
        let a = arg.trim_start_matches('@');
        if a.len() == 36 && a.matches('-').count() == 4 {
            return Ok(a.to_string());
        }
        let conv = self.post("startChat", json!({ "user_ids": [a] })).await?;
        Ok(conv["id"].as_str().context("startChat: no conversation id")?.to_string())
    }

    // ── bot streaming write API (used by the agent daemon) ──
    pub async fn create_draft(&self, chat_id: &str) -> Result<String> {
        let r = self.post("botCreateDraft", json!({ "chat_id": chat_id })).await?;
        Ok(r["id"].as_str().context("botCreateDraft: no id")?.to_string())
    }
    pub async fn append_delta(&self, message_id: &str, delta: &str) -> Result<()> {
        self.post("botAppendDelta", json!({ "message_id": message_id, "delta": delta })).await?;
        Ok(())
    }
    pub async fn finalize(&self, message_id: &str) -> Result<()> {
        self.post("botFinalize", json!({ "message_id": message_id })).await?;
        Ok(())
    }

    pub fn ws_url(&self) -> String {
        let ws = self.base.replacen("https://", "wss://", 1).replacen("http://", "ws://", 1);
        format!("{ws}/api/ws?token={}", self.token)
    }
}
