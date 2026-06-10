//! mafold-cli — Mafold from your terminal.
//!
//!   mafold-cli agent --token mb_… --workdir ~/repo   # run Claude Code as your bot
//!   mafold-cli --token mb_… chats                     # list conversations
//!   mafold-cli --token mb_… send @alice "hi there"    # send a message
//!
//! Auth is a bot token (`mb_…`) via --token or $MAFOLD_BOT_TOKEN.

mod agent;
mod client;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use client::Client;

#[derive(Parser)]
#[command(name = "mafold-cli", version, about = "Mafold from your terminal — CLI client + Claude Code agent")]
struct Cli {
    #[arg(long, env = "MAFOLD_BASE", default_value = "https://api.mafold.com", global = true)]
    base: String,
    #[arg(long, env = "MAFOLD_BOT_TOKEN", global = true)]
    token: Option<String>,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run Claude Code as your bot (daemon): receive messages, reply with the
    /// local `claude` in the working directory.
    Agent {
        #[arg(long, env = "MAFOLD_WORKDIR", default_value = ".")]
        workdir: String,
    },
    /// List your conversations.
    Chats,
    /// Send a message. <chat> is a conversation id or a @username.
    Send {
        chat: String,
        #[arg(trailing_var_arg = true, required = true)]
        text: Vec<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let token = cli
        .token
        .context("set --token or $MAFOLD_BOT_TOKEN (your bot's mb_ token — create a bot in the Mafold app)")?;
    let client = Client::new(cli.base, token);

    match cli.cmd {
        Cmd::Agent { workdir } => agent::run(client, workdir).await?,
        Cmd::Chats => chats(&client).await?,
        Cmd::Send { chat, text } => send(&client, &chat, &text.join(" ")).await?,
    }
    Ok(())
}

async fn chats(client: &Client) -> Result<()> {
    let me = client.me().await?;
    let my = me["username"].as_str().unwrap_or_default().to_lowercase();
    let result = client.chats().await?;
    let items = result["items"].as_array().cloned().unwrap_or_default();
    if items.is_empty() {
        println!("(no conversations)");
        return Ok(());
    }
    for c in items {
        let title = c["title"].as_str().map(str::to_string).unwrap_or_else(|| {
            // DM → the other participant's display name.
            c["participants"].as_array().and_then(|ps| {
                ps.iter().find(|p| p["username"].as_str().map(str::to_lowercase) != Some(my.clone()))
            }).and_then(|p| p["display_name"].as_str()).unwrap_or("Chat").to_string()
        });
        let preview = c["last_message"]["content"].as_str().unwrap_or("—");
        let unread = c["unread_count"].as_u64().unwrap_or(0);
        let badge = if unread > 0 { format!("  ({unread})") } else { String::new() };
        let oneline = preview.replace('\n', " ");
        let oneline = if oneline.chars().count() > 60 {
            format!("{}…", oneline.chars().take(60).collect::<String>())
        } else {
            oneline
        };
        println!("• {title}{badge}\n  {oneline}");
    }
    Ok(())
}

async fn send(client: &Client, chat: &str, text: &str) -> Result<()> {
    let chat_id = client.resolve_chat(chat).await?;
    client.send(&chat_id, text).await?;
    println!("✓ sent to {chat}");
    Ok(())
}
