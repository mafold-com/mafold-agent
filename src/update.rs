//! Self-update: check the latest GitHub release and replace the binary in place.
//! Used by `mafold update` (manual) and the agent's auto-update.

use anyhow::{Context, Result};
use std::path::PathBuf;

const REPO: &str = "mafold-com/mafold-cli";

pub fn current_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// This build's release-asset target triple (matches the release workflow).
fn target_triple() -> Option<&'static str> {
    if cfg!(all(target_os = "macos", target_arch = "aarch64")) {
        Some("aarch64-apple-darwin")
    } else if cfg!(all(target_os = "macos", target_arch = "x86_64")) {
        Some("x86_64-apple-darwin")
    } else if cfg!(all(target_os = "linux", target_arch = "x86_64")) {
        Some("x86_64-unknown-linux-gnu")
    } else {
        None
    }
}

fn parse_semver(v: &str) -> (u64, u64, u64) {
    let v = v.trim().trim_start_matches('v');
    let mut it = v.split('.').map(|p| {
        p.chars().take_while(|c| c.is_ascii_digit()).collect::<String>().parse().unwrap_or(0)
    });
    (it.next().unwrap_or(0), it.next().unwrap_or(0), it.next().unwrap_or(0))
}

fn is_newer(latest: &str, current: &str) -> bool {
    parse_semver(latest) > parse_semver(current)
}

/// Latest release: (version tag, download URL for this platform's asset).
async fn latest(http: &reqwest::Client) -> Result<(String, String)> {
    let url = format!("https://api.github.com/repos/{REPO}/releases/latest");
    let v: serde_json::Value = http
        .get(&url)
        .header("User-Agent", "mafold-cli")
        .header("Accept", "application/vnd.github+json")
        .send().await?
        .error_for_status()?
        .json().await?;
    let tag = v["tag_name"].as_str().context("no tag_name")?.to_string();
    let target = target_triple().context("unsupported platform for self-update")?;
    let asset_name = format!("mafold-{target}");
    let dl = v["assets"].as_array().and_then(|a| {
        a.iter()
            .find(|x| x["name"].as_str() == Some(&asset_name))
            .and_then(|x| x["browser_download_url"].as_str())
    }).with_context(|| format!("release has no asset {asset_name}"))?.to_string();
    Ok((tag, dl))
}

/// The real binary path (resolve a PATH symlink to the file we replace).
fn binary_path() -> Result<PathBuf> {
    let exe = std::env::current_exe()?;
    Ok(std::fs::canonicalize(&exe).unwrap_or(exe))
}

async fn download_and_replace(http: &reqwest::Client, url: &str) -> Result<()> {
    let bin = binary_path()?;
    let bytes = http
        .get(url)
        .header("User-Agent", "mafold-cli")
        .send().await?
        .error_for_status()?
        .bytes().await?;
    let tmp = bin.with_extension("new");
    std::fs::write(&tmp, &bytes)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o755))?;
    }
    // Atomic on the same filesystem; replacing a running binary is safe on Unix.
    std::fs::rename(&tmp, &bin).context("failed to replace the binary")?;
    Ok(())
}

/// Is a newer release available? Returns (version, download URL) — no download.
pub async fn check(http: &reqwest::Client) -> Result<Option<(String, String)>> {
    let (tag, url) = latest(http).await?;
    if is_newer(&tag, current_version()) {
        Ok(Some((tag.trim_start_matches('v').to_string(), url)))
    } else {
        Ok(None)
    }
}

/// Download the asset at `url` and replace the running binary in place.
pub async fn apply(http: &reqwest::Client, url: &str) -> Result<()> {
    download_and_replace(http, url).await
}

/// Check + update if a newer release exists. Returns the new version if updated.
pub async fn update_to_latest(http: &reqwest::Client) -> Result<Option<String>> {
    match check(http).await? {
        Some((v, url)) => { apply(http, &url).await?; Ok(Some(v)) }
        None => Ok(None),
    }
}

/// Replace the current process image with the (freshly updated) binary, keeping
/// the same args + env. Never returns on success.
#[cfg(unix)]
pub fn reexec() -> std::io::Error {
    use std::os::unix::process::CommandExt;
    let exe = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("mafold"));
    std::process::Command::new(exe)
        .args(std::env::args_os().skip(1))
        .exec()
}
