use serde::Serialize;

#[derive(Serialize)]
pub struct Environment {
    pub git_sha: Option<String>,
    pub rustc: Option<String>,
    pub target: String,
    pub os: String,
    pub cpu_model: Option<String>,
    pub tokio_workers: usize,
    pub dependency_fingerprint: Option<String>,
}

fn command_output(program: &str, args: &[&str]) -> Option<String> {
    let output = std::process::Command::new(program)
        .args(args)
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

pub fn rss() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|line| line.starts_with("VmHWM:"))?;
    line.split_whitespace()
        .nth(1)?
        .parse::<u64>()
        .ok()?
        .checked_mul(1024)
}

pub fn environment() -> Environment {
    let cpu_model = std::fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|text| {
            text.lines().find_map(|line| {
                line.strip_prefix("model name\t:")
                    .map(str::trim)
                    .map(str::to_owned)
            })
        });
    let fingerprint = std::fs::read("Cargo.lock").ok().map(|bytes| {
        format!(
            "{:016x}",
            bytes
                .iter()
                .fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| (hash
                    ^ u64::from(*byte))
                .wrapping_mul(0x100_0000_01b3))
        )
    });
    Environment {
        git_sha: command_output("git", &["rev-parse", "HEAD"]),
        rustc: command_output("rustc", &["--version"]),
        target: command_output("rustc", &["-vV"])
            .and_then(|details| {
                details
                    .lines()
                    .find_map(|line| line.strip_prefix("host: ").map(str::to_owned))
            })
            .unwrap_or_else(|| std::env::consts::ARCH.to_owned()),
        os: std::env::consts::OS.to_owned(),
        cpu_model,
        tokio_workers: 4,
        dependency_fingerprint: fingerprint,
    }
}
