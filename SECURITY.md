# Security Policy

## Supported Versions

Netplane is currently an unpublished library scaffold. Security fixes are applied to
the `main` branch.

| Version | Supported |
| ------- | --------- |
| latest `main` | ✅ |
| older snapshots | ❌ |

## Reporting a Vulnerability

**Do not open a public GitHub issue for security vulnerabilities.**

Report vulnerabilities by opening a
[GitHub Security Advisory](https://github.com/janagor/netplane/security/advisories/new).

Please include:

- A description of the vulnerability
- Steps to reproduce
- Potential impact
- Suggested fix (if any)

The maintainer will respond through the private advisory channel.

## Rust-Specific Security Practices

Netplane uses the following security practices:

### Dependency Auditing

```bash
# Check advisories, licenses, banned crates, and sources
cargo deny check
```

This check runs on pull requests and pushes to `main`.

### Supply Chain Security

- `deny.toml` configures `cargo-deny` with:
  - Allowed licenses list
  - Banned crates list
  - Advisory database checks
- Dependabot is configured to propose dependency updates weekly

### Unsafe Code

- The library forbids unsafe code.

### Secrets and Credentials

- Never commit secrets, tokens, API keys, or credentials
- Use environment variables or secret management systems
- The `privacy-first` skill in `.agents/skills/privacy-first/` enforces no email
  addresses in the codebase

Repository security settings, including Dependabot alerts and branch protection,
are managed on GitHub and are separate from the checks defined in this repository.
