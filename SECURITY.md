## Reporting a Vulnerability

If you discover a security vulnerability, please **do not** open a public GitHub issue.

Instead, report it privately via email: **kirill.a.fran@gmail.com**

Include in your report:
- Description of the vulnerability
- Steps to reproduce
- Potential impact
- Suggested fix (if any)

**Response time:** I aim to respond within 72 hours and release a fix within 14 days for confirmed issues.

## Scope

This bot manages server infrastructure via Telegram. Key security concerns include:

- **Authentication bypass** — unauthorized access to bot commands
- **Command injection** — via SSH or subprocess calls
- **Privilege escalation** — gaining admin access without authorization
- **Sensitive data exposure** — API tokens, passwords, server credentials

## Out of Scope

- Denial of service attacks
- Issues in third-party dependencies (report to their maintainers)
- Theoretical vulnerabilities without proof of concept

## Security Measures

- Admin access is restricted to whitelisted Telegram user IDs
- All server commands execute via SSH with key-based authentication
- Secrets are stored in environment variables, not in code
