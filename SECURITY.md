# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in this project, please report it responsibly.

**Do not open a public GitHub issue for security vulnerabilities.**

Instead, please email: **security@hapster.io**

Include:
- Description of the vulnerability
- Steps to reproduce
- Potential impact

We will acknowledge your report within 48 hours and work to resolve the issue promptly.

## Credentials

This project requires Firebase credentials to store data. These credentials must **never** be committed to the repository.

- Store credentials outside the repo or in a `.env` file (which is gitignored)
- Never hardcode API keys, tokens, or passwords in source code
- Rotate any credentials that may have been exposed
