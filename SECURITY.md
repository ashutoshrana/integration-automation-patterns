# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| 0.2.x   | Yes       |
| 0.1.x   | No        |

## Reporting a Vulnerability

**Do not report security vulnerabilities through public GitHub issues.**

To report a security vulnerability, please use the [GitHub Security Advisory](../../security/advisories/new) feature, or email the maintainer directly.

You should receive a response within 72 hours. If you do not, please follow up to ensure your message was received.

Please include:
- Description of the vulnerability
- Steps to reproduce
- Potential impact
- Suggested fix (if any)

## Disclosure Policy

- We will confirm receipt within 72 hours
- We will provide an initial assessment within 7 days
- We aim to release a patch within 30 days of confirmed vulnerability
- We will coordinate with you on the disclosure timeline
- Credit will be given in the release notes unless you prefer anonymity

## Notes on Scope

This library implements **enterprise integration and webhook automation patterns**. The security surface is:
- Idempotency key handling — ensure duplicate event processing cannot be exploited to replay transactions
- Webhook signature verification helpers — HMAC validation logic for inbound payloads
- Input validation in event schema parsers (source_id, correlation_id, payload fields)
- Audit record generation and log output — ensure no secrets or credentials leak into log lines
- Optional dependency imports (lazy import safety for HTTP client and queue libraries)

This library does **not** manage authentication, network access, or cryptography directly. Integrating applications are responsible for securing API keys, message broker credentials, and webhook endpoint authentication.

**Idempotency Note:** If you discover a pattern that could allow idempotency bypass leading to duplicate financial or transactional operations, treat it as a high-severity vulnerability and report immediately.
