# Security policy

## Supported versions

Only the latest released version is supported with security updates.

## Reporting a vulnerability

Do not open a public issue containing a Meridian email address, login code, refresh token, ID token, account number, ICP, address, usage history, bill, or diagnostics file that you have not reviewed.

Use GitHub's private security-advisory feature for vulnerabilities. Revoke a potentially exposed Meridian session by signing out of MyMeridian sessions or contacting Meridian, then reauthenticate the Home Assistant integration.

## Credential model

- Passwords are never requested.
- OTPs are sent directly to Meridian and are not persisted.
- Short-lived Firebase ID tokens remain in memory.
- A Firebase refresh token is persisted in the Home Assistant config entry so scheduled imports can continue.
- Diagnostics and logs must never contain request bodies, authorization headers or raw API responses.

The Firebase API key embedded in the integration is a public client configuration value also shipped in Meridian's web application. It is not a customer credential.

