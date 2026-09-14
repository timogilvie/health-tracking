# Security policy

## Supported versions

Until version 1.0, only the latest release on the default branch receives security fixes. There is
no hosted service and no guarantee of response time. See `docs/releases.md` for compatibility.

## Reporting a vulnerability or privacy exposure

Use GitHub's private vulnerability-reporting or draft security-advisory feature for this
repository. If that feature is unavailable, contact the repository owner privately. Do not open a
public issue for a credential, personal-health-data exposure, authentication bypass, raw-store
integrity defect, or path traversal.

Include the affected version, a minimal synthetic reproduction, expected impact, and suggested
mitigation. Do not attach a real export, database, token file, screenshot containing measurements,
or logs with identifying metadata. Revoke or rotate any credential that may have been exposed.

The maintainer will acknowledge a report when practical, assess it, coordinate a fix and release,
and credit the reporter if requested. Please do not access another person's data or disrupt a
provider while testing.

## Security boundaries

Health Tracking is a local command-line application. It binds the dashboard only to loopback and
does not provide multi-user authentication, cloud storage, medical-device controls, or a hosted
service. Users are responsible for operating-system account security, disk encryption, backups,
provider credentials, and access to generated exports. The detailed threat model is in
`docs/security-and-privacy.md`.
