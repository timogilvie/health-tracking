# Security, privacy, and provider boundaries

## Data flow and trust boundaries

The application runs locally. Provider responses and user-supplied files cross into an immutable
raw store, are normalized into a local DuckDB database, and may be rendered through a loopback-only
dashboard or written to private export directories. No project-operated server receives health
data or credentials.

Sensitive assets include OAuth client secrets and rotating tokens, provider archives, raw payloads,
DuckDB files, snapshots, generated reports, CSV/Parquet exports, manual notes, and metadata that can
identify a person or device. These belong under ignored `data/` directories or another private
location with restrictive operating-system permissions.

## Threat model

| Threat | Existing control | User responsibility / residual risk |
| --- | --- | --- |
| Accidental Git disclosure | Ignore rules plus tracked/staged and full-history `privacy-check` | Review remotes and rotate exposed credentials; history rewriting requires separate approval |
| Token theft | Tokens stored separately with private permissions, atomic replacement, and redacted manifests | Protect the OS account, disk, backups, terminal history, and `.env` |
| Malicious or malformed export | Bounded archive/file limits, schema validation, streaming XML, path checks, atomic writes | Import only expected archives; parsers may still contain defects |
| Raw-data tampering | SHA-256 and byte-length manifests verified before replay | Protect local storage; hashes detect changes but do not prevent deletion |
| Dashboard exposure | Binds to `127.0.0.1`, no remote assets, CSP, no-store, no framing | Do not proxy or rebind it to a public interface without adding authentication and TLS |
| Export disclosure | New directories, no overwrite, private file modes, ignored default path | Secure files copied outside the project and remove them when no longer needed |
| Dependency compromise | Locked dependencies, provenance policy, license inventory, SBOM, vulnerability audit | Review updates and respond to advisories; scans cannot prove absence of compromise |
| Incorrect health interpretation | Source provenance, duplicate links, plausibility checks, non-medical disclaimer | Verify against original records and qualified professionals |

The application does not defend against an administrator or malware already controlling the local
machine. It does not encrypt the database itself; use full-disk encryption or an encrypted volume.

## Provider-specific terms and credentials

Users create and operate their own provider applications and must comply with each provider's
current developer terms, privacy policy, rate limits, data-export rules, and branding requirements.
The repository does not ship provider client IDs, client secrets, redirect registrations, access
tokens, refresh tokens, or permission to redistribute provider data.

- Apple Health ingestion uses a file the user exports from their own device. Apple and source-app
  terms continue to apply to that data.
- Withings API access requires a user-supplied application registration; manual account export is
  supported when API access is unavailable.
- Oura API access requires a user-supplied OAuth application and approved scopes.
- Eight Sleep data currently arrives through Apple Health. Any future direct adapter must document
  its supported user export or official API and must not depend on credential scraping.

Provider names are used only to describe interoperability. The Apache-2.0 license grants no
trademark rights.

## Public reporting rules

Public issues and pull requests may include synthetic fixtures, command names, sanitized error
categories, run IDs, and aggregate counts. They must not include measurements, dates from a real
record, source filenames containing a person's name, provider request bodies, device identifiers,
or raw metadata. Use the private process in `SECURITY.md` for suspected exposure.
