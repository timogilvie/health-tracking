# Health Tracking Privacy Policy

**Effective date: September 14, 2026**

This Privacy Policy explains how the Health Tracking software (the **Software**) handles
information. Health Tracking is local, self-hosted, open-source software. It is not a hosted
service.

## The short version

The project maintainers do not collect, receive, use, sell, rent, monetize, or share your health
data. There are no project-operated data servers, user accounts, advertising systems, analytics,
or telemetry. The Software processes your data on your computer, at your direction, only to
provide the features you choose to run.

Because the project maintainers do not receive your health data, they cannot see it, use it for
their own purposes, disclose it, or delete it for you.

## Information processed locally

Depending on the features you use, the Software may process:

- health, wellness, activity, sleep, workout, and body-measurement records;
- dates, times, device and source identifiers, and other provider metadata;
- files that you export from Apple Health, Withings, or another supported source;
- OAuth client credentials, access tokens, and refresh tokens for provider APIs; and
- local configuration, import history, quality checks, duplicate links, reports, and exports.

This information is stored in files on your computer, normally under the configured `data/`
directory. It may be shown through a dashboard bound to your computer's loopback interface or
written to an export directory you select.

## How the Software uses information

The Software processes information locally only when you direct it to authenticate with a
provider, import or synchronize records, normalize and reconcile data, calculate summaries,
display the local dashboard, run quality checks, or create exports. The project maintainers do not
use your information.

The Software does not use health data for advertising, marketing, profiling, eligibility or
employment decisions, sale, licensing, model training, or development of artificial intelligence
systems. It does not transmit health data to the project maintainers or make it available to other
users.

## Provider connections

If you choose to connect a provider API, the Software communicates directly from your computer to
that provider using credentials and permissions you supply. The provider receives normal API and
network information and handles it under its own terms and privacy policy. Provider authorization
screens control the categories of data you permit the Software to access.

For Oura, the Software requests only the configured scopes needed for the selected functions and
uses authorized Oura data solely to import, store, analyze, display, or export it locally at your
direction. Oura data is not shared with any third party or provided to an artificial intelligence
or machine-learning system. Oura and other providers may independently log or monitor API use as
described in their policies.

The project is not affiliated with, sponsored by, approved by, or endorsed by Apple, Withings,
Oura, Eight Sleep, or any other provider.

## Storage, retention, and security

Health records, raw provider responses, tokens, and generated exports remain on storage you
control. The repository ignores the default private-data paths, but the Software does not encrypt
the database itself. You are responsible for device access controls, full-disk encryption,
backups, exported copies, and any synchronization service you choose for the project directory.
Such services operate under their own privacy policies.

The Software retains local data until you delete it. Keep it only as long as needed for your
personal use and any applicable provider requirements. The project maintainers have no remote
access to your files and cannot set or enforce a retention period on your computer.

## Your choices: access, revocation, and deletion

You can inspect and export your local data using the Software. You can stop future provider access
at any time by revoking the application's authorization in the provider's account settings and
removing the related token files from `data/secrets/`.

To delete information processed by the Software, stop the dashboard and other Health Tracking
commands, then delete the applicable local database, raw files, snapshots, exports, and token
files under `data/`, plus any copies or backups you created. Deletion from your computer is under
your control and takes effect when you remove those files. Revoking provider authorization does
not automatically delete records already stored locally, and deleting local records does not
delete records held by a provider.

## GitHub, support, and information you choose to send

Do not put real health data, provider exports, credentials, tokens, identifying dates, or
screenshots containing measurements in a GitHub issue, pull request, or support message. Public
GitHub activity and account information are processed by GitHub under GitHub's own privacy policy.
The project maintainers do not use that information for advertising, profiling, sale, or any
purpose unrelated to maintaining the project.

If you voluntarily send information in a private security or privacy report, the recipient may
review it only to investigate and respond to that report. Do not send health data. Information may
also be retained or disclosed when strictly required by applicable law; the project cannot promise
otherwise. This limited support interaction does not change the fact that the Software itself
does not transmit your health data to the maintainers.

## Children

The Software is not directed to children. Do not use it to process a child's information unless
you are legally authorized to do so and have obtained any required consent.

## Changes to this policy

If the project ever adds a hosted service, telemetry, advertising, or any feature that sends
health data to a project-operated or third-party service, this policy must be updated and
appropriate notice and consent must be provided before that collection begins.

We may otherwise update this policy by changing the effective date and publishing the revised
version. The version in the copy of the Software you use describes that version's behavior.

## Contact

For a privacy question, contact the repository owner through GitHub without including personal or
health information. Report a suspected exposure privately using the process in
[SECURITY.md](SECURITY.md).
