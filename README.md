# X Browser Collector

Bounded, read-only X timeline collection using an existing owner-authorized Chrome session over the Chrome DevTools Protocol. No API key or cookie export is included.

## Included
- Request allowlisting, traffic/data budgets, adaptive polling and a single-process lock.
- A STOP file; fail-closed handling of login, challenges, rate limits and browser disconnects.
- First-page timeline normalization, known-ID overlap, receipt export and 48-hour post-body retention.
- Network-free regression tests using synthetic data.

This is the browser collector from a larger private research system. Its separate Python/twscrape adapter and private operational integrations are not included. No third-party scraper implementation has been copied into this edition.

## Requirements
Node.js 22+ with built-in WebSocket. Chrome with owner-approved local remote debugging and an existing signed-in X tab. Keep the debugging endpoint loopback-only.

## Configuration (PowerShell)
~~~powershell
$env:X_AUTHORS = "example_author"
$env:X_COLLECTOR_HOME = "$PWD/.state"
$env:CDP_ACTIVE_PORT_FILE = "$env:LOCALAPPDATA/Google/Chrome/User Data/DevToolsActivePort"
node collector.mjs --check
node collector.mjs --author=example_author
~~~
Replace the example handle. The --check command is offline. Collection opens and closes only its own background tab. --serve --shadow-hours=1 is a bounded session, not an installed service. Create .state/STOP to stop; blocking conditions require review, not retries. Never expose browser debugging publicly.

## Tests
npm test performs no X requests and needs no account.

## Limits and evidence
Only bounded first-page collection has historical acceptance. A past run had three successful scheduled cycles before stopping at its configured traffic cap. Sustained 24-hour service and complete pagination were not accepted. X may change its internal response shape. No realtime SLA, full historical coverage or anti-bot bypass is claimed. Runtime payloads are private and must never be committed.

See LICENSE for source inspection rights.

## CI configuration
The GitHub Actions example is included at ci/github-actions.yml as a reference, not an active workflow. Run the documented verification commands locally. To enable Actions in your own repository, place this file under .github/workflows/.
