# Verification
Verified on 2026-11-18, Node.js 22+.
- npm test: all 3 offline suites passed (36 policy, 10 retention and 15 lifecycle checks).
- Synthetic inputs only: no browser session, cookies or real X requests used for these tests.
- README documents historical first-page acceptance separately. Complete pagination and sustained unattended service were not accepted.

## CI configuration
The GitHub Actions example is included at ci/github-actions.yml as a reference, not an active workflow. Run the documented verification commands locally. To enable Actions in your own repository, place this file under .github/workflows/.
