# Issue Tracker

This repo tracks executable software work in GitHub Issues.

| Field | Value |
| --- | --- |
| Tracker | GitHub Issues |
| Remote | `git@github.com:charleschan2006-alias/codex-telegram-bridge.git` |
| CLI | `gh` |

When a Matt Pocock skill says to publish to the issue tracker, use GitHub Issues for this repo.

Common commands:

```sh
gh issue list --state open
gh issue view <number> --comments
gh issue create --title "<title>" --body-file <file>
gh issue edit <number> --add-label "ready-for-agent"
gh issue comment <number> --body-file <file>
```

Use local markdown only for scratch thinking before promotion, not as the canonical tracker for repo-bound work.
