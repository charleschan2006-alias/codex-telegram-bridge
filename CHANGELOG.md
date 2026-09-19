# Changelog

All notable changes to `codex-telegram-bridge` will be documented here.

## Unreleased

- When a Telegram reply cannot reach a thread because another Codex session that is not connected to the bridge holds it (`thread … already has an active writer`), explain that and suggest the `codex resume <id> --remote <url>` command instead of forwarding the raw JSON-RPC error.

## 0.2.0 - 2026-09-18

This fork is now the maintained version: both [HanifCarroll/codex-telegram-bridge](https://github.com/HanifCarroll/codex-telegram-bridge) and [zhang0098/codex-telegram-bridge](https://github.com/zhang0098/codex-telegram-bridge) are archived. Repository and install URLs point at [charleschan2006-alias/codex-telegram-bridge](https://github.com/charleschan2006-alias/codex-telegram-bridge).

- Add `setup --codex-home` and `telegram setup --codex-home`; setup stores `~/.codex` when omitted, and the daemon passes the saved `CODEX_HOME` to its managed Codex App Server.
- Keep a persistent App Server subscription to active threads and bridge native command, file-change, and permissions approval requests to Telegram with exact request-bound `Allow once`, `Allow session`, and `Deny` buttons.
- Bridge Codex questions (`item/tool/requestUserInput`, asked in Plan mode) to Telegram: one message per question with option buttons and `Skip`, Telegram Reply for free-form answers when Codex allows them, and all answers sent back in a single App Server response once every question is answered or skipped. Answered questions show the choice and lose their buttons; secret answers are deleted from the chat and kept out of local logs.
- Mark question option buttons with colour dots and letters (🔴A, 🟠B, …) that match the option list in the message. Keep every question to a single message and every button within Telegram's text limit.
- Fix `/away` hanging the daemon forever when it had to start the shared backend without `screen`: the launcher's long-lived children no longer inherit the pipe the daemon reads to EOF.
- Report Telegram API failures with Telegram's own error description instead of a bare `http status: 400`.
- Keep `cargo test` from stopping a locally installed daemon or touching the real `~/.codex-telegram-bridge`: environment-changing tests share one lock, and test builds never resolve the real state directory.
- Redact service-manager stdout and stderr from `daemon status` so inherited environment variables and credentials are never returned in its JSON output.

## 0.1.1 - 2026-08-08

- Remove the Discord transport entirely: the `discord` CLI surface, DiscordConfig, daemon polling/delivery paths, and `docs/discord.md` are gone.
- Remove the `/discord_on`, `/discord_off`, `/telegram_on`, and `/telegram_off` chat commands.
- Remove `telegram enable`/`telegram disable` and the `TelegramConfig.enabled` flag: Telegram is always treated as enabled once configured, so the daemon never silently stops polling the channel.
- Drop `telegramEnabled`/`discordEnabled` from `doctor`, `remote status`, `telegram status`, and the macOS menu bar app.
- Add a Chinese translation of the README with a language switcher on the repository homepage.
- Point install, release, and crate metadata URLs at the maintained repository.

## 0.1.0 - 2026-04-15

Initial OSS release candidate.

- Inspect Codex threads with `threads`, `show`, `waiting`, `inbox`, and `sync`.
- Take thread actions with `new`, `fork`, `reply`, `approve`, `archive`, and `unarchive`.
- Stream normalized JSON events with `follow` and `watch`.
- Run trusted local hooks with `watch --exec`.
- Configure the full product path with `setup`.
- Gate outbound Telegram notifications with `away on/off` so users are not notified while present.
- Send proactive Codex notifications directly through Telegram with `telegram setup`, `telegram test`, and the local daemon.
- Route Telegram reply-to-message text and approval buttons back to the originating Codex thread.
- Expose a stdio MCP server for Hermes with structured Codex control tools.
- Expose MCP resources and prompts for Codex thread context and safer Hermes workflows.
- Add a `hermes install` helper that registers the bridge through `hermes mcp add`.
- Prune legacy away-summary and hidden MCP control paths so the daemon and documented MCP tools are the only notification/control lanes.
- Hide advanced local sync, event-stream, and maintenance commands from default CLI help while keeping them available for automation.
- Reframe MCP as an optional local agent adapter while keeping Telegram away-mode as the primary product flow.
- Keep hook examples generic and leave Telegram delivery to the bridge daemon.
