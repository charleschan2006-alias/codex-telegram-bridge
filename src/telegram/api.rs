use anyhow::{anyhow, bail, Context, Result};
use serde_json::{json, Value};
use std::time::Duration;

use crate::config::TelegramConfig;

pub(crate) fn telegram_api_post(
    bot_token: &str,
    method: &str,
    payload: &Value,
    timeout: Duration,
) -> Result<Value> {
    let url = format!(
        "https://api.telegram.org/bot{}/{}",
        bot_token.trim(),
        method.trim()
    );
    telegram_api_post_url(&url, bot_token, method, payload, timeout)
}

fn telegram_api_post_url(
    url: &str,
    bot_token: &str,
    method: &str,
    payload: &Value,
    timeout: Duration,
) -> Result<Value> {
    let agent = ureq::Agent::config_builder()
        .timeout_global(Some(timeout))
        // Telegram explains a failed call in the JSON body of its 4xx response (for example
        // "Bad Request: message to delete not found"). Read that body instead of letting
        // ureq reduce the call to "http status: 400", so callers can act on the reason.
        .http_status_as_error(false)
        .build()
        .new_agent();
    let mut response = agent
        .post(url)
        .send_json(payload.clone())
        .map_err(|error| {
            anyhow!(
                "Telegram API {method} request failed: {}",
                crate::redact_secret_text(&error.to_string(), bot_token)
            )
        })?;
    let status = response.status();
    let value: Value = response
        .body_mut()
        .read_json()
        .with_context(|| format!("Telegram API {method} returned invalid JSON (HTTP {status})"))?;
    if value.get("ok").and_then(Value::as_bool) != Some(true) {
        bail!("Telegram API {method} returned error: {value}");
    }
    Ok(value)
}

pub(crate) fn telegram_delete_webhook(bot_token: &str, timeout: Duration) -> Result<Value> {
    telegram_api_post(
        bot_token,
        "deleteWebhook",
        &json!({ "drop_pending_updates": false }),
        timeout,
    )
}

pub(crate) fn telegram_get_updates(
    bot_token: &str,
    offset: Option<i64>,
    timeout_seconds: u64,
    timeout: Duration,
) -> Result<Value> {
    let mut body = serde_json::Map::new();
    if let Some(offset) = offset {
        body.insert("offset".to_string(), json!(offset));
    }
    body.insert("timeout".to_string(), json!(timeout_seconds));
    body.insert(
        "allowed_updates".to_string(),
        json!(["message", "callback_query"]),
    );
    telegram_api_post(bot_token, "getUpdates", &Value::Object(body), timeout)
}

pub(crate) fn telegram_send_message(
    telegram: &TelegramConfig,
    payload: &Value,
    timeout: Duration,
) -> Result<Value> {
    telegram_api_post(&telegram.bot_token, "sendMessage", payload, timeout)
}

pub(crate) fn telegram_send_text(
    telegram: &TelegramConfig,
    text: &str,
    timeout: Duration,
) -> Result<Value> {
    telegram_send_message(
        telegram,
        &json!({
            "chat_id": telegram.chat_id,
            "text": text,
            "disable_web_page_preview": true
        }),
        timeout,
    )
}

pub(crate) fn telegram_send_text_to_chat(
    telegram: &TelegramConfig,
    chat_id: &str,
    text: &str,
    timeout: Duration,
) -> Result<Value> {
    telegram_send_message(
        telegram,
        &json!({
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": true
        }),
        timeout,
    )
}

pub(crate) fn telegram_send_text_message_id(
    telegram: &TelegramConfig,
    text: &str,
    timeout: Duration,
) -> Result<i64> {
    telegram_send_text(telegram, text, timeout)?
        .pointer("/result/message_id")
        .and_then(Value::as_i64)
        .context("Telegram sendMessage response missing result.message_id")
}

#[cfg(not(test))]
pub(crate) fn telegram_send_chat_action(
    telegram: &TelegramConfig,
    action: &str,
    timeout: Duration,
) -> Result<Value> {
    telegram_api_post(
        &telegram.bot_token,
        "sendChatAction",
        &json!({
            "chat_id": telegram.chat_id,
            "action": action
        }),
        timeout,
    )
}

#[cfg(test)]
pub(crate) fn telegram_send_chat_action(
    telegram: &TelegramConfig,
    action: &str,
    _timeout: Duration,
) -> Result<Value> {
    Ok(json!({
        "ok": true,
        "result": true,
        "chat_id": telegram.chat_id,
        "action": action
    }))
}

pub(crate) fn telegram_bot_commands() -> Vec<Value> {
    vec![
        json!({ "command": "start", "description": "Pair and show remote control help" }),
        json!({ "command": "help", "description": "Show Telegram remote control commands" }),
        json!({ "command": "away", "description": "Start remote Codex mode" }),
        json!({ "command": "back", "description": "Stop remote Codex mode" }),
        json!({ "command": "repair", "description": "Fix remote Codex mode" }),
        json!({ "command": "status", "description": "Show remote Codex status" }),
        json!({ "command": "threads", "description": "Show recent Codex threads" }),
        json!({ "command": "new", "description": "Start a new Codex thread" }),
        json!({ "command": "project", "description": "Show or switch the current project" }),
    ]
}

pub(crate) fn telegram_set_my_commands(
    telegram: &TelegramConfig,
    timeout: Duration,
) -> Result<Value> {
    telegram_api_post(
        &telegram.bot_token,
        "setMyCommands",
        &json!({ "commands": telegram_bot_commands() }),
        timeout,
    )
}

pub(crate) fn telegram_answer_callback_query(
    telegram: &TelegramConfig,
    callback_query_id: &str,
    text: &str,
    timeout: Duration,
) -> Result<Value> {
    telegram_api_post(
        &telegram.bot_token,
        "answerCallbackQuery",
        &json!({
            "callback_query_id": callback_query_id,
            "text": text,
            "show_alert": false
        }),
        timeout,
    )
}

/// Replaces a bot message's text. Omitting `reply_markup` also removes its inline keyboard.
pub(crate) fn telegram_edit_message_text(
    telegram: &TelegramConfig,
    message_id: i64,
    text: &str,
    timeout: Duration,
) -> Result<Value> {
    telegram_api_post(
        &telegram.bot_token,
        "editMessageText",
        &json!({
            "chat_id": telegram.chat_id,
            "message_id": message_id,
            "text": text,
            "disable_web_page_preview": true
        }),
        timeout,
    )
}

pub(crate) fn telegram_remove_inline_keyboard(
    telegram: &TelegramConfig,
    message_id: i64,
    timeout: Duration,
) -> Result<Value> {
    telegram_api_post(
        &telegram.bot_token,
        "editMessageReplyMarkup",
        &json!({
            "chat_id": telegram.chat_id,
            "message_id": message_id,
            "reply_markup": { "inline_keyboard": [] }
        }),
        timeout,
    )
}

pub(crate) fn telegram_delete_message(
    telegram: &TelegramConfig,
    chat_id: &str,
    message_id: i64,
    timeout: Duration,
) -> Result<Value> {
    telegram_api_post(
        &telegram.bot_token,
        "deleteMessage",
        &json!({
            "chat_id": chat_id,
            "message_id": message_id
        }),
        timeout,
    )
}

pub(crate) fn telegram_updates_array(updates: &Value) -> Result<&[Value]> {
    updates
        .get("result")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .context("Telegram getUpdates response did not contain result array")
}

pub(crate) fn telegram_chat_id(message: &Value) -> Option<String> {
    message
        .get("chat")
        .and_then(|chat| chat.get("id"))
        .and_then(|value| {
            value
                .as_i64()
                .map(|id| id.to_string())
                .or_else(|| value.as_str().map(str::to_string))
        })
}

pub(crate) fn telegram_from_user_id(message: &Value) -> Option<String> {
    message
        .get("from")
        .and_then(|from| from.get("id"))
        .and_then(|value| {
            value
                .as_i64()
                .map(|id| id.to_string())
                .or_else(|| value.as_str().map(str::to_string))
        })
}

pub(crate) fn telegram_message_id(message: &Value) -> Option<i64> {
    message.get("message_id").and_then(Value::as_i64)
}

pub(crate) fn telegram_bot_id(bot_token: &str) -> String {
    crate::sha256_hex(bot_token.as_bytes())[..16].to_string()
}

/// The bot's own numeric id, the part of the token before the colon. Unlike
/// `telegram_bot_id`, it survives a token rotation for the same bot.
pub(crate) fn telegram_stable_bot_id(bot_token: &str) -> String {
    bot_token
        .trim()
        .split_once(':')
        .map(|(id, _)| id)
        .filter(|id| !id.is_empty() && id.bytes().all(|byte| byte.is_ascii_digit()))
        .map_or_else(|| telegram_bot_id(bot_token), str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

    /// Serves one HTTP request with Telegram's usual 400 reply for a missing message.
    fn spawn_telegram_error_server() -> (String, std::thread::JoinHandle<()>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind fake telegram");
        let url = format!(
            "http://{}/bot123:secret/deleteMessage",
            listener.local_addr().unwrap()
        );
        let join = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept");
            let mut request = Vec::new();
            let mut buffer = [0u8; 4096];
            loop {
                let read = stream.read(&mut buffer).expect("read request");
                request.extend_from_slice(&buffer[..read]);
                let text = String::from_utf8_lossy(&request).to_string();
                if let Some(head_end) = text.find("\r\n\r\n") {
                    let length = text[..head_end]
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().ok())
                                .flatten()
                        })
                        .unwrap_or(0);
                    if request.len() >= head_end + 4 + length || read == 0 {
                        break;
                    }
                }
            }
            let body = r#"{"ok":false,"error_code":400,"description":"Bad Request: message to delete not found"}"#;
            write!(
                stream,
                "HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .expect("write response");
        });
        (url, join)
    }

    #[test]
    fn telegram_errors_keep_the_description_from_the_4xx_body() {
        let (url, server) = spawn_telegram_error_server();

        let error = telegram_api_post_url(
            &url,
            "123:secret",
            "deleteMessage",
            &json!({ "chat_id": "456", "message_id": 1 }),
            Duration::from_secs(5),
        )
        .expect_err("a 400 reply is an error");

        let message = format!("{error:#}");
        assert!(
            message.contains("message to delete not found"),
            "the deletion retry logic keys on Telegram's description: {message}"
        );
        assert!(
            !message.contains("secret"),
            "the token must not leak: {message}"
        );
        server.join().expect("fake telegram");
    }

    #[test]
    fn stable_bot_id_survives_token_rotation() {
        assert_eq!(
            telegram_stable_bot_id("8854269525:AAHfirst-secret"),
            "8854269525"
        );
        assert_eq!(
            telegram_stable_bot_id(" 8854269525:AAHrotated-secret "),
            telegram_stable_bot_id("8854269525:AAHfirst-secret")
        );
        assert_ne!(
            telegram_stable_bot_id("111:secret"),
            telegram_stable_bot_id("222:secret")
        );
        assert_eq!(
            telegram_stable_bot_id("not-a-token"),
            telegram_bot_id("not-a-token"),
            "an unexpected token shape falls back to the hashed id"
        );
    }

    #[test]
    fn telegram_bot_commands_are_registered_for_core_remote_actions() {
        let commands = telegram_bot_commands();
        let names = commands
            .iter()
            .filter_map(|command| command.get("command").and_then(Value::as_str))
            .collect::<Vec<_>>();

        assert_eq!(
            names,
            vec!["start", "help", "away", "back", "repair", "status", "threads", "new", "project",]
        );
        for removed in [
            "away_on",
            "away_off",
            "live_on",
            "live_reset",
            "new_thread",
            "projects",
            "inbox",
            "waiting",
            "recent",
            "settings",
        ] {
            assert!(
                !names.contains(&removed),
                "removed telegram bot command is still advertised: {removed}"
            );
        }
        for required in [
            "start", "help", "away", "back", "repair", "status", "threads", "new", "project",
        ] {
            assert!(
                names.contains(&required),
                "missing required telegram bot command {required}"
            );
        }
        for command in commands {
            assert!(
                command["command"].as_str().expect("command").len() <= 32,
                "Telegram command names must fit BotCommand limits"
            );
            assert!(
                !command["description"]
                    .as_str()
                    .expect("description")
                    .trim()
                    .is_empty(),
                "Telegram commands must include human-readable descriptions"
            );
        }
    }
}
