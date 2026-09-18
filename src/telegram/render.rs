use anyhow::{bail, Result};
use rusqlite::Connection;
use serde_json::{json, Value};
use std::path::Path;

use crate::codex::get_away_mode;
use crate::config::{DaemonConfig, RegisteredProject};
use crate::live::live_backend_status;
use crate::load_daemon_config;
use crate::projects::derive_project_label;
use crate::state::{
    derive_thread_display_name, list_waiting_from_db, pending_outbound_count, BridgeThreadSnapshot,
    ObservedWorkspace, PendingPrompt, TelegramCallbackAction, TelegramCallbackRoute,
};

const TELEGRAM_CONTINUE_THREAD_HINT: &str =
    "💬 To continue this thread, use Telegram's Reply action on this message.";
const TELEGRAM_ANSWER_THREAD_HINT: &str =
    "💬 To answer Codex, use Telegram's Reply action on this message.";
const TELEGRAM_APPROVAL_HINT: &str =
    "Choose one option below. The button is bound to this exact Codex request.";
const TELEGRAM_INFERRED_APPROVAL_HINT: &str =
    "Open the active Codex client to answer this approval.";
pub(crate) const TELEGRAM_MESSAGE_CHAR_LIMIT: usize = 4096;
const TELEGRAM_THREAD_SNAPSHOT_DETAIL_LIMIT: usize = 3000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PreparedTelegramDelivery {
    pub(crate) payloads: Vec<Value>,
    pub(crate) thread_id: Option<String>,
    pub(crate) event_id: String,
    pub(crate) callback_routes: Vec<TelegramCallbackRoute>,
}

fn telegram_event_title(event_type: &str, event: &Value) -> &'static str {
    if telegram_event_is_approval(event) {
        return "🔐 Codex needs approval";
    }
    match event_type {
        "thread_waiting" => "🟡 Codex needs you",
        "thread_completed" => "✅ Codex finished",
        "thread_status_changed" => "🔄 Codex changed",
        _ => "🧵 Codex update",
    }
}

fn telegram_event_reply_hint(event_type: &str, event: &Value) -> &'static str {
    if telegram_event_is_native_approval(event) {
        TELEGRAM_APPROVAL_HINT
    } else if telegram_event_is_approval(event) {
        TELEGRAM_INFERRED_APPROVAL_HINT
    } else {
        match event_type {
            "thread_waiting" => TELEGRAM_ANSWER_THREAD_HINT,
            _ => TELEGRAM_CONTINUE_THREAD_HINT,
        }
    }
}

fn telegram_event_display_name(event: &Value) -> String {
    event
        .pointer("/thread/displayName")
        .and_then(Value::as_str)
        .or_else(|| event.pointer("/thread/name").and_then(Value::as_str))
        .or_else(|| event.get("threadId").and_then(Value::as_str))
        .unwrap_or("Codex thread")
        .to_string()
}

fn telegram_event_detail(event: &Value) -> Option<String> {
    event
        .pointer("/approvalRequest/summary")
        .and_then(Value::as_str)
        .or_else(|| {
            event
                .pointer("/thread/pendingPrompt/question")
                .and_then(Value::as_str)
        })
        .or_else(|| event.pointer("/thread/lastPreview").and_then(Value::as_str))
        .or_else(|| event.get("lastPreview").and_then(Value::as_str))
        .filter(|value| !value.trim().is_empty())
        .map(sanitize_telegram_detail)
}

fn telegram_event_is_approval(event: &Value) -> bool {
    telegram_event_is_native_approval(event)
        || event.get("promptKind").and_then(Value::as_str) == Some("approval")
        || event
            .pointer("/thread/pendingPrompt/promptKind")
            .and_then(Value::as_str)
            == Some("approval")
        || event
            .pointer("/thread/pendingPrompt/kind")
            .and_then(Value::as_str)
            == Some("approval")
}

fn telegram_event_is_native_approval(event: &Value) -> bool {
    event
        .pointer("/approvalRequest/approvalKey")
        .and_then(Value::as_str)
        .is_some()
}

fn telegram_callback_id(event_id: &str, action: TelegramCallbackAction) -> String {
    let digest = crate::sha256_hex(format!("{event_id}:{}", action.as_str()).as_bytes());
    format!("cb_{}", &digest[..24])
}

fn split_telegram_text(text: &str, max_chars: usize) -> Vec<String> {
    assert!(
        max_chars > 0,
        "Telegram message chunk size must be non-zero"
    );

    let mut chunks = Vec::new();
    let mut chunk = String::new();
    let mut chunk_chars = 0;

    for ch in text.chars() {
        if chunk_chars == max_chars {
            chunks.push(std::mem::take(&mut chunk));
            chunk_chars = 0;
        }
        chunk.push(ch);
        chunk_chars += 1;
    }

    if !chunk.is_empty() {
        chunks.push(chunk);
    }

    if chunks.is_empty() {
        chunks.push(String::new());
    }

    chunks
}

fn sanitize_telegram_detail(detail: &str) -> String {
    let mut sanitized = String::with_capacity(detail.len());
    let mut rest = detail;
    while let Some(start) = rest.find('[') {
        let (before, candidate_start) = rest.split_at(start);
        sanitized.push_str(before);
        let Some(end_offset) = candidate_start.find(']') else {
            sanitized.push_str(candidate_start);
            return sanitized;
        };
        let candidate = &candidate_start[1..end_offset];
        if let Some(replacement) = compact_telegram_file_reference(candidate) {
            sanitized.push_str(&replacement);
            rest = &candidate_start[end_offset + 1..];
        } else {
            sanitized.push('[');
            rest = &candidate_start[1..];
        }
    }
    sanitized.push_str(rest);
    sanitized
}

fn compact_telegram_file_reference(candidate: &str) -> Option<String> {
    let normalized = candidate
        .strip_prefix("F:")
        .or_else(|| candidate.strip_prefix("f:"))
        .unwrap_or(candidate);
    if !normalized.starts_with('/') {
        return None;
    }
    let (path, line_ref) = normalized.split_once('†').unwrap_or((normalized, ""));
    let file_name = Path::new(path).file_name()?.to_string_lossy();
    let line_ref = line_ref.trim();
    if line_ref.is_empty() {
        Some(file_name.into_owned())
    } else {
        Some(format!("{file_name} {line_ref}"))
    }
}

fn telegram_question_callback_id(
    event_id: &str,
    action: TelegramCallbackAction,
    option_index: usize,
) -> String {
    let digest =
        crate::sha256_hex(format!("{event_id}:{}:{option_index}", action.as_str()).as_bytes());
    format!("cb_{}", &digest[..24])
}

/// Solid colour dots that tie each option button to its line in the message body.
const OPTION_DOTS: [&str; 8] = ["🔴", "🟠", "🟡", "🟢", "🔵", "🟣", "🟤", "⚫"];

/// Cuts `value` to at most `max_chars` characters, marking a cut with a trailing "…".
pub(crate) fn truncate_to_char_limit(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_string();
    }
    let mut truncated = value
        .chars()
        .take(max_chars.saturating_sub(1))
        .collect::<String>();
    if max_chars > 0 {
        truncated.push('…');
    }
    truncated
}

fn option_letter(index: usize) -> String {
    char::from_u32('A' as u32 + index as u32)
        .filter(char::is_ascii_uppercase)
        .map_or_else(|| (index + 1).to_string(), String::from)
}

fn option_marker(index: usize) -> String {
    format!(
        "{}{}",
        OPTION_DOTS[index % OPTION_DOTS.len()],
        option_letter(index)
    )
}

/// Telegram caps inline button text at 64 UTF-16 code units (most emoji take two).
const TELEGRAM_BUTTON_TEXT_UTF16_LIMIT: usize = 64;

fn option_button_text(position: usize, label: &str) -> String {
    let text = format!(
        "{} {}",
        option_marker(position),
        label.split_whitespace().collect::<Vec<_>>().join(" ")
    );
    if text.encode_utf16().count() <= TELEGRAM_BUTTON_TEXT_UTF16_LIMIT {
        return text;
    }
    // Leave one unit for the "…" that marks the cut.
    let mut units = 0;
    let mut truncated = text
        .chars()
        .take_while(|ch| {
            units += ch.len_utf16();
            units < TELEGRAM_BUTTON_TEXT_UTF16_LIMIT
        })
        .collect::<String>();
    truncated.push('…');
    truncated
}

/// A question from `item/tool/requestUserInput`: option buttons plus Skip, and a Reply for
/// free-form answers when Codex allows one.
fn prepare_question_delivery(
    chat_id: &str,
    event: &Value,
    question: &Value,
    event_id: String,
    thread_id: Option<String>,
) -> PreparedTelegramDelivery {
    let text = |field: &str| {
        question
            .get(field)
            .and_then(Value::as_str)
            .map(str::trim)
            .unwrap_or_default()
    };
    let index = question.get("index").and_then(Value::as_u64).unwrap_or(1);
    let total = question.get("total").and_then(Value::as_u64).unwrap_or(1);
    let is_secret = question
        .get("isSecret")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let options = question
        .get("options")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default()
        .iter()
        .filter_map(|option| {
            let label = option.get("label").and_then(Value::as_str)?.trim();
            let description = option
                .get("description")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .trim();
            Some((label.to_string(), description.to_string()))
        })
        .collect::<Vec<_>>();
    let accepts_free_text = question
        .get("isOther")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        || options.is_empty();

    let title = if total > 1 {
        format!("❓ Codex has a question ({index}/{total})")
    } else {
        "❓ Codex has a question".to_string()
    };
    let mut head = vec![title, format!("🧵 {}", telegram_event_display_name(event))];
    if let Some(project) = event.pointer("/thread/project").and_then(Value::as_str) {
        head.push(format!("📁 {project}"));
    }
    head.push(String::new());
    if !text("header").is_empty() {
        head.push(format!("【{}】", text("header")));
    }
    let described = options
        .iter()
        .any(|(label, description)| !description.is_empty() && description != label);
    let mut option_lines = Vec::new();
    if described {
        option_lines.push(String::new());
        for (position, (label, description)) in options.iter().enumerate() {
            if description.is_empty() || description == label {
                option_lines.push(format!("{} {label}", option_marker(position)));
            } else {
                option_lines.push(format!(
                    "{} {label} — {description}",
                    option_marker(position)
                ));
            }
        }
    }
    let mut lines = vec![String::new()];
    lines.push(
        match (options.is_empty(), accepts_free_text) {
            (true, _) => "💬 Use Telegram's Reply on this message to type your answer.",
            (false, true) => {
                "Tap an option, or use Telegram's Reply on this message to type your own answer."
            }
            (false, false) => "Tap one option below.",
        }
        .to_string(),
    );
    if is_secret {
        lines.push(
            "🔒 Codex marked this answer as secret: your reply is deleted from this chat once Codex has it."
                .to_string(),
        );
    }
    if total > 1 {
        lines.push(format!(
            "Codex gets the answers once all {total} questions are answered or skipped."
        ));
    }
    let tail = lines;

    // A question is always one message: its buttons and Reply routing are bound to a single
    // message id, and a split delivery that fails half way could strand that binding.
    // Option descriptions go first (the buttons still show the labels), then the question
    // text is cut to fit.
    let compose = |question_text: &str, option_lines: &[String]| {
        let mut all = head.clone();
        all.push(question_text.to_string());
        all.extend(option_lines.iter().cloned());
        all.extend(tail.iter().cloned());
        all.join("\n")
    };
    let mut body = compose(text("question"), &option_lines);
    if body.chars().count() > TELEGRAM_MESSAGE_CHAR_LIMIT {
        let budget = TELEGRAM_MESSAGE_CHAR_LIMIT.saturating_sub(compose("", &[]).chars().count());
        body = compose(&truncate_to_char_limit(text("question"), budget), &[]);
    }
    let body = truncate_to_char_limit(&body, TELEGRAM_MESSAGE_CHAR_LIMIT);
    let mut payloads = vec![json!({
        "chat_id": chat_id,
        "text": body,
        "disable_web_page_preview": true
    })];

    let mut callback_routes = Vec::new();
    let question_key = question.get("questionKey").and_then(Value::as_str);
    let question_id = question.get("questionId").and_then(Value::as_str);
    if let (Some(question_key), Some(question_id), Some(thread_id)) =
        (question_key, question_id, thread_id.as_ref())
    {
        let mut keyboard = Vec::new();
        let mut route = |action, option_index, answer: Option<&str>| {
            let callback_id = telegram_question_callback_id(&event_id, action, option_index);
            callback_routes.push(TelegramCallbackRoute {
                callback_id: callback_id.clone(),
                chat_id: chat_id.to_string(),
                message_id: None,
                thread_id: thread_id.clone(),
                action,
                approval_key: Some(question_key.to_string()),
                question_id: Some(question_id.to_string()),
                answer: answer.map(str::to_string),
            });
            format!("codex:{callback_id}")
        };
        for (position, (label, _)) in options.iter().enumerate() {
            let callback_data = route(
                TelegramCallbackAction::AnswerOption,
                position,
                Some(label.as_str()),
            );
            keyboard.push(json!([{
                "text": option_button_text(position, label),
                "callback_data": callback_data,
            }]));
        }
        let skip_data = route(TelegramCallbackAction::SkipQuestion, 0, None);
        keyboard.push(json!([{ "text": "⏭ Skip", "callback_data": skip_data }]));
        payloads[0]["reply_markup"] = json!({ "inline_keyboard": keyboard });
    }

    PreparedTelegramDelivery {
        payloads,
        thread_id,
        event_id,
        callback_routes,
    }
}

pub(crate) fn prepare_telegram_delivery(
    chat_id: &str,
    event: &Value,
) -> Result<PreparedTelegramDelivery> {
    let event_type = event
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("codex_event");
    let event_id = crate::notification_event_id(event);
    let thread_id = crate::event_thread_id(event);
    if let Some(question) = event.get("questionRequest") {
        return Ok(prepare_question_delivery(
            chat_id, event, question, event_id, thread_id,
        ));
    }
    let mut lines = vec![
        telegram_event_title(event_type, event).to_string(),
        format!("🧵 {}", telegram_event_display_name(event)),
    ];
    if let Some(project) = event.pointer("/thread/project").and_then(Value::as_str) {
        lines.push(format!("📁 {project}"));
    }
    if let Some(detail) = telegram_event_detail(event) {
        lines.push(String::new());
        lines.push(detail);
    }
    if thread_id.is_some() {
        lines.push(String::new());
        lines.push(telegram_event_reply_hint(event_type, event).to_string());
    }

    let mut payloads = split_telegram_text(&lines.join("\n"), TELEGRAM_MESSAGE_CHAR_LIMIT)
        .into_iter()
        .map(|text| {
            json!({
                "chat_id": chat_id,
                "text": text,
                "disable_web_page_preview": true
            })
        })
        .collect::<Vec<_>>();

    let mut callback_routes = Vec::new();
    if let Some(approval_key) = event
        .pointer("/approvalRequest/approvalKey")
        .and_then(Value::as_str)
    {
        if let Some(thread_id) = thread_id.as_ref() {
            let approve_id = telegram_callback_id(&event_id, TelegramCallbackAction::Approve);
            let approve_session_id =
                telegram_callback_id(&event_id, TelegramCallbackAction::ApproveForSession);
            let deny_id = telegram_callback_id(&event_id, TelegramCallbackAction::Deny);
            payloads[0]["reply_markup"] = json!({
                "inline_keyboard": [
                    [
                        { "text": "✅ Allow once", "callback_data": format!("codex:{approve_id}") },
                        { "text": "🔓 Allow session", "callback_data": format!("codex:{approve_session_id}") }
                    ],
                    [
                        { "text": "🛑 Deny", "callback_data": format!("codex:{deny_id}") }
                    ]
                ]
            });
            callback_routes.push(TelegramCallbackRoute {
                callback_id: approve_id,
                chat_id: chat_id.to_string(),
                message_id: None,
                thread_id: thread_id.clone(),
                action: TelegramCallbackAction::Approve,
                approval_key: Some(approval_key.to_string()),
                question_id: None,
                answer: None,
            });
            callback_routes.push(TelegramCallbackRoute {
                callback_id: approve_session_id,
                chat_id: chat_id.to_string(),
                message_id: None,
                thread_id: thread_id.clone(),
                action: TelegramCallbackAction::ApproveForSession,
                approval_key: Some(approval_key.to_string()),
                question_id: None,
                answer: None,
            });
            callback_routes.push(TelegramCallbackRoute {
                callback_id: deny_id,
                chat_id: chat_id.to_string(),
                message_id: None,
                thread_id: thread_id.clone(),
                action: TelegramCallbackAction::Deny,
                approval_key: Some(approval_key.to_string()),
                question_id: None,
                answer: None,
            });
        }
    }

    Ok(PreparedTelegramDelivery {
        payloads,
        thread_id,
        event_id,
        callback_routes,
    })
}

fn trim_for_telegram_detail(value: &str, max_chars: usize) -> Option<String> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    if value.chars().count() <= max_chars {
        return Some(value.to_string());
    }
    let take = max_chars.saturating_sub(3);
    Some(format!(
        "{}...",
        value.chars().take(take).collect::<String>().trim_end()
    ))
}

fn thread_snapshot_event_type(snapshot: &BridgeThreadSnapshot) -> &'static str {
    if snapshot.pending_prompt.is_some() {
        "thread_waiting"
    } else if snapshot.last_turn_status.as_deref() == Some("completed") {
        "thread_completed"
    } else {
        "thread_status_changed"
    }
}

fn pending_prompt_value(prompt: &PendingPrompt) -> Value {
    json!({
        "promptId": prompt.prompt_id,
        "promptKind": prompt.kind,
        "promptStatus": prompt.status,
        "kind": prompt.kind,
        "status": prompt.status,
        "question": prompt
            .question
            .as_deref()
            .and_then(|question| trim_for_telegram_detail(
                question,
                TELEGRAM_THREAD_SNAPSHOT_DETAIL_LIMIT
            ))
    })
}

fn thread_snapshot_event(snapshot: &BridgeThreadSnapshot) -> Value {
    let project = derive_project_label(snapshot.cwd.as_deref());
    let display_name = derive_thread_display_name(
        snapshot.name.as_deref(),
        project.as_deref(),
        snapshot
            .pending_prompt
            .as_ref()
            .and_then(|prompt| prompt.question.as_deref()),
        &snapshot.thread_id,
    );
    let display_name = trim_for_telegram_line(&display_name, 160);
    let last_preview = snapshot.last_preview.as_deref().and_then(|preview| {
        trim_for_telegram_detail(preview, TELEGRAM_THREAD_SNAPSHOT_DETAIL_LIMIT)
    });

    json!({
        "type": thread_snapshot_event_type(snapshot),
        "threadId": snapshot.thread_id,
        "updatedAt": snapshot.updated_at,
        "lastPreview": last_preview,
        "thread": {
            "threadId": snapshot.thread_id,
            "name": snapshot.name,
            "displayName": display_name,
            "project": project,
            "cwd": snapshot.cwd,
            "updatedAt": snapshot.updated_at,
            "statusType": snapshot.status_type,
            "statusFlags": snapshot.status_flags,
            "lastTurnStatus": snapshot.last_turn_status,
            "lastPreview": last_preview,
            "pendingPrompt": snapshot.pending_prompt.as_ref().map(pending_prompt_value)
        }
    })
}

pub(crate) fn prepare_telegram_thread_snapshot_delivery(
    chat_id: &str,
    snapshot: &BridgeThreadSnapshot,
) -> Result<PreparedTelegramDelivery> {
    let prepared = prepare_telegram_delivery(chat_id, &thread_snapshot_event(snapshot))?;
    if prepared.payloads.len() != 1 {
        bail!("thread snapshot Telegram delivery exceeded one message");
    }
    Ok(prepared)
}

pub(crate) fn telegram_projects_text(
    config: &DaemonConfig,
    current_project: Option<&RegisteredProject>,
    observed: &[ObservedWorkspace],
) -> String {
    let mut lines = vec!["Projects".to_string(), String::new()];
    match current_project {
        Some(project) => lines.push(format!("Current: {} ({})", project.id, project.label)),
        None => lines.push("Current: none selected".to_string()),
    }
    if config.projects.is_empty() {
        lines.push(String::new());
        lines.push("No projects are configured yet.".to_string());
    } else {
        lines.push(String::new());
        lines.push("Configured:".to_string());
        for project in &config.projects {
            let current = current_project
                .map(|current| current.id == project.id)
                .unwrap_or(false);
            let marker = if current { "•" } else { "-" };
            lines.push(format!(
                "{marker} {} - {}",
                project.id,
                trim_for_telegram_line(&project.label, 80)
            ));
            lines.push(format!("  {}", project.cwd));
        }
    }
    if !observed.is_empty() {
        lines.push(String::new());
        lines.push("Observed from recent Codex history:".to_string());
        for workspace in observed.iter().take(5) {
            lines.push(format!(
                "- {} - {}",
                workspace.label,
                trim_for_telegram_line(&workspace.cwd, 90)
            ));
        }
        lines.push(
            "Run `codex-telegram-bridge projects import` locally to promote observed workspaces into the curated registry."
                .to_string(),
        );
    }
    lines.push(String::new());
    lines.push("Use /project <id> to switch the current project.".to_string());
    lines.join("\n")
}

pub(crate) fn telegram_project_text(project: Option<&RegisteredProject>) -> String {
    match project {
        Some(project) => format!(
            "Current project\n\n{} ({})\n{}\n\nNew Telegram threads will start here until you switch again.",
            project.id, project.label, project.cwd
        ),
        None => "No current project is selected.\n\nUse /project to inspect the registry, then /project <id> to choose one."
            .to_string(),
    }
}

pub(crate) fn telegram_help_text() -> String {
    [
        "Codex remote is ready.",
        "",
        "Use Telegram's Reply action on a Codex notification to continue that exact thread.",
        "",
        "/away - start remote Codex mode",
        "/back - stop remote Codex mode",
        "/repair - fix remote Codex mode",
        "/status - show remote status",
        "/threads - show the 5 most recent Codex threads",
        "/threads <count> - show that many recent Codex threads",
        "/new <prompt> - start a new Codex thread",
        "/new - ask for a prompt in a reply",
        "/project - list projects",
        "/project <id> - switch the current project",
    ]
    .join("\n")
}

fn telegram_live_backend_status_line() -> String {
    let config = match load_daemon_config() {
        Ok(config) => config,
        Err(error) => {
            return format!(
                "Shared live backend: config unavailable ({error:#}). Run setup locally."
            );
        }
    };
    let Some(codex) = config.codex.as_ref() else {
        return "Shared live backend: not configured. Run setup locally.".to_string();
    };

    match live_backend_status(codex) {
        Ok(status) => {
            let health = if status.healthy {
                "healthy"
            } else {
                "unhealthy"
            };
            let pid = status
                .pid
                .map(|pid| pid.to_string())
                .unwrap_or_else(|| "none".to_string());
            let recovery = if status.healthy {
                "Use /away before you leave."
            } else {
                "Use /repair to recover."
            };
            format!(
                "Shared live backend: {health}, {}, pid {pid}. {recovery}",
                status.websocket_url
            )
        }
        Err(error) => format!(
            "Shared live backend: unhealthy, {} ({error:#}). Use /repair to recover.",
            codex.websocket_url
        ),
    }
}

fn trim_for_telegram_line(value: &str, max_chars: usize) -> String {
    let mut trimmed = value.trim().replace('\n', " ");
    if trimmed.chars().count() <= max_chars {
        return trimmed;
    }
    trimmed = trimmed.chars().take(max_chars.saturating_sub(1)).collect();
    trimmed.push_str("...");
    trimmed
}

pub(crate) fn telegram_status_text(conn: &Connection) -> Result<String> {
    let away = get_away_mode(conn)?;
    let pending = pending_outbound_count(conn)?;
    let waiting = list_waiting_from_db(conn, None, 5)?;
    let away_label = if away["away"].as_bool() == Some(true) {
        "on"
    } else {
        "off"
    };
    Ok(format!(
        "Codex remote status\n\nRemote mode: {away_label}\n{}\nPending Telegram notifications: {pending}\nThreads waiting for you: {}\n\nUse /away before you leave. Use /repair if the backend is unhealthy. Use /back when you return.",
        telegram_live_backend_status_line(),
        waiting.summary.count
    ))
}

pub(crate) fn telegram_new_thread_confirmation_text(
    project: &RegisteredProject,
    result: &Value,
) -> Result<String> {
    let cwd = result.get("cwd").and_then(Value::as_str);
    Ok(match cwd {
        Some(cwd) if !cwd.trim().is_empty() => format!(
            "Started a new Codex thread in {}.\n{cwd}\n\nCodex is working on the first answer now. I will send the answer here when it finishes.\n\nUse Telegram's Reply action on this message to continue it.",
            project.label
        ),
        _ => format!(
            "Started a new Codex thread in {} with no explicit working directory reported back.\n\nCodex is working on the first answer now. I will send the answer here when it finishes.\n\nUse Telegram's Reply action on this message to continue it.",
            project.label
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn telegram_message_payload_contains_reply_buttons_for_approval_events() {
        let event = json!({
            "type": "thread_waiting",
            "threadId": "thr_approval",
            "updatedAt": 42,
            "approvalRequest": {
                "approvalKey": "approval_exact_request",
                "summary": "Deploy to production?"
            },
            "thread": {
                "displayName": "Approve deploy",
                "project": "infra",
                "pendingPrompt": {
                    "promptKind": "approval",
                    "question": "Deploy to production?"
                },
                "lastPreview": "Need approval"
            }
        });

        let prepared = prepare_telegram_delivery("999", &event).expect("prepared telegram event");

        assert_eq!(prepared.thread_id.as_deref(), Some("thr_approval"));
        assert_eq!(prepared.payloads.len(), 1);
        assert_eq!(prepared.callback_routes.len(), 3);
        let reply_markup = prepared.payloads[0]["reply_markup"]["inline_keyboard"]
            .as_array()
            .expect("inline keyboard");
        assert_eq!(reply_markup.len(), 2);
        let allow_buttons = reply_markup[0].as_array().expect("allow buttons");
        let deny_buttons = reply_markup[1].as_array().expect("deny buttons");
        assert_eq!(allow_buttons.len(), 2);
        assert_eq!(deny_buttons.len(), 1);
        assert_eq!(allow_buttons[0]["text"], "✅ Allow once");
        assert_eq!(allow_buttons[1]["text"], "🔓 Allow session");
        assert_eq!(deny_buttons[0]["text"], "🛑 Deny");
        assert_eq!(
            prepared.callback_routes[0].action,
            TelegramCallbackAction::Approve
        );
        assert_eq!(
            prepared.callback_routes[1].action,
            TelegramCallbackAction::ApproveForSession
        );
        assert!(prepared
            .callback_routes
            .iter()
            .all(|route| route.approval_key.as_deref() == Some("approval_exact_request")));
        for button in allow_buttons.iter().chain(deny_buttons) {
            let callback_data = button["callback_data"].as_str().expect("callback data");
            assert!(
                callback_data.len() <= 64,
                "Telegram callback_data must fit the Bot API limit"
            );
        }
    }

    #[test]
    fn telegram_message_payload_preserves_full_multiline_codex_body() {
        let full_message =
            "Done.\n\nFirst line stays.\nSecond line also stays.\n\nThird paragraph remains intact.";
        let event = json!({
            "type": "thread_completed",
            "threadId": "thr_done",
            "updatedAt": 42,
            "thread": {
                "displayName": "LinkedIn Network",
                "project": "growth",
                "lastPreview": full_message
            }
        });

        let prepared = prepare_telegram_delivery("999", &event).expect("prepared telegram event");
        let text = prepared.payloads[0]["text"]
            .as_str()
            .expect("telegram text");
        assert!(text.contains(full_message));
        assert!(!text.contains("ID "));
        assert!(!text.contains("\n🤖 Codex\n"));
    }

    #[test]
    fn telegram_message_payload_uses_remote_codex_chrome_only() {
        let full_message = "Done.\n\n::inbox-item{title=\"vault commands checked\" summary=\"No local Claude commands; LifeOS has four\"}";
        let event = json!({
            "type": "thread_completed",
            "threadId": "thr_done",
            "updatedAt": 42,
            "thread": {
                "displayName": "Vault commands checked",
                "project": "ops",
                "lastPreview": full_message
            }
        });

        let prepared = prepare_telegram_delivery("999", &event).expect("prepared telegram event");
        let text = prepared.payloads[0]["text"]
            .as_str()
            .expect("telegram text");
        assert!(text.starts_with("✅ Codex finished\n🧵 Vault commands checked\n📁 ops"));
        assert!(!text.contains("🤖 Codex"));
        assert!(!text.contains("ID "));
        assert!(text.contains("::inbox-item"));
        assert!(text
            .contains("💬 To continue this thread, use Telegram's Reply action on this message."));
    }

    #[test]
    fn telegram_message_payload_shortens_app_file_reference_tokens() {
        let preview = "Updated the docs. [F:/Users/hanifcarroll/projects/ui-experiment/README.md†L1-L24] [F:/Users/hanifcarroll/projects/ui-experiment/docs/airbnb-design-implementation.md†L1-L19]";
        let event = json!({
            "type": "thread_completed",
            "threadId": "thr_docs",
            "updatedAt": 42,
            "thread": {
                "displayName": "Docs updated",
                "project": "ui-exp",
                "lastPreview": preview
            }
        });

        let prepared = prepare_telegram_delivery("999", &event).expect("prepared telegram event");
        let text = prepared.payloads[0]["text"]
            .as_str()
            .expect("telegram text");
        assert!(text.contains("README.md L1-L24"));
        assert!(text.contains("airbnb-design-implementation.md L1-L19"));
        assert!(!text.contains("[F:/Users/hanifcarroll/projects/ui-experiment/README.md"));
    }

    #[test]
    fn telegram_question_payload_has_option_buttons_skip_and_bound_routes() {
        let long_label =
            "A very long option label that keeps going well past what fits on a button";
        let event = json!({
            "type": "thread_waiting",
            "eventKey": "question_abc:color",
            "threadId": "thr_q",
            "promptKind": "question",
            "thread": { "displayName": "Pick a color", "project": "demo" },
            "questionRequest": {
                "questionKey": "question_abc",
                "questionId": "color",
                "index": 1,
                "total": 2,
                "header": "Color",
                "question": "Which color?",
                "isOther": true,
                "isSecret": false,
                "options": [
                    { "label": "Red (Recommended)", "description": "Warm" },
                    { "label": long_label, "description": long_label }
                ]
            }
        });

        let prepared = prepare_telegram_delivery("456", &event).expect("prepare question");

        let text = prepared.payloads[0]["text"].as_str().expect("text");
        assert!(text.starts_with("❓ Codex has a question (1/2)\n🧵 Pick a color\n📁 demo"));
        assert!(text.contains("【Color】\nWhich color?"));
        assert!(text.contains("🔴A Red (Recommended) — Warm"));
        assert!(text.contains(&format!("🟠B {long_label}\n")));
        assert!(text.contains("use Telegram's Reply on this message to type your own answer"));
        assert!(text.contains("once all 2 questions are answered or skipped"));

        let keyboard = prepared.payloads[0]["reply_markup"]["inline_keyboard"]
            .as_array()
            .expect("keyboard");
        let buttons = keyboard
            .iter()
            .map(|row| row[0]["text"].as_str().expect("button text"))
            .collect::<Vec<_>>();
        assert_eq!(buttons[0], "🔴A Red (Recommended)");
        assert!(buttons[1].starts_with("🟠B A very long") && buttons[1].ends_with('…'));
        for button in &buttons {
            assert!(
                button.encode_utf16().count() <= 64,
                "Telegram rejects button text over 64 UTF-16 units: {button}"
            );
        }
        assert_eq!(buttons[2], "⏭ Skip");
        for row in keyboard {
            let data = row[0]["callback_data"].as_str().expect("callback data");
            assert!(data.starts_with("codex:cb_"));
            assert!(
                data.len() <= 64,
                "callback_data must fit Telegram's 64 bytes"
            );
        }

        assert_eq!(prepared.callback_routes.len(), 3);
        let answers = prepared
            .callback_routes
            .iter()
            .map(|route| (route.action, route.answer.as_deref()))
            .collect::<Vec<_>>();
        assert_eq!(
            answers,
            vec![
                (
                    TelegramCallbackAction::AnswerOption,
                    Some("Red (Recommended)")
                ),
                (TelegramCallbackAction::AnswerOption, Some(long_label)),
                (TelegramCallbackAction::SkipQuestion, None),
            ]
        );
        for route in &prepared.callback_routes {
            assert_eq!(route.approval_key.as_deref(), Some("question_abc"));
            assert_eq!(route.question_id.as_deref(), Some("color"));
            assert_eq!(route.thread_id, "thr_q");
        }
        let unique = prepared
            .callback_routes
            .iter()
            .map(|route| route.callback_id.as_str())
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(unique.len(), 3, "every button needs its own callback id");
    }

    #[test]
    fn an_oversized_question_is_cut_to_one_message_that_keeps_its_buttons() {
        let long_question = "Why? ".repeat(2000);
        let long_description = "detail ".repeat(700);
        let event = json!({
            "type": "thread_waiting",
            "eventKey": "question_big:q",
            "threadId": "thr_q",
            "questionRequest": {
                "questionKey": "question_big",
                "questionId": "q",
                "index": 1,
                "total": 1,
                "header": "Big",
                "question": long_question,
                "isOther": true,
                "isSecret": false,
                "options": [
                    { "label": "Yes", "description": long_description },
                    { "label": "No", "description": long_description }
                ]
            }
        });

        let prepared = prepare_telegram_delivery("456", &event).expect("prepare question");

        assert_eq!(prepared.payloads.len(), 1, "a question is never split");
        let text = prepared.payloads[0]["text"].as_str().expect("text");
        assert!(text.chars().count() <= TELEGRAM_MESSAGE_CHAR_LIMIT);
        assert!(text.contains("【Big】\nWhy? Why?"));
        assert!(text.contains("…\n\nTap an option, or use Telegram's Reply"));
        assert!(
            !text.contains("detail detail"),
            "descriptions are dropped first"
        );
        let keyboard = prepared.payloads[0]["reply_markup"]["inline_keyboard"]
            .as_array()
            .expect("keyboard");
        assert_eq!(
            keyboard.len(),
            3,
            "both options and Skip keep their buttons"
        );
    }

    #[test]
    fn option_buttons_fit_telegrams_utf16_limit_even_with_emoji_labels() {
        let emoji_label = "🚀".repeat(40);
        let text = option_button_text(0, &emoji_label);
        assert!(text.starts_with("🔴A 🚀"));
        assert!(text.ends_with('…'));
        assert!(text.encode_utf16().count() <= TELEGRAM_BUTTON_TEXT_UTF16_LIMIT);
        assert_eq!(option_button_text(1, "Blue\nsky"), "🟠B Blue sky");
    }

    #[test]
    fn truncation_marks_the_cut_and_respects_the_limit() {
        assert_eq!(truncate_to_char_limit("short", 10), "short");
        assert_eq!(truncate_to_char_limit("abcdef", 4), "abc…");
        assert_eq!(truncate_to_char_limit("漢字漢字漢字", 3), "漢字…");
        assert_eq!(truncate_to_char_limit("abc", 0), "");
    }

    #[test]
    fn option_markers_pair_a_colour_dot_with_a_letter() {
        assert_eq!(option_marker(0), "🔴A");
        assert_eq!(option_marker(4), "🔵E");
        assert_eq!(option_marker(7), "⚫H");
        assert_eq!(
            option_marker(8),
            "🔴I",
            "colours repeat after eight options"
        );
    }

    #[test]
    fn telegram_free_text_and_secret_questions_say_how_to_answer() {
        let event = json!({
            "type": "thread_waiting",
            "eventKey": "question_abc:token",
            "threadId": "thr_q",
            "questionRequest": {
                "questionKey": "question_abc",
                "questionId": "token",
                "index": 1,
                "total": 1,
                "header": "",
                "question": "Paste the API token",
                "isOther": false,
                "isSecret": true,
                "options": null
            }
        });

        let prepared = prepare_telegram_delivery("456", &event).expect("prepare question");

        let text = prepared.payloads[0]["text"].as_str().expect("text");
        assert!(text.starts_with("❓ Codex has a question\n"));
        assert!(text.contains("Use Telegram's Reply on this message to type your answer."));
        assert!(text.contains("🔒 Codex marked this answer as secret"));
        assert!(!text.contains("questions are answered or skipped"));
        let keyboard = prepared.payloads[0]["reply_markup"]["inline_keyboard"]
            .as_array()
            .expect("keyboard");
        assert_eq!(keyboard.len(), 1, "a free-text question only offers Skip");
        assert_eq!(keyboard[0][0]["text"], "⏭ Skip");
    }

    #[test]
    fn telegram_approval_payload_uses_approval_title_and_button_footer() {
        let event = json!({
            "type": "thread_waiting",
            "threadId": "thr_approval",
            "updatedAt": 42,
            "approvalRequest": {
                "approvalKey": "approval_hotfix",
                "summary": "Ship the hotfix?"
            },
            "thread": {
                "displayName": "Deploy request",
                "project": "infra",
                "pendingPrompt": {
                    "kind": "approval",
                    "question": "Ship the hotfix?"
                }
            }
        });

        let prepared = prepare_telegram_delivery("999", &event).expect("prepared telegram event");
        let text = prepared.payloads[0]["text"]
            .as_str()
            .expect("telegram text");
        assert!(text.starts_with("🔐 Codex needs approval"));
        assert!(text.contains("Ship the hotfix?"));
        assert!(text
            .contains("Choose one option below. The button is bound to this exact Codex request."));
    }

    #[test]
    fn telegram_message_payload_splits_without_truncating_codex_body() {
        let long_preview = "x".repeat(10_000);
        let event = json!({
            "type": "thread_completed",
            "threadId": "thr_split",
            "updatedAt": 42,
            "thread": {
                "displayName": "Long answer",
                "project": "ops",
                "lastPreview": long_preview
            }
        });

        let prepared = prepare_telegram_delivery("999", &event).expect("prepared telegram event");
        assert!(
            prepared.payloads.len() > 1,
            "long telegram messages should split"
        );
        for payload in &prepared.payloads {
            let text = payload["text"].as_str().expect("telegram text");
            assert!(text.chars().count() <= TELEGRAM_MESSAGE_CHAR_LIMIT);
        }
        let combined = prepared
            .payloads
            .iter()
            .map(|payload| payload["text"].as_str().expect("telegram text"))
            .collect::<Vec<_>>()
            .join("");
        assert!(combined.contains("Long answer"));
        assert!(combined.contains("💬 To continue this thread"));
        assert!(combined.contains(&"x".repeat(5000)));
    }

    #[test]
    fn telegram_thread_snapshot_payload_uses_update_template_as_one_message() {
        let snapshot = crate::state::BridgeThreadSnapshot {
            thread_id: "thr_done".to_string(),
            name: Some("Release checklist".to_string()),
            cwd: Some("/Users/hanifcarroll/projects/tools/codex-telegram-bridge".to_string()),
            updated_at: Some(42),
            status_type: "notLoaded".to_string(),
            status_flags: Vec::new(),
            last_turn_status: Some("completed".to_string()),
            last_preview: Some("x".repeat(10_000)),
            pending_prompt: None,
        };

        let prepared = prepare_telegram_thread_snapshot_delivery("999", &snapshot)
            .expect("prepared thread snapshot");

        assert_eq!(prepared.thread_id.as_deref(), Some("thr_done"));
        assert_eq!(
            prepared.payloads.len(),
            1,
            "explicit /threads results must be one Telegram message per thread"
        );
        let text = prepared.payloads[0]["text"]
            .as_str()
            .expect("telegram text");
        assert!(text.starts_with("✅ Codex finished\n🧵 Release checklist"));
        assert!(text.contains("📁 codex-telegram-bridge"));
        assert!(text.contains("💬 To continue this thread"));
        assert!(
            text.chars().count() <= TELEGRAM_MESSAGE_CHAR_LIMIT,
            "snapshot messages must fit Telegram's single-message limit"
        );
    }

    #[test]
    fn telegram_new_thread_confirmation_reports_working_directory() {
        let message = telegram_new_thread_confirmation_text(
            &RegisteredProject {
                id: "ui-exp".to_string(),
                label: "UI Experiment".to_string(),
                cwd: "/Users/hanifcarroll/projects/ui-experiment".to_string(),
                aliases: Vec::new(),
            },
            &json!({
                "threadId": "thr_123",
                "cwd": "/Users/hanifcarroll/projects/ui-experiment"
            }),
        )
        .expect("confirmation text");

        assert!(message.contains("Started a new Codex thread in UI Experiment."));
        assert!(message.contains("/Users/hanifcarroll/projects/ui-experiment"));
        assert!(message.contains("Codex is working on the first answer now."));
        assert!(message.contains("I will send the answer here when it finishes."));
        assert!(message.contains("Use Telegram's Reply action on this message to continue it."));
    }

    #[test]
    fn telegram_help_text_uses_reduced_remote_command_set() {
        let text = telegram_help_text();
        for expected in [
            "/away - start remote Codex mode",
            "/back - stop remote Codex mode",
            "/repair - fix remote Codex mode",
            "/status - show remote status",
            "/threads - show the 5 most recent Codex threads",
            "/threads <count> - show that many recent Codex threads",
            "/new <prompt> - start a new Codex thread",
            "/project <id> - switch the current project",
        ] {
            assert!(
                text.contains(expected),
                "help text missing expected command: {expected}"
            );
        }
        for removed in [
            "/away_on",
            "/away_off",
            "/live_on",
            "/live_reset",
            "/new_thread",
            "/projects",
            "/inbox",
            "/waiting",
            "/recent",
            "/settings",
        ] {
            assert!(
                !text.contains(removed),
                "help text still contains removed command: {removed}"
            );
        }
    }
}
