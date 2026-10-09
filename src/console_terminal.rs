//! Controller-side terminal pipe.
//!
//! The controlled peer stays a stock RustDesk host: this sends the same
//! `TerminalAction` messages the Flutter terminal already sends, and it
//! passes the desktop client's stored `access_token` as the rendezvous
//! punch token. A self-hosted rendezvous does not require that login.

use std::{
    io::{Read, Write},
    sync::{Arc, RwLock},
    thread,
};

use async_trait::async_trait;
use base::{
    config::keys,
    message_proto::{
        login_response, message, terminal_action, terminal_response, CloseTerminal, Hash, Message,
        OpenTerminal, PeerInfo, TerminalAction, TerminalData, TerminalResponse, TestDelay,
        WindowsSession,
    },
};
use hbb_common::{
    anyhow::anyhow,
    bail,
    config::{Config, LocalConfig, READ_TIMEOUT},
    log,
    protobuf::Message as _,
    rendezvous_proto::ConnType,
    timeout,
    tokio::{self, sync::mpsc},
    ResultType, Stream,
};

use crate::client::{self, Data, Interface, LoginConfigHandler};

const USAGE: &str = "rustdesk --exec <id> [--password <password>] [--relay] [--command <command>]";

/// Marker line the remote shell prints after a one-shot command. It is
/// ordinary PTY input, not a new protocol field.
const EXIT_MARK: &str = "__RD_EXIT_";

const TERMINAL_ID: i32 = 1;
const DEFAULT_ROWS: u32 = 24;
const DEFAULT_COLS: u32 = 80;

const PUBLIC_LOGIN_REQUIRED: &str = "This public server requires a login. Log in once with the desktop client on this machine, then retry. This command reuses that saved login and does not start a new one.";

pub struct ExecRequest {
    pub id: String,
    pub password: String,
    pub relay: bool,
    pub command: Option<String>,
}

pub fn parse_exec_args(args: &[String]) -> Result<ExecRequest, String> {
    if args.first().map(String::as_str) != Some("--exec") {
        return Err(USAGE.to_owned());
    }
    let mut id = None;
    let mut password = String::new();
    let mut relay = false;
    let mut command = None;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--password" => {
                i += 1;
                password = args
                    .get(i)
                    .cloned()
                    .ok_or_else(|| "--password requires a value".to_owned())?;
            }
            "--relay" => relay = true,
            "--command" => {
                i += 1;
                let cmd = args
                    .get(i)
                    .cloned()
                    .filter(|cmd| !cmd.is_empty())
                    .ok_or_else(|| "--command requires a value".to_owned())?;
                command = Some(cmd);
            }
            other if other.starts_with('-') => {
                return Err(format!("unknown option {other}"));
            }
            other => {
                if id.is_some() {
                    return Err(format!("unexpected argument {other}"));
                }
                id = Some(other.to_owned());
            }
        }
        i += 1;
    }
    let Some(id) = id.filter(|id| !id.is_empty()) else {
        return Err(USAGE.to_owned());
    };
    Ok(ExecRequest {
        id,
        password,
        relay,
        command,
    })
}

/// `id@server` selects that rendezvous, matching `LoginConfigHandler`.
/// An empty result is the public rendezvous.
pub fn effective_rendezvous(configured_server: &str, peer_id: &str) -> String {
    if let Some((_, rest)) = peer_id.split_once('@') {
        let server = rest.split('?').next().unwrap_or("").trim();
        if server.is_empty() || server == "public" {
            return String::new();
        }
        return server.to_owned();
    }
    configured_server.trim().to_owned()
}

pub fn rendezvous_requires_stored_login(server: &str) -> bool {
    let server = server.trim();
    server.is_empty() || crate::common::is_public(server)
}

pub fn stored_login_gate(server: &str, stored_token: &str) -> Result<(), &'static str> {
    if rendezvous_requires_stored_login(server) && stored_token.is_empty() {
        Err(PUBLIC_LOGIN_REQUIRED)
    } else {
        Ok(())
    }
}

fn stored_access_token() -> String {
    LocalConfig::get_option("access_token")
}

pub fn open_terminal_message(terminal_id: i32, rows: u32, cols: u32) -> Message {
    let mut action = TerminalAction::new();
    action.set_open(OpenTerminal {
        terminal_id,
        rows,
        cols,
        ..Default::default()
    });
    let mut msg = Message::new();
    msg.set_terminal_action(action);
    msg
}

pub fn terminal_input_message(terminal_id: i32, data: &[u8]) -> Message {
    let mut action = TerminalAction::new();
    action.set_data(TerminalData {
        terminal_id,
        data: bytes::Bytes::from(data.to_vec()),
        ..Default::default()
    });
    let mut msg = Message::new();
    msg.set_terminal_action(action);
    msg
}

pub fn close_terminal_message(terminal_id: i32) -> Message {
    let mut action = TerminalAction::new();
    action.set_close(CloseTerminal {
        terminal_id,
        ..Default::default()
    });
    let mut msg = Message::new();
    msg.set_terminal_action(action);
    msg
}

/// Keystrokes for one command, then a shell line that prints `EXIT_MARK` and
/// the command status. The host starts a PTY shell, so this is input, not exec.
pub fn command_keystrokes(command: &str, windows: bool) -> Vec<u8> {
    if windows {
        // `$LASTEXITCODE` is unset for a cmdlet, so fall back to `$?`.
        format!(
            "{command}\r\nWrite-Output (\"{EXIT_MARK}\" + $(if ($null -ne $LASTEXITCODE) {{ $LASTEXITCODE }} elseif ($?) {{ 0 }} else {{ 1 }}))\r\n"
        )
        .into_bytes()
    } else {
        format!("{command}\nprintf '\\n{EXIT_MARK}%d\\n' $?\n").into_bytes()
    }
}

fn decode_terminal_data(data: &TerminalData) -> Vec<u8> {
    if data.compressed {
        hbb_common::compress::decompress(&data.data)
    } else {
        data.data.to_vec()
    }
}

pub fn terminal_response_output(response: &TerminalResponse) -> Option<Vec<u8>> {
    match &response.union {
        Some(terminal_response::Union::Data(data)) => Some(decode_terminal_data(data)),
        _ => None,
    }
}

/// Append `chunk` and return how much of `raw` is safe to print, plus the
/// command status once the shell's mark line is complete.
pub fn push_local_output(raw: &mut Vec<u8>, printed: usize, chunk: &[u8]) -> (usize, Option<i32>) {
    raw.extend_from_slice(chunk);
    if let Some((before, code)) = split_completed_output(raw) {
        return (before.len().max(printed), Some(code));
    }
    let hold = raw
        .iter()
        .rposition(|b| *b == b'\n' || *b == b'\r')
        .map(|i| i + 1)
        .unwrap_or(0);
    (hold.max(printed), None)
}

fn split_completed_output(buf: &[u8]) -> Option<(&[u8], i32)> {
    let mark = EXIT_MARK.as_bytes();
    if mark.is_empty() || buf.len() < mark.len() {
        return None;
    }
    let mut search_from = 0;
    while let Some(rel) = find_slice(&buf[search_from..], mark) {
        let abs = search_from + rel;
        let line_start = buf[..abs]
            .iter()
            .rposition(|b| *b == b'\n' || *b == b'\r')
            .map(|i| i + 1)
            .unwrap_or(0);
        if line_start == abs {
            let rest = &buf[abs + mark.len()..];
            if let Some(end) = rest.iter().position(|b| *b == b'\n' || *b == b'\r') {
                if let Ok(text) = std::str::from_utf8(&rest[..end]) {
                    if let Ok(code) = text.parse::<i32>() {
                        return Some((&buf[..line_start], code));
                    }
                }
            } else {
                return None;
            }
        }
        search_from = abs + mark.len();
    }
    None
}

fn find_slice(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len())
        .position(|window| window == needle)
}

#[tokio::main(flavor = "current_thread")]
pub async fn run(args: &[String]) -> ResultType<i32> {
    let request = parse_exec_args(args).map_err(|err| anyhow!(err))?;
    let configured = crate::common::get_custom_rendezvous_server(Config::get_option(
        keys::OPTION_CUSTOM_RENDEZVOUS_SERVER,
    ));
    let rendezvous = effective_rendezvous(&configured, &request.id);
    let token = stored_access_token();
    if let Err(err) = stored_login_gate(&rendezvous, &token) {
        bail!(err);
    }
    let key = crate::get_key(true).await;
    let mut lc = LoginConfigHandler::default();
    lc.initialize(
        request.id.clone(),
        ConnType::TERMINAL,
        None,
        request.relay,
        None,
        None,
        None,
    );
    let handler = ConsoleHandler {
        lc: Arc::new(RwLock::new(lc)),
        password: request.password.clone(),
    };
    let id = handler.get_id();
    let started = client::Client::start(&id, &key, &token, ConnType::TERMINAL, handler.clone())
        .await
        .map_err(|err| anyhow!(err))?;
    let ((mut stream, _direct, _pk, kcp, _kind), (feedback, rendezvous_server)) = started;
    let _kcp = kcp;
    if !stream.is_secured() && !crate::common::is_direct_ip_access(&id) {
        bail!("connection is not encrypted");
    }
    let _keepalive = client::hc_connection(feedback, rendezvous_server, &token).await;
    let platform = login_terminal(&handler, &mut stream).await?;
    stream
        .send(&open_terminal_message(
            TERMINAL_ID,
            DEFAULT_ROWS,
            DEFAULT_COLS,
        ))
        .await?;
    let mut stdout = std::io::stdout();
    if let Some(command) = request.command.as_deref() {
        run_command(&mut stream, &mut stdout, command, &platform).await
    } else {
        attach_stdio(&mut stream, &mut stdout).await
    }
}

async fn login_terminal(handler: &ConsoleHandler, stream: &mut Stream) -> ResultType<String> {
    loop {
        let next = match timeout(READ_TIMEOUT, stream.next()).await {
            Err(_) => bail!("timed out waiting for the remote terminal"),
            Ok(Some(Ok(bytes))) => bytes,
            Ok(Some(Err(err))) => bail!("connection closed: {err}"),
            Ok(None) => bail!("connection closed"),
        };
        let msg = Message::parse_from_bytes(&next)?;
        match msg.union {
            Some(message::Union::Hash(hash)) => {
                if !client::handle_hash(
                    handler.lc.clone(),
                    &handler.password,
                    hash,
                    handler,
                    stream,
                )
                .await
                {
                    bail!("login failed");
                }
            }
            Some(message::Union::LoginResponse(lr)) => match lr.union {
                Some(login_response::Union::Error(err)) => {
                    if err == client::LOGIN_MSG_NO_PASSWORD_ACCESS {
                        eprintln!(
                            "Please wait for the remote side to accept your session request..."
                        );
                        continue;
                    }
                    if client::retry_with_default_connect_password(
                        handler.lc.clone(),
                        &err,
                        stream,
                    )
                    .await
                    {
                        continue;
                    }
                    bail!(err);
                }
                Some(login_response::Union::PeerInfo(pi)) => {
                    if !peer_supports_terminal(&pi) {
                        bail!("Remote terminal is not supported by the remote side");
                    }
                    return Ok(pi.platform);
                }
                _ => {}
            },
            Some(message::Union::TestDelay(t)) => {
                client::handle_test_delay(t, stream).await;
            }
            _ => log::trace!("ignored message before the terminal opened"),
        }
    }
}

fn peer_supports_terminal(pi: &PeerInfo) -> bool {
    pi.features
        .as_ref()
        .map(|features| features.terminal)
        .unwrap_or(false)
}

async fn run_command(
    stream: &mut Stream,
    stdout: &mut impl Write,
    command: &str,
    platform: &str,
) -> ResultType<i32> {
    wait_until_open(stream).await?;
    let windows = platform.eq_ignore_ascii_case("windows");
    let strokes = command_keystrokes(command, windows);
    stream
        .send(&terminal_input_message(TERMINAL_ID, &strokes))
        .await?;
    let mut raw = Vec::new();
    let mut printed = 0usize;
    loop {
        let Some(response) = next_terminal_response(stream, false).await? else {
            if raw.len() > printed {
                stdout.write_all(&raw[printed..])?;
                stdout.flush()?;
            }
            let _ = close_terminal(stream).await;
            bail!("connection closed before the command finished");
        };
        if let Some(outcome) = apply_terminal_response(response, stdout, &mut raw, &mut printed)? {
            let _ = close_terminal(stream).await;
            return Ok(outcome);
        }
    }
}

async fn attach_stdio(stream: &mut Stream, stdout: &mut impl Write) -> ResultType<i32> {
    wait_until_open(stream).await?;
    let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();
    thread::spawn(move || {
        let mut stdin = std::io::stdin().lock();
        let mut buf = [0u8; 4096];
        loop {
            match stdin.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    if tx.send(buf[..n].to_vec()).is_err() {
                        break;
                    }
                }
                Err(err) => {
                    log::debug!("stdin read failed: {err}");
                    break;
                }
            }
        }
    });
    let mut stdin_open = true;
    loop {
        tokio::select! {
            chunk = rx.recv(), if stdin_open => {
                match chunk {
                    Some(chunk) => {
                        stream.send(&terminal_input_message(TERMINAL_ID, &chunk)).await?;
                    }
                    None => {
                        stdin_open = false;
                        close_terminal(stream).await?;
                    }
                }
            }
            incoming = next_terminal_response(stream, !stdin_open) => {
                let Some(response) = incoming? else {
                    return Ok(1);
                };
                if let Some(code) = write_terminal_response(response, stdout)? {
                    return Ok(code);
                }
            }
        }
    }
}

async fn wait_until_open(stream: &mut Stream) -> ResultType<()> {
    loop {
        let Some(response) = next_terminal_response(stream, true).await? else {
            bail!("connection closed before the terminal opened");
        };
        match response.union {
            Some(terminal_response::Union::Opened(opened)) => {
                if !opened.success {
                    if opened.message.is_empty() {
                        bail!("remote terminal failed to open");
                    }
                    bail!(opened.message);
                }
                return Ok(());
            }
            Some(terminal_response::Union::Error(err)) => {
                if err.message.is_empty() {
                    bail!("remote terminal failed to open");
                }
                bail!(err.message);
            }
            Some(terminal_response::Union::Data(_)) => log::trace!("terminal output before open"),
            Some(terminal_response::Union::Closed(_)) => {
                bail!("remote terminal closed before it opened");
            }
            _ => {}
        }
    }
}

async fn next_terminal_response(
    stream: &mut Stream,
    timed: bool,
) -> ResultType<Option<TerminalResponse>> {
    loop {
        let next = if timed {
            match timeout(READ_TIMEOUT, stream.next()).await {
                Err(_) => bail!("timed out waiting for the remote terminal"),
                Ok(item) => item,
            }
        } else {
            stream.next().await
        };
        let bytes = match next {
            Some(Ok(bytes)) => bytes,
            Some(Err(err)) => bail!("connection closed: {err}"),
            None => return Ok(None),
        };
        let msg = Message::parse_from_bytes(&bytes)?;
        match msg.union {
            Some(message::Union::TerminalResponse(response)) => return Ok(Some(response)),
            Some(message::Union::TestDelay(t)) => {
                client::handle_test_delay(t, stream).await;
            }
            _ => log::trace!("ignored session message"),
        }
    }
}

fn apply_terminal_response(
    response: TerminalResponse,
    stdout: &mut impl Write,
    raw: &mut Vec<u8>,
    printed: &mut usize,
) -> ResultType<Option<i32>> {
    match response.union {
        Some(terminal_response::Union::Data(data)) => {
            let chunk = decode_terminal_data(&data);
            let (end, code) = push_local_output(raw, *printed, &chunk);
            if end > *printed {
                stdout.write_all(&raw[*printed..end])?;
                stdout.flush()?;
                *printed = end;
            }
            Ok(code)
        }
        Some(terminal_response::Union::Closed(closed)) => Ok(Some(closed.exit_code)),
        Some(terminal_response::Union::Error(err)) => {
            if err.message.is_empty() {
                bail!("remote terminal failed");
            }
            bail!(err.message);
        }
        _ => Ok(None),
    }
}

fn write_terminal_response(
    response: TerminalResponse,
    stdout: &mut impl Write,
) -> ResultType<Option<i32>> {
    match response.union {
        Some(terminal_response::Union::Data(data)) => {
            let bytes = decode_terminal_data(&data);
            stdout.write_all(&bytes)?;
            stdout.flush()?;
            Ok(None)
        }
        Some(terminal_response::Union::Closed(closed)) => Ok(Some(closed.exit_code)),
        Some(terminal_response::Union::Error(err)) => {
            if err.message.is_empty() {
                bail!("remote terminal failed");
            }
            bail!(err.message);
        }
        _ => Ok(None),
    }
}

async fn close_terminal(stream: &mut Stream) -> ResultType<()> {
    stream.send(&close_terminal_message(TERMINAL_ID)).await?;
    Ok(())
}

#[derive(Clone)]
struct ConsoleHandler {
    lc: Arc<RwLock<LoginConfigHandler>>,
    password: String,
}

#[async_trait]
impl Interface for ConsoleHandler {
    fn send(&self, _data: Data) {}

    fn msgbox(&self, _msgtype: &str, title: &str, text: &str, _link: &str) {
        if text.is_empty() {
            if !title.is_empty() {
                eprintln!("{title}");
            }
        } else if title.is_empty() {
            eprintln!("{text}");
        } else {
            eprintln!("{title}: {text}");
        }
    }

    fn handle_login_error(&self, err: &str) -> bool {
        client::handle_login_error(self.lc.clone(), err, self)
    }

    fn handle_peer_info(&self, _pi: PeerInfo) {}

    fn set_multiple_windows_session(&self, _sessions: Vec<WindowsSession>) {}

    async fn handle_hash(&self, pass: &str, hash: Hash, peer: &mut Stream) -> bool {
        client::handle_hash(self.lc.clone(), pass, hash, self, peer).await
    }

    async fn handle_login_from_ui(
        &self,
        os_username: String,
        os_password: String,
        password: String,
        remember: bool,
        peer: &mut Stream,
    ) {
        client::handle_login_from_ui(
            self.lc.clone(),
            os_username,
            os_password,
            password,
            remember,
            peer,
        )
        .await
    }

    async fn handle_test_delay(&self, t: TestDelay, peer: &mut Stream) {
        client::handle_test_delay(t, peer).await
    }

    fn get_lch(&self) -> Arc<RwLock<LoginConfigHandler>> {
        self.lc.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    #[test]
    fn exec_command_is_framed_as_terminal_input() {
        let request = parse_exec_args(&args(&[
            "--exec",
            "peer",
            "--password",
            "pw",
            "--command",
            "echo hi",
        ]))
        .unwrap();
        assert_eq!(request.id, "peer");
        assert_eq!(request.password, "pw");
        assert_eq!(request.command.as_deref(), Some("echo hi"));

        let open = open_terminal_message(TERMINAL_ID, DEFAULT_ROWS, DEFAULT_COLS);
        match open.union {
            Some(message::Union::TerminalAction(action)) => match action.union {
                Some(terminal_action::Union::Open(open)) => {
                    assert_eq!(open.terminal_id, TERMINAL_ID);
                    assert_eq!((open.rows, open.cols), (DEFAULT_ROWS, DEFAULT_COLS));
                }
                _ => panic!("expected OpenTerminal"),
            },
            _ => panic!("expected TerminalAction"),
        }

        let strokes = command_keystrokes("echo hi", false);
        let text = String::from_utf8(strokes.clone()).unwrap();
        assert!(text.starts_with("echo hi\n"), "{text}");
        assert!(text.contains(EXIT_MARK));
        assert!(!text.contains("pw"));
        let windows = String::from_utf8(command_keystrokes("echo hi", true)).unwrap();
        assert!(windows.starts_with("echo hi\r\n"));
        assert!(windows.contains(EXIT_MARK));

        let msg = terminal_input_message(TERMINAL_ID, &strokes);
        match msg.union {
            Some(message::Union::TerminalAction(action)) => match action.union {
                Some(terminal_action::Union::Data(data)) => {
                    assert_eq!(data.terminal_id, TERMINAL_ID);
                    assert!(!data.compressed);
                    assert_eq!(data.data.as_ref(), strokes.as_slice());
                }
                _ => panic!("expected TerminalData"),
            },
            _ => panic!("expected TerminalAction"),
        }

        let attach = parse_exec_args(&args(&["--exec", "peer", "--relay"])).unwrap();
        assert!(attach.command.is_none());
        assert!(attach.relay);
    }

    #[test]
    fn terminal_response_is_printed_as_local_output() {
        let mut response = TerminalResponse::new();
        let mut data = TerminalData::new();
        data.terminal_id = TERMINAL_ID;
        data.data = bytes::Bytes::from("hello\n");
        response.set_data(data);
        assert_eq!(
            terminal_response_output(&response).as_deref(),
            Some(&b"hello\n"[..])
        );

        let plain = b"hello from the remote pty\n".repeat(40);
        let compressed = hbb_common::compress::compress(&plain);
        assert!(!compressed.is_empty());
        let mut data = TerminalData::new();
        data.terminal_id = TERMINAL_ID;
        data.compressed = true;
        data.data = bytes::Bytes::from(compressed);
        let mut response = TerminalResponse::new();
        response.set_data(data);
        assert_eq!(
            terminal_response_output(&response).as_deref(),
            Some(plain.as_slice())
        );

        let mut closed = TerminalResponse::new();
        closed.set_closed(Default::default());
        assert!(terminal_response_output(&closed).is_none());

        let mut raw = Vec::new();
        let mut printed = 0usize;
        let (end, code) = push_local_output(&mut raw, printed, b"hello\n__RD_EX");
        assert_eq!(&raw[..end], b"hello\n");
        assert!(code.is_none());
        printed = end;
        let (end, code) = push_local_output(&mut raw, printed, b"IT_4\n");
        assert_eq!(end, printed);
        assert_eq!(code, Some(4));
    }

    #[test]
    fn public_server_reuses_stored_login_self_hosted_does_not() {
        let missing = stored_login_gate("", "").unwrap_err();
        assert!(missing.contains("desktop client"));
        assert!(stored_login_gate("rs-ny.rustdesk.com", "").is_err());
        assert!(stored_login_gate("rs-ny.rustdesk.com:21116", "").is_err());
        assert!(stored_login_gate("", "saved-login").is_ok());
        assert!(stored_login_gate("hbbs.example.test", "").is_ok());
        assert!(stored_login_gate("hbbs.example.test:21116", "").is_ok());
        assert!(stored_login_gate(&effective_rendezvous("", "name@hbbs.example.test"), "").is_ok());
        assert!(stored_login_gate(
            &effective_rendezvous("hbbs.example.test", "name@public"),
            ""
        )
        .is_err());
    }
}
