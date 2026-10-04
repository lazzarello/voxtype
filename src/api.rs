//! Disabled-by-default local WebSocket streaming API.
use crate::text::TextProcessor;
use crate::transcribe::{StreamingEvent, Transcriber};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch, Semaphore};
use tokio_tungstenite::{
    accept_hdr_async_with_config,
    tungstenite::{
        handshake::server::{Request, Response},
        http::StatusCode,
        protocol::WebSocketConfig,
        Message,
    },
};

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum ClientMessage {
    Start { protocol: u8, audio: Audio },
    Stop,
    Cancel,
    Ping,
}
#[derive(Debug, Deserialize)]
struct Audio {
    encoding: String,
    sample_rate: u32,
    channels: u8,
}
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum ServerMessage {
    Ready { protocol: u8, sample_rate: u32 },
    Partial { text: String, revision: u64 },
    Final { text: String, revision: u64 },
    Complete { text: String },
    Error { code: String, message: String },
    Pong,
}

#[derive(Default)]
struct Snapshot {
    committed: String,
    provisional: String,
    revision: u64,
}
impl Snapshot {
    fn append(&mut self, s: &str) {
        self.provisional.push_str(s);
    }
    fn finish_segment(&mut self, s: &str) {
        self.committed.push_str(&self.provisional);
        self.provisional.clear();
        self.committed.push_str(s);
    }
    fn replace(&mut self, n: usize, s: &str) {
        let keep = self.provisional.chars().count().saturating_sub(n);
        self.provisional = self.provisional.chars().take(keep).collect();
        self.provisional.push_str(s);
    }
    fn text(&self) -> String {
        format!("{}{}", self.committed, self.provisional)
    }
    fn complete(&mut self) {
        self.committed.push_str(&self.provisional);
        self.provisional.clear();
    }
}

fn allowed(addr: SocketAddr) -> bool {
    match addr.ip() {
        IpAddr::V4(ip) => {
            ip.is_loopback()
                || ip.is_private()
                || ip.is_link_local()
                || (ip.octets()[0] == 100 && (64..=127).contains(&ip.octets()[1]))
        }
        IpAddr::V6(ip) => ip.is_loopback() || ip.is_unique_local(),
    }
}

pub async fn serve(
    bind: &str,
    max_secs: u32,
    transcriber: Arc<dyn Transcriber>,
    text_config: crate::config::TextConfig,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let addr: SocketAddr = bind.parse()?;
    anyhow::ensure!(allowed(addr), "refusing non-private API bind address");
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(%addr, "WebSocket API listening");
    let sessions = Arc::new(Semaphore::new(1));
    let mut connections = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let sessions = sessions.clone();
                let transcriber = transcriber.clone();
                let text_config = text_config.clone();
                let shutdown = shutdown.clone();
                connections.spawn(async move {
                    let _ = connection(stream, sessions, transcriber, TextProcessor::new(&text_config), max_secs, shutdown).await;
                });
            }
            _ = connections.join_next(), if !connections.is_empty() => {}
            changed = shutdown.changed() => {
                if changed.is_ok() && *shutdown.borrow() { break Ok(()); }
            }
        }
    }
}

async fn connection(
    stream: TcpStream,
    sessions: Arc<Semaphore>,
    transcriber: Arc<dyn Transcriber>,
    processor: TextProcessor,
    max_secs: u32,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let ws = accept_hdr_async_with_config(
        stream,
        |r: &Request, response: Response| {
            if r.uri().path() == "/v1/stream" {
                Ok(response)
            } else {
                Err(Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Some("not found".into()))
                    .unwrap())
            }
        },
        Some(WebSocketConfig {
            max_message_size: Some(64 * 1024),
            max_frame_size: Some(64 * 1024),
            ..Default::default()
        }),
    )
    .await?;
    let (mut sink, mut input) = ws.split();
    let Some(Ok(Message::Text(first))) = input.next().await else {
        return Ok(());
    };
    let Ok(ClientMessage::Start { protocol, audio }) = serde_json::from_str(&first) else {
        send_error(&mut sink, "bad_request", "first message must be start").await?;
        return Ok(());
    };
    if protocol != 1
        || audio.encoding != "pcm_s16le"
        || audio.sample_rate != 16000
        || audio.channels != 1
    {
        send_error(
            &mut sink,
            "unsupported_audio",
            "requires mono 16 kHz pcm_s16le",
        )
        .await?;
        return Ok(());
    }
    let Ok(_permit) = sessions.try_acquire() else {
        send_error(&mut sink, "busy", "another session is active").await?;
        return Ok(());
    };
    let (audio_tx, audio_rx) = mpsc::channel(2);
    let Some(streaming) = transcriber.as_streaming() else {
        send_error(
            &mut sink,
            "streaming_unavailable",
            "streaming transcription is not configured",
        )
        .await?;
        return Ok(());
    };
    let mut handle = match streaming.start_stream(audio_rx) {
        Ok(handle) => handle,
        Err(error) => {
            send_error(
                &mut sink,
                "transcription_failed",
                "failed to start transcription",
            )
            .await?;
            tracing::warn!(%error, "API streaming session failed to start");
            return Ok(());
        }
    };
    sink.send(json(ServerMessage::Ready {
        protocol: 1,
        sample_rate: 16000,
    })?)
    .await?;
    let mut cancel = Some(handle.cancel);
    let mut audio_tx = Some(audio_tx);
    let mut decoder = Decoder::default();
    let mut snapshot = Snapshot::default();
    let deadline = tokio::time::sleep(std::time::Duration::from_secs(max_secs.max(1) as u64));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    let _ = send_error(&mut sink, "server_shutdown", "server is shutting down").await;
                    audio_tx = None;
                    let _ = cancel.take().map(|tx| tx.send(()));
                    break;
                }
            }
            _ = &mut deadline => { send_error(&mut sink, "session_timeout", "session exceeded maximum duration").await?; audio_tx = None; let _ = cancel.take().map(|tx| tx.send(())); break; }
            msg = input.next() => { let Some(msg) = msg else { audio_tx = None; let _ = cancel.take().map(|tx| tx.send(())); break }; match msg? {
                Message::Binary(b) => { if b.len() > 64*1024 { send_error(&mut sink,"bad_request","audio message too large").await?; audio_tx = None; let _=cancel.take().map(|tx| tx.send(())); break } let samples=decoder.push(&b); if !samples.is_empty() && audio_tx.as_ref().is_some_and(|tx| tx.try_send(samples).is_err()) { send_error(&mut sink,"audio_overrun","audio queue is full").await?; audio_tx = None; let _=cancel.take().map(|tx| tx.send(())); break } }
                Message::Text(t) => match serde_json::from_str::<ClientMessage>(&t) { Ok(ClientMessage::Stop) => { if decoder.odd { send_error(&mut sink,"bad_request","odd trailing PCM byte").await?; audio_tx = None; let _=cancel.take().map(|tx| tx.send(())); break } audio_tx=None; }, Ok(ClientMessage::Cancel) => { audio_tx = None; let _=cancel.take().map(|tx| tx.send(())); break }, Ok(ClientMessage::Ping) => { sink.send(json(ServerMessage::Pong)?).await?; }, _ => { send_error(&mut sink,"bad_request","unexpected message").await?; } }, Message::Close(_) => { audio_tx = None; let _=cancel.take().map(|tx| tx.send(())); break }, _ => {} }
            }
            event = handle.events.recv() => { match event { Some(StreamingEvent::Partial{text,..}) => { snapshot.append(&text); snapshot.revision+=1; send_snapshot(&mut sink,false,&snapshot,&processor).await? }, Some(StreamingEvent::Final{text,..}) => { snapshot.finish_segment(&text); snapshot.revision+=1; send_snapshot(&mut sink,true,&snapshot,&processor).await? }, Some(StreamingEvent::Replace{backspace,text,..}) => { snapshot.replace(backspace,&text); snapshot.revision+=1; send_snapshot(&mut sink,false,&snapshot,&processor).await? }, Some(StreamingEvent::Error(_)) => { send_error(&mut sink,"transcription_failed","transcription failed").await?; audio_tx = None; let _=cancel.take().map(|tx| tx.send(())); break }, Some(StreamingEvent::Ended)|None => { snapshot.complete(); sink.send(json(ServerMessage::Complete { text: processor.process(&snapshot.text()) })?).await?; break } } }
        }
    }
    let _ = handle.task.await;
    Ok(())
}

#[derive(Default)]
struct Decoder {
    carry: Option<u8>,
    odd: bool,
}
impl Decoder {
    fn push(&mut self, bytes: &[u8]) -> Vec<f32> {
        let mut b = Vec::with_capacity(bytes.len() + 1);
        if let Some(x) = self.carry.take() {
            b.push(x)
        }
        b.extend_from_slice(bytes);
        self.odd = b.len() % 2 == 1;
        if self.odd {
            self.carry = b.pop()
        }
        let mut out = Vec::with_capacity(b.len() / 2);
        for x in b.chunks_exact(2) {
            out.push(i16::from_le_bytes([x[0], x[1]]) as f32 / 32768.0)
        }
        out
    }
}
fn json(m: ServerMessage) -> anyhow::Result<Message> {
    Ok(Message::Text(serde_json::to_string(&m)?.into()))
}
async fn send_error<S: SinkExt<Message> + Unpin>(
    s: &mut S,
    code: &str,
    message: &str,
) -> anyhow::Result<()>
where
    S::Error: std::error::Error + Send + Sync + 'static,
{
    s.send(json(ServerMessage::Error {
        code: code.into(),
        message: message.into(),
    })?)
    .await?;
    Ok(())
}
async fn send_snapshot<S: SinkExt<Message> + Unpin>(
    s: &mut S,
    final_: bool,
    snap: &Snapshot,
    p: &TextProcessor,
) -> anyhow::Result<()>
where
    S::Error: std::error::Error + Send + Sync + 'static,
{
    let text = p.process(&snap.text());
    s.send(json(if final_ {
        ServerMessage::Final {
            text,
            revision: snap.revision,
        }
    } else {
        ServerMessage::Partial {
            text,
            revision: snap.revision,
        }
    })?)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn pcm_odd_boundary() {
        let mut d = Decoder::default();
        assert!(d.push(&[0, 1, 2]).len() == 1);
        assert_eq!(d.push(&[3]), vec![0.0078125]);
    }
    #[test]
    fn bind_rejects_public() {
        assert!(!allowed("8.8.8.8:1".parse().unwrap()));
    }
}
