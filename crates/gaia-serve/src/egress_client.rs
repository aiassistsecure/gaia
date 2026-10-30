//! Proposal egress transport: the fork POSTing proposals to SI
//! (S-2b, G-9 §8).
//!
//! The watcher records proposals gaia-side; this module is the
//! wire half of getting them gated. One proposal goes out as
//! one `POST /gaia/proposals` whose body is the proposal record
//! verbatim ([`Proposal::to_json`], §12.1) — the fork adds
//! nothing and hides nothing between recording and submission,
//! exactly as it adds nothing between proposing and recording.
//!
//! SI ACKs *after* journaling (§8.3): the 200 body carries the
//! gate's verdict and the journal sequence that is the intent's
//! identity for every later flow. That ordering is what makes
//! the retry posture here simple and safe. SI dedups retries
//! by `caused_by`, returning the identical ACK for a proposal
//! it has already gated, so a send whose answer was lost — or
//! whose answer arrived but could not be read — is re-sent on
//! a later egress pass rather than guessed at. The only answer
//! that is *not* retried is a 4xx: §8.3 calls it terminal, a
//! shape defect that re-sending the same bytes cannot fix, and
//! the caller records it rather than dropping it silently.
//!
//! The transport is std-only networking, deliberately, as
//! everywhere in this workspace: no HTTP client crate, just a
//! raw [`TcpStream`] speaking the small piece of HTTP/1.1 this
//! one route needs. The bind on the SI side is loopback in the
//! deployments this seam is designed for, and there is no
//! authentication on the route, as there is none anywhere in
//! this workspace — no secrets live in this module.

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use serde_json::Value;

use crate::proposals::Proposal;

/// The §8.3 ACK vocabulary: the verdict SI's gate returned for
/// a proposal, spelled SI-style — uppercase `ADMIT`, `REJECT`,
/// or `ESCALATE`.
///
/// This is **not** `gaia_api::Verdict` (admit/reject, spelled
/// lowercase in its JSON form), and the two must not be merged:
/// that enum belongs to work submission on the fork's own
/// agency routes, a different crossing with a different
/// lifecycle, while this one is the gate's answer at proposal
/// ingest — including `ESCALATE`, which work submission's
/// vocabulary does not have. They rhyme because both are gates
/// speaking; they are not the same gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateVerdict {
    Admit,
    Reject,
    Escalate,
}

impl GateVerdict {
    /// Parse the wire spelling. Only the three uppercase
    /// spellings of §8.3 are a verdict; anything else is `None`,
    /// because guessing at a gate's answer — lowercasing it into
    /// shape, treating an unknown word as a rejection — would
    /// record a decision the gate never made.
    pub fn parse(s: &str) -> Option<GateVerdict> {
        match s {
            "ADMIT" => Some(GateVerdict::Admit),
            "REJECT" => Some(GateVerdict::Reject),
            "ESCALATE" => Some(GateVerdict::Escalate),
            _ => None,
        }
    }

    /// The wire spelling of §8.3, uppercase.
    pub fn as_str(&self) -> &'static str {
        match self {
            GateVerdict::Admit => "ADMIT",
            GateVerdict::Reject => "REJECT",
            GateVerdict::Escalate => "ESCALATE",
        }
    }
}

/// SI's ACK for one proposal, sent after the gate's verdict was
/// journaled (§8.3 step 5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ack {
    /// The seam task's sequence on the SI side. Present in the
    /// §8.3 shape; optional here because the fork's egress
    /// record joins on `journal_seq` and the verdict, and an
    /// ACK that omits this field still answers the proposal —
    /// refusing it would re-send a proposal SI has gated, for
    /// a field the retry would return identically anyway.
    pub task_seq: Option<u64>,
    /// The sequence of the journaled self-edit carrying the
    /// verdict — the intent's identity for every later flow
    /// (§9.2 makes work submission cite exactly this seq).
    /// Required: an ACK without it proves nothing about
    /// journaling, so it is not an ACK.
    pub journal_seq: u64,
    pub verdict: GateVerdict,
}

/// What one send attempt concluded, in the caller's terms.
///
/// The three cases are the three dispositions of §8.3, and the
/// distinction that matters is retry: `Acked` and `Terminal`
/// are answers the egress pass records and moves on from;
/// `Retryable` is the absence of an answer, and the proposal
/// stays pending for a later pass.
#[derive(Debug, Clone, PartialEq)]
pub enum SendOutcome {
    /// 200 with a readable ACK body: SI gated the proposal and
    /// journaled the verdict before answering.
    Acked(Ack),
    /// A 4xx: a terminal answer (§8.3) — recorded, never
    /// retried, because the same bytes would draw the same
    /// refusal.
    Terminal(u16),
    /// A transport failure, a 5xx, or an unreadable ACK: the
    /// proposal stays pending. Re-sending is the safe direction
    /// in every one of these cases, because SI dedups by
    /// `caused_by` and returns the identical ACK for a proposal
    /// it already gated (§8.3 step 2).
    Retryable(String),
}

/// Where proposals go. The egress pass drives this trait so
/// tests and compositions without a wired SI ingest can stand
/// in their own sink; [`HttpProposalSink`] is the real one.
pub trait ProposalSink {
    fn post_proposal(&self, proposal: &Proposal) -> SendOutcome;
}

/// The real sink: raw HTTP over a [`TcpStream`] to SI's
/// proposal ingest at `addr` (`HOST:PORT`).
pub struct HttpProposalSink {
    addr: String,
}

impl HttpProposalSink {
    pub fn new(addr: &str) -> HttpProposalSink {
        HttpProposalSink {
            addr: addr.to_string(),
        }
    }

    /// The `HOST:PORT` of SI's ingest this sink POSTs to.
    pub fn addr(&self) -> &str {
        &self.addr
    }
}

/// Connect to the first address `addr` resolves to that
/// accepts a connection. Resolution and connection failures are
/// named for the caller, because a `Retryable` that does not
/// say what failed leaves the operator guessing whether SI is
/// down, the address is wrong, or the network is.
fn connect(addr: &str) -> Result<TcpStream, String> {
    let resolved: Vec<_> = match addr.to_socket_addrs() {
        Ok(iter) => iter.collect(),
        Err(e) => {
            return Err(format!("cannot resolve SI ingest address {addr:?}: {e}"));
        }
    };
    if resolved.is_empty() {
        return Err(format!(
            "SI ingest address {addr:?} resolved to no addresses"
        ));
    }
    let mut last_err = String::new();
    for sock_addr in &resolved {
        match TcpStream::connect_timeout(sock_addr, Duration::from_secs(5)) {
            Ok(stream) => return Ok(stream),
            Err(e) => {
                last_err = format!("cannot connect to SI ingest at {sock_addr}: {e}");
            }
        }
    }
    Err(last_err)
}

/// Decode a `Transfer-Encoding: chunked` body. Hand-rolled
/// because the workspace carries no HTTP client crate, and
/// small because the ACK bodies this decodes are a few hundred
/// bytes of JSON: hex chunk sizes (extensions after `;`
/// ignored, as the framing allows), each chunk followed by a
/// CRLF, a zero chunk, then trailers up to the blank line,
/// which are read past and discarded. `None` is any framing
/// that does not parse — the caller treats it as an unreadable
/// response, never as a partial body worth trusting.
fn decode_chunked(body: &[u8]) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    let mut pos = 0usize;
    loop {
        let line_end = find(body, pos, b"\r\n")?;
        let size_line = std::str::from_utf8(&body[pos..line_end]).ok()?;
        let size_hex = size_line.split(';').next()?.trim();
        let size = usize::from_str_radix(size_hex, 16).ok()?;
        pos = line_end + 2;
        if size == 0 {
            // Trailers, if any, run to the blank line; the body
            // is what was decoded, whatever they carried.
            return Some(out);
        }
        if pos + size > body.len() {
            return None;
        }
        out.extend_from_slice(&body[pos..pos + size]);
        pos += size;
        // Each chunk's data is followed by a CRLF that is
        // framing, not content.
        if body.get(pos..pos + 2) != Some(b"\r\n") {
            return None;
        }
        pos += 2;
    }
}

/// The offset of the first occurrence of `needle` in `haystack`
/// at or after `from`.
fn find(haystack: &[u8], from: usize, needle: &[u8]) -> Option<usize> {
    if from > haystack.len() {
        return None;
    }
    haystack[from..]
        .windows(needle.len())
        .position(|w| w == needle)
        .map(|p| from + p)
}

/// Parse a 200 body into an [`Ack`]. Every required field that
/// is missing or misshapen is a parse failure: an unreadable
/// ACK may still have been a real ACK — SI journaled before
/// answering, so the gating happened — and re-sending is
/// deduped by SI into the identical ACK, which makes retry the
/// safe direction and a guessed-at ACK the dangerous one.
fn parse_ack(body: &[u8]) -> Result<Ack, String> {
    let value: Value =
        serde_json::from_slice(body).map_err(|e| format!("ACK body is not readable JSON: {e}"))?;
    let journal_seq = value
        .get("journal_seq")
        .and_then(Value::as_u64)
        .ok_or_else(|| "ACK body has no u64 \"journal_seq\"".to_string())?;
    let verdict_raw = value
        .get("verdict")
        .and_then(Value::as_str)
        .ok_or_else(|| "ACK body has no string \"verdict\"".to_string())?;
    let verdict = GateVerdict::parse(verdict_raw)
        .ok_or_else(|| format!("ACK verdict {verdict_raw:?} is not an §8.3 verdict"))?;
    let task_seq = match value.get("task_seq") {
        None => None,
        Some(v) => Some(
            v.as_u64()
                .ok_or_else(|| "ACK \"task_seq\" is present but not a u64".to_string())?,
        ),
    };
    Ok(Ack {
        task_seq,
        journal_seq,
        verdict,
    })
}

impl ProposalSink for HttpProposalSink {
    fn post_proposal(&self, proposal: &Proposal) -> SendOutcome {
        let body = proposal.to_json().to_string();
        let request = format!(
            "POST /gaia/proposals HTTP/1.1\r\n\
             Host: {}\r\n\
             Content-Type: application/json\r\n\
             Content-Length: {}\r\n\
             Connection: close\r\n\
             \r\n\
             {}",
            self.addr,
            body.len(),
            body
        );

        let mut stream = match connect(&self.addr) {
            Ok(stream) => stream,
            Err(message) => return SendOutcome::Retryable(message),
        };
        if let Err(e) = stream.set_read_timeout(Some(Duration::from_secs(5))) {
            return SendOutcome::Retryable(format!(
                "cannot set read timeout on the SI ingest connection: {e}"
            ));
        }
        if let Err(e) = stream.set_write_timeout(Some(Duration::from_secs(5))) {
            return SendOutcome::Retryable(format!(
                "cannot set write timeout on the SI ingest connection: {e}"
            ));
        }
        if let Err(e) = stream.write_all(request.as_bytes()) {
            return SendOutcome::Retryable(format!(
                "failed writing the proposal POST to SI ingest at {}: {e}",
                self.addr
            ));
        }
        if let Err(e) = stream.flush() {
            return SendOutcome::Retryable(format!(
                "failed flushing the proposal POST to SI ingest at {}: {e}",
                self.addr
            ));
        }

        // `Connection: close` makes EOF the end of the response:
        // read until SI closes, then split head from body.
        let mut raw: Vec<u8> = Vec::new();
        if let Err(e) = stream.read_to_end(&mut raw) {
            return SendOutcome::Retryable(format!(
                "failed reading the ACK from SI ingest at {}: {e}",
                self.addr
            ));
        }
        let head_end = match find(&raw, 0, b"\r\n\r\n") {
            Some(pos) => pos,
            None => {
                return SendOutcome::Retryable(
                    "response from SI ingest had no complete head".to_string(),
                );
            }
        };
        let head = String::from_utf8_lossy(&raw[..head_end]).into_owned();
        let status: u16 = match head
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|token| token.parse::<u16>().ok())
        {
            Some(status) => status,
            None => {
                return SendOutcome::Retryable(format!(
                    "response from SI ingest had an unparseable status line: {:?}",
                    head.lines().next().unwrap_or("")
                ));
            }
        };

        match status {
            200 => {
                let body_bytes = &raw[head_end + 4..];
                // Chunked framing, if the head declares it, is
                // decoded before the body is read as JSON; a
                // body that does not decode was never read.
                let chunked = head.lines().any(|line| {
                    let lower = line.to_ascii_lowercase();
                    lower.starts_with("transfer-encoding:") && lower.contains("chunked")
                });
                let decoded;
                let body: &[u8] = if chunked {
                    match decode_chunked(body_bytes) {
                        Some(bytes) => {
                            decoded = bytes;
                            &decoded
                        }
                        None => {
                            return SendOutcome::Retryable(
                                "200 response from SI ingest had an undecodable \
                                 chunked body"
                                    .to_string(),
                            );
                        }
                    }
                } else {
                    body_bytes
                };
                match parse_ack(body) {
                    Ok(ack) => SendOutcome::Acked(ack),
                    Err(message) => SendOutcome::Retryable(message),
                }
            }
            // A 4xx is §8.3's terminal answer: the proposal's
            // shape was refused, and re-sending the same bytes
            // would draw the same refusal. Recorded, not retried.
            400..=499 => SendOutcome::Terminal(status),
            // 5xx — and anything else unexpected — says nothing
            // about the proposal itself: SI may have journaled
            // before whatever went wrong, and its dedup makes
            // the retry return the original ACK if it did.
            other => SendOutcome::Retryable(format!(
                "SI ingest answered status {other} for a proposal POST"
            )),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread::JoinHandle;

    use hearth_store::EventRef;
    use serde_json::json;

    /// The fixture mirrors the one in `proposals.rs` tests —
    /// model "muse", the restart tool, the cause duplicated in
    /// `args.spine_event` as the watcher builds it — with its
    /// own hash/seq, so this module's tests say exactly which
    /// loss their proposal answers.
    fn proposal() -> Proposal {
        let hash = "hash-egress";
        let seq = 41u64;
        Proposal {
            model: "muse".to_string(),
            tool: "hearth.restart".to_string(),
            args: json!({
                "model": "muse",
                "spine_event": { "hash": hash, "seq": seq },
            }),
            reason: format!("observed loss at seq {seq}"),
            caused_by: EventRef {
                hash: hash.to_string(),
                seq,
            },
        }
    }

    /// A canned 200 response carrying `body` as its JSON.
    fn ok_response(body: &str) -> String {
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
             Content-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        )
    }

    /// A canned response with an empty body at `status`.
    fn status_response(status: u16, text: &str) -> String {
        format!(
            "HTTP/1.1 {status} {text}\r\nContent-Length: 0\r\n\
             Connection: close\r\n\r\n"
        )
    }

    /// Start a one-shot test server: it accepts one connection,
    /// reads the full request (head plus the `Content-Length`
    /// body), writes `response` verbatim, and closes. The
    /// returned handle yields the captured request text, so a
    /// test asserts on what actually went over the wire.
    fn one_shot_server(response: String) -> (String, JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut buf: Vec<u8> = Vec::new();
            let mut chunk = [0u8; 2048];
            let captured = loop {
                // Read until the head is complete, learn the
                // body length from it, then read until head and
                // body together have arrived — a single `read`
                // is only ever owed some of the bytes in flight.
                if let Some(head_end) = find(&buf, 0, b"\r\n\r\n") {
                    let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
                    let content_length: usize = head
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            if name.trim().eq_ignore_ascii_case("content-length") {
                                value.trim().parse().ok()
                            } else {
                                None
                            }
                        })
                        .unwrap_or(0);
                    if buf.len() >= head_end + 4 + content_length {
                        break String::from_utf8_lossy(&buf).into_owned();
                    }
                }
                match stream.read(&mut chunk) {
                    Ok(0) => break String::from_utf8_lossy(&buf).into_owned(),
                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    Err(e) => panic!("test server failed reading the request: {e}"),
                }
            };
            stream.write_all(response.as_bytes()).unwrap();
            stream.flush().unwrap();
            captured
        });
        (addr, handle)
    }

    #[test]
    fn sends_the_proposal_verbatim_and_parses_a_full_admit_ack() {
        let proposal = proposal();
        let (addr, server) = one_shot_server(ok_response(
            "{\"task_seq\":7,\"journal_seq\":99,\"verdict\":\"ADMIT\"}",
        ));
        let sink = HttpProposalSink::new(&addr);
        assert_eq!(sink.addr(), addr.as_str());

        let outcome = sink.post_proposal(&proposal);
        assert_eq!(
            outcome,
            SendOutcome::Acked(Ack {
                task_seq: Some(7),
                journal_seq: 99,
                verdict: GateVerdict::Admit,
            })
        );
        assert_eq!(GateVerdict::Admit.as_str(), "ADMIT");

        let captured = server.join().unwrap();
        let request_line = captured.lines().next().unwrap_or("");
        assert_eq!(request_line, "POST /gaia/proposals HTTP/1.1");
        assert!(
            captured.contains("Content-Type: application/json"),
            "{captured}"
        );
        let body = captured
            .split_once("\r\n\r\n")
            .map(|(_, body)| body)
            .unwrap_or("");
        let sent: Value = serde_json::from_str(body).unwrap();
        assert_eq!(
            sent,
            proposal.to_json(),
            "the body on the wire is the proposal record verbatim (§12.1)"
        );
    }

    #[test]
    fn reject_and_escalate_acks_parse() {
        for (wire, expected) in [
            ("REJECT", GateVerdict::Reject),
            ("ESCALATE", GateVerdict::Escalate),
        ] {
            let (addr, server) = one_shot_server(ok_response(&format!(
                "{{\"task_seq\":3,\"journal_seq\":50,\"verdict\":\"{wire}\"}}"
            )));
            let sink = HttpProposalSink::new(&addr);
            let outcome = sink.post_proposal(&proposal());
            assert_eq!(
                outcome,
                SendOutcome::Acked(Ack {
                    task_seq: Some(3),
                    journal_seq: 50,
                    verdict: expected,
                }),
                "verdict {wire}"
            );
            assert_eq!(expected.as_str(), wire);
            let _ = server.join().unwrap();
        }
        // Nothing outside the three spellings is a verdict.
        assert_eq!(GateVerdict::parse("admit"), None);
        assert_eq!(GateVerdict::parse("MAYBE"), None);
    }

    #[test]
    fn an_ack_without_task_seq_parses_with_none() {
        let (addr, server) =
            one_shot_server(ok_response("{\"journal_seq\":12,\"verdict\":\"ADMIT\"}"));
        let sink = HttpProposalSink::new(&addr);
        let outcome = sink.post_proposal(&proposal());
        assert_eq!(
            outcome,
            SendOutcome::Acked(Ack {
                task_seq: None,
                journal_seq: 12,
                verdict: GateVerdict::Admit,
            })
        );
        let _ = server.join().unwrap();
    }

    #[test]
    fn a_400_is_terminal() {
        let (addr, server) = one_shot_server(status_response(400, "Bad Request"));
        let sink = HttpProposalSink::new(&addr);
        assert_eq!(sink.post_proposal(&proposal()), SendOutcome::Terminal(400));
        let _ = server.join().unwrap();
    }

    #[test]
    fn a_503_is_retryable() {
        let (addr, server) = one_shot_server(status_response(503, "Service Unavailable"));
        let sink = HttpProposalSink::new(&addr);
        match sink.post_proposal(&proposal()) {
            SendOutcome::Retryable(message) => {
                assert!(message.contains("503"), "{message}");
            }
            other => panic!("a 503 must be retryable, got {other:?}"),
        }
        let _ = server.join().unwrap();
    }

    #[test]
    fn a_200_with_an_unreadable_ack_is_retryable() {
        let (addr, server) = one_shot_server(ok_response("{}"));
        let sink = HttpProposalSink::new(&addr);
        match sink.post_proposal(&proposal()) {
            SendOutcome::Retryable(_) => {}
            other => panic!("an ACK without journal_seq/verdict must be retryable, got {other:?}"),
        }
        let _ = server.join().unwrap();
    }

    #[test]
    fn a_refused_connection_is_retryable() {
        // Bind a listener to learn a port, then drop it: the
        // port is dead, and connecting draws a refusal rather
        // than a server that might have journaled anything.
        let dead_addr = {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap().to_string();
            drop(listener);
            addr
        };
        let sink = HttpProposalSink::new(&dead_addr);
        match sink.post_proposal(&proposal()) {
            SendOutcome::Retryable(message) => {
                assert!(message.contains("connect"), "{message}");
            }
            other => panic!("a refused connection must be retryable, got {other:?}"),
        }
    }
}
