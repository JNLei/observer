use super::*;
use crate::{
    authority::TraceCapture,
    identity::Identity,
    identity::{lower_hex, TimestampNs},
    trace_store::SpanPage,
    traces,
};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTracePartialSuccess, ExportTraceServiceResponse,
};
use std::collections::HashMap;

const REJECTION: &str =
    "invalid span context, timestamps, kind/status, write window or per-span capture size";

fn success(rejected: usize, is_json: bool) -> Reply {
    if rejected == 0 {
        return if is_json {
            json_reply(200, json!({}))
        } else {
            reply(200, PROTO, Bytes::new())
        };
    }
    if is_json {
        json_reply(
            200,
            json!({"partialSuccess":{"rejectedSpans":rejected.to_string(),"errorMessage":REJECTION}}),
        )
    } else {
        reply(
            200,
            PROTO,
            ExportTraceServiceResponse {
                partial_success: Some(ExportTracePartialSuccess {
                    rejected_spans: rejected as i64,
                    error_message: REJECTION.into(),
                }),
            }
            .encode_to_vec(),
        )
    }
}

pub(super) async fn ingest(
    app: Arc<App>,
    key: Key,
    req: Request<Incoming>,
    is_json: bool,
) -> Result<Reply> {
    let grant = app
        .authority
        .trace_grant(&key, None, Action::IngestTraces)?;
    let mut slot = app.trace_writes.enter(grant.principal.tenant())?;
    let gzip = encoding(&req)?;
    let raw = body(req, BYTE_LIMIT).await?;
    let decoder = app
        .trace_decoders
        .clone()
        .try_acquire_owned()
        .map_err(|_| Error::Busy)?;
    tokio::spawn(async move {
        let batch = blocking(move || {
            let _decoder = decoder;
            traces::prepare(&raw, gzip, is_json, &grant)
        })
        .await?;
        let grant = app
            .authority
            .trace_grant(&key, None, Action::IngestTraces)?;
        let n = batch.records.len();
        let rejected = batch.rejected;
        let mut response = if n == 0 && rejected != 0 {
            counts(otlp_status(400, REJECTION, is_json), 0, rejected, 0, 0)
        } else {
            let sent = if n == 0 {
                Ok(())
            } else {
                app.trace_store.write(&grant, &batch.records).await
            };
            match sent {
                Ok(()) => counts(success(rejected, is_json), n, rejected, 0, 0),
                Err(StoreError::Offline | StoreError::Deadline) => {
                    counts(otlp_error(Error::Unavailable, is_json), 0, rejected, 0, n)
                }
                Err(e) => {
                    if e == StoreError::Indeterminate {
                        slot.retain();
                    }
                    counts(otlp_error(Error::Unavailable, is_json), 0, rejected, n, 0)
                }
            }
        };
        response
            .headers_mut()
            .insert("x-observe-durable-records", 0.into());
        Ok(response)
    })
    .await
    .map_err(|_| Error::Internal)?
}

fn span_result(
    tenant: &str,
    source: &str,
    capture: TraceCapture,
    q: &Query,
    page: SpanPage,
) -> Result<Value> {
    let mut multiplicity = HashMap::new();
    for r in &page.records {
        *multiplicity
            .entry(&r.captured.span().span_id)
            .or_insert(0usize) += 1;
    }
    let mut spans = Vec::new();
    let mut references = Vec::new();
    let mut limitations = vec!["sampling_retention_and_late_arrival_unknown"];
    if let Some(limit) = page.truncation {
        limitations.push(match limit {
            query::Truncation::RowLimit => "row_limit",
            query::Truncation::ResponseByteLimit => "capture_byte_limit",
        });
    }
    if multiplicity.values().any(|&n| n > 1) {
        limitations.push("repeated_span_id");
    }
    if capture == TraceCapture::MetadataOnly {
        limitations.push("text_and_attributes_withheld");
    }
    let path = format!(
        "/api/v1/tenants/{tenant}/traces/{}",
        q.trace_id.as_deref().unwrap()
    );
    for record in &page.records {
        let s = record.captured.span();
        let span_id = hex::encode(&s.span_id);
        let parent = (!s.parent_span_id.is_empty()).then(|| hex::encode(&s.parent_span_id));
        let relation = if parent.is_none() {
            "no_parent_reported"
        } else if s.parent_span_id == s.span_id {
            "self_reference"
        } else {
            match multiplicity.get(&s.parent_span_id) {
                None => "not_in_returned_page",
                Some(1) => "one_observed_candidate",
                Some(_) => "ambiguous",
            }
        };
        let identity = Identity {
            tenant,
            producer_id: &record.producer,
            service: &record.service,
            trace_id: q.trace_id.as_deref(),
            span_id: Some(&span_id),
            parent_span_id: parent.as_deref(),
            event_ns: Some(TimestampNs(s.start_time_unix_nano)),
            source,
        };
        spans.push(json!({"record_id":record.captured.id,"identity":identity,
            "parent_resolution":relation,"span_id_occurrences_in_page":multiplicity[&s.span_id],
            "otlp":traces::render(&record.captured.unit)?}));
        references.push(json!({"kind":"query","method":"POST","path":path,"record_id":record.captured.id,
            "query":{"start_ns":s.start_time_unix_nano.to_string(),"end_ns":(s.start_time_unix_nano+1).to_string(),
                "record_id":record.captured.id,"limit":1}}));
    }
    let extent = page
        .records
        .first()
        .zip(page.records.last())
        .map(|(first, last)| {
            json!({
                "start_ns":first.captured.span().start_time_unix_nano.to_string(),
                "end_ns":(last.captured.span().start_time_unix_nano+1).to_string()
            })
        });
    Ok(
        json!({"outcome":if page.truncation.is_some() {"partial"} else if spans.is_empty() {"no_matches"} else {"matches"},
        "requested_range":q.range(),"source_query_range":q.range(),"effective_range":extent,"coverage":"unknown",
        "limitations":limitations,"spans":spans,"references":references,
        "provenance":{"tenant":tenant,"source":source,"operation":"get_trace","transform":"otlp-trace-capture-v1",
            "query":q,"retrieved_at_ns":now_ns().to_string()}}),
    )
}

async fn span_page(
    app: &App,
    key: &Key,
    tenant: &str,
    q: &Query,
    deadline: Instant,
) -> Result<ChargedPage> {
    let authority = app.authority.clone();
    let key = key.clone();
    let target = tenant.to_owned();
    let store = app.trace_store.clone();
    let query = q.clone();
    source_job(&app.trace_reads, tenant, deadline, async move {
        let grant = authority.trace_grant(&key, Some(&target), Action::QueryTraces)?;
        let page = match store.scan(&grant, &query, deadline).await {
            Ok(page) => page,
            Err(e) => return Ok(Err(e)),
        };
        let result =
            blocking(move || span_result(&target, store.source(), grant.capture, &query, page))
                .await?;
        Ok(Ok(result))
    })
    .await
}

pub(super) async fn read(
    app: Arc<App>,
    key: Key,
    tenant: &str,
    trace_id: &str,
    mut q: Query,
    evidence: bool,
    started: Instant,
) -> Result<Reply> {
    if !lower_hex(trace_id, 32)
        || trace_id.bytes().all(|b| b == b'0')
        || q.trace_id.is_some()
        || (evidence && q.record_id.is_some())
    {
        return Err(Error::Invalid);
    }
    q.trace_id = Some(trace_id.to_owned());
    q.validate()?;
    let deadline = started + Duration::from_millis(q.deadline_ms);
    let trace = span_page(&app, &key, tenant, &q, deadline);
    let result = if evidence {
        let (spans, logs) = tokio::join!(trace, logs_page(&app, &key, tenant, &q, true, deadline));
        let mut reservations = Vec::with_capacity(2);
        let mut source_value = |result: Result<ChargedPage>, source| match result {
            Ok((value, slot)) => {
                reservations.push(slot);
                value
            }
            Err(e) => unavailable(source, &q, e),
        };
        let spans = source_value(spans, app.trace_store.source());
        let logs = source_value(logs, app.store.source());
        let range = q.range();
        if reservations.is_empty() {
            // Both sources failed. This fixed-size diagnostic needs no assembly worker.
            Ok(evidence_reply(trace_id, range, spans, logs))
        } else {
            let id = trace_id.to_owned();
            build_query_reply(deadline, reservations, move || {
                evidence_reply(&id, range, spans, logs)
            })
            .await
        }
    } else {
        page_reply(trace.await, deadline).await
    };
    // Recheck access to the finished bytes, including a revocation during assembly.
    app.authority
        .trace_grant(&key, Some(tenant), Action::QueryTraces)?;
    if evidence {
        app.authority.authorize(&key, Some(tenant), Action::Query)?;
    }
    Ok(match check_deadline(deadline).and(result) {
        Ok(reply) => reply,
        Err(e) => {
            let source = if evidence {
                "trace_evidence"
            } else {
                app.trace_store.source()
            };
            json_reply(e.response().0, unavailable(source, &q, e))
        }
    })
}

fn evidence_reply(trace_id: &str, range: Value, spans: Value, logs: Value) -> Reply {
    let mut by_span_id: HashMap<&str, Vec<&Value>> = HashMap::new();
    for span in spans["spans"].as_array().into_iter().flatten() {
        if let Some(id) = span["identity"]["span_id"].as_str() {
            by_span_id.entry(id).or_default().push(&span["record_id"]);
        }
    }
    let mut relationships = Vec::new();
    if let Some(rows) = logs["records"].as_array() {
        for row in rows {
            let candidates = row["span_id"]
                .as_str()
                .and_then(|id| by_span_id.get(id))
                .map(Vec::as_slice)
                .unwrap_or_default();
            relationships.push(json!({"method":"exact_trace_id","basis":"producer_supplied_ids",
                "log_record_id":row["id"],"trace_id":trace_id,"matching_span_record_ids":candidates}));
        }
    }
    let outcomes = [&spans["outcome"], &logs["outcome"]];
    let both_unavailable = outcomes.iter().all(|v| *v == "unavailable");
    let outcome = if both_unavailable {
        "unavailable"
    } else if outcomes
        .iter()
        .any(|v| *v == "unavailable" || *v == "partial")
    {
        "partial"
    } else if outcomes.iter().all(|v| *v == "no_matches") {
        "no_matches"
    } else {
        "matches"
    };
    let mut envelope = json!({"schema_version":"v0.3","operation":"trace_evidence",
        "outcome":outcome,"coverage":"unknown","requested_range":range});
    envelope["traces"] = spans;
    envelope["logs"] = logs;
    envelope["relationships"] = Value::Array(relationships);
    json_reply(if both_unavailable { 503 } else { 200 }, envelope)
}
