//! Provisional raw query and Evidence API representations.
use crate::{
    authority::name_ok,
    logs::{now_ns, Record},
    Error, Result,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct Query {
    pub start_ns: String,
    pub end_ns: String,
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default = "default_deadline")]
    pub deadline_ms: u64,
    pub service: Option<String>,
    pub trace_id: Option<String>,
    pub record_id: Option<String>,
}
fn default_limit() -> usize {
    50
}
fn default_deadline() -> u64 {
    2000
}

pub fn lower_hex(id: &str, len: usize) -> bool {
    id.len() == len
        && id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

impl Query {
    pub fn bounds(&self) -> Result<(u64, u64)> {
        fn nanos(s: &str) -> Result<u64> {
            let n = s.parse::<u64>().map_err(|_| Error::Invalid)?;
            if n == 0 || n > i64::MAX as u64 || n.to_string() != s {
                return Err(Error::Invalid);
            }
            Ok(n)
        }
        let (start, end) = (nanos(&self.start_ns)?, nanos(&self.end_ns)?);
        if end <= start || end - start > 3_600_000_000_000 {
            return Err(Error::Invalid);
        }
        Ok((start, end))
    }
    pub fn validate(&self) -> Result<()> {
        self.bounds()?;
        if !(1..=100).contains(&self.limit)
            || !(1..=2000).contains(&self.deadline_ms)
            || self.service.as_ref().is_some_and(|s| !name_ok(s))
            || [self.trace_id.as_ref(), self.record_id.as_ref()]
                .into_iter()
                .flatten()
                .any(|id| !lower_hex(id, 32))
        {
            return Err(Error::Invalid);
        }
        Ok(())
    }
    pub fn range(&self) -> Value {
        json!({"start_ns":self.start_ns,"end_ns":self.end_ns})
    }
}

pub struct Page {
    pub records: Vec<Record>,
    pub truncated: bool,
    pub warning: bool,
}

pub fn result(tenant: &str, source: &str, query: &Query, page: Page, evidence: bool) -> Value {
    let mut reasons = vec!["collection_retention_and_late_arrival_unknown"];
    if page.truncated {
        reasons.push("row_limit");
    }
    if page.warning {
        reasons.push("source_warning");
    }
    let outcome = if page.truncated || page.warning {
        "partial"
    } else if page.records.is_empty() {
        "no_matches"
    } else {
        "matches"
    };
    let extent = page
        .records
        .first()
        .zip(page.records.last())
        .map(|(first, last)| {
            json!({
                "start_ns": first.timestamp_ns.to_string(),
                "end_ns": (last.timestamp_ns + 1).to_string()
            })
        });
    let mut data = json!({
        "outcome": outcome,
        "requested_range": query.range(),
        "source_query_range": query.range(),
        "effective_range": extent,
        "coverage": "unknown",
        "limitations": reasons,
        "records": page.records
    });
    if evidence {
        let path = format!("/api/v1/tenants/{tenant}/logs/query");
        let references: Vec<_> = page
            .records
            .iter()
            .map(|r| {
                json!({
                    "kind": "query",
                    "method": "POST",
                    "path": path,
                    "query": {
                        "record_id": r.id,
                        "start_ns": r.timestamp_ns.to_string(),
                        "end_ns": (r.timestamp_ns + 1).to_string(),
                        "limit": 1
                    },
                    "record_id": r.id,
                    "producer_id": r.producer_id
                })
            })
            .collect();
        data["schema_version"] = json!("v0.1");
        data["references"] = json!(references);
        data["provenance"] = json!({
            "tenant": tenant,
            "source": source,
            "operation": "search_logs",
            "query": query,
            "retrieved_at_ns": now_ns().to_string(),
            "producer_attribution": "authenticated_credential",
            "transform": "otlp-log-capture-v1"
        });
    }
    data
}
