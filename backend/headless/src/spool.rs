//! Single-process custody of sanitized Loki write units. Every state transition
//! commits on the same SQLite connection. Network I/O never owns this lock.
use crate::{
    authority::{check_private, Grant},
    logs::{now_ns, Record},
    Error, Result,
};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use serde_json::{json, Value};
use std::{
    fs::OpenOptions,
    os::unix::fs::OpenOptionsExt,
    path::Path,
    sync::{Arc, Mutex, MutexGuard},
};

const TENANT_BATCHES: i64 = 64;
const GLOBAL_BATCHES: i64 = 256;
const TENANT_BYTES: i64 = 2 * 1024 * 1024;
const GLOBAL_BYTES: i64 = 8 * 1024 * 1024;

pub struct Entry {
    pub id: i64,
    pub tenant: String,
    pub subject: String,
    pub records: Vec<Record>,
}

#[derive(Clone, Copy)]
pub enum Outcome {
    Ack,
    Offline,
    Unknown(u64),
    DestinationDenied,
    LostReply,
    Held(&'static str),
}

pub struct Spool {
    connection: Mutex<Connection>,
    fault: std::sync::atomic::AtomicBool,
}

pub async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|_| Error::Internal)?
}

impl Spool {
    pub fn open(dir: &Path) -> Result<Arc<Self>> {
        let path = dir.join("log-spool.sqlite");
        match OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&path)
        {
            Ok(_) => (),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => (),
            Err(e) => return Err(e.into()),
        }
        check_private(&path, false)?;
        let mut db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
        db.execute_batch(
            "PRAGMA synchronous=FULL; PRAGMA journal_mode=DELETE;
            PRAGMA temp_store=MEMORY; PRAGMA busy_timeout=100; PRAGMA secure_delete=ON;",
        )?;
        let max_pages = match std::env::var("OBSERVE_SPOOL_MAX_PAGES") {
            Ok(value) => value.parse::<u32>().map_err(|_| Error::Invalid)?,
            Err(std::env::VarError::NotPresent) => 8192,
            Err(_) => return Err(Error::Invalid),
        };
        if !(8..=8192).contains(&max_pages) {
            return Err(Error::Invalid);
        }
        let effective: u32 =
            db.pragma_update_and_check(None, "max_page_count", max_pages, |r| r.get(0))?;
        if effective != max_pages {
            return Err(Error::State);
        }
        let tx = db.transaction()?;
        let version: i32 = tx.query_row("PRAGMA user_version", [], |r| r.get(0))?;
        if version == 0 {
            tx.execute_batch("CREATE TABLE entries (
                id INTEGER PRIMARY KEY, tenant TEXT NOT NULL, subject TEXT NOT NULL,
                payload BLOB NOT NULL, n INTEGER NOT NULL CHECK(n>0), state TEXT NOT NULL,
                due_ms INTEGER NOT NULL DEFAULT 0, retries INTEGER NOT NULL DEFAULT 0,
                peer INTEGER, replay INTEGER NOT NULL DEFAULT 0, unknown INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE residual_writes (tenant TEXT NOT NULL, peer INTEGER NOT NULL, replay INTEGER NOT NULL, n INTEGER NOT NULL);
                CREATE TABLE totals (tenant TEXT PRIMARY KEY, durable INTEGER NOT NULL DEFAULT 0,
                    ack INTEGER NOT NULL DEFAULT 0, unknown_attempt_records INTEGER NOT NULL DEFAULT 0,
                    capacity_refused INTEGER NOT NULL DEFAULT 0);
                PRAGMA user_version=1;")?;
        } else if version != 1 {
            return Err(Error::State);
        }
        // The process lock excludes another live owner. An interrupted send may
        // have reached Loki. Keep its charge until that specific peer has exited.
        tx.execute_batch("UPDATE totals SET unknown_attempt_records=unknown_attempt_records+
            (SELECT COALESCE(SUM(n),0) FROM entries WHERE entries.tenant=totals.tenant AND state='sending');
            INSERT INTO residual_writes SELECT tenant,peer,replay,n FROM entries WHERE state='sending';
            UPDATE entries SET state='ready',peer=NULL,unknown=1 WHERE state='sending';
            UPDATE entries SET state='ready' WHERE state IN ('owned','held_destination');")?;
        tx.commit()?;
        Ok(Arc::new(Self {
            connection: Mutex::new(db),
            fault: false.into(),
        }))
    }

    fn db(&self) -> Result<MutexGuard<'_, Connection>> {
        self.connection.lock().map_err(|_| Error::State)
    }

    /// Stop new dispatch on uncertain local state. The single owner reconciles
    /// committed rows on restart after the operator restores private storage.
    fn update<T>(&self, f: impl FnOnce(&mut Connection) -> Result<T>) -> Result<T> {
        use std::sync::atomic::Ordering::SeqCst;
        let mut db = self.db()?;
        if self.fault.load(SeqCst) {
            return Err(Error::State);
        }
        let result = f(&mut db);
        if matches!(result, Err(Error::State)) {
            self.fault.store(true, SeqCst);
            eprintln!("observe-backend: log spool fault; check private storage and restart");
        }
        result
    }

    pub fn admit(&self, grant: &Grant, records: Vec<Record>) -> Result<Entry> {
        if !records.iter().all(|r| r.matches_writer(grant)) {
            return Err(Error::Denied);
        }
        let payload = serde_json::to_vec(&records).map_err(|_| Error::State)?;
        self.update(|db| {
            let tx = db.transaction()?;
            let tenant = grant.tenant();
            let (entries, bytes, mine, my_bytes): (i64, i64, i64, i64) = tx.query_row(
                "SELECT count(*),coalesce(sum(length(payload)),0),count(*) FILTER(WHERE tenant=?1),
             coalesce(sum(length(payload)) FILTER(WHERE tenant=?1),0) FROM entries",
                [tenant],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )?;
            let size = payload.len() as i64;
            let n = records.len() as i64;
            tx.execute("INSERT OR IGNORE INTO totals(tenant) VALUES(?1)", [tenant])?;
            if entries >= GLOBAL_BATCHES
                || mine >= TENANT_BATCHES
                || bytes + size > GLOBAL_BYTES
                || my_bytes + size > TENANT_BYTES
            {
                tx.execute(
                    "UPDATE totals SET capacity_refused=capacity_refused+?2 WHERE tenant=?1",
                    params![tenant, n],
                )?;
                tx.commit()?;
                return Err(Error::Busy);
            }
            tx.execute(
                "INSERT INTO entries(tenant,subject,payload,n,state) VALUES(?1,?2,?3,?4,'owned')",
                params![tenant, grant.subject, payload, n],
            )?;
            let id = tx.last_insert_rowid();
            tx.execute(
                "UPDATE totals SET durable=durable+?2 WHERE tenant=?1",
                params![tenant, n],
            )?;
            tx.commit()?;
            Ok(Entry {
                id,
                tenant: tenant.to_owned(),
                subject: grant.subject.clone(),
                records,
            })
        })
    }

    pub fn begin_send(&self, id: i64, tenant: &str, pid: i32, replay: bool) -> Result<()> {
        self.update(|db| {
            // Charge lost exchanges independently, including after a retry receives 204.
            let (all, mine): (usize, usize) = db.query_row(
                "SELECT count(*),count(*) FILTER(WHERE tenant=?1) FROM (
               SELECT tenant FROM entries WHERE state='sending' AND replay=?2 UNION ALL
               SELECT tenant FROM residual_writes WHERE replay=?2)",
                params![tenant, replay],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            let (global, per_tenant) = if replay { (2, 1) } else { (8, 2) };
            if all >= global || mine >= per_tenant {
                return Err(Error::Busy);
            }
            let changed = db.execute(
                "UPDATE entries SET state='sending',peer=?2,replay=?3 WHERE id=?1 AND state='owned'",
                params![id, pid, replay],
            )?;
            if changed != 1 {
                return Err(Error::State);
            }
            Ok(())
        })
    }

    pub fn finish(&self, id: i64, outcome: Outcome) -> Result<()> {
        self.update(|db| {
            let tx = db.transaction()?;
            let (tenant, n, retries): (String, i64, u32) = tx.query_row(
                "SELECT tenant,n,retries FROM entries WHERE id=?1 AND state IN ('sending','owned')",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )?;
            match outcome {
                Outcome::Ack => {
                    tx.execute(
                        "UPDATE totals SET ack=ack+?2 WHERE tenant=?1",
                        params![tenant, n],
                    )?;
                    tx.execute("DELETE FROM entries WHERE id=?1", [id])?;
                }
                Outcome::Held(reason) => {
                    tx.execute(
                        "UPDATE entries SET state=?2,peer=NULL WHERE id=?1",
                        params![id, reason],
                    )?;
                }
                Outcome::Offline
                | Outcome::Unknown(_)
                | Outcome::DestinationDenied
                | Outcome::LostReply => {
                    let unknown = !matches!(outcome, Outcome::Offline);
                    if matches!(outcome, Outcome::LostReply) {
                        tx.execute("INSERT INTO residual_writes SELECT tenant,peer,replay,n FROM entries WHERE id=?1",[id])?;
                    }
                    let backoff = 500_i64 << retries.min(4);
                    let retry_after = match outcome {
                        Outcome::Unknown(s) => {
                            i64::try_from(s.saturating_mul(1000)).unwrap_or(i64::MAX)
                        }
                        _ => 0,
                    };
                    let jitter = id % 127;
                    let due_ms = ((now_ns() / 1_000_000) as i64 + jitter)
                        .saturating_add(backoff.max(retry_after));
                    tx.execute(
                        "UPDATE entries SET state=?2,peer=NULL,
                        unknown=max(unknown,?3),retries=min(retries+1,16),due_ms=?4 WHERE id=?1",
                        params![
                            id,
                            match outcome {
                                Outcome::DestinationDenied => "held_destination",
                                _ => "ready",
                            },
                            unknown,
                            due_ms
                        ],
                    )?;
                    if unknown {
                        tx.execute("UPDATE totals SET unknown_attempt_records=unknown_attempt_records+?2 WHERE tenant=?1",
                            params![tenant,n])?;
                    }
                }
            }
            tx.commit()?;
            Ok(())
        })
    }

    pub fn claim(&self, after_tenant: &str) -> Result<Option<Entry>> {
        self.update(|db| {
            let tx = db.transaction()?;
            let peers = tx
                .prepare("SELECT DISTINCT peer FROM residual_writes")?
                .query_map([], |r| r.get::<_, i32>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            for pid in peers {
                // Signal zero only checks existence. PID reuse delays replay conservatively.
                if pid > 0
                    && unsafe { libc::kill(pid, 0) } == -1
                    && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
                {
                    tx.execute("DELETE FROM residual_writes WHERE peer=?1", [pid])?;
                }
            }
            let row = tx
                .query_row(
                    "SELECT id,tenant,subject,payload,n FROM entries WHERE state='ready' AND due_ms<=?1
                ORDER BY tenant<=?2,tenant,due_ms,id LIMIT 1",
                    params![(now_ns() / 1_000_000) as i64, after_tenant],
                    |r| {
                        Ok((
                            r.get::<_, i64>(0)?,
                            r.get(1)?,
                            r.get(2)?,
                            r.get::<_, Vec<u8>>(3)?,
                            r.get::<_, usize>(4)?,
                        ))
                    },
                )
                .optional()?;
            let entry = if let Some((id, tenant, subject, bytes, n)) = row {
                let records: Vec<Record> =
                    serde_json::from_slice(&bytes).map_err(|_| Error::State)?;
                if n != records.len() || n == 0 {
                    return Err(Error::State);
                }
                tx.execute("UPDATE entries SET state='owned' WHERE id=?1", [id])?;
                Some(Entry {
                    id,
                    tenant,
                    subject,
                    records,
                })
            } else {
                None
            };
            tx.commit()?;
            Ok(entry)
        })
    }

    pub fn status(&self, tenant: &str) -> Result<Value> {
        let db = self.db()?;
        let totals = db.query_row(
            "SELECT durable,ack,unknown_attempt_records,capacity_refused FROM totals WHERE tenant=?1",
            [tenant], |r| Ok([r.get::<_, i64>(0)?, r.get(1)?, r.get(2)?, r.get(3)?])
        ).optional()?.unwrap_or_default();
        let residual: i64 = db.query_row(
            "SELECT coalesce(sum(n),0) FROM residual_writes WHERE tenant=?1",
            [tenant],
            |r| r.get(0),
        )?;
        let pending = db.query_row(
            "SELECT coalesce(sum(n),0), coalesce(sum(length(payload)),0), coalesce(sum(n*unknown),0),
                coalesce(sum(n) FILTER(WHERE state='held_authority'),0),
                coalesce(sum(n) FILTER(WHERE state='held_window'),0),
                coalesce(sum(n) FILTER(WHERE state='held_invalid'),0),
                coalesce(sum(n) FILTER(WHERE state='held_destination'),0),
                coalesce(sum(n) FILTER(WHERE state='sending'),0)
             FROM entries WHERE tenant=?1", [tenant], |r| Ok([
                r.get::<_, i64>(0)?,r.get(1)?,r.get(2)?,r.get(3)?,
                r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?
             ])
        )?;
        let [durable, acknowledged, unknown_attempts, refused] = totals;
        let [records, bytes, prior_unknown, authority, window, invalid, destination, sending] =
            pending;
        Ok(json!({
            "durable_records": durable,
            "store_acknowledged_records": acknowledged,
            "unknown_record_attempts": unknown_attempts,
            "capacity_refused_records": refused,
            "pending_records": records,
            "pending_bytes": bytes,
            "pending_with_prior_unknown": prior_unknown,
            "unsettled_dispatch_records": sending,
            "residual_remote_attempt_records": residual,
            "held_authority_records": authority,
            "held_time_window_records": window,
            "held_invalid_records": invalid,
            "held_destination_records": destination,
            "storage_fault": self.fault.load(std::sync::atomic::Ordering::SeqCst),
            "query_visibility": "requires_authorized_query",
        }))
    }
}
