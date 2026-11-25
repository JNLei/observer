//! Operator-owned metadata. Only this module constructs a tenant grant.
use crate::{Error, Result};
use hyper::header::{HeaderMap, AUTHORIZATION};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, DirBuilder, File, OpenOptions},
    io::Write,
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt},
    path::Path,
    sync::{Mutex, MutexGuard},
    time::Duration,
};

pub fn name_ok(s: &str) -> bool {
    (1..=48).contains(&s.len())
        && s.as_bytes()[0].is_ascii_lowercase()
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'_' | b'-'))
}

pub fn random_hex<const N: usize>() -> Result<String> {
    let mut bytes = [0u8; N];
    getrandom::fill(&mut bytes).map_err(|_| Error::Internal)?;
    Ok(hex::encode(bytes))
}

#[derive(Clone)]
pub struct Key([u8; 32]);
impl Key {
    fn from_secret(secret: &[u8]) -> Self {
        Self(Sha256::digest(secret).into())
    }

    pub fn from_headers(headers: &HeaderMap) -> Result<Self> {
        let mut values = headers.get_all(AUTHORIZATION).iter();
        let secret = values
            .next()
            .and_then(|s| s.as_bytes().strip_prefix(b"Bearer "))
            .filter(|s| s.len() == 64 && s.iter().all(u8::is_ascii_hexdigit))
            .ok_or(Error::Unauthenticated)?;
        if values.next().is_some() {
            return Err(Error::Unauthenticated);
        }
        Ok(Self::from_secret(secret))
    }
}

#[derive(Clone, Copy, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Capture {
    MetadataOnly,
    TextBody,
}

#[derive(Clone, Copy, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    Ingest,
    Query,
    IngestTraces,
    QueryTraces,
}

#[derive(Clone, Copy, Default, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum TraceCapture {
    #[default]
    MetadataOnly,
    Full,
}

#[derive(Clone)]
pub struct TraceGrant {
    pub principal: Grant,
    pub account: u32,
    pub capture: TraceCapture,
}

#[derive(Clone)]
pub struct Grant {
    tenant: String,
    pub subject: String,
    pub service: String,
    pub capture: Capture,
}
impl Grant {
    pub fn tenant(&self) -> &str {
        &self.tenant
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TenantConfig {
    pub tenant: String,
    pub capture: Capture,
    #[serde(default)]
    pub trace_capture: TraceCapture,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CredentialConfig {
    service: String,
    permissions: Vec<Action>,
}

#[derive(Serialize)]
pub struct Credential {
    pub id: String,
    pub token: String,
}

pub struct Authority(Mutex<Connection>);

pub(crate) fn check_private(path: &Path, directory: bool) -> Result<()> {
    let m = fs::symlink_metadata(path).map_err(|_| Error::State)?;
    // SAFETY: geteuid takes no arguments and has no failure mode.
    if m.uid() != unsafe { libc::geteuid() }
        || m.mode() & 0o077 != 0
        || (directory && !m.is_dir())
        || (!directory && (!m.is_file() || m.nlink() != 1))
    {
        return Err(Error::State);
    }
    Ok(())
}

impl Authority {
    pub fn open(dir: &Path, initialize: bool) -> Result<Self> {
        if !dir.is_absolute() {
            return Err(Error::State);
        }
        if initialize {
            if let Err(err) = DirBuilder::new().mode(0o700).create(dir) {
                if err.kind() != std::io::ErrorKind::AlreadyExists {
                    return Err(Error::State);
                }
            }
        }
        check_private(dir, true)?;
        let path = dir.join("authority.sqlite");
        if initialize {
            match OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&path)
            {
                Ok(_) => (),
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => (),
                Err(_) => return Err(Error::State),
            }
        }
        check_private(&path, false)?;
        let db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
        db.busy_timeout(Duration::from_millis(if initialize { 2000 } else { 100 }))?;
        db.execute_batch("PRAGMA foreign_keys=ON; PRAGMA synchronous=FULL;")?;
        // Additive schema. Existing log credentials have no implicit trace permissions.
        db.execute_batch(
            "BEGIN IMMEDIATE;
            CREATE TABLE IF NOT EXISTS trace_tenants (
                account INTEGER PRIMARY KEY AUTOINCREMENT CHECK(account BETWEEN 1 AND 4294967295),
                tenant TEXT NOT NULL UNIQUE REFERENCES tenants(name), full_capture INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS trace_permissions (
                subject TEXT PRIMARY KEY REFERENCES credentials(id) ON DELETE CASCADE,
                write_spans INTEGER NOT NULL, read_spans INTEGER NOT NULL
            );
            COMMIT;",
        )?;
        Ok(Self(Mutex::new(db)))
    }

    fn db(&self) -> Result<MutexGuard<'_, Connection>> {
        self.0.lock().map_err(|_| Error::State)
    }

    pub fn provision(&self, dir: &Path, recover: bool) -> Result<()> {
        let mut db = self.db()?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if !recover {
            tx.execute_batch("
                CREATE TABLE IF NOT EXISTS authority (slot INTEGER PRIMARY KEY CHECK(slot=1), ticket BLOB, owner BLOB);
                CREATE TABLE IF NOT EXISTS tenants (name TEXT PRIMARY KEY, text_body INTEGER NOT NULL CHECK(text_body IN (0,1)));
                CREATE TABLE IF NOT EXISTS credentials (
                    id TEXT PRIMARY KEY, digest BLOB NOT NULL UNIQUE, tenant TEXT NOT NULL REFERENCES tenants(name),
                    service TEXT NOT NULL, ingest INTEGER NOT NULL, query_logs INTEGER NOT NULL
                );")?;
        }
        let exists: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM authority WHERE slot=1)",
            [],
            |r| r.get(0),
        )?;
        if exists && !recover {
            return Ok(());
        }
        if !exists && recover {
            return Err(Error::State);
        }

        let token = random_hex::<32>()?;
        tx.execute(
            "INSERT OR REPLACE INTO authority(slot,ticket,owner) VALUES(1,?1,NULL)",
            [Key::from_secret(token.as_bytes()).0.as_slice()],
        )?;
        // Publish a private ticket before committing its hash. A crash in between
        // can require local owner recovery, but cannot grant a second owner.
        let tmp = dir.join(format!(".ticket-{}", random_hex::<16>()?));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&tmp)?;
        writeln!(file, "{token}")?;
        file.sync_all()?;
        fs::rename(tmp, dir.join("bootstrap.token"))?;
        File::open(dir)?.sync_all()?;
        tx.commit()?;
        Ok(())
    }

    pub fn bootstrap(&self, key: &Key) -> Result<Credential> {
        let token = random_hex::<32>()?;
        let count = self.db()?.execute(
            "UPDATE authority SET ticket=NULL,owner=?1 WHERE slot=1 AND ticket=?2",
            params![
                Key::from_secret(token.as_bytes()).0.as_slice(),
                key.0.as_slice()
            ],
        )?;
        if count != 1 {
            return Err(Error::Unauthenticated);
        }
        Ok(Credential {
            id: "owner".into(),
            token,
        })
    }

    fn require_owner(db: &Connection, key: &Key) -> Result<()> {
        let owner: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM authority WHERE owner=?1)",
            [key.0.as_slice()],
            |r| r.get(0),
        )?;
        if owner {
            Ok(())
        } else {
            Err(Error::Denied)
        }
    }

    pub fn owner(&self, key: &Key) -> Result<()> {
        Self::require_owner(&*self.db()?, key)
    }

    pub fn create_tenant(&self, key: &Key, spec: TenantConfig) -> Result<()> {
        let mut db = self.db()?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::require_owner(&tx, key)?;
        if !name_ok(&spec.tenant) {
            return Err(Error::Invalid);
        }
        let changed = tx.execute(
            "INSERT INTO tenants(name,text_body) VALUES(?1,?2) ON CONFLICT(name) DO NOTHING",
            params![spec.tenant, spec.capture == Capture::TextBody],
        )?;
        if changed == 0 {
            return Err(Error::Conflict);
        }
        tx.execute(
            "INSERT INTO trace_tenants(tenant,full_capture) VALUES(?1,?2)",
            params![spec.tenant, spec.trace_capture == TraceCapture::Full],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn issue(&self, key: &Key, tenant: &str, spec: CredentialConfig) -> Result<Credential> {
        let mut db = self.db()?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::require_owner(&tx, key)?;
        if !name_ok(&spec.service)
            || spec.permissions.is_empty()
            || spec.permissions.len() > 4
            || spec
                .permissions
                .iter()
                .enumerate()
                .any(|(i, p)| spec.permissions[..i].contains(p))
        {
            return Err(Error::Invalid);
        }
        let id = random_hex::<16>()?;
        let token = random_hex::<32>()?;
        let changed = tx.execute(
            "INSERT INTO credentials(id,digest,tenant,service,ingest,query_logs)
            SELECT ?1,?2,name,?3,?4,?5 FROM tenants WHERE name=?6",
            params![
                id,
                Key::from_secret(token.as_bytes()).0.as_slice(),
                spec.service,
                spec.permissions.contains(&Action::Ingest),
                spec.permissions.contains(&Action::Query),
                tenant
            ],
        )?;
        if changed == 0 {
            return Err(Error::Missing);
        }
        let write = spec.permissions.contains(&Action::IngestTraces);
        let read = spec.permissions.contains(&Action::QueryTraces);
        if write || read {
            // Upgrade v0.2 tenants on their first explicit trace grant.
            tx.execute(
                "INSERT INTO trace_tenants(tenant,full_capture) SELECT ?1,0
                        WHERE NOT EXISTS(SELECT 1 FROM trace_tenants WHERE tenant=?1)",
                [tenant],
            )?;
            tx.execute(
                "INSERT INTO trace_permissions VALUES(?1,?2,?3)",
                params![id, write, read],
            )?;
        }
        tx.commit()?;
        Ok(Credential { id, token })
    }

    pub fn revoke(&self, key: &Key, tenant: &str, id: &str) -> Result<()> {
        let mut db = self.db()?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::require_owner(&tx, key)?;
        tx.execute(
            "DELETE FROM credentials WHERE tenant=?1 AND id=?2",
            params![tenant, id],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// The authority lock orders revocation against the local custody transaction.
    /// Replay uses the immutable provisioned credential ID, never a stored bearer.
    pub fn with_writer<T>(
        &self,
        tenant: &str,
        subject: &str,
        f: impl FnOnce(&Grant) -> Result<T>,
    ) -> Result<T> {
        let db = self.db()?;
        let grant = db
            .query_row(
                "SELECT c.tenant,c.id,c.service,t.text_body FROM credentials c
             JOIN tenants t ON t.name=c.tenant WHERE c.tenant=?1 AND c.id=?2 AND c.ingest=1",
                params![tenant, subject],
                read_grant,
            )
            .optional()?
            .ok_or(Error::Denied)?;
        f(&grant)
    }

    pub fn trace_grant(
        &self,
        key: &Key,
        target: Option<&str>,
        action: Action,
    ) -> Result<TraceGrant> {
        let db = self.db()?;
        let (principal, _, _) = verified_principal(&db, key, target)?;
        let (account, full, write, read) = db
            .query_row(
                "SELECT tt.account, tt.full_capture, p.write_spans, p.read_spans
             FROM trace_tenants tt JOIN trace_permissions p ON p.subject=?2 WHERE tt.tenant=?1",
                params![principal.tenant(), principal.subject],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get::<_, bool>(1)?,
                        row.get(2)?,
                        row.get(3)?,
                    ))
                },
            )
            .optional()?
            .ok_or(Error::Denied)?;
        let permitted = match action {
            Action::IngestTraces => write,
            Action::QueryTraces => read,
            _ => false,
        };
        if !permitted {
            return Err(Error::Denied);
        }
        Ok(TraceGrant {
            principal,
            account,
            capture: if full {
                TraceCapture::Full
            } else {
                TraceCapture::MetadataOnly
            },
        })
    }

    pub fn authorize(&self, key: &Key, target: Option<&str>, action: Action) -> Result<Grant> {
        let db = self.db()?;
        let (grant, ingest, query) = verified_principal(&db, key, target)?;
        let permitted = match action {
            Action::Ingest => ingest,
            Action::Query => query,
            _ => false,
        };
        if !permitted {
            return Err(Error::Denied);
        }
        Ok(grant)
    }
}

fn verified_principal(
    db: &Connection,
    key: &Key,
    target: Option<&str>,
) -> Result<(Grant, bool, bool)> {
    let (grant, ingest, query) = db
        .query_row(
            "SELECT c.tenant,c.id,c.service,t.text_body,c.ingest,c.query_logs
             FROM credentials c JOIN tenants t ON t.name=c.tenant WHERE c.digest=?1",
            [key.0.as_slice()],
            |row| {
                Ok((
                    read_grant(row)?,
                    row.get::<_, bool>(4)?,
                    row.get::<_, bool>(5)?,
                ))
            },
        )
        .optional()?
        .ok_or(Error::Unauthenticated)?;
    if target.is_some_and(|tenant| tenant != grant.tenant()) {
        return Err(Error::Denied);
    }
    Ok((grant, ingest, query))
}

fn read_grant(row: &rusqlite::Row<'_>) -> rusqlite::Result<Grant> {
    Ok(Grant {
        tenant: row.get(0)?,
        subject: row.get(1)?,
        service: row.get(2)?,
        capture: if row.get::<_, bool>(3)? {
            Capture::TextBody
        } else {
            Capture::MetadataOnly
        },
    })
}
