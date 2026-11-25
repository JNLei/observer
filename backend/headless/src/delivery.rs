//! The fresh request and the recovery worker use the same dispatch checks.
use crate::{
    api::App,
    identity::now_ns,
    spool::{blocking, Entry, Outcome},
    storage::StoreError,
    Error, Result,
};
use std::{sync::Arc, time::Duration};

pub async fn send(app: Arc<App>, entry: Entry, replay: bool) -> Result<Outcome> {
    let id = entry.id;
    let outcome = match app.store.writer().await {
        Ok(writer) => {
            let worker = app.clone();
            let pid = writer.peer_pid();
            // Look up the current writer by its provisioned ID and hold the
            // authority lock through the durable dispatch reservation.
            let prepared = blocking(move || {
                let grant =
                    worker
                        .authority
                        .with_writer(&entry.tenant, &entry.subject, |grant| {
                            if !entry.records.iter().all(|r| r.matches_writer(grant)) {
                                return Err(Error::Invalid);
                            }
                            let window = worker.store.write_time_range(now_ns());
                            if !entry
                                .records
                                .iter()
                                .all(|r| window.contains(&r.timestamp_ns.0))
                            {
                                return Err(Error::Expired);
                            }
                            worker.spool.begin_send(id, grant.tenant(), pid, replay)?;
                            Ok(grant.clone())
                        })?;
                Ok((grant, entry))
            })
            .await;
            match prepared {
                Ok((grant, entry)) => match writer.write(&grant, &entry.records).await {
                    Ok(()) => Outcome::Ack,
                    Err(StoreError::Offline | StoreError::Deadline) => Outcome::Offline,
                    Err(StoreError::Indeterminate) => Outcome::LostReply,
                    Err(StoreError::Response | StoreError::ResponseBudget) => Outcome::Unknown(0),
                    Err(StoreError::Throttled(delay)) => Outcome::Unknown(delay),
                    Err(StoreError::WriteDenied) => Outcome::DestinationDenied,
                },
                Err(Error::Denied) => Outcome::Held("held_authority"),
                Err(Error::Invalid) => Outcome::Held("held_invalid"),
                Err(Error::Expired) => Outcome::Held("held_window"),
                Err(Error::Busy | Error::State) => Outcome::Offline,
                Err(e) => return Err(e),
            }
        }
        Err(_) => Outcome::Offline,
    };
    blocking(move || app.spool.finish(id, outcome)).await?;
    Ok(outcome)
}

pub async fn recover(app: Arc<App>) {
    let mut after = String::new();
    loop {
        let spool = app.spool.clone();
        let cursor = after.clone();
        let result = match blocking(move || spool.claim(&cursor)).await {
            Ok(Some(entry)) => {
                after.clone_from(&entry.tenant);
                send(app.clone(), entry, true).await.map(|_| ())
            }
            Ok(None) => Ok(()),
            Err(e) => Err(e),
        };
        let pause = if result.is_err() {
            Duration::from_secs(5)
        } else {
            Duration::from_millis(50)
        };
        tokio::time::sleep(pause).await;
    }
}
