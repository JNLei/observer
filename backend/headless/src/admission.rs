//! Independent global and per-tenant reservations. Only active tenants occupy memory.
use crate::{Error, Result};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

pub struct Lane {
    global: usize,
    per_tenant: usize,
    active: Mutex<HashMap<String, usize>>,
}

impl Lane {
    pub fn new(global: usize, per_tenant: usize) -> Arc<Self> {
        Arc::new(Self {
            global,
            per_tenant,
            active: Mutex::default(),
        })
    }
    pub fn enter(self: &Arc<Self>, tenant: &str) -> Result<Reservation> {
        let mut counts = self.active.lock().map_err(|_| Error::Internal)?;
        if counts.values().sum::<usize>() >= self.global
            || counts.get(tenant).copied().unwrap_or(0) >= self.per_tenant
        {
            return Err(Error::Busy);
        }
        *counts.entry(tenant.to_owned()).or_default() += 1;
        Ok(Reservation {
            lane: self.clone(),
            tenant: tenant.to_owned(),
            indeterminate: false,
        })
    }
}

pub struct Reservation {
    lane: Arc<Lane>,
    tenant: String,
    indeterminate: bool,
}
impl Reservation {
    // A lost post-dispatch transport cannot attest that remote work terminated.
    // Keep this count charged until a coordinated backend/store restart.
    pub fn retain(&mut self) {
        self.indeterminate = true;
    }
}
impl Drop for Reservation {
    fn drop(&mut self) {
        if !self.indeterminate {
            let mut counts = self.lane.active.lock().unwrap();
            let active = counts.get_mut(&self.tenant).expect("active reservation");
            *active -= 1;
            if *active == 0 {
                counts.remove(&self.tenant);
            }
        }
    }
}
