use chrono::{DateTime, Utc};
use serde::Serialize;

#[derive(Clone, Serialize, Debug)]
pub enum TransferType {
    Sent,
    Received,
}

#[derive(Clone, Serialize)]
pub struct Transfer {
    pub date: DateTime<Utc>,
    pub amount: f64,
    pub transfer_type: TransferType,
    pub signature: String,
}