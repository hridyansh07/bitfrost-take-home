/// Domain Events that are read and ingested into the service
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct HlEvent {
    pub seq: u64,
    pub event_id: String,
    pub ts: u64,
    pub account: String,
    pub symbol: String,
    pub side: String,
    pub px: String,
    pub qty: String,
    pub fee: String,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PmEvent {
    pub sequence: u64,
    pub id: String,
    pub timestamp_ms: u64,
    pub user: String,
    pub market: String,
    pub outcome: String,
    pub action: String,
    pub price: f64,
    pub size: i64,
    pub fee_bps: i64,
}
