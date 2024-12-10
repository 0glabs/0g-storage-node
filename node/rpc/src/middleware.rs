use std::{collections::HashMap, sync::Arc, time::Instant};

use jsonrpsee::core::middleware::Middleware;
use metrics::{register_meter_with_group, Histogram, Meter, Sample};
use parking_lot::RwLock;

struct RpcMetric {
    qps: Arc<dyn Meter>,
    latency: Arc<dyn Histogram>,
}

impl RpcMetric {
    fn new(method_name: &String) -> Self {
        let group = format!("rpc_{}", method_name);

        Self {
            qps: register_meter_with_group(group.as_str(), "qps"),
            latency: Sample::ExpDecay(0.015).register_with_group(group.as_str(), "latency", 1024),
        }
    }
}

#[derive(Clone, Default)]
pub struct Metrics {
    metrics_by_method: Arc<RwLock<HashMap<String, RpcMetric>>>,
}

impl Middleware for Metrics {
    type Instant = Instant;

    fn on_request(&self) -> Self::Instant {
        Instant::now()
    }

    fn on_call(&self, name: &str) {
        let mut metrics_by_method = self.metrics_by_method.write();
        let entry = metrics_by_method
            .entry(name.to_string())
            .or_insert_with_key(RpcMetric::new);
        entry.qps.mark(1);
    }

    fn on_result(&self, name: &str, _success: bool, started_at: Self::Instant) {
        let mut metrics_by_method = self.metrics_by_method.write();
        let entry = metrics_by_method
            .entry(name.to_string())
            .or_insert_with_key(RpcMetric::new);
        entry.latency.update_since(started_at);
    }
}
