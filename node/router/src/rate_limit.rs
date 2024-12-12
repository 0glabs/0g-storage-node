use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use network::{
    rpc::rate_limiter::{Limiter, Quota, RateLimitedErr},
    types::GossipKind,
    PeerId, PubsubMessage,
};

pub struct PubsubRateLimiter {
    init_time: Instant,
    limiters: Limiter<PeerId>,
    limiters_by_topic: HashMap<GossipKind, Limiter<PeerId>>,
}

impl PubsubRateLimiter {
    pub fn new(n: u64, period: Duration) -> Result<Self, String> {
        Ok(Self {
            init_time: Instant::now(),
            limiters: Limiter::from_quota(Quota::n_every(n, period))?,
            limiters_by_topic: Default::default(),
        })
    }

    pub fn limit_by_topic(
        mut self,
        kind: GossipKind,
        n: u64,
        period: Duration,
    ) -> Result<Self, String> {
        let limiter = Limiter::from_quota(Quota::n_every(n, period))?;
        self.limiters_by_topic.insert(kind, limiter);
        Ok(self)
    }

    pub fn allows(
        &mut self,
        peer_id: &PeerId,
        msg: &PubsubMessage,
    ) -> Result<(), (Option<GossipKind>, RateLimitedErr)> {
        let time_since_start = self.init_time.elapsed();

        if let Err(err) = self.limiters.allows(time_since_start, peer_id, 1) {
            return Err((None, err));
        }

        let kind = msg.kind();
        if let Some(limiter) = self.limiters_by_topic.get_mut(&kind) {
            if let Err(err) = limiter.allows(time_since_start, peer_id, 1) {
                return Err((Some(kind), err));
            }
        }

        Ok(())
    }

    pub fn prune(&mut self) {
        let time_since_start = self.init_time.elapsed();

        self.limiters.prune(time_since_start);

        for limiter in self.limiters_by_topic.values_mut() {
            limiter.prune(time_since_start);
        }
    }
}
