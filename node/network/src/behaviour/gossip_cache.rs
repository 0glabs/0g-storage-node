use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use crate::types::GossipKind;
use crate::GossipTopic;

use tokio_util::time::delay_queue::{DelayQueue, Key};

/// Store of gossip messages that we failed to publish and will try again later. By default, all
/// messages are ignored. This behaviour can be changed using `GossipCacheBuilder::default_timeout`
/// to apply the same delay to every kind. Individual timeouts for specific kinds can be set and
/// will overwrite the default_timeout if present.
#[derive(Default)]
pub struct GossipCache {
    /// Expire timeouts for each topic-msg pair.
    expirations: DelayQueue<(GossipTopic, Vec<u8>)>,
    /// Messages cached for each topic.
    topic_msgs: HashMap<GossipTopic, HashMap<Vec<u8>, Key>>,

    default_timeout: Option<Duration>,
    /// Timeout for pubsub messages.
    timeouts: HashMap<GossipKind, Duration>,
}

impl GossipCache {
    #[cfg(test)]
    pub fn new_with_default_timeout(timeout: Duration) -> Self {
        Self {
            default_timeout: Some(timeout),
            ..Default::default()
        }
    }

    // Insert a message to be sent later.
    pub fn insert(&mut self, topic: GossipTopic, data: Vec<u8>) {
        let expire_timeout = self
            .timeouts
            .get(topic.kind())
            .cloned()
            .or(self.default_timeout);

        let expire_timeout = match expire_timeout {
            Some(expire_timeout) => expire_timeout,
            None => return,
        };

        match self
            .topic_msgs
            .entry(topic.clone())
            .or_default()
            .entry(data.clone())
        {
            Entry::Occupied(key) => self.expirations.reset(key.get(), expire_timeout),
            Entry::Vacant(entry) => {
                let key = self.expirations.insert((topic, data), expire_timeout);
                entry.insert(key);
            }
        }
    }

    // Get the registered messages for this topic.
    pub fn retrieve(&mut self, topic: &GossipTopic) -> Option<impl Iterator<Item = Vec<u8>> + '_> {
        if let Some(msgs) = self.topic_msgs.remove(topic) {
            for (_, key) in msgs.iter() {
                self.expirations.remove(key);
            }
            Some(msgs.into_keys())
        } else {
            None
        }
    }
}

impl futures::stream::Stream for GossipCache {
    type Item = Result<GossipTopic, String>; // We don't care to retrieve the expired data.

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.expirations.poll_expired(cx) {
            Poll::Ready(Some(Ok(expired))) => {
                let expected_key = expired.key();
                let (topic, data) = expired.into_inner();
                match self.topic_msgs.get_mut(&topic) {
                    Some(msgs) => {
                        let key = msgs.remove(&data);
                        debug_assert_eq!(key, Some(expected_key));
                        if msgs.is_empty() {
                            // no more messages for this topic.
                            self.topic_msgs.remove(&topic);
                        }
                    }
                    None => {
                        #[cfg(debug_assertions)]
                        panic!("Topic for registered message is not present.")
                    }
                }
                Poll::Ready(Some(Ok(topic)))
            }
            Poll::Ready(Some(Err(x))) => Poll::Ready(Some(Err(x.to_string()))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::types::GossipKind;

    use super::*;
    use futures::stream::StreamExt;

    #[tokio::test]
    async fn test_stream() {
        let mut cache = GossipCache::new_with_default_timeout(Duration::from_millis(300));
        let test_topic =
            GossipTopic::new(GossipKind::Example, crate::types::GossipEncoding::SSZSnappy);
        cache.insert(test_topic, vec![]);
        tokio::time::sleep(Duration::from_millis(300)).await;
        while cache.next().await.is_some() {}
        assert!(cache.expirations.is_empty());
        assert!(cache.topic_msgs.is_empty());
    }
}
