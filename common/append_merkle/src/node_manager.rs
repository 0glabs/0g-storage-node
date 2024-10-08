use crate::HashElement;
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::error;

pub struct NodeManager<E: HashElement> {
    cache: HashMap<(usize, usize), E>,
    db: Arc<dyn NodeDatabase<E>>,
}

impl<E: HashElement> NodeManager<E> {
    pub fn new(db: Arc<dyn NodeDatabase<E>>) -> Self {
        Self {
            cache: HashMap::new(),
            db,
        }
    }

    pub fn get_node(&self, layer: usize, pos: usize) -> Option<E> {
        match self.cache.get(&(layer, pos)) {
            Some(node) => Some(node.clone()),
            None => self.db.get_node(layer, pos).unwrap_or_else(|e| {
                error!("Failed to get node: {}", e);
                None
            }),
        }
    }

    pub fn add_node(&mut self, layer: usize, pos: usize, node: E) {
        if let Err(e) = self.db.save_node(layer, pos, &node) {
            error!("Failed to save node: {}", e);
        }
        self.cache.insert((layer, pos), node);
    }
}

pub trait NodeDatabase<E: HashElement>: Send + Sync {
    fn get_node(&self, layer: usize, pos: usize) -> Result<Option<E>>;
    fn save_node(&self, layer: usize, pos: usize, node: &E) -> Result<()>;
}

/// A dummy database structure for in-memory merkle tree that will not read/write db.
pub struct EmptyNodeDatabase {}
impl<E: HashElement> NodeDatabase<E> for EmptyNodeDatabase {
    fn get_node(&self, _layer: usize, _pos: usize) -> Result<Option<E>> {
        Ok(None)
    }

    fn save_node(&self, _layer: usize, _pos: usize, _node: &E) -> Result<()> {
        Ok(())
    }
}
