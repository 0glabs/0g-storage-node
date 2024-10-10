use crate::HashElement;
use anyhow::Result;
use lru::LruCache;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tracing::error;

pub struct NodeManager<E: HashElement> {
    cache: LruCache<(usize, usize), E>,
    layer_size: Vec<usize>,
    db: Arc<dyn NodeDatabase<E>>,
}

impl<E: HashElement> NodeManager<E> {
    pub fn new(db: Arc<dyn NodeDatabase<E>>, capacity: usize) -> Self {
        Self {
            cache: LruCache::new(NonZeroUsize::new(capacity).expect("capacity should be non-zero")),
            layer_size: vec![],
            db,
        }
    }

    pub fn new_dummy() -> Self {
        Self {
            cache: LruCache::unbounded(),
            layer_size: vec![],
            db: Arc::new(EmptyNodeDatabase {}),
        }
    }

    pub fn push_node(&mut self, layer: usize, node: E) {
        self.add_node(layer, self.layer_size[layer], node);
        self.layer_size[layer] += 1;
    }

    pub fn append_nodes(&mut self, layer: usize, nodes: &[E]) {
        let pos = &mut self.layer_size[layer];
        let mut saved_nodes = Vec::with_capacity(nodes.len());
        for node in nodes {
            self.cache.put((layer, *pos), node.clone());
            saved_nodes.push((layer, *pos, node));
            *pos += 1;
        }
        if let Err(e) = self.db.save_node_list(&saved_nodes) {
            error!("Failed to save node list: {:?}", e);
        }
    }

    pub fn get_node(&self, layer: usize, pos: usize) -> Option<E> {
        match self.cache.peek(&(layer, pos)) {
            Some(node) => Some(node.clone()),
            None => self.db.get_node(layer, pos).unwrap_or_else(|e| {
                error!("Failed to get node: {}", e);
                None
            }),
        }
    }

    pub fn get_nodes(&self, layer: usize, start_pos: usize, end_pos: usize) -> NodeIterator<E> {
        NodeIterator {
            node_manager: self,
            layer,
            start_pos,
            end_pos,
        }
    }

    pub fn add_node(&mut self, layer: usize, pos: usize, node: E) {
        if let Err(e) = self.db.save_node(layer, pos, &node) {
            error!("Failed to save node: {}", e);
        }
        self.cache.put((layer, pos), node);
    }

    pub fn add_layer(&mut self) {
        self.layer_size.push(0);
    }

    pub fn layer_size(&self, layer: usize) -> usize {
        self.layer_size[layer]
    }

    pub fn num_layers(&self) -> usize {
        self.layer_size.len()
    }

    pub fn truncate_nodes(&mut self, layer: usize, pos_end: usize) {
        let mut removed_nodes = Vec::new();
        for pos in pos_end..self.layer_size[layer] {
            self.cache.pop(&(layer, pos));
            removed_nodes.push((layer, pos));
        }
        if let Err(e) = self.db.remove_node_list(&removed_nodes) {
            error!("Failed to remove node list: {:?}", e);
        }
        self.layer_size[layer] = pos_end;
    }

    pub fn truncate_layer(&mut self, layer: usize) {
        self.truncate_nodes(layer, 0);
        if layer == self.num_layers() - 1 {
            self.layer_size.pop();
        }
    }
}

pub struct NodeIterator<'a, E: HashElement> {
    node_manager: &'a NodeManager<E>,
    layer: usize,
    start_pos: usize,
    end_pos: usize,
}

impl<'a, E: HashElement> Iterator for NodeIterator<'a, E> {
    type Item = E;

    fn next(&mut self) -> Option<Self::Item> {
        if self.start_pos < self.end_pos {
            let r = self.node_manager.get_node(self.layer, self.start_pos);
            self.start_pos += 1;
            r
        } else {
            None
        }
    }
}

pub trait NodeDatabase<E: HashElement>: Send + Sync {
    fn get_node(&self, layer: usize, pos: usize) -> Result<Option<E>>;
    fn save_node(&self, layer: usize, pos: usize, node: &E) -> Result<()>;
    /// `nodes` are a list of tuples `(layer, pos, node)`.
    fn save_node_list(&self, nodes: &[(usize, usize, &E)]) -> Result<()>;
    fn remove_node_list(&self, nodes: &[(usize, usize)]) -> Result<()>;
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

    fn save_node_list(&self, _nodes: &[(usize, usize, &E)]) -> Result<()> {
        Ok(())
    }

    fn remove_node_list(&self, _nodes: &[(usize, usize)]) -> Result<()> {
        Ok(())
    }
}
