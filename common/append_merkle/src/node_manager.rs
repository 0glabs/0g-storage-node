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
    db_tx: Option<Box<dyn NodeTransaction<E>>>,
}

impl<E: HashElement> NodeManager<E> {
    pub fn new(db: Arc<dyn NodeDatabase<E>>, capacity: usize) -> Self {
        Self {
            cache: LruCache::new(NonZeroUsize::new(capacity).expect("capacity should be non-zero")),
            layer_size: vec![],
            db,
            db_tx: None,
        }
    }

    pub fn new_dummy() -> Self {
        Self {
            cache: LruCache::unbounded(),
            layer_size: vec![],
            db: Arc::new(EmptyNodeDatabase {}),
            db_tx: None,
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
        let size = *pos;
        self.db_tx().save_layer_size(layer, size);
        self.db_tx().save_node_list(&saved_nodes);
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
        // No need to insert if the value is unchanged.
        if self.cache.get(&(layer, pos)) != Some(&node) {
            self.db_tx().save_node(layer, pos, &node);
            self.cache.put((layer, pos), node);
        }
    }

    pub fn add_layer(&mut self) {
        self.layer_size.push(0);
        let layer = self.layer_size.len() - 1;
        self.db_tx().save_layer_size(layer, 0);
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
        self.db_tx().remove_node_list(&removed_nodes);
        self.layer_size[layer] = pos_end;
        self.db_tx().save_layer_size(layer, pos_end);
    }

    pub fn truncate_layer(&mut self, layer: usize) {
        self.truncate_nodes(layer, 0);
        if layer == self.num_layers() - 1 {
            self.layer_size.pop();
            self.db_tx().remove_layer_size(layer);
        }
    }

    pub fn start_transaction(&mut self) {
        if self.db_tx.is_none() {
            error!("start new tx before commit");
        }
        self.db_tx = Some(self.db.start_transaction());
    }

    pub fn commit(&mut self) {
        let tx = match self.db_tx.take() {
            Some(tx) => tx,
            None => {
                error!("db_tx is None");
                return;
            }
        };
        if let Err(e) = self.db.commit(tx) {
            error!("Failed to commit db transaction: {}", e);
        }
    }

    fn db_tx(&mut self) -> &mut dyn NodeTransaction<E> {
        (*self.db_tx.as_mut().expect("tx checked")).as_mut()
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
    fn get_layer_size(&self, layer: usize) -> Result<Option<usize>>;
    fn start_transaction(&self) -> Box<dyn NodeTransaction<E>>;
    fn commit(&self, tx: Box<dyn NodeTransaction<E>>) -> Result<()>;
}

pub trait NodeTransaction<E: HashElement>: Send + Sync {
    fn save_node(&mut self, layer: usize, pos: usize, node: &E);
    /// `nodes` are a list of tuples `(layer, pos, node)`.
    fn save_node_list(&mut self, nodes: &[(usize, usize, &E)]);
    fn remove_node_list(&mut self, nodes: &[(usize, usize)]);
    fn save_layer_size(&mut self, layer: usize, size: usize);
    fn remove_layer_size(&mut self, layer: usize);
}

/// A dummy database structure for in-memory merkle tree that will not read/write db.
pub struct EmptyNodeDatabase {}
pub struct EmptyNodeTransaction {}
impl<E: HashElement> NodeDatabase<E> for EmptyNodeDatabase {
    fn get_node(&self, _layer: usize, _pos: usize) -> Result<Option<E>> {
        Ok(None)
    }
    fn get_layer_size(&self, _layer: usize) -> Result<Option<usize>> {
        Ok(None)
    }
    fn start_transaction(&self) -> Box<dyn NodeTransaction<E>> {
        Box::new(EmptyNodeTransaction {})
    }
    fn commit(&self, _tx: Box<dyn NodeTransaction<E>>) -> Result<()> {
        Ok(())
    }
}

impl<E: HashElement> NodeTransaction<E> for EmptyNodeTransaction {
    fn save_node(&mut self, _layer: usize, _pos: usize, _node: &E) {}

    fn save_node_list(&mut self, _nodes: &[(usize, usize, &E)]) {}

    fn remove_node_list(&mut self, _nodes: &[(usize, usize)]) {}

    fn save_layer_size(&mut self, _layer: usize, _size: usize) {}

    fn remove_layer_size(&mut self, _layer: usize) {}
}
