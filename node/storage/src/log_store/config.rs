use anyhow::{anyhow, Result};
use kvdb::{DBKey, DBOp};
use ssz::{Decode, Encode};

use crate::log_store::log_manager::{COL_MISC, DATA_DB_KEY, FLOW_DB_KEY};
use crate::LogManager;

macro_rules! db_operation {
    ($self:expr, $dest:expr, get, $key:expr) => {{
        let db = match $dest {
            DATA_DB_KEY => &$self.data_db,
            FLOW_DB_KEY => &$self.flow_db,
            _ => return Err(anyhow!("Invalid destination")),
        };
        Ok(db.get(COL_MISC, $key)?)
    }};

    ($self:expr, $dest:expr, put, $key:expr, $value:expr) => {{
        let db = match $dest {
            DATA_DB_KEY => &$self.data_db,
            FLOW_DB_KEY => &$self.flow_db,
            _ => return Err(anyhow!("Invalid destination")),
        };
        Ok(db.put(COL_MISC, $key, $value)?)
    }};

    ($self:expr, $dest:expr, delete, $key:expr) => {{
        let db = match $dest {
            DATA_DB_KEY => &$self.data_db,
            FLOW_DB_KEY => &$self.flow_db,
            _ => return Err(anyhow!("Invalid destination")),
        };
        Ok(db.delete(COL_MISC, $key)?)
    }};

    ($self:expr, $dest:expr, transaction, $tx:expr) => {{
        let db = match $dest {
            DATA_DB_KEY => &$self.data_db,
            FLOW_DB_KEY => &$self.flow_db,
            _ => return Err(anyhow!("Invalid destination")),
        };
        let mut db_tx = db.transaction();
        db_tx.ops = $tx.ops;
        Ok(db.write(db_tx)?)
    }};
}

pub trait Configurable {
    fn get_config(&self, key: &[u8], dest: &str) -> Result<Option<Vec<u8>>>;
    fn set_config(&self, key: &[u8], value: &[u8], dest: &str) -> Result<()>;
    fn remove_config(&self, key: &[u8], dest: &str) -> Result<()>;

    fn exec_configs(&self, tx: ConfigTx, dest: &str) -> Result<()>;
}

#[derive(Default)]
pub struct ConfigTx {
    ops: Vec<DBOp>,
}

impl ConfigTx {
    pub fn append(&mut self, other: &mut Self) {
        self.ops.append(&mut other.ops);
    }

    pub fn set_config<K: AsRef<[u8]>, T: Encode>(&mut self, key: &K, value: &T) {
        self.ops.push(DBOp::Insert {
            col: COL_MISC,
            key: DBKey::from_slice(key.as_ref()),
            value: value.as_ssz_bytes(),
        });
    }

    pub fn remove_config<K: AsRef<[u8]>>(&mut self, key: &K) {
        self.ops.push(DBOp::Delete {
            col: COL_MISC,
            key: DBKey::from_slice(key.as_ref()),
        });
    }
}

pub trait ConfigurableExt: Configurable {
    fn get_config_decoded<K: AsRef<[u8]>, T: Decode>(
        &self,
        key: &K,
        dest: &str,
    ) -> Result<Option<T>> {
        match self.get_config(key.as_ref(), dest)? {
            Some(val) => Ok(Some(
                T::from_ssz_bytes(&val).map_err(|e| anyhow!("SSZ decode error: {:?}", e))?,
            )),
            None => Ok(None),
        }
    }

    fn set_config_encoded<K: AsRef<[u8]>, T: Encode>(
        &self,
        key: &K,
        value: &T,
        dest: &str,
    ) -> Result<()> {
        self.set_config(key.as_ref(), &value.as_ssz_bytes(), dest)
    }

    fn remove_config_by_key<K: AsRef<[u8]>>(&self, key: &K, dest: &str) -> Result<()> {
        self.remove_config(key.as_ref(), dest)
    }
}

impl<T: ?Sized + Configurable> ConfigurableExt for T {}

impl Configurable for LogManager {
    fn get_config(&self, key: &[u8], dest: &str) -> Result<Option<Vec<u8>>> {
        db_operation!(self, dest, get, key)
    }

    fn set_config(&self, key: &[u8], value: &[u8], dest: &str) -> Result<()> {
        db_operation!(self, dest, put, key, value)
    }

    fn remove_config(&self, key: &[u8], dest: &str) -> Result<()> {
        db_operation!(self, dest, delete, key)
    }

    fn exec_configs(&self, tx: ConfigTx, dest: &str) -> Result<()> {
        db_operation!(self, dest, transaction, tx)
    }
}
