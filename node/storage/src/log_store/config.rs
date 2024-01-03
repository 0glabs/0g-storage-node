use anyhow::{anyhow, Result};
use kvdb::{DBKey, DBOp};
use ssz::{Decode, Encode};

use crate::LogManager;

use super::log_manager::COL_MISC;

pub trait Configurable {
    fn get_config(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn set_config(&self, key: &[u8], value: &[u8]) -> Result<()>;
    fn remove_config(&self, key: &[u8]) -> Result<()>;

    fn exec_configs(&self, tx: ConfigTx) -> Result<()>;
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
    fn get_config_decoded<K: AsRef<[u8]>, T: Decode>(&self, key: &K) -> Result<Option<T>> {
        match self.get_config(key.as_ref())? {
            Some(val) => Ok(Some(
                T::from_ssz_bytes(&val).map_err(|e| anyhow!("SSZ decode error: {:?}", e))?,
            )),
            None => Ok(None),
        }
    }

    fn set_config_encoded<K: AsRef<[u8]>, T: Encode>(&self, key: &K, value: &T) -> Result<()> {
        self.set_config(key.as_ref(), &value.as_ssz_bytes())
    }

    fn remove_config_by_key<K: AsRef<[u8]>>(&self, key: &K) -> Result<()> {
        self.remove_config(key.as_ref())
    }
}

impl<T: ?Sized + Configurable> ConfigurableExt for T {}

impl Configurable for LogManager {
    fn get_config(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.db.get(COL_MISC, key)?)
    }

    fn set_config(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.db.put(COL_MISC, key, value)?;
        Ok(())
    }

    fn remove_config(&self, key: &[u8]) -> Result<()> {
        Ok(self.db.delete(COL_MISC, key)?)
    }

    fn exec_configs(&self, tx: ConfigTx) -> Result<()> {
        let mut db_tx = self.db.transaction();
        db_tx.ops = tx.ops;
        self.db.write(db_tx)?;

        Ok(())
    }
}
