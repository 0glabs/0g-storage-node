//! Helper functions and an extension trait for Ethereum 2 ENRs.

pub use discv5::enr::{CombinedKey, EnrBuilder};

use super::enr_ext::CombinedKeyExt;
use super::ENR_FILENAME;
use crate::types::Enr;
use crate::NetworkConfig;
use discv5::enr::EnrKey;
use libp2p::core::identity::Keypair;
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;
use std::str::FromStr;

/// Either use the given ENR or load an ENR from file if it exists and matches the current NodeId
/// and sequence number.
/// If an ENR exists, with the same NodeId, this function checks to see if the loaded ENR from
/// disk is suitable to use, otherwise we increment the given ENR's sequence number.
pub fn use_or_load_enr(
    enr_key: &CombinedKey,
    local_enr: &mut Enr,
    config: &NetworkConfig,
) -> Result<(), String> {
    let enr_f = config.network_dir.join(ENR_FILENAME);
    if let Ok(mut enr_file) = File::open(enr_f.clone()) {
        let mut enr_string = String::new();
        match enr_file.read_to_string(&mut enr_string) {
            Err(_) => debug!("Could not read ENR from file"),
            Ok(_) => {
                match Enr::from_str(&enr_string) {
                    Ok(disk_enr) => {
                        // if the same node id, then we may need to update our sequence number
                        if local_enr.node_id() == disk_enr.node_id() {
                            if compare_enr(local_enr, &disk_enr) {
                                debug!(file = ?enr_f, "ENR loaded from disk");
                                // the stored ENR has the same configuration, use it
                                *local_enr = disk_enr;
                                return Ok(());
                            }

                            // same node id, different configuration - update the sequence number
                            // Note: local_enr is generated with default(0) attnets value,
                            // so a non default value in persisted enr will also update sequence number.
                            let new_seq_no = disk_enr.seq().checked_add(1).ok_or("ENR sequence number on file is too large. Remove it to generate a new NodeId")?;
                            local_enr.set_seq(new_seq_no, enr_key).map_err(|e| {
                                format!("Could not update ENR sequence number: {:?}", e)
                            })?;
                            debug!(seq = new_seq_no, "ENR sequence number increased");
                        }
                    }
                    Err(e) => {
                        warn!(error = ?e, "ENR from file could not be decoded");
                    }
                }
            }
        }
    }

    save_enr_to_disk(&config.network_dir, local_enr);

    Ok(())
}

/// Loads an ENR from file if it exists and matches the current NodeId and sequence number. If none
/// exists, generates a new one.
///
/// If an ENR exists, with the same NodeId, this function checks to see if the loaded ENR from
/// disk is suitable to use, otherwise we increment our newly generated ENR's sequence number.
pub fn build_or_load_enr(local_key: Keypair, config: &NetworkConfig) -> Result<Enr, String> {
    // Build the local ENR.
    // Note: Discovery should update the ENR record's IP to the external IP as seen by the
    // majority of our peers, if the CLI doesn't expressly forbid it.
    let enr_key = CombinedKey::from_libp2p(&local_key)?;
    let mut local_enr = build_enr(&enr_key, config)?;

    use_or_load_enr(&enr_key, &mut local_enr, config)?;
    Ok(local_enr)
}

pub fn create_enr_builder_from_config<T: EnrKey>(
    config: &NetworkConfig,
    enable_tcp: bool,
) -> EnrBuilder<T> {
    let mut builder = EnrBuilder::new("v4");
    if let Some(enr_address) = config.enr_address {
        builder.ip(enr_address);
    }
    if let Some(udp_port) = config.enr_udp_port {
        builder.udp(udp_port);
    }
    // we always give it our listening tcp port
    if enable_tcp {
        let tcp_port = config.enr_tcp_port.unwrap_or(config.libp2p_port);
        builder.tcp(tcp_port);
    }
    builder
}

/// Builds a lighthouse ENR given a `NetworkConfig`.
pub fn build_enr(enr_key: &CombinedKey, config: &NetworkConfig) -> Result<Enr, String> {
    let mut builder = create_enr_builder_from_config(config, true);

    builder
        .build(enr_key)
        .map_err(|e| format!("Could not build Local ENR: {:?}", e))
}

/// Defines the conditions under which we use the locally built ENR or the one stored on disk.
/// If this function returns true, we use the `disk_enr`.
fn compare_enr(local_enr: &Enr, disk_enr: &Enr) -> bool {
    // take preference over disk_enr address if one is not specified
    (local_enr.ip().is_none() || local_enr.ip() == disk_enr.ip())
        // tcp ports must match
        && local_enr.tcp() == disk_enr.tcp()
        // take preference over disk udp port if one is not specified
        && (local_enr.udp().is_none() || local_enr.udp() == disk_enr.udp())
}

/// Loads enr from the given directory
pub fn load_enr_from_disk(dir: &Path) -> Result<Enr, String> {
    let enr_f = dir.join(ENR_FILENAME);
    let mut enr_file =
        File::open(enr_f).map_err(|e| format!("Failed to open enr file: {:?}", e))?;
    let mut enr_string = String::new();
    match enr_file.read_to_string(&mut enr_string) {
        Err(_) => Err("Could not read ENR from file".to_string()),
        Ok(_) => match Enr::from_str(&enr_string) {
            Ok(disk_enr) => Ok(disk_enr),
            Err(e) => Err(format!("ENR from file could not be decoded: {:?}", e)),
        },
    }
}

/// Saves an ENR to disk
pub fn save_enr_to_disk(dir: &Path, enr: &Enr) {
    let _ = std::fs::create_dir_all(dir);
    match File::create(dir.join(Path::new(ENR_FILENAME)))
        .and_then(|mut f| f.write_all(enr.to_base64().as_bytes()))
    {
        Ok(_) => {
            debug!("ENR written to disk");
        }
        Err(e) => {
            warn!(
                file = %format!("{:?}{:?}",dir, ENR_FILENAME),
                error = %e,
                "Could not write ENR to file",
            );
        }
    }
}
