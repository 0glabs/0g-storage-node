use lighthouse_metrics::{try_create_int_counter, IntCounter, Result};

lazy_static! {
    pub static ref SCRATCH_PAD_ITER_COUNT: Result<IntCounter> = try_create_int_counter(
        "miner_scratch_pad_iter",
        "Number of scratch pad iterations for PoRA"
    );
    pub static ref LOADING_COUNT: Result<IntCounter> = try_create_int_counter(
        "miner_loading_iter",
        "Number of loading iterations for PoRA"
    );
    pub static ref PAD_MIX_COUNT: Result<IntCounter> = try_create_int_counter(
        "miner_mix_iter",
        "Number of mix sealed data with scratch pad iterations for PoRA"
    );
    pub static ref HIT_COUNT: Result<IntCounter> =
        try_create_int_counter("miner_hit", "Number of hit for PoRA");
}

pub fn report() -> String {
    let s = |counter: &Result<IntCounter>| match counter {
        Ok(x) => format!("{}", x.get()),
        Err(_) => format!("n/a"),
    };
    format!(
        "scratch pad: {}, loading: {}, pad_mix: {}, hit: {}",
        s(&SCRATCH_PAD_ITER_COUNT),
        s(&LOADING_COUNT),
        s(&PAD_MIX_COUNT),
        s(&HIT_COUNT)
    )
}
