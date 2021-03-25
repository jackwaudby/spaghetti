use config::Config;
use rand::rngs::StdRng;
use rand::Rng;
use std::sync::Arc;

/// Split account range into cold and hot.
pub fn split_accounts(num_accounts: u32, hotspot_size: usize) -> (Vec<String>, Vec<String>) {
    let mut hot = vec![];
    let mut cold = vec![];

    for account in 1..=hotspot_size {
        hot.push(format!("cust{}", account));
    }

    for account in hotspot_size + 1..=num_accounts as usize {
        cold.push(format!("cust{}", account));
    }

    (hot, cold)
}

/// Calculate size of hotspot.
pub fn get_hotspot_size(config: Arc<Config>) -> usize {
    let accounts = config.get_int("accounts").unwrap();
    let use_fixed_size = config.get_bool("hotspot_use_fixed_size").unwrap();

    if use_fixed_size {
        config.get_int("hotspot_fixed_size").unwrap() as usize
    } else {
        let percent = config.get_float("hotspot_percentage").unwrap();
        (accounts as f64 * percent) as usize
    }
}

/// Get customer name.
pub fn get_name(rng: &mut StdRng, config: Arc<Config>) -> String {
    let accounts = config.get_int("accounts").unwrap() as u32;
    let hotspot_size = get_hotspot_size(config);
    let (hot, cold) = split_accounts(accounts, hotspot_size);

    let n: f32 = rng.gen();
    match n {
        // Choose from hot.
        x if x < 0.9 => {
            let ind = rng.gen_range(0..hotspot_size);
            hot[ind].clone()
        }
        // Choose from cold.
        _ => {
            let cold_size = cold.len();
            let ind = rng.gen_range(0..cold_size);
            cold[ind].clone()
        }
    }
}

/// Get customer names.
pub fn get_names(rng: &mut StdRng, config: Arc<Config>) -> (String, String) {
    let name1 = get_name(rng, Arc::clone(&config));
    let mut name2 = get_name(rng, Arc::clone(&config));

    while name1 == name2 {
        name2 = get_name(rng, Arc::clone(&config));
    }
    (name1, name2)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;

    #[test]
    fn helpers_test() {
        let mut rng = StdRng::seed_from_u64(42);
        assert_eq!(
            split_accounts(5, 2),
            (
                vec!["cust1".to_string(), "cust2".to_string()],
                vec![
                    "cust3".to_string(),
                    "cust4".to_string(),
                    "cust5".to_string()
                ]
            )
        );

        let mut c = Config::default();
        c.merge(config::File::with_name("Test-smallbank.toml"))
            .unwrap();
        let config = Arc::new(c);

        assert_eq!(get_hotspot_size(config.clone()), 2);
        assert_eq!(get_name(&mut rng, config), "cust1".to_string());
    }
}
