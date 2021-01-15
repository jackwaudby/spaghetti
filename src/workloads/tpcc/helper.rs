use config::Config;
use rand::distributions::Alphanumeric;
use rand::rngs::StdRng;
use rand::Rng;
use std::sync::Arc;

/// Compute the primary key for a district.
pub fn district_key(config: Arc<Config>, d_w_id: u64, d_id: u64) -> u64 {
    let dpw = config.get_int("districts").unwrap();
    (d_w_id * dpw as u64) + d_id
}

/// Compute the primary key for a customer.
pub fn customer_key(config: Arc<Config>, c_w_id: u64, c_d_id: u64, c_id: u64) -> u64 {
    let cpd = config.get_int("customers").unwrap() as u64;
    (district_key(config, c_w_id, c_d_id) * cpd) + c_id
}

/// Compute the primary key for an order line.
pub fn order_line_key(
    config: Arc<Config>,
    ol_w_id: u64,
    ol_d_id: u64,
    ol_o_id: u64,
    ol_number: u64,
) -> u64 {
    (district_key(config, ol_w_id, ol_d_id) * (ol_o_id * 15)) + ol_number
}

/// Compute the primary key for an order.
pub fn order_key(config: Arc<Config>, o_w_id: u64, o_d_id: u64, o_id: u64) -> u64 {
    district_key(config, o_w_id, o_d_id) * o_id
}

/// Compute the primary key for stock
pub fn stock_key(config: Arc<Config>, s_w_id: u64, s_i_id: u64) -> u64 {
    let max_items = config.get_int("max_items").unwrap();

    (s_w_id * max_items as u64) + s_i_id
}

/// Generate a string of some length.
pub fn random_string(lower: u32, upper: u32, rng: &mut StdRng) -> String {
    let size = rng.gen_range(lower..=upper);

    rng.sample_iter(Alphanumeric)
        .take(size as usize)
        .map(char::from)
        .collect()
}

/// Generate a decimal between some interval.
pub fn random_float(lower: f32, upper: f32, dp: usize, rng: &mut StdRng) -> String {
    let f = rng.gen_range(lower..upper);
    format!("{:.1$}", f, dp)
}

/// Generate a zip code.
pub fn zip(rng: &mut StdRng) -> String {
    const CHARSET: &[u8] = b"0123456789";
    const LEN: usize = 4;

    let base_zip: String = (0..LEN)
        .map(|_| {
            let idx = rng.gen_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect();
    format!("{}11111", base_zip)
}

/// Generate a last name.
pub fn last_name(c_id: u64, rng: &mut StdRng) -> String {
    const NAMESET: &[&str] = &[
        "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING",
    ];

    let num;
    if c_id < 1000 {
        num = rng.gen_range(0..1000).to_string();
    } else {
        num = nu_rand(0, 999, rng).to_string();
    }

    let f = match num.len() {
        1 => format!("00{}", num),
        2 => format!("0{}", num),
        3 => format!("{}", num),
        _ => panic!("generated too big of a number: {}, c_id: {}", num, c_id),
    };
    let mut result = String::new();
    for i in f.chars() {
        let ind = i.to_digit(10).unwrap();
        result.push_str(NAMESET[ind as usize]);
    }
    result
}

/// Generate a random number between [x, y].
pub fn rand<T: Rng>(x: u64, y: u64, rng: &mut T) -> u64 {
    rng.gen_range(x..=y)
}

/// Generate a non-uniform random number.
pub fn nu_rand<T: Rng>(x: u64, y: u64, rng: &mut T) -> u64 {
    let a = match y {
        999 => 255,
        3000 => 1023,
        100000 => 8191,
        _ => panic!("invalid nurand range"),
    };

    let c = rng.gen_range(0..=a);

    let p1 = (rng.gen_range(0..a) | rng.gen_range(x..=y)) + c;
    let p2 = y - x + 1;

    let res = (((p1 % p2) + p2) % p2) + x; // TODO: off by 1 error, hack for now.
    if res == 1000 {
        999
    } else {
        res
    }
}

/// Generate item data `i_data`.
pub fn item_data(rng: &mut StdRng) -> String {
    let og: f32 = rng.gen();
    if og <= 0.1 {
        let size = rng.gen_range(26..=50);
        let pos = rng.gen_range(0..=size - 8);
        let start: String = rng
            .sample_iter(&Alphanumeric)
            .take(pos as usize)
            .map(char::from)
            .collect();
        let middle = "ORIGINAL";
        let end: String = rng
            .sample_iter(&Alphanumeric)
            .take((size - (pos + 8)) as usize)
            .map(char::from)
            .collect();
        format!("{}{}{}", start, middle, end)
    } else {
        random_string(26, 50 + 1, rng)
    }
}
