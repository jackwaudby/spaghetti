use rand::rngs::StdRng;
use rand::Rng;

/// Compute the primary key for a `AccessInfo` record.
pub fn access_info_key(s_id: u64, ai_type: u64) -> u64 {
    (s_id * 10) + ai_type
}

/// Compute the primary key for a `SpecialFacility` record.
pub fn special_facility_key(s_id: u64, sf_type: u64, is_active: u64) -> u64 {
    (s_id * 10) + sf_type + (is_active * 5)
}

/// Compute the primary key for a `CallForwarding` record.
pub fn call_forwarding_key(s_id: u64, sf_type: u64, start_time: u64) -> u64 {
    let base = (s_id * 10) + sf_type;
    let x = match start_time {
        0 => 1,
        8 => 2,
        16 => 3,
        _ => unimplemented!(),
    };
    (base * 10) + x
}

/// Convert subscriber id to `String`.
pub fn to_sub_nbr(s_id: u64) -> String {
    let mut num = s_id.to_string();
    for _i in 0..15 {
        if num.len() == 15 {
            break;
        }
        num = format!("0{}", num);
    }
    num
}

/// Generate a start time.
pub fn get_start_time(rng: &mut StdRng) -> u64 {
    let n: f32 = rng.gen();

    match n {
        x if x < 0.3333 => 0,
        x if x < 0.6666 => 8,
        _ => 16,
    }
}

/// Generate active status.
pub fn is_active(rng: &mut StdRng) -> u64 {
    let f: f32 = rng.gen();
    if f < 0.15 {
        0
    } else {
        1
    }
}

/// Get a 15 digit number.
pub fn get_number_x(rng: &mut StdRng) -> String {
    const CHARSET: &[u8] = b"0123456789";
    const LEN: usize = 15;

    let numb_x: String = (0..LEN)
        .map(|_| {
            let idx = rng.gen_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect();
    numb_x
}

/// Generate random string from upper case A-Z of length `n`.
pub fn get_data_x(n: usize, rng: &mut StdRng) -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    let data_x: String = (0..n)
        .map(|_| {
            let idx = rng.gen_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect();
    data_x
}

/// Get constant A.
fn get_a(subscribers: u64) -> u64 {
    match subscribers {
        x if x <= 1000000 => 65535,
        x if x <= 10000000 => 1048575,
        _ => 2097151,
    }
}

/// Get subscriber id
pub fn nurand_sid(rng: &mut StdRng, subscribers: u64, start_id: u64) -> u64 {
    let x = start_id;
    let y = x + subscribers;
    let a = get_a(subscribers);

    let gr1 = rng.gen_range(0..=a);
    let gr2 = rng.gen_range(x..=y);

    let p1 = gr1 | gr2;
    let p2 = y - x + 1;
    (p1.rem_euclid(p2)) + x
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;

    #[test]
    fn helpers_test() {
        assert_eq!(access_info_key(3, 2), 32);
        assert_eq!(special_facility_key(10, 1, 1), 106);
        assert_eq!(call_forwarding_key(2, 2, 8), 222);
        assert_eq!(to_sub_nbr(367), String::from("000000000000367"));

        let mut rng = StdRng::seed_from_u64(42);
        assert_eq!(get_data_x(3, &mut rng), String::from("NOQ"));
        assert_eq!(get_number_x(&mut rng), String::from("404781095152050"));
        assert_eq!(get_start_time(&mut rng), 8);
        assert_eq!(is_active(&mut rng), 1);
    }
}
