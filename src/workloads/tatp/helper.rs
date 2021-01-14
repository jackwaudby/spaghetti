use rand::rngs::ThreadRng;
use rand::Rng;

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

pub fn get_start_time(rng: &mut ThreadRng) -> u8 {
    let n: f32 = rng.gen();

    match n {
        x if x < 0.3333 => 0,
        x if x < 0.6666 => 8,
        _ => 16,
    }
}

/// Compute the primary key for a `AccessInfo` record.
pub fn access_info_key(s_id: u64, ai_type: u64) -> u64 {
    (s_id * 10) + ai_type
}

/// Generate random string from upper case A-Z of length `n`.
pub fn get_data_x(n: usize, rng: &mut ThreadRng) -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    // const LEN: usize = 3;

    let data_x: String = (0..n)
        .map(|_| {
            let idx = rng.gen_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect();
    data_x
}
