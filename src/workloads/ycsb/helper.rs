use rand::distributions::Alphanumeric;
use rand::rngs::StdRng;
use rand::Rng;

pub fn generate_random_string(rng: &mut StdRng) -> String {
    let s: String = rng
        .sample_iter(&Alphanumeric)
        .take(100)
        .map(char::from)
        .collect();
    s
}

pub fn alpha(theta: f64) -> f64 {
    1.0 / (1.0 - theta)
}

pub fn zeta(population: usize, theta: f64) -> f64 {
    let mut sum = 0.0;
    for i in 1..population {
        let x = 1.0 / i as f64;
        sum += f64::powf(x, theta);
    }
    sum
}

pub fn zeta_2_theta(theta: f64) -> f64 {
    let mut sum = 0.0;
    for i in 1..2 {
        let x = 1.0 / i as f64;
        sum += f64::powf(x, theta);
    }
    sum
}

pub fn eta(population: usize, theta: f64, zeta_2_thetan: f64, zetan: f64) -> f64 {
    let x = 2.0 / population as f64;
    (1.0 - f64::powf(x, 1.0 - theta)) / (1.0 - zeta_2_thetan / zetan)
}

pub fn zipf(rng: &mut StdRng, population: usize, theta: f64) -> usize {
    let alpha = alpha(theta);
    let zetan = zeta(population, theta);
    let zeta_2_thetan = zeta_2_theta(theta);
    let eta = eta(population, theta, zeta_2_thetan, zetan);
    let u: f64 = rng.gen();
    let uz = u * zetan;
    if uz < 1.0 {
        1
    } else if uz < (1.0 + f64::powf(0.5, theta)) {
        2
    } else {
        (1.0 + (population as f64 * f64::powf(eta * u - eta + 1.0, alpha))) as usize
    }
}

pub fn zipf2(
    rng: &mut StdRng,
    population: usize,
    theta: f64,
    alpha: f64,
    zetan: f64,
    eta: f64,
) -> usize {
    let u: f64 = rng.gen();
    let uz = u * zetan;
    if uz < 1.0 {
        1
    } else if uz < (1.0 + f64::powf(0.5, theta)) {
        2
    } else {
        (1.0 + (population as f64 * f64::powf(eta * u - eta + 1.0, alpha))) as usize
    }
}
