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
