// client generates a transactions => pick parameters
// express transaction in terms of index operations

// New order transaction

// Data generation
// w_id, home warehouse number, fixed across measurement interval
// d_id, district number, randomly selected within [1..10]
// c_id, customer number, selected using nurand(1023,1,3000)
// ol_cnt, items in order, selected within [5,15]
// rbk, rollback, [1,100]

// for each item in ol_cnt
//  ol_i_id, selected using nurand(8191,1,100000)
// ol_supply_w_id, remote or home
// ol_quantity, selected with [1,10]
// o_entry_d

// steps
// select row from warehouse table by w_id, get w_tax
// select row from district table by (d_w_id, d_id), get d_tax and d_next_o_id
// increment d_next_o_id
// select row from customer table by (c_w_id, c_d_id, c_id), get c_disocunt, c_last, c_credit
// insert into new order table and order table

struct Transaction {}

// parameters

// BEGIN

// SELECT w_tax FROM warehouse WHERE w_id = :w_id;
// SELECT d_tax, d_next_o_id FROM district WHERE d_w_id = :w_id AND d_id = :d_id
// UPDATE district SET d _next_o_id = :d _next_o_id + 1 WHERE d _id = :d_id AND d_w _id = :w _id;
