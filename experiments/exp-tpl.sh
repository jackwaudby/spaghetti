cd ../

rm results.csv
rm exp-tpl-results.csv

cargo build --release

# smallbank
for cores in 1 5 10 20 30 40; do
    ./target/release/spag --protocol tpl --scalefactor 1 --cores $cores --workload smallbank --transactions $1
    sleep 5
done

# tatp
for cores in 1 5 10 20 30 40; do
    ./target/release/spag --protocol tpl --scalefactor 1 --cores $cores --workload tatp --transactions $1
    sleep 5
done

# ycsb
for cores in 1 5 10 20 30 40; do
    ./target/release/spag --protocol tpl --scalefactor 2 --cores $cores --workload ycsb --transactions $1 --theta 0.8
    sleep 5
done

mv results.csv results/exp-tpl-results.csv

cd experiments
