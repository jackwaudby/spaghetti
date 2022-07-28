rm results.csv
rm exp-tpl-results.csv

cargo build --release

for cores in 1 5 10 20 30 40; do
    ./target/release/spag --protocol tpl --scalefactor 1 --cores $cores --workload smallbank --transactions $1
    sleep 5
done

mv results.csv results/exp-tpl-results.csv

cd experiments
