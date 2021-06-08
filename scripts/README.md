## Utilities

### Report Generation
Reports are produced per workload using:
```
./generate-report.sh <workload>
```


### VM Setup

Grab a fresh Ubuntu Server 18.04 image on Azure.
Install Rust, get the spaghetti repo, and build for release.
```
yes | sudo apt-get update &&
yes | sudo apt install build-essential &&
curl --proto '=https' --tlsv1.2 https://sh.rustup.rs -sSf | sh -s -- -y &&
source $HOME/.cargo/env &&
 git clone https://github.com/jackwaudby/spaghetti.git &&
cd spaghetti/ &&
cargo build --release
```

Fix permissions and ssh into machine.
```
sudo chmod 600 ./ssh_keys/<key>.pem
ssh -i ./ssh_keys/<key>.pem azureuser@<pubic-ip>
```

Copy results from VM to local machine.
```
scp -r -i ./ssh_keys/<key>.pem azureuser@<pubic-ip>:~/spaghetti/results ./
```

### NFN Setup
```
git clone https://github.com/durner/No-False-Negatives.git &&
wget https://github.com/oneapi-src/oneTBB/releases/download/2018/tbb2018_20170726oss_lin.tgz &&
tar -xzvf tbb2018_20170726oss_lin.tgz &&
sudo mv tbb2018_20170726oss/include/ /usr/local/include/ &&
git clone https://github.com/viktorleis/perfevent.git &&
sudo mv perfevent/ /usr/local/include/ &&
yes | sudo apt  install cmake &&
git clone https://github.com/jemalloc/jemalloc.git &&
yes | sudo apt-get update &&
yes | sudo apt-get install autoconf &&
cd jemalloc/ &&
./autogen.sh &&
sudo make &&
sudo make install &&
cd ~ &&
yes |sudo apt install libtbb-dev &&
cd No-False-Negatives/ &&
rm -rf build && mkdir build && cd build && cmake -DCMAKE_BUILD_TYPE=Release ../ && make -j16 &&
./bin/db svcc_tatp NoFalseNegatives 100 100000 4
```

###  Profiling
Valgrind suite:
- *memcheck* is a memory error detector
- *massif* is a heap profiler
- *cachegrind* is a cache profiler. Simulates a machine with two levels of instruction and data caching. Also, displays branch misprediction. Note conditional branches jump to a location based on some condition, whereas indirect branches jump based on the results of previous instructions.
```
valgrind --tool=memcheck <program>
valgrind --tool=massif <program>
valgrind --tool=cachegrind --branch-sim=yes <program>
```

Perf
```
perf stat --event task-clock,context-switches,page-faults,cycles,instructions,branches,branch-misses,cache-references,cache-misses <program> > /dev/null
```
