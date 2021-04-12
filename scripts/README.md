## Utilities

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
sudo chmod 600 ~/.ssh-keys/<key>.pem
ssh -i ~/.ssh-keys/<key>.pem azureuser@<pubic-ip>
scp -r -i ./ssh_keys/<key>.pem azureuser@azureuser@<pubic-ip>:~/spaghetti/results ./
```
