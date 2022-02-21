#!/bin/bash

# Usage: ./plot.sh <filename>

read -n1 -p "Download $1? [y,n]: " doit
case $doit in
  y|Y) scp -i ./ssh_keys/64cores_key.pem azureuser@13.91.107.67:~/spaghetti/results.csv ./data/$1 ;;
  n|N) echo no ;;
  *) echo dont know ;;
esac

TPL=false
SGT=false

read -n1 -p "Include 2PL? [y,n]: " doit
case $doit in
  y|Y) TPL=true ;;
  n|N) TPL=false ;;
  *) echo dont know ;;
esac

read -n1 -p "Include SGT? [y,n]: " doit
case $doit in
  y|Y) SGT=true ;;
  n|N) SGT=false ;;
  *) echo dont know ;;
esac

Rscript ./scripts/plots.R ./data/$1 $TPL $SGT true

open ./graphics/*
