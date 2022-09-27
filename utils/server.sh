#!/bin/bash

if [ $1 == "start" ]
then
    az vm start --resource-group=64cores_group --name=64cores;
fi

if [ $1 == "stop" ]
then
    az vm deallocate --resource-group=64cores_group --name=64cores;
fi

if [ $1 == "login" ]
then
    ssh -i ../ssh_keys/64cores_key.pem azureuser@13.91.107.67
fi
