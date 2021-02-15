#!/usr/bin/env sh

cfssl gencert -initca ca-csr.json | cfssljson -bare ca
cfssl gencert -ca=./ca.pem -ca-key=./ca-key.pem  -config=./ca-config.json -profile=peer peer-csr.json | cfssljson -bare peer
