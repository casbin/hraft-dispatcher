#!/usr/bin/env sh

cfssl gencert -initca ca-csr.json | cfssljson -bare ca
cfssl gencert -ca=./ca.pem -ca-key=./ca-key.pem  -config=./ca-config.json -profile=client client-csr.json | cfssljson -bare client

# reference
# https://developers.cloudflare.com/access/service-auth/mtls-testing#generating-a-client-certificate