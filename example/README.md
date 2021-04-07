## Introduction

The example provides how to use hraft-dispatcher. We started an HTTP service to forward the received user request to casbin.

### Service details

Leader node:
- HTTP service - 127.0.0.1:6780
- Dispatcher
  - Raft service - 127.0.0.1:6781
  - HTTP(S) service - 127.0.0.1:6781
    
Follower node:
- HTTP service - 127.0.0.1:6790
- Dispatcher
  - Raft service - 127.0.0.1:6781
  - HTTP(S) service - 127.0.0.1:6781

### HTTP API

#### Add a policy

---

**Method**: `PUT`  
**URL**: `/policies`  
**Data Type**: `String[]`  
**Data Example**: `["role:admin", "/", "GET"]`

#### Remove a policy

---

**Method**: `DELETE`  
**URL**: `/policies`  
**Data Type**: `String[]`  
**Data Example**: `["role:admin", "/", "GET"]`

#### Enforcer

---

**Method**: `PUT`  
**URL**: `/enforcer`  
**Data Type**: `String[]`  
**Data Example**: `["role:admin", "/", "GET"]`

## Getting started

To run the leader node:
```shell
make run-leader
```

To run the follower node:
```shell
make run-follower
```

The test case is [here](./test.http).
