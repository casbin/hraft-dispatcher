# hraft-dispatcher 

A dispatcher based on Hashicorp's Raft for Casbin.


## Project Status

hraft-dispatcher is beta version.

## Getting started

### Installation

Go version 1.14+ and Casbin vervsion 2.24+ is required.

```shell
go get github.com/casbin/hraft-dispatcher
```

### Prerequisite

You have to provide a completely new Casbin environment without Adapter, all the policies are handled by hraft-dispatcher. 
When the leader node starts for the first time, you can add the default policy to hraft-dispatcher.

### Example

An example is provided [here](./example).

## Architecture

hraft-dispatcher is a [dispatcher](https://casbin.org/docs/en/dispatchers) plug-in based on [hashicorp/raft](https://github.com/hashicorp/raft) implementation.

hraft-dispatcher includes an HTTP service, and a Raft service:

- HTTP service is used to forward data from follower node to follower node
- Raft service is used to maintain the policy consistency of each node

If you set up a dispatcher in Casbin, it forwards the following request to dispatcher:

- AddPolicy
- RemovePolicy
- AddPolicies
- RemovePolicies
- RemoveFilteredPolicy
- UpdatePolicy
- UpdatePolicies
- ClearPolicy

In dispatcher, we are use Raft consensus protocol to maintain the policy, and use the [bbolt](https://github.com/etcd-io/bbolt) to storage the policy of each node.

hraft-dispatcher overall architecture looks like this:

![overall architecture](./docs/images/dispatcher-architecture.svg)

## Limitations

- Adapter: You cannot use Adapter in Casbin, hraft-dispatcher has its own Adapter, which uses the [bbolt](https://github.com/etcd-io/bbolt) to storage the policy.
- You cannot call the following methods, which will affect data consistency:
  - LoadPolicy - All policies are maintained by hraft-dispatcher
  - SavePolicy - All policies are maintained by hraft-dispatcher


## Project reference

Much of the inspiration comes from the following projects:

- [rqlite](https://github.com/rqlite/rqlite)
- [vault](https://github.com/hashicorp/vault)

Thanks for everyone's contribution.

## Contribution

Thank you for your interest in contributing!

## License

This project is under Apache 2.0 License. See the [LICENSE](LICENSE) file for the full license text.