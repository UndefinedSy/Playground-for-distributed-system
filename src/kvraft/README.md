# TODO

## Client Side
- [x] Basic implementation of send RPC requests
- [ ] a smarter way to find the leader

## Server Side
- [x] Basic implementation of receive RPC requests(just print out the request is fine)
- [ ] A UID generator for request to uniquely identify a certain request in PENDED queue
- [ ] Combine the kv-server with the raft instance
  - [ ] RPC thread to iterate PENDED queue
  - [ ] Sending back response logic


## Tools
- [ ] A C-implemented snowflake-liked UID generator
- [ ] try C implementation in Go