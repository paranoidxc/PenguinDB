## PenguinDB

Persistent Key-Value store based on Bitcask paper.

Fast, Lightweight, and Scalable key-value store written in golang, based on bitcask.

It's maintains an active datafile to which data is written. When it crosses a threshold, the datafile is made inactive and new datafile is created.
As time passes, expired/deleted keys take up space which is not useful; Hence, a process called `merge` is done which removes all expired/deleted keys and frees up space.

## ToDo List

- [x] Base Idea
- [x] Data storage is not limited by RAM
- [x] Thread-Safe
- [x] Portable
- [x] Supports Merge
- [x] Single disk-seek write
- [x] Block cache for faster reads.
- [ ] Compatible Redis(Rich data structure: KV, List, Hash, ZSet, Set.)
- [ ] Custom Expiry
- [ ] HTTP API support, JSON output
- [ ] Supports cluster, use RAFT

## Documentation

```go
todo ...
```
