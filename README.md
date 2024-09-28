## PenguinDB

Persistent Key-Value store based on Bitcask paper.

Fast, Lightweight, and Scalable key-value store written in golang, based on bitcask.

It's maintains an active datafile to which data is written. When it crosses a threshold, the datafile is made inactive and new datafile is created.
As time passes, expired/deleted keys take up space which is not useful; Hence, a process called `merge` is done which removes all expired/deleted keys and frees up space.

## ToDo List

- [ ] Base Idea
- [ ] Data storage is not limited by RAM
- [ ] Thread-Safe
- [ ] Portable
- [ ] Custom Expiry
- [ ] Supports Merge
- [ ] Single disk-seek write
- [ ] Block cache for faster reads.
- [ ] Compatible Redis(Rich data structure: KV, List, Hash, ZSet, Set.)
- [ ] HTTP API support, JSON output
- [ ] Supports cluster, use RAFT

## Documentation

```go
todo ...
```
