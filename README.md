# Redis Clone — Built from Scratch in Python

A Redis-compatible server built from scratch using Python and asyncio.
No Redis libraries used — just raw TCP sockets and the RESP protocol.

Built as part of the [CodeCrafters](https://codecrafters.io) "Build Your Own Redis" challenge.

---

## Features

- **Core** — PING, ECHO, SET with expiry (PX), GET, INCR
- **Lists** — LPUSH, RPUSH, LRANGE, LLEN, LPOP, BLPOP
- **Streams** — XADD (auto/partial/explicit IDs), XRANGE, XREAD with BLOCK
- **Transactions** — MULTI, EXEC, DISCARD
- **Replication** — Master-replica setup with REPLCONF, PSYNC, RDB handshake and command propagation

---

## Run

```bash
# Install dependencies
pip install asyncio

# Start master
python3 main.py --port 6379

# Start replica
python3 main.py --port 6380 --replicaof localhost 6379
