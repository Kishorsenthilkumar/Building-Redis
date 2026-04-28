# Python Redis Server Clone 🚀

![Python Version](https://img.shields.io/badge/python-3.8%2B-blue)
![Zero Dependencies](https://img.shields.io/badge/dependencies-0-brightgreen)
![License](https://img.shields.io/badge/license-MIT-green)

A fully functional, asynchronous Redis server built entirely from scratch in Python **using only the standard library**—absolutely no external dependencies required!

This project implements the **RESP (REdis Serialization Protocol)** and supports a wide array of advanced Redis features, making it a highly capable lightweight drop-in replacement for basic Redis operations. 





## 🌟 Features

### Core Capabilities
* **RESP Protocol:** Fully parses and serializes RESP arrays, bulk strings, simple strings, and integers.
* **Concurrency:** Handles multiple concurrent client connections using Python's `asyncio` event loop.
* **Zero Dependencies:** Built entirely with Python's built-in modules (`asyncio`, `socket`, `os`, `hashlib`, etc.).

### Data Structures & Commands
* **Strings:** `SET` (with `PX` millisecond expiry), `GET`, `TYPE`, `KEYS *`.
* **Lists:** `LPUSH`, `RPUSH`, `LPOP`, `LRANGE`, `LLEN`, and blocking operations (`BLPOP`).
* **Streams:** `XADD` (with auto-ID generation), `XRANGE`, and `XREAD` (with blocking support).
* **Sorted Sets:** `ZADD`, `ZRANK`, `ZRANGE`, `ZCARD`, `ZSCORE`, `ZREM`.
* **Geospatial:** `GEOADD`, `GEOPOS`, `GEODIST`, `GEOSEARCH` (using custom Geohash bit-interleaving and Haversine distance calculations).

### High Availability & Durability
* **AOF Persistence (Append-Only File):** * Writes mutating commands to disk in real-time.
  * Uses modern Multi-Part AOF architecture (manifest files + incremental `.incr.aof` logs).
  * Automatically recovers state from disk on startup.
* **RDB Parsing:** Can natively read, parse, and load pre-existing binary `.rdb` snapshot files.
* **Replication:** * Master/Replica architecture (`--replicaof`).
  * Handles `PING`, `REPLCONF`, and `PSYNC` handshakes.
  * Real-time command propagation and byte-offset tracking.
  * Synchronous replication support via the `WAIT` command.

### Advanced Systems
* **Transactions (Optimistic Locking):** Supports `MULTI`, `EXEC`, `DISCARD`, `WATCH`, and `UNWATCH` to ensure atomic, connection-safe queue execution.
* **Pub/Sub:** Real-time messaging with `SUBSCRIBE`, `PUBLISH`, and `UNSUBSCRIBE`.
* **Security (ACL):** User authentication via `AUTH`, along with `ACL WHOAMI`, `ACL GETUSER`, and `ACL SETUSER` using SHA-256 password hashing.

🧠 Architecture & Design

handle_client: An asynchronous task spawned for every connected socket. Manages transaction states, queued commands, and connection-specific ACL authentication.

process_command: The core routing engine. Decodes RESP parts, modifies the global database dictionary, and triggers AOF writes or replication propagation.

background_conn: A dedicated listener for Replica servers to continuously ingest propagated bytes from the Master without blocking standard client requests.

dbfile_manager: A custom binary parser that decodes standard Redis .rdb files, handling complex length encodings, string conversions, and expiry timestamps byte-by-byte.



