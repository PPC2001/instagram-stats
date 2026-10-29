# System Architecture & Design Decisions

This document details the architectural principles, data flow, component interactions, and engineering design decisions implemented in **Instagram Stats**.

---

## 1. System Overview & Core Philosophy

The primary objective of this system is to handle **high-throughput, bursty engagement events** (likes, follows, post views) with minimal latency for the client, high fault tolerance, and predictable resource utilization.

```
                                 ┌─────────────────────────┐
                                 │   Web / Client Request  │
                                 └────────────┬────────────┘
                                              │ HTTP (x-user-id)
                                              ▼
                                 ┌─────────────────────────┐
                                 │      Fastify API        │
                                 │   (Clustered Workers)   │
                                 └──────┬───────────┬──────┘
                                        │           │
                     Read Path (Fast)   │           │ Write Path (Asynchronous)
                                        ▼           ▼
                           ┌────────────────┐   ┌───────────────────────┐
                           │  Mongoose /    │   │  Kafka Producer       │
                           │  Redis Reader  │   │  (likes/views/follows)│
                           └───────┬────────┘   └───────────┬───────────┘
                                   │                        │
               ┌───────────────────┴───┐                    ▼
               ▼                       ▼        ┌───────────────────────┐
      ┌─────────────────┐    ┌────────────────┐ │  Kafka Consumer       │
      │  Redis Cache    │    │ MongoDB        │ │  (Batch Worker)       │
      │  (Counters, HLL,│    │ Replica Set    │ └───────────┬───────────┘
      │  Cuckoo Filter) │    │ (fk-rs)        │             │
      └─────────────────┘    └────────────────┘             ▼
               ▲                                ┌───────────────────────┐
               └────────────────────────────────┤ Updates Redis State & │
                                                │ Writes Mongo Entities │
                                                └───────────────────────┘
```

---

## 2. Key Design Decisions

### Decision 1: Decoupled Asynchronous Write Path (Kafka)
* **Problem**: In social platforms, likes, follows, and view events create high write amplification. Direct synchronous updates to MongoDB for every click would quickly exhaust database connection pools and degrade response times.
* **Decision**: 
  - The API acts solely as a **Kafka Producer** for engagement events.
  - Endpoints (`POST /posts/:id/like`, `POST /posts/:id/view`, `POST /profiles/:username/follow`) validate inputs, emit messages to Kafka, and immediately reply with **`202 Accepted`**.
  - Background workers (`src/consumers.ts`) consume Kafka partitions in batches, updating MongoDB and Redis.
* **Partitioning Strategy**: Topics are partitioned with 3 partitions using the entity ID (e.g., `postId`) as the message key. This guarantees that events for the same entity maintain strict ordering while load is balanced across consumers.

---

### Decision 2: HyperLogLog for Unique View Counting
* **Problem**: Tracking unique viewers on high-traffic posts via traditional sets (`SADD post:{id}:viewers {userId}`) consumes unbounded memory ($O(N)$ with respect to users). Storing 1M user IDs in a Redis set requires ~40 MB per post.
* **Decision**: 
  - In [`src/features/views/views.consumer.ts`](file:///c:/Users/Pratik/Desktop/Projects/instagram-stats/src/features/views/views.consumer.ts), unique views are tracked using Redis **HyperLogLog** (`PFADD post:{id}:uv <userIds>`).
  - Read via `pfCount(uniqueViewsKey(id))` in [`src/features/posts/posts.stats.ts`](file:///c:/Users/Pratik/Desktop/Projects/instagram-stats/src/features/posts/posts.stats.ts).
* **Trade-off**: HyperLogLog uses a fixed maximum memory footprint of **12 KB per post** with a standard error of ~0.81%, which is acceptable for social analytics while protecting Redis from memory exhaustion.

---

### Decision 3: Custom Cuckoo Filter for Fast Relationship Lookups
* **Problem**: When rendering a feed, the server must compute flags like `isLiked`, `isFollowing`, and `isSeen` for the current viewer. Synchronously querying MongoDB for every post in a feed generates $N$ queries or large `$in` scans.
* **Decision**:
  - Implemented a custom pure-Lua **Cuckoo Filter** stored in Redis Hashes (`src/shared/cuckoo-core.ts`).
  - Unlike Bloom filters, Cuckoo filters **support deletions** (crucial for unliking and unfollowing).
  - Empty buckets are not allocated upfront; memory grows dynamically with insertions.
  - **Zero False Negatives**: If the Cuckoo filter returns `0` (false), the item definitely does not exist, eliminating 100% of negative database lookups. If it returns `1` (true), a fallback check to MongoDB is made only for confirmations where strict accuracy is needed (e.g. `likedIds`).

---

### Decision 4: MongoDB Replica Set Resilience & Circuit Breaker (`mongoGate`)
* **Problem**: MongoDB replica set elections or failovers take 2–10 seconds. During this window, incoming HTTP requests would block on driver socket timeouts (`serverSelectionTimeoutMS`), piling up in memory and crashing the Node.js event loop.
* **Decision**:
  - **Topology Monitoring**: Real-time monitoring of replica set state (`src/bootstrap/mongo-topology.ts`).
  - **`mongoGate` Hook**: Every incoming request passes through [`src/shared/mongo-gate.ts`](file:///c:/Users/Pratik/Desktop/Projects/instagram-stats/src/shared/mongo-gate.ts). If an election is in progress or consecutive failures trip the circuit breaker, write requests are rejected immediately with **`503 Service Unavailable`** and a `Retry-After` header.
  - **Read Preference**: Reads are configured with `primaryPreferred`, allowing read queries to be served by secondaries during primary elections.
  - **Auto-Detection**: `MONGO_MODE=auto` dynamically probes whether Mongo is running as standalone, replica set, or sharded cluster, and adjusts write concerns (`w: 1` vs `w: majority`) without code modification.

---

### Decision 5: Transparent Docker Host DNS Resolution
* **Problem**: In local Docker configurations, MongoDB replica members advertise hostnames like `host.docker.internal:27017`. While Docker containers resolve this, host Node.js processes on Linux and certain macOS/Windows setups fail with `ENOTFOUND host.docker.internal`.
* **Decision**:
  - Implemented [`src/bootstrap/mongo-host-dns.ts`](file:///c:/Users/Pratik/Desktop/Projects/instagram-stats/src/bootstrap/mongo-host-dns.ts), which monkey-patches Node's native `dns.lookup` and `dns.promises.lookup` to rewrite `host.docker.internal` to `127.0.0.1`.
  - Developers do not need root privileges or manual edits to `/etc/hosts`.

---

### Decision 6: Connection Pooling & `bufferCommands: false`
* **Problem**: By default, Mongoose buffers queries indefinitely in memory when disconnected. In high-traffic environments, a brief database disconnect causes memory leaks and massive cascading query storms upon reconnect.
* **Decision**:
  - `mongoose.set("bufferCommands", false)` is enabled globally.
  - Queries fail fast if the connection is dead rather than piling up in Node process memory.
  - Pre-warmed connection pool (`minPoolSize: 10`, `maxPoolSize: 50` per worker) prevents latency spikes caused by opening connections two at a time during traffic bursts.

---

### Decision 7: Stable Cursor-Based Pagination
* **Problem**: Traditional offset-based pagination (`$skip(page * limit)`) degrades to $O(N)$ query execution time because the database must scan and discard all preceding documents. Furthermore, new posts added to the feed cause "page drift" (duplicate or missed posts).
* **Decision**:
  - Implemented cursor pagination in [`src/shared/pagination.ts`](file:///c:/Users/Pratik/Desktop/Projects/instagram-stats/src/shared/pagination.ts) using the monotonic `_id` field (`{ _id: { $gt: cursor } }`).
  - Hard upper limit enforced (`MAX_LIMIT = 100`, `DEFAULT_LIMIT = 20`) to prevent denial-of-service queries.

---

### Decision 8: Process Clustering & Graceful Teardown
* **Problem**: Node.js runs on a single thread. Single-process execution leaves multi-core CPUs underutilized and risks full downtime on uncaught failures.
* **Decision**:
  - [`src/server.ts`](file:///c:/Users/Pratik/Desktop/Projects/instagram-stats/src/server.ts) utilizes Node's built-in `cluster` module. A primary process forks worker processes matching `WORKERS` (default 1 in dev, configurable for production).
  - If a worker exits unexpectedly, the primary logs the crash and automatically forks a replacement worker.
  - Coordinated graceful shutdown: `SIGINT`/`SIGTERM` stops accepting new HTTP connections, drains in-flight requests, and cleanly closes Kafka producers, Redis sockets, and Mongoose connection pools.

---

### Decision 9: Request-Level Timeout Protection
* **Problem**: Stale socket timeouts fire unpredictably when HTTP keep-alive connections are reused across multiple requests.
* **Decision**:
  - In [`src/shared/timeout.ts`](file:///c:/Users/Pratik/Desktop/Projects/instagram-stats/src/shared/timeout.ts), an `onRequest` Fastify hook arms a per-request timer (`REQUEST_TIMEOUT_MS = 30000`).
  - If the handler exceeds 30 seconds, it sends a `503 Request timed out` and cleans up event listeners when the socket closes.

---

### Decision 10: Fail-Fast Startup Validation
* **Problem**: Misconfigured JWT secrets or missing database variables often lead to runtime exceptions or insecure silent defaults in production.
* **Decision**:
  - [`src/bootstrap/env.ts`](file:///c:/Users/Pratik/Desktop/Projects/instagram-stats/src/bootstrap/env.ts) uses a strict **Zod** schema.
  - Secrets (`JWT_ACCESS_SECRET`, `JWT_REFRESH_SECRET`) require a minimum length of 32 characters.
  - Production mode (`NODE_ENV=production`) enforces that MongoDB must be running in `replica` or `sharded` mode; standalone configurations fail to boot immediately.

---

## 3. Component Directory Structure

```
src/
├── app.ts                  # Fastify configuration, plugin registration, routing
├── server.ts               # Primary/Worker clustering, process management
├── consumers.ts            # Standalone Kafka consumer process runner
├── bootstrap/              # Initialization modules
│   ├── env.ts              # Zod environment schema & validation
│   ├── logger.ts           # Winston logger setup
│   ├── database.ts         # Mongoose client, pool tuning, connection probing
│   ├── mongo-topology.ts   # Replica set state tracker
│   ├── mongo-host-dns.ts   # Docker host.docker.internal remapper
│   ├── redis.ts            # Redis client connection & backoff
│   ├── kafka.ts            # Kafka producer & consumer bootstrap
│   └── shutdown.ts         # Signal trapping and graceful exit handler
├── shared/                 # Cross-cutting utilities
│   ├── cuckoo.ts           # Cuckoo filter wrapper
│   ├── cuckoo-core.ts      # Pure Lua Cuckoo filter engine
│   ├── mongo-gate.ts       # Circuit breaker for DB elections/failures
│   ├── pagination.ts       # Cursor pagination helpers
│   ├── timeout.ts          # Request timeout hook
│   └── error.ts            # Centralized Fastify error handler
└── features/               # Domain modules
    ├── posts/              # Post/Reel models, stats computation, routes
    ├── profiles/           # User profile models, routes
    ├── likes/              # Like model, controller, Kafka consumer
    ├── views/              # View events, HyperLogLog aggregation, Kafka consumer
    └── follows/            # Follow model, controller, Kafka consumer
```
