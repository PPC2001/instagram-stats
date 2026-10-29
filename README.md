# Instagram Stats & Backend API

A high-throughput, event-driven Instagram clone and analytics backend built with **Fastify 5**, **TypeScript**, **MongoDB Replica Set**, **Apache Kafka**, and **Redis**.

Includes an interactive single-page web UI (`public/index.html`), OpenAPI/Swagger documentation, and background event consumers designed to handle high-concurrency engagement traffic (likes, views, follows) asynchronously.

---

## Architecture & System Design

```
                     ┌──────────────────┐
                     │   Browser / UI   │ (http://localhost:5003)
                     └────────┬─────────┘
                              │ HTTP Requests (x-user-id)
                              ▼
                     ┌──────────────────┐
                     │  Fastify 5 API   │
                     └────┬────────┬────┘
           Read / Sync    │        │ Async Events (likes, views, follows)
                          ▼        ▼
┌──────────────┐     ┌────────┐  ┌─────────────┐
│  Redis Cache │◄────┤ Mongoose├──┤ Kafka Broker│ (localhost:9092)
│  (Counters,  │     │ Driver │  └──────┬──────┘
│  HLL, Cuckoo)│     └────┬───┘         │ Event Streams
└──────────────┘          │             ▼
       ▲                  │      ┌─────────────────┐
       │                  │      │ Kafka Consumers │ (pnpm consumers)
       └──────────────────┴──────┤ (Writes Mongo & │
                                 │ updates Redis)  │
                                 └─────────────────┘
```

### Key Architectural Highlights
* **Asynchronous Write Path**: High-velocity actions (likes, follows, and view tracking) are published to Kafka topics (`likes`, `follows`, `views`) and immediately return `202 Accepted` to minimize API latency.
* **Decoupled Consumer Workers**: A separate worker process (`pnpm consumers`) processes Kafka partitions independently, persisting relationships to MongoDB and maintaining real-time metrics in Redis.
* **Redis Analytics & Probabilistic Data Structures**:
  * **Counters**: Real-time follower/following/likes counters.
  * **HyperLogLog (`HLL`)**: Efficient unique views (`uv`) calculation per post.
  * **Cuckoo Filters**: Fast set-membership checks (e.g. `isLiked`, `isFollowing`, `isSeen`).
* **MongoDB Fault Tolerance**: Configured for MongoDB Replica Set (`fk-rs`) with `w: majority` writes and automatic topology detection (`MONGO_MODE=auto`).

---

## Tech Stack

| Component | Choice | Description |
|---|---|---|
| **Runtime** | Node.js (ESM) | Tested on Node v20+ |
| **Language** | TypeScript ^7.0 | Strict type safety |
| **HTTP Framework** | Fastify 5 | High-speed, low-overhead Node.js web framework |
| **Message Broker** | Apache Kafka 4.x (KRaft) | Single-broker KRaft cluster (no Zookeeper required) |
| **Primary Database** | MongoDB (Replica Set `fk-rs`) | 1 Primary + 2 Secondaries via Docker Compose |
| **Cache & State** | Redis | Fast counters, HyperLogLog, and Cuckoo filters |
| **Validation** | Zod | Startup environment validation fail-fast |
| **Logging** | Winston | Formatted console + rotating file logs |
| **Build Tool** | tsup (esbuild) | Fast bundling with watch and minification modes |
| **API Docs** | Swagger / OpenAPI UI | Auto-generated interactive documentation |

---

## Prerequisites

Ensure the following tools are installed on your machine:
1. **Node.js** v20.x or higher
2. **pnpm** (`npm install -g pnpm`)
3. **Docker & Docker Compose** (for running MongoDB replica set & Kafka)
4. **Redis Server** running locally on port `6379` (or via Docker)

---

## How to Run Locally

### 1. Clone & Install Dependencies

```bash
pnpm install
```

### 2. Configure Environment Variables

Create your `.env` file from the example:

```bash
# On Windows PowerShell:
Copy-Item .env.example .env

# On macOS/Linux:
cp .env.example .env
```

Open `.env` and verify the values. **Important:** Zod schema validation requires `JWT_ACCESS_SECRET` and `JWT_REFRESH_SECRET` to be **at least 32 characters** long.

```env
PORT=5003
MONGO_URI=mongodb://127.0.0.1:27017,127.0.0.1:27018,127.0.0.1:27019/instagram
MONGO_MODE=auto
REDIS_URL=redis://127.0.0.1:6379
KAFKA_BROKERS=127.0.0.1:9092
KAFKA_CLIENT_ID=insta-stats
LOG_LEVEL=info
NODE_ENV=development
ENABLE_ZSTD=false
COMPRESSION_LEVEL=1

# Minimum 32 characters required
JWT_ACCESS_SECRET=your_super_secret_access_key_min_32_chars!
JWT_REFRESH_SECRET=your_super_secret_refresh_key_min_32_chars!
JWT_ACCESS_EXPIRES_IN=6h
JWT_REFRESH_EXPIRES_IN=7d

MONGO_CIRCUIT_MS=2000
MONGO_CIRCUIT_FAILS=3
MONGO_SELECT_TIMEOUT_MS=10000
MONGO_MAX_POOL=50
MONGO_MIN_POOL=10
```

> **Note on Standalone MongoDB:** If you use a single standalone MongoDB instance instead of Docker (e.g. `mongodb://127.0.0.1:27017/instagram`), keep `MONGO_MODE=auto`. The server will detect standalone mode and adjust write concerns accordingly.

---

### 3. Start Infrastructure Services

#### A. Start MongoDB Replica Set
```bash
pnpm mongo:up
```
*Spins up 1 primary (`27017`) and 2 secondary (`27018`, `27019`) containers and executes replica initiation (`fk-rs`).*

Check status anytime:
```bash
pnpm mongo:status
```

#### B. Start Kafka Broker
```bash
pnpm kafka:up
```
*Starts Apache Kafka in KRaft mode listening on `localhost:9092`.*

#### C. Start Redis
Make sure Redis is active on `127.0.0.1:6379`. If you do not have Redis installed natively, run:
```bash
docker run -d --name insta-redis -p 6379:6379 redis:alpine
```

---

### 4. Seed Demo Data

Populate demo profiles, posts, reels, and comments:
```bash
pnpm seed:insta
```

*(Optional: To bulk-seed ecommerce products for load testing, run `pnpm seed:products`).*

---

### 5. Start the Application

Open **two terminal windows**:

#### Terminal 1 — Start the API Server:
```bash
pnpm dev
```
Starts Fastify on `http://localhost:5003` with hot-reloading enabled.

#### Terminal 2 — Start the Event Consumers:
```bash
pnpm consumers
```
Runs the Kafka consumers for likes, views, and follows to process events and sync with Mongo/Redis.

---

### 6. Verify and Access

| Interface | URL | Description |
|---|---|---|
| **Web UI** | [http://localhost:5003](http://localhost:5003) | Interactive Instagram web interface |
| **Swagger UI** | [http://localhost:5003/api-docs](http://localhost:5003/api-docs) | Interactive API documentation |
| **Health Check** | [http://localhost:5003/health](http://localhost:5003/health) | Simple service ping |
| **Readiness Check** | [http://localhost:5003/ready](http://localhost:5003/ready) | Checks MongoDB replica topology & Redis status |

---

## Testing & Simulating Engagement (Views, Likes, Comments, Followers)

There are two primary ways to test and increase metrics in the system:
1. **The CLI Boost Tool (`pnpm boost`)**: For simulated high-volume traffic (hundreds to hundreds of thousands of events).
2. **The Direct REST API (cURL / HTTP)**: For interactive, individual event testing.

> **Important**: Ensure both `pnpm dev` and `pnpm consumers` are running. Because metrics are processed asynchronously through Kafka, numbers update in MongoDB and Redis as consumers process the stream.

---

### Method 1: High-Volume Traffic via `pnpm boost`

The `pnpm boost` script generates realistic traffic through Kafka using unique simulated user accounts (`boost-<timestamp>-<n>`). It ensures MongoDB entities, Redis counters, HyperLogLog (`uniqueViews`), and Cuckoo filters stay fully consistent.

> [!NOTE]
> Running `pnpm boost` without `--yes` performs a **dry-run** and only prints the execution plan. Always append `--yes` to send the events.

#### First: Find an existing Post ID or Username
Seed the demo data first (`pnpm seed:insta`), then fetch an ID:
```bash
# Get demo profiles:
curl -s http://localhost:5003/profiles

# Get posts to pick a post ID:
curl -s http://localhost:5003/posts
```
*(Demo usernames: `virat.kohli`, `alia.bhatt`, `tech.insider`)*

#### A. Increase Views & Unique Views on a Post
* `views`: Total number of view events sent.
* `unique`: Number of distinct viewers (calculated via Redis **HyperLogLog**). Cannot exceed `views`.
```bash
# Add 10,000 total views with 2,500 unique viewers:
pnpm boost -- --post=<POST_ID> --views=10000 --unique=2500 --yes
```

#### B. Increase Likes on a Post
```bash
# Add 1,500 likes to a post:
pnpm boost -- --post=<POST_ID> --likes=1500 --yes
```

#### C. Increase Comments on a Post
Inserts comments directly to MongoDB and increments the Redis comment counter:
```bash
# Add 250 comments to a post:
pnpm boost -- --post=<POST_ID> --comments=250 --yes
```

#### D. Increase Followers for a Profile
```bash
# Add 50,000 followers to virat.kohli:
pnpm boost -- --profile=virat.kohli --followers=50000 --yes
```

#### E. All-In-One Post & Profile Boost
You can combine all parameters in a single command:
```bash
pnpm boost -- --post=<POST_ID> --profile=virat.kohli --views=20000 --unique=5000 --likes=2000 --comments=100 --followers=10000 --yes
```

---

### Method 2: Manual Testing via Direct REST API

You can also send real-time individual requests using `curl` or any API client (Postman, HTTPie). All write actions require an `x-user-id` header representing the acting user.

#### A. Increase View Count on a Post
```bash
# Call when a post is opened:
curl -X POST http://localhost:5003/posts/<POST_ID>/view \
  -H "x-user-id: user_123"
```
*(Queues to Kafka topic `views`; consumer increments total views counter and adds `user_123` to the HyperLogLog).*

#### B. Like a Post
```bash
curl -X POST http://localhost:5003/posts/<POST_ID>/like \
  -H "x-user-id: user_123"
```
*(Queues to Kafka topic `likes`; consumer adds like in Mongo, increments Redis counter, and registers in user's Cuckoo filter).*

To unlike:
```bash
curl -X DELETE http://localhost:5003/posts/<POST_ID>/like \
  -H "x-user-id: user_123"
```

#### C. Follow a Profile
```bash
curl -X POST http://localhost:5003/profiles/virat.kohli/follow \
  -H "x-user-id: user_123"
```
*(Queues to Kafka topic `follows`; consumer increments followers/following in Redis and records relation in Mongo).*

To unfollow:
```bash
curl -X DELETE http://localhost:5003/profiles/virat.kohli/follow \
  -H "x-user-id: user_123"
```

#### D. Check the Updated Stats

View updated metrics for a post (including `totalViews`, `uniqueViews`, `likes`, `comments`):
```bash
curl http://localhost:5003/posts/<POST_ID> \
  -H "x-user-id: user_123"
```

View updated profile stats (`postsCount`, `followers`, `following`):
```bash
curl http://localhost:5003/profiles/virat.kohli \
  -H "x-user-id: user_123"
```

---

## Available Scripts

| Script | Purpose |
|---|---|
| `pnpm dev` | Starts server in watch mode using `tsup` |
| `pnpm consumers` | Runs background Kafka consumers |
| `pnpm seed:insta` | Seeds demo Instagram accounts, posts, reels, and comments |
| `pnpm seed:products` | Seeds bulk products with configurable volume |
| `pnpm boost -- --post=<id> --likes=N --views=N --yes` | Simulates high volume traffic through Kafka |
| `pnpm build` | Compiles TypeScript code with sourcemaps |
| `pnpm build:prod` | Clean build with minification for production |
| `pnpm start:prod` | Runs production build in cluster mode |
| `pnpm typecheck` | Runs `tsc --noEmit` to validate types |
| `pnpm mongo:up` | Starts Dockerized MongoDB replica set |
| `pnpm mongo:down` | Stops MongoDB containers (preserves volumes) |
| `pnpm mongo:reset` | Resets MongoDB volumes and re-initiates |
| `pnpm kafka:up` | Starts Dockerized Kafka broker |
| `pnpm kafka:down` | Stops Kafka container |
| `pnpm kafka:logs` | Streams recent Kafka logs |

---

## API Summary

All list endpoints support cursor-based pagination via `?cursor=<id>&limit=<n>` (default 20, max 100). Authenticated actions accept the user ID in the `x-user-id` header.

### Instagram Endpoints
* `GET /profiles` — List all user profiles
* `GET /profiles/:username` — Get user profile stats (posts count, followers, following)
* `POST /profiles/:username/follow` — Follow user (queued via Kafka)
* `DELETE /profiles/:username/follow` — Unfollow user (queued via Kafka)
* `GET /posts` — Feed of posts/reels (`?authorId&type=post|reel&cursor&limit`)
* `GET /posts/:id` — Single post details
* `GET /posts/:id/comments` — Comments for a post
* `GET /reels` — Reels feed
* `POST /posts/:id/view` — Record a view (queued via Kafka)
* `POST /posts/:id/like` — Like a post (queued via Kafka)
* `DELETE /posts/:id/like` — Unlike a post (queued via Kafka)

---

## License
ISC
