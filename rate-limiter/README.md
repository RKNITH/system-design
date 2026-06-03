# 🚦 Rate Limiter — System Design (HLD + LLD)
> Industry-standard, FAANG-level deep dive into designing a distributed rate limiter from scratch.

---

## Table of Contents

1. [Problem Statement](#1-problem-statement)
2. [Requirements Gathering](#2-requirements-gathering)
3. [Capacity Estimation](#3-capacity-estimation)
4. [High-Level Design (HLD)](#4-high-level-design-hld)
   - 4.1 [Where to Place the Rate Limiter](#41-where-to-place-the-rate-limiter)
   - 4.2 [System Architecture Diagram](#42-system-architecture-diagram)
   - 4.3 [Core Components](#43-core-components)
   - 4.4 [Rate Limiter Algorithms — Comparison](#44-rate-limiter-algorithms--comparison)
   - 4.5 [Distributed Rate Limiting Challenges](#45-distributed-rate-limiting-challenges)
   - 4.6 [Storage Strategy](#46-storage-strategy)
   - 4.7 [API Contract](#47-api-contract)
   - 4.8 [Configuration Service](#48-configuration-service)
   - 4.9 [Observability & Monitoring](#49-observability--monitoring)
5. [Low-Level Design (LLD)](#5-low-level-design-lld)
   - 5.1 [Token Bucket — JavaScript Implementation](#51-token-bucket--javascript-implementation)
   - 5.2 [Sliding Window Log — JavaScript Implementation](#52-sliding-window-log--javascript-implementation)
   - 5.3 [Sliding Window Counter — JavaScript Implementation](#53-sliding-window-counter--javascript-implementation)
   - 5.4 [Leaky Bucket — JavaScript Implementation](#54-leaky-bucket--javascript-implementation)
   - 5.5 [Fixed Window Counter — JavaScript Implementation](#55-fixed-window-counter--javascript-implementation)
   - 5.6 [Distributed Rate Limiter with Redis — JavaScript Implementation](#56-distributed-rate-limiter-with-redis--javascript-implementation)
   - 5.7 [Express Middleware Integration](#57-express-middleware-integration)
   - 5.8 [Rule Engine & Configuration](#58-rule-engine--configuration)
   - 5.9 [Class Diagram & Data Models](#59-class-diagram--data-models)
6. [Edge Cases & Failure Scenarios](#6-edge-cases--failure-scenarios)
7. [Performance Optimizations](#7-performance-optimizations)
8. [Trade-offs & Design Decisions](#8-trade-offs--design-decisions)
9. [Real-world References](#9-real-world-references)

---

## 1. Problem Statement

Design a **Rate Limiter** that controls the rate of requests a client can make to an API. It should:

- Prevent abuse, DDoS attacks, and resource starvation
- Enforce SLA tiers (free, pro, enterprise)
- Work at massive scale (millions of requests/sec)
- Be distributed across multiple nodes/data centers
- Have minimal latency overhead (< 5ms p99)
- Be highly available (no single point of failure)

**Real-world examples:** GitHub API (5000 req/hr), Twitter API, Stripe, Cloudflare, AWS API Gateway.

---

## 2. Requirements Gathering

### Functional Requirements

| # | Requirement |
|---|-------------|
| FR1 | Limit requests per user/IP/API-key within a time window |
| FR2 | Support multiple rate limit rules (per endpoint, per user tier) |
| FR3 | Return appropriate HTTP headers (`X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset`) |
| FR4 | Return `HTTP 429 Too Many Requests` when limit is exceeded |
| FR5 | Support different algorithms (token bucket, sliding window, etc.) |
| FR6 | Allow rule configuration without redeployment |
| FR7 | Support burst allowance (short bursts above steady-state rate) |

### Non-Functional Requirements

| # | Requirement | Target |
|---|-------------|--------|
| NFR1 | Low latency | < 5ms added overhead (p99) |
| NFR2 | High availability | 99.99% uptime |
| NFR3 | Scalability | 10M+ req/sec |
| NFR4 | Accuracy | < 0.1% error in rate counting |
| NFR5 | Fault tolerance | Graceful degradation if limiter is down |
| NFR6 | Consistency | Eventual consistency acceptable; strong preferred |

### Out of Scope

- Authentication / Authorization
- Load balancing logic
- Billing enforcement (rate limiting is a mechanism, not a billing engine)

---

## 3. Capacity Estimation

```
Assumptions:
- 500 million users
- Average 10 API requests/user/day
- Peak traffic: 10x average

Daily requests     = 500M × 10           = 5 billion/day
Requests per second (avg) = 5B / 86400  ≈ 58,000 req/sec
Requests per second (peak) = 58K × 10  ≈ 580,000 req/sec

Storage (Redis):
- Per user entry: ~100 bytes (key + counter + TTL)
- Active users at peak: ~10M
- Storage needed: 10M × 100 bytes          = ~1 GB RAM in Redis

Bandwidth:
- Each rate-limit check: ~200 bytes in/out
- 580K req/sec × 200 bytes                 = ~116 MB/sec

Redis throughput:
- Redis handles ~100K-1M ops/sec per node
- With 580K req/sec, need ~1-6 Redis nodes (with pipelining)
- Use Redis Cluster with 6 shards for headroom
```

---

## 4. High-Level Design (HLD)

### 4.1 Where to Place the Rate Limiter

There are three placement options:

#### Option A: Client-Side
- ❌ Unreliable — clients can bypass
- ❌ No centralized enforcement

#### Option B: API Gateway / Edge (Recommended ✅)
- ✅ Centralized enforcement before hitting services
- ✅ No code changes in individual microservices
- ✅ Can inspect full request context (IP, headers, API key)
- Used by: AWS API Gateway, Kong, Nginx, Cloudflare

#### Option C: Application-Level (Per Service)
- ✅ Fine-grained control per service
- ❌ Duplicate implementation across services
- ❌ Harder to enforce global limits

#### Option D: Hybrid (Best for FAANG-scale ✅✅)
- Edge layer handles IP-based and global limits
- Application layer handles per-user, per-endpoint, per-tenant limits
- Both share a central Redis store

---

### 4.2 System Architecture Diagram

```
                         ┌─────────────────────────────────────────────────────┐
                         │                   CLIENT                             │
                         └───────────────────────┬─────────────────────────────┘
                                                 │  HTTP Request
                                                 ▼
                         ┌─────────────────────────────────────────────────────┐
                         │              DNS / Load Balancer                     │
                         │           (GeoDNS, Anycast routing)                  │
                         └───────────────────────┬─────────────────────────────┘
                                                 │
                  ┌──────────────────────────────┼───────────────────────────────┐
                  ▼                              ▼                               ▼
         ┌─────────────┐              ┌─────────────────┐              ┌──────────────┐
         │  Edge Node  │              │   Edge Node     │              │  Edge Node   │
         │ (Region US) │              │  (Region EU)    │              │ (Region Asia)│
         └──────┬──────┘              └────────┬────────┘              └──────┬───────┘
                │                              │                              │
                └──────────────────────────────┼──────────────────────────────┘
                                               │
                                               ▼
                         ┌─────────────────────────────────────────────────────┐
                         │              API GATEWAY                             │
                         │  ┌──────────────────────────────────────────────┐   │
                         │  │           Rate Limiter Middleware             │   │
                         │  │  1. Extract identifier (IP/UserID/APIKey)    │   │
                         │  │  2. Fetch rule from Config Service           │   │
                         │  │  3. Check + Increment counter in Redis       │   │
                         │  │  4. Allow / Reject request                   │   │
                         │  └──────────────────────────────────────────────┘   │
                         └───────────────────────┬─────────────────────────────┘
                                    ┌────────────┴───────────┐
                                    ▼                        ▼
                         ┌──────────────────┐    ┌───────────────────────┐
                         │  Config Service  │    │   Redis Cluster        │
                         │  (Rate Limit     │    │   (Counter Storage)    │
                         │   Rules DB)      │    │                        │
                         │  - Rules by tier │    │  ┌──────┐ ┌──────┐   │
                         │  - Endpoint cfg  │    │  │Shard1│ │Shard2│   │
                         │  - Cached in     │    │  └──────┘ └──────┘   │
                         │    local memory  │    │  ┌──────┐ ┌──────┐   │
                         └──────────────────┘    │  │Shard3│ │Shard4│   │
                                                 │  └──────┘ └──────┘   │
                                                 └───────────────────────┘
                                                           │
                                                           ▼
                                                ┌──────────────────────┐
                                                │  Monitoring / Alerts  │
                                                │  (Prometheus/Grafana) │
                                                └──────────────────────┘
                                    │
                                    ▼
                         ┌─────────────────────────────────────────────────────┐
                         │              BACKEND MICROSERVICES                   │
                         │  ┌─────────────┐  ┌──────────────┐  ┌───────────┐  │
                         │  │ User Service│  │Order Service │  │Pay Service│  │
                         │  └─────────────┘  └──────────────┘  └───────────┘  │
                         └─────────────────────────────────────────────────────┘
```

---

### 4.3 Core Components

#### 1. Rate Limiter Middleware
- Intercepts every incoming request
- Extracts the **rate limit key** (user ID, API key, IP, or composite)
- Calls the **Rate Limit Engine** to check/update counters
- Returns `429` or passes request forward

#### 2. Rate Limit Engine
- Implements the chosen algorithm (token bucket, sliding window, etc.)
- Atomically reads and writes to Redis using Lua scripts
- Returns: `{ allowed: bool, remaining: int, retryAfter: int }`

#### 3. Config/Rules Service
- Stores rate limit rules per user tier, endpoint, or tenant
- Rules cached locally (in-memory LRU cache, TTL = 60s) to avoid DB round-trip
- Rule structure: `{ identifier, limit, windowSizeSeconds, algorithm, burstLimit }`

#### 4. Redis Cluster
- Primary storage for counters and sliding window logs
- Lua scripts ensure atomic operations
- Key schema: `ratelimit:{userId}:{endpoint}:{windowStart}`
- TTL auto-expires stale keys

#### 5. Fallback / Circuit Breaker
- If Redis is unreachable → fail open (allow requests) or fail closed (block all)
- Configurable per deployment (safety vs. availability tradeoff)

#### 6. Response Headers
```
X-RateLimit-Limit: 1000
X-RateLimit-Remaining: 450
X-RateLimit-Reset: 1719235200
Retry-After: 30          (only on 429)
```

---

### 4.4 Rate Limiter Algorithms — Comparison

| Algorithm | Memory | Accuracy | Burst Handling | Complexity | Best For |
|---|---|---|---|---|---|
| **Fixed Window Counter** | Low | Low (edge spike) | Poor | Simple | Simple APIs, coarse limits |
| **Sliding Window Log** | High | Perfect | Good | Medium | Strict per-request fairness |
| **Sliding Window Counter** | Low | Good | Good | Medium | Most production use cases ✅ |
| **Token Bucket** | Low | Good | Excellent (burst) | Medium | APIs with burst requirements ✅ |
| **Leaky Bucket** | Low | Good | Smooths traffic | Medium | Downstream protection |

**FAANG recommendation:** Use **Token Bucket** for user-facing APIs (allows burst) and **Sliding Window Counter** for backend service-to-service limits (smooth, predictable).

---

#### Algorithm Deep-Dives

**Fixed Window Counter**
```
|--window1--|--window2--|--window3--|
  0   500  1000   0   500  1000
  ↑ Problem: 1000 requests in 1 second at window boundary (500 end + 500 start)
```

**Sliding Window Log**
```
Maintain a sorted set of request timestamps.
Count entries in [now - windowSize, now].
Most accurate but O(n) memory per user.
```

**Sliding Window Counter (Hybrid) — Recommended**
```
count = (prev_window_count × overlap_ratio) + current_window_count

If window = 60s, we're 40s into current window:
  overlap = (60-40)/60 = 33% of previous window
  effective_count = prev_count × 0.33 + curr_count
```

**Token Bucket**
```
Bucket starts full (capacity = burstLimit).
Tokens refill at rate = limit/windowSize per second.
Each request consumes 1 token.
If tokens available → allow. Else → reject.
```

**Leaky Bucket**
```
Requests enter a queue (bucket).
Processed at a fixed rate (leak rate).
If bucket full → reject.
Smooths bursty traffic into steady output.
```

---

### 4.5 Distributed Rate Limiting Challenges

#### Problem 1: Race Condition
Two nodes simultaneously read counter = 99 (limit = 100). Both allow. Counter becomes 101.

**Solution:** Atomic Redis operations using Lua scripts (read-increment-expire in one transaction).

#### Problem 2: Clock Skew Between Nodes
Window boundaries differ by a few milliseconds across nodes.

**Solution:** Use Redis server time (`TIME` command) as the authoritative clock, not application server time.

#### Problem 3: Hot Keys
A popular API key generates millions of requests — all hitting the same Redis key.

**Solution:**
- **Key sharding:** Spread into N sub-keys (`key:0` to `key:N-1`), sum across shards
- **Local counters + sync:** Each app node maintains a local counter, syncs to Redis every 100ms (approximate but fast)

#### Problem 4: Redis Node Failure
Single Redis node goes down → rate limiting breaks.

**Solution:**
- **Redis Cluster:** Auto-sharding + replication
- **Redis Sentinel:** Automatic failover for primary nodes
- **Circuit breaker:** Fail open on Redis timeout (allow requests) with alerting

#### Problem 5: Multi-Region Consistency
Users span multiple data centers. Which region's Redis is authoritative?

**Solutions:**
- **Per-region limits:** Each region enforces N/R requests (N = global limit, R = regions)
- **Central Redis with regional replicas:** Writes to primary, reads from replica (slight lag)
- **CRDT-based counters:** Eventually consistent distributed counters (used by Cloudflare)

---

### 4.6 Storage Strategy

#### Redis Key Schema

```
# Fixed Window Counter
ratelimit:fw:{userId}:{endpoint}:{windowTimestamp}
  → value: integer counter
  → TTL: 2 × windowSize

# Sliding Window Log (Sorted Set)
ratelimit:swl:{userId}:{endpoint}
  → sorted set: { score: timestamp, member: requestId }
  → TTL: windowSize

# Sliding Window Counter
ratelimit:swc:{userId}:{endpoint}:prev    → integer
ratelimit:swc:{userId}:{endpoint}:curr    → integer
ratelimit:swc:{userId}:{endpoint}:wstart  → epoch seconds

# Token Bucket
ratelimit:tb:{userId}:{endpoint}
  → hash: { tokens: float, lastRefill: epoch_ms }
```

#### Redis Data Structures

| Algorithm | Redis Structure | Why |
|---|---|---|
| Fixed Window | `STRING` + `INCR` | Atomic increment, simple TTL |
| Sliding Window Log | `ZSET` (sorted set) | Efficient range queries by timestamp |
| Sliding Window Counter | `STRING` × 2 | Simple, low memory |
| Token Bucket | `HASH` | Store multiple fields atomically |

---

### 4.7 API Contract

#### Rate Limit Check (Internal)

```
POST /internal/ratelimit/check
Content-Type: application/json

Request:
{
  "identifier": "user:12345",
  "endpoint": "/api/v1/search",
  "method": "GET",
  "requestId": "req-uuid-abc"
}

Response (200 - Allowed):
{
  "allowed": true,
  "limit": 1000,
  "remaining": 450,
  "resetAt": 1719235200,
  "retryAfter": null
}

Response (429 - Throttled):
{
  "allowed": false,
  "limit": 1000,
  "remaining": 0,
  "resetAt": 1719235200,
  "retryAfter": 30,
  "error": {
    "code": "RATE_LIMIT_EXCEEDED",
    "message": "Too many requests. Please retry after 30 seconds."
  }
}
```

#### Rate Limit Rules CRUD (Admin)

```
GET    /admin/ratelimit/rules              → List all rules
POST   /admin/ratelimit/rules              → Create rule
PUT    /admin/ratelimit/rules/:id          → Update rule
DELETE /admin/ratelimit/rules/:id          → Delete rule
GET    /admin/ratelimit/status/:identifier → Current usage for identifier
POST   /admin/ratelimit/reset/:identifier  → Reset limits (for testing/ops)
```

---

### 4.8 Configuration Service

Rules are stored in a **database** (e.g., PostgreSQL) and cached in application memory:

```json
{
  "rules": [
    {
      "id": "rule-001",
      "name": "Free Tier Default",
      "priority": 10,
      "matcher": { "userTier": "free" },
      "limit": 100,
      "windowSeconds": 3600,
      "algorithm": "sliding_window_counter",
      "burstMultiplier": 1.5
    },
    {
      "id": "rule-002",
      "name": "Search Endpoint - All Users",
      "priority": 20,
      "matcher": { "endpoint": "/api/v1/search" },
      "limit": 30,
      "windowSeconds": 60,
      "algorithm": "token_bucket",
      "burstMultiplier": 2.0
    },
    {
      "id": "rule-003",
      "name": "Enterprise Unlimited",
      "priority": 1,
      "matcher": { "userTier": "enterprise" },
      "limit": 1000000,
      "windowSeconds": 3600,
      "algorithm": "token_bucket"
    }
  ]
}
```

**Rule Matching Priority:** Higher priority number = more specific = wins.

---

### 4.9 Observability & Monitoring

#### Key Metrics (Prometheus)

```
# Counter: total rate limit decisions
ratelimiter_requests_total{result="allowed|throttled", tier="free|pro|enterprise", endpoint="/api/v1/..."}

# Histogram: latency of rate limit check
ratelimiter_check_duration_ms{quantile="0.5|0.95|0.99"}

# Gauge: current token/counter values
ratelimiter_tokens_remaining{userId="...", endpoint="..."}

# Counter: Redis errors
ratelimiter_redis_errors_total{type="timeout|connection|lua_error"}

# Gauge: throttle rate (% of requests throttled)
ratelimiter_throttle_rate{tier="...", endpoint="..."}
```

#### Alerting Rules

```yaml
- alert: HighThrottleRate
  expr: ratelimiter_throttle_rate > 0.10
  for: 5m
  annotations:
    summary: "More than 10% of requests being throttled"

- alert: RedisLatencyHigh
  expr: ratelimiter_check_duration_ms{quantile="0.99"} > 10
  for: 2m
  annotations:
    summary: "Rate limiter p99 latency exceeds 10ms"

- alert: RedisDown
  expr: ratelimiter_redis_errors_total rate > 10
  for: 1m
  annotations:
    summary: "Redis errors detected - rate limiter may be degraded"
```

---

## 5. Low-Level Design (LLD)

### 5.1 Token Bucket — JavaScript Implementation

```javascript
// TokenBucket.js
// In-memory implementation (for single-node or testing)
// Production version uses Redis backend (see Section 5.6)

class TokenBucket {
  /**
   * @param {number} capacity      - Max tokens (burst limit)
   * @param {number} refillRate    - Tokens added per second
   */
  constructor(capacity, refillRate) {
    this.capacity = capacity;
    this.refillRate = refillRate;    // tokens per second
    this.tokens = capacity;         // start full
    this.lastRefillTime = Date.now();
  }

  /**
   * Refill tokens based on elapsed time since last refill.
   * Called lazily on each consume() — no background timer needed.
   */
  _refill() {
    const now = Date.now();
    const elapsedSeconds = (now - this.lastRefillTime) / 1000;
    const tokensToAdd = elapsedSeconds * this.refillRate;

    if (tokensToAdd > 0) {
      this.tokens = Math.min(this.capacity, this.tokens + tokensToAdd);
      this.lastRefillTime = now;
    }
  }

  /**
   * Attempt to consume `tokens` from the bucket.
   * @param {number} tokens - Number of tokens to consume (default: 1)
   * @returns {{ allowed: boolean, remaining: number, retryAfterMs: number }}
   */
  consume(tokens = 1) {
    this._refill();

    if (this.tokens >= tokens) {
      this.tokens -= tokens;
      return {
        allowed: true,
        remaining: Math.floor(this.tokens),
        retryAfterMs: 0,
      };
    }

    // Calculate how long until enough tokens accumulate
    const tokensNeeded = tokens - this.tokens;
    const retryAfterMs = Math.ceil((tokensNeeded / this.refillRate) * 1000);

    return {
      allowed: false,
      remaining: 0,
      retryAfterMs,
    };
  }

  /**
   * Get current state without consuming.
   */
  getState() {
    this._refill();
    return {
      tokens: Math.floor(this.tokens),
      capacity: this.capacity,
      refillRate: this.refillRate,
    };
  }
}


// ─── TokenBucketStore ────────────────────────────────────────────────────────
// Manages per-user/per-key buckets with automatic cleanup

class TokenBucketStore {
  constructor() {
    // Map<string, { bucket: TokenBucket, lastAccessed: number }>
    this.buckets = new Map();
    // Cleanup idle buckets every 5 minutes
    this._cleanupInterval = setInterval(() => this._cleanup(), 5 * 60 * 1000);
  }

  /**
   * Get or create a bucket for the given key.
   * @param {string} key
   * @param {object} config - { capacity, refillRate }
   */
  getBucket(key, config) {
    if (!this.buckets.has(key)) {
      this.buckets.set(key, {
        bucket: new TokenBucket(config.capacity, config.refillRate),
        lastAccessed: Date.now(),
      });
    }

    const entry = this.buckets.get(key);
    entry.lastAccessed = Date.now();
    return entry.bucket;
  }

  /**
   * Remove buckets not accessed in the last 10 minutes.
   */
  _cleanup() {
    const tenMinutesAgo = Date.now() - 10 * 60 * 1000;
    for (const [key, entry] of this.buckets.entries()) {
      if (entry.lastAccessed < tenMinutesAgo) {
        this.buckets.delete(key);
      }
    }
  }

  destroy() {
    clearInterval(this._cleanupInterval);
  }
}


// ─── Usage Example ────────────────────────────────────────────────────────────

const store = new TokenBucketStore();

function checkRateLimit(userId, config) {
  const bucket = store.getBucket(`user:${userId}`, config);
  return bucket.consume();
}

// Config: 100 requests/hour with burst up to 200
const config = { capacity: 200, refillRate: 100 / 3600 };

console.log(checkRateLimit('user123', config));
// { allowed: true, remaining: 199, retryAfterMs: 0 }
```

---

### 5.2 Sliding Window Log — JavaScript Implementation

```javascript
// SlidingWindowLog.js
// Most accurate algorithm — stores timestamps of all requests

class SlidingWindowLog {
  /**
   * @param {number} limit        - Max requests allowed
   * @param {number} windowMs     - Window size in milliseconds
   */
  constructor(limit, windowMs) {
    this.limit = limit;
    this.windowMs = windowMs;
    // Map<key, number[]> — sorted timestamps
    this.logs = new Map();
  }

  /**
   * Check and record a request for the given key.
   * Time complexity: O(n) where n = requests in window
   * Space complexity: O(n × users) — HIGH — use Redis ZSET in production
   *
   * @param {string} key
   * @returns {{ allowed: boolean, remaining: number, resetAt: number }}
   */
  consume(key) {
    const now = Date.now();
    const windowStart = now - this.windowMs;

    // Get existing log for this key
    let timestamps = this.logs.get(key) || [];

    // Remove expired timestamps outside the window — O(log n) with binary search
    const validIdx = this._lowerBound(timestamps, windowStart);
    timestamps = timestamps.slice(validIdx);

    if (timestamps.length < this.limit) {
      // Allow: add current timestamp
      timestamps.push(now);
      this.logs.set(key, timestamps);

      return {
        allowed: true,
        remaining: this.limit - timestamps.length,
        resetAt: timestamps[0] + this.windowMs,   // when oldest request expires
      };
    }

    // Deny: window is full
    const oldestTimestamp = timestamps[0];
    const resetAt = oldestTimestamp + this.windowMs;

    return {
      allowed: false,
      remaining: 0,
      resetAt,
      retryAfterMs: resetAt - now,
    };
  }

  /**
   * Binary search: find first index where timestamps[i] >= target
   */
  _lowerBound(arr, target) {
    let lo = 0, hi = arr.length;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (arr[mid] < target) lo = mid + 1;
      else hi = mid;
    }
    return lo;
  }
}


// ─── Redis-backed Sliding Window Log (Production) ────────────────────────────
// Uses Redis Sorted Sets (ZSET) for distributed accuracy

const SLIDING_WINDOW_LOG_SCRIPT = `
local key = KEYS[1]
local now = tonumber(ARGV[1])
local windowMs = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local requestId = ARGV[4]
local windowStart = now - windowMs

-- Remove entries outside the window
redis.call('ZREMRANGEBYSCORE', key, '-inf', windowStart)

-- Count current entries
local count = redis.call('ZCARD', key)

if count < limit then
  -- Add this request with current timestamp as score
  redis.call('ZADD', key, now, requestId)
  -- Set TTL to avoid stale keys
  redis.call('PEXPIRE', key, windowMs * 2)
  
  local remaining = limit - count - 1
  local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
  local resetAt = oldest[2] and (tonumber(oldest[2]) + windowMs) or (now + windowMs)
  
  return { 1, remaining, resetAt }
else
  local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
  local resetAt = oldest[2] and (tonumber(oldest[2]) + windowMs) or (now + windowMs)
  return { 0, 0, resetAt }
end
`;

// Usage with ioredis:
// const result = await redis.eval(
//   SLIDING_WINDOW_LOG_SCRIPT,
//   1,                                     // numkeys
//   `ratelimit:swl:${userId}:${endpoint}`, // key
//   Date.now(),                            // now
//   windowMs,                              // windowMs
//   limit,                                 // limit
//   crypto.randomUUID()                    // unique requestId
// );
```

---

### 5.3 Sliding Window Counter — JavaScript Implementation

```javascript
// SlidingWindowCounter.js
// Hybrid: combines fixed window counters with weighted interpolation
// Low memory, good accuracy (~0.1-1% error at window edges)

class SlidingWindowCounter {
  /**
   * @param {number} limit      - Max requests in window
   * @param {number} windowMs   - Window size in milliseconds
   */
  constructor(limit, windowMs) {
    this.limit = limit;
    this.windowMs = windowMs;

    // Map<key, { prevCount, currCount, windowStart }>
    this.windows = new Map();
  }

  /**
   * @param {string} key
   * @returns {{ allowed: boolean, remaining: number, resetAt: number }}
   */
  consume(key) {
    const now = Date.now();

    if (!this.windows.has(key)) {
      this.windows.set(key, {
        prevCount: 0,
        currCount: 0,
        windowStart: now,
      });
    }

    const state = this.windows.get(key);
    const windowElapsed = now - state.windowStart;

    // Advance window if current window has expired
    if (windowElapsed >= this.windowMs) {
      const windowsElapsed = Math.floor(windowElapsed / this.windowMs);

      if (windowsElapsed === 1) {
        // Slide by one window
        state.prevCount = state.currCount;
        state.currCount = 0;
        state.windowStart += this.windowMs;
      } else {
        // Multiple windows elapsed — reset fully
        state.prevCount = 0;
        state.currCount = 0;
        state.windowStart = now;
      }
    }

    // Calculate weighted count:
    // What fraction of the previous window overlaps with our current window?
    const currentWindowElapsed = now - state.windowStart;
    const prevWindowWeight = 1 - (currentWindowElapsed / this.windowMs);
    const weightedCount = (state.prevCount * prevWindowWeight) + state.currCount;

    if (weightedCount < this.limit) {
      state.currCount++;
      const remaining = Math.floor(this.limit - weightedCount - 1);
      const resetAt = state.windowStart + this.windowMs;

      return { allowed: true, remaining: Math.max(0, remaining), resetAt };
    }

    const resetAt = state.windowStart + this.windowMs;
    const retryAfterMs = resetAt - now;

    return { allowed: false, remaining: 0, resetAt, retryAfterMs };
  }
}


// ─── Redis Lua Script for Sliding Window Counter ──────────────────────────────

const SLIDING_WINDOW_COUNTER_SCRIPT = `
local currKey = KEYS[1]   -- ratelimit:swc:{id}:curr
local prevKey = KEYS[2]   -- ratelimit:swc:{id}:prev
local limit   = tonumber(ARGV[1])
local windowMs = tonumber(ARGV[2])
local now = tonumber(ARGV[3])

-- Get window start from curr key metadata (stored as sorted set or separate key)
local currCount = tonumber(redis.call('GET', currKey) or 0)
local prevCount = tonumber(redis.call('GET', prevKey) or 0)

-- Get TTL of curr key to calculate elapsed time in window
local ttl = redis.call('PTTL', currKey)
local elapsed = 0
if ttl > 0 then
  elapsed = windowMs - ttl
end

-- Weighted estimate
local prevWeight = 1 - (elapsed / windowMs)
local weightedCount = (prevCount * prevWeight) + currCount

if weightedCount < limit then
  -- Increment current window
  redis.call('INCR', currKey)
  if ttl < 0 then
    -- First request in this window — set TTL
    redis.call('PEXPIRE', currKey, windowMs)
    -- Rotate: curr → prev
    redis.call('SET', prevKey, 0)
    redis.call('PEXPIRE', prevKey, windowMs * 2)
  end
  
  local remaining = math.floor(limit - weightedCount - 1)
  return { 1, math.max(0, remaining), now + (ttl > 0 and ttl or windowMs) }
else
  local resetAt = now + (ttl > 0 and ttl or windowMs)
  return { 0, 0, resetAt }
end
`;
```

---

### 5.4 Leaky Bucket — JavaScript Implementation

```javascript
// LeakyBucket.js
// Ensures a smooth, constant output rate regardless of burstiness

class LeakyBucket {
  /**
   * @param {number} capacity     - Max queue size (burst tolerance)
   * @param {number} leakRateMs   - How often one request "leaks" (processed), in ms
   */
  constructor(capacity, leakRateMs) {
    this.capacity = capacity;
    this.leakRateMs = leakRateMs;   // process 1 request every N ms
    this.queue = 0;                 // current requests in bucket
    this.lastLeakTime = Date.now();
  }

  /**
   * Simulate leaking (processing) requests based on elapsed time.
   */
  _leak() {
    const now = Date.now();
    const elapsed = now - this.lastLeakTime;
    const leaked = Math.floor(elapsed / this.leakRateMs);

    if (leaked > 0) {
      this.queue = Math.max(0, this.queue - leaked);
      this.lastLeakTime += leaked * this.leakRateMs; // avoid drift
    }
  }

  /**
   * Try to add a request to the bucket.
   * @returns {{ allowed: boolean, queueDepth: number, retryAfterMs: number }}
   */
  consume() {
    this._leak();

    if (this.queue < this.capacity) {
      this.queue++;
      return {
        allowed: true,
        queueDepth: this.queue,
        retryAfterMs: 0,
      };
    }

    // Bucket full — calculate how long until space opens
    const retryAfterMs = this.leakRateMs;   // wait for one slot to free up

    return {
      allowed: false,
      queueDepth: this.queue,
      retryAfterMs,
    };
  }
}
```

---

### 5.5 Fixed Window Counter — JavaScript Implementation

```javascript
// FixedWindowCounter.js
// Simplest algorithm — baseline for comparisons

class FixedWindowCounter {
  /**
   * @param {number} limit      - Max requests per window
   * @param {number} windowMs   - Window size in milliseconds
   */
  constructor(limit, windowMs) {
    this.limit = limit;
    this.windowMs = windowMs;
    // Map<key, { count, windowStart }>
    this.counters = new Map();
  }

  /**
   * @param {string} key
   * @returns {{ allowed: boolean, remaining: number, resetAt: number }}
   */
  consume(key) {
    const now = Date.now();
    const currentWindowStart = Math.floor(now / this.windowMs) * this.windowMs;

    const entry = this.counters.get(key);

    // Reset if new window started
    if (!entry || entry.windowStart !== currentWindowStart) {
      this.counters.set(key, { count: 1, windowStart: currentWindowStart });
      return {
        allowed: true,
        remaining: this.limit - 1,
        resetAt: currentWindowStart + this.windowMs,
      };
    }

    if (entry.count < this.limit) {
      entry.count++;
      return {
        allowed: true,
        remaining: this.limit - entry.count,
        resetAt: currentWindowStart + this.windowMs,
      };
    }

    const resetAt = currentWindowStart + this.windowMs;
    return {
      allowed: false,
      remaining: 0,
      resetAt,
      retryAfterMs: resetAt - now,
    };
  }
}

// ─── Redis Lua Script (Atomic Fixed Window) ───────────────────────────────────

const FIXED_WINDOW_SCRIPT = `
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local windowMs = tonumber(ARGV[2])

local count = redis.call('INCR', key)

if count == 1 then
  redis.call('PEXPIRE', key, windowMs)
end

if count <= limit then
  local ttl = redis.call('PTTL', key)
  return { 1, limit - count, ttl }
else
  local ttl = redis.call('PTTL', key)
  return { 0, 0, ttl }
end
`;
```

---

### 5.6 Distributed Rate Limiter with Redis — JavaScript Implementation

```javascript
// DistributedRateLimiter.js
// Production-grade, Redis-backed rate limiter
// Supports all algorithms via pluggable strategy pattern

const crypto = require('crypto');

// ─── Redis Lua Scripts (Atomic operations) ───────────────────────────────────

const SCRIPTS = {
  TOKEN_BUCKET: `
    local key = KEYS[1]
    local capacity = tonumber(ARGV[1])
    local refillRate = tonumber(ARGV[2])   -- tokens per ms
    local now = tonumber(ARGV[3])
    local requested = tonumber(ARGV[4]) or 1

    local data = redis.call('HMGET', key, 'tokens', 'lastRefill')
    local tokens = tonumber(data[1]) or capacity
    local lastRefill = tonumber(data[2]) or now

    -- Refill tokens based on elapsed time
    local elapsed = now - lastRefill
    local newTokens = math.min(capacity, tokens + (elapsed * refillRate))

    if newTokens >= requested then
      newTokens = newTokens - requested
      redis.call('HMSET', key, 'tokens', newTokens, 'lastRefill', now)
      redis.call('PEXPIRE', key, math.ceil(capacity / refillRate) * 2)
      return { 1, math.floor(newTokens), 0 }
    else
      -- Time until enough tokens available
      local needed = requested - newTokens
      local retryAfterMs = math.ceil(needed / refillRate)
      redis.call('HMSET', key, 'tokens', newTokens, 'lastRefill', now)
      redis.call('PEXPIRE', key, math.ceil(capacity / refillRate) * 2)
      return { 0, 0, retryAfterMs }
    end
  `,

  FIXED_WINDOW: `
    local key = KEYS[1]
    local limit = tonumber(ARGV[1])
    local windowMs = tonumber(ARGV[2])
    local count = redis.call('INCR', key)
    if count == 1 then
      redis.call('PEXPIRE', key, windowMs)
    end
    local ttl = redis.call('PTTL', key)
    if count <= limit then
      return { 1, limit - count, ttl }
    else
      return { 0, 0, ttl }
    end
  `,
};


// ─── RateLimitResult ─────────────────────────────────────────────────────────

class RateLimitResult {
  constructor({ allowed, limit, remaining, resetAt, retryAfterMs = 0 }) {
    this.allowed = allowed;
    this.limit = limit;
    this.remaining = remaining;
    this.resetAt = resetAt;             // Unix timestamp (seconds)
    this.retryAfterMs = retryAfterMs;
    this.retryAfterSeconds = Math.ceil(retryAfterMs / 1000);
  }

  toHeaders() {
    const headers = {
      'X-RateLimit-Limit': String(this.limit),
      'X-RateLimit-Remaining': String(Math.max(0, this.remaining)),
      'X-RateLimit-Reset': String(this.resetAt),
    };
    if (!this.allowed) {
      headers['Retry-After'] = String(this.retryAfterSeconds);
    }
    return headers;
  }
}


// ─── DistributedRateLimiter ───────────────────────────────────────────────────

class DistributedRateLimiter {
  /**
   * @param {object} redis       - ioredis client instance
   * @param {object} [options]
   * @param {boolean} [options.failOpen=true]    - Allow requests if Redis is down
   * @param {number}  [options.keyPrefix='rl']   - Redis key prefix
   */
  constructor(redis, options = {}) {
    this.redis = redis;
    this.failOpen = options.failOpen !== false;
    this.keyPrefix = options.keyPrefix || 'rl';

    // Cache SHA1 hashes of loaded Lua scripts for efficiency
    this._scriptSHAs = new Map();
  }

  /**
   * Load a Lua script into Redis and cache its SHA.
   * Uses EVALSHA for subsequent calls (faster than EVAL).
   */
  async _loadScript(name, script) {
    if (this._scriptSHAs.has(name)) {
      return this._scriptSHAs.get(name);
    }
    const sha = await this.redis.script('LOAD', script);
    this._scriptSHAs.set(name, sha);
    return sha;
  }

  /**
   * Execute a cached Lua script, falling back to EVAL if SHA not found.
   */
  async _evalScript(name, script, numkeys, ...args) {
    try {
      const sha = await this._loadScript(name, script);
      return await this.redis.evalsha(sha, numkeys, ...args);
    } catch (err) {
      if (err.message.includes('NOSCRIPT')) {
        // Script not in Redis cache (e.g., after SCRIPT FLUSH) — re-evaluate
        this._scriptSHAs.delete(name);
        return await this.redis.eval(script, numkeys, ...args);
      }
      throw err;
    }
  }

  /**
   * Build a Redis key for a given identifier and endpoint.
   * @param {string} identifier   - user ID, IP, API key
   * @param {string} endpoint     - API endpoint path
   * @param {string} suffix       - algorithm-specific suffix
   */
  _key(identifier, endpoint, suffix = '') {
    // Sanitize identifier to prevent key injection
    const safeId = identifier.replace(/[^a-zA-Z0-9_\-:.@]/g, '_');
    const safeEndpoint = endpoint.replace(/[^a-zA-Z0-9_\-:/]/g, '_');
    return `${this.keyPrefix}:${safeId}:${safeEndpoint}${suffix ? ':' + suffix : ''}`;
  }

  /**
   * Token Bucket rate limit check.
   *
   * @param {string} identifier
   * @param {string} endpoint
   * @param {object} rule       - { limit, windowSeconds, burstMultiplier }
   * @returns {Promise<RateLimitResult>}
   */
  async checkTokenBucket(identifier, endpoint, rule) {
    const { limit, windowSeconds, burstMultiplier = 1.5 } = rule;
    const capacity = Math.floor(limit * burstMultiplier);
    const refillRatePerMs = limit / (windowSeconds * 1000);  // tokens per ms
    const key = this._key(identifier, endpoint, 'tb');
    const now = Date.now();

    try {
      const [allowed, remaining, retryAfterMs] = await this._evalScript(
        'TOKEN_BUCKET',
        SCRIPTS.TOKEN_BUCKET,
        1,           // numkeys
        key,         // KEYS[1]
        capacity,    // ARGV[1]
        refillRatePerMs, // ARGV[2]
        now,         // ARGV[3]
        1            // ARGV[4] — tokens to consume
      );

      return new RateLimitResult({
        allowed: allowed === 1,
        limit,
        remaining: Number(remaining),
        resetAt: Math.floor((now + (capacity / refillRatePerMs)) / 1000),
        retryAfterMs: Number(retryAfterMs),
      });

    } catch (err) {
      return this._handleRedisError(err, identifier, endpoint, rule);
    }
  }

  /**
   * Fixed Window rate limit check.
   */
  async checkFixedWindow(identifier, endpoint, rule) {
    const { limit, windowSeconds } = rule;
    const windowMs = windowSeconds * 1000;
    const windowStart = Math.floor(Date.now() / windowMs) * windowMs;
    const key = this._key(identifier, endpoint, `fw:${windowStart}`);
    const now = Date.now();

    try {
      const [allowed, remaining, ttlMs] = await this._evalScript(
        'FIXED_WINDOW',
        SCRIPTS.FIXED_WINDOW,
        1,
        key,
        limit,
        windowMs
      );

      return new RateLimitResult({
        allowed: allowed === 1,
        limit,
        remaining: Number(remaining),
        resetAt: Math.floor((now + Number(ttlMs)) / 1000),
        retryAfterMs: allowed === 0 ? Number(ttlMs) : 0,
      });

    } catch (err) {
      return this._handleRedisError(err, identifier, endpoint, rule);
    }
  }

  /**
   * Handle Redis errors gracefully.
   * failOpen=true  → allow request (availability over accuracy)
   * failOpen=false → deny request (safety over availability)
   */
  _handleRedisError(err, identifier, endpoint, rule) {
    console.error('[RateLimiter] Redis error:', {
      error: err.message,
      identifier,
      endpoint,
    });

    // Emit metric: ratelimiter_redis_errors_total++

    if (this.failOpen) {
      // Allow request — log warning
      return new RateLimitResult({
        allowed: true,
        limit: rule.limit,
        remaining: rule.limit,
        resetAt: Math.floor(Date.now() / 1000) + rule.windowSeconds,
      });
    } else {
      // Deny request — safe mode
      return new RateLimitResult({
        allowed: false,
        limit: rule.limit,
        remaining: 0,
        resetAt: Math.floor(Date.now() / 1000) + 60,
        retryAfterMs: 60000,
      });
    }
  }

  /**
   * Main entry point: check rate limit using the configured algorithm.
   *
   * @param {string} identifier   - Rate limit subject (userId, IP, API key)
   * @param {string} endpoint     - Request endpoint
   * @param {object} rule         - Rate limit rule from config
   * @returns {Promise<RateLimitResult>}
   */
  async check(identifier, endpoint, rule) {
    switch (rule.algorithm) {
      case 'token_bucket':
        return this.checkTokenBucket(identifier, endpoint, rule);
      case 'fixed_window':
        return this.checkFixedWindow(identifier, endpoint, rule);
      case 'sliding_window_counter':
        // Implementation similar to above using sliding window script
        return this.checkFixedWindow(identifier, endpoint, rule); // simplified
      default:
        return this.checkTokenBucket(identifier, endpoint, rule);
    }
  }
}

module.exports = { DistributedRateLimiter, RateLimitResult };
```

---

### 5.7 Express Middleware Integration

```javascript
// rateLimiterMiddleware.js
// Drop-in Express middleware using the DistributedRateLimiter

const { DistributedRateLimiter } = require('./DistributedRateLimiter');
const RuleEngine = require('./RuleEngine');

/**
 * Extract the best identifier from a request.
 * Priority: API Key > Authenticated User ID > IP
 */
function extractIdentifier(req) {
  // API Key from header or query param
  const apiKey = req.headers['x-api-key'] || req.query.api_key;
  if (apiKey) return `apikey:${apiKey}`;

  // Authenticated user (set by auth middleware)
  if (req.user?.id) return `user:${req.user.id}`;

  // IP address (handle proxies, load balancers)
  const forwarded = req.headers['x-forwarded-for'];
  const ip = forwarded
    ? forwarded.split(',')[0].trim()
    : req.socket.remoteAddress;
  return `ip:${ip}`;
}

/**
 * Create rate limiter middleware.
 *
 * @param {object} redis         - ioredis instance
 * @param {object} [options]
 * @param {Function} [options.onThrottle]    - Custom handler for 429 responses
 * @param {Function} [options.identifierFn]  - Custom identifier extractor
 */
function createRateLimiterMiddleware(redis, options = {}) {
  const limiter = new DistributedRateLimiter(redis, {
    failOpen: options.failOpen !== false,
  });
  const ruleEngine = new RuleEngine(options.rulesConfig);
  const identifierFn = options.identifierFn || extractIdentifier;

  return async function rateLimiterMiddleware(req, res, next) {
    const startTime = Date.now();

    try {
      const identifier = identifierFn(req);
      const endpoint = `${req.method}:${req.route?.path || req.path}`;

      // Find the most specific matching rule
      const rule = ruleEngine.findRule({
        identifier,
        endpoint,
        userTier: req.user?.tier || 'free',
        method: req.method,
      });

      if (!rule) {
        // No rule → allow (or apply a default strict rule)
        return next();
      }

      const result = await limiter.check(identifier, endpoint, rule);

      // Always attach rate limit headers
      const headers = result.toHeaders();
      Object.entries(headers).forEach(([k, v]) => res.setHeader(k, v));

      // Record latency metric
      const latencyMs = Date.now() - startTime;
      // metrics.histogram('ratelimiter_check_duration_ms', latencyMs);

      if (result.allowed) {
        return next();
      }

      // Handle throttle
      if (options.onThrottle) {
        return options.onThrottle(req, res, result);
      }

      return res.status(429).json({
        error: {
          code: 'RATE_LIMIT_EXCEEDED',
          message: `Too many requests. Please retry after ${result.retryAfterSeconds} seconds.`,
          retryAfter: result.retryAfterSeconds,
          limit: result.limit,
          resetAt: result.resetAt,
        },
      });

    } catch (err) {
      // Unexpected error — fail open, log, and continue
      console.error('[RateLimiter] Unexpected middleware error:', err);
      return next();
    }
  };
}


// ─── Usage in Express App ─────────────────────────────────────────────────────

/*
const Redis = require('ioredis');
const express = require('express');
const { createRateLimiterMiddleware } = require('./rateLimiterMiddleware');

const app = express();
const redis = new Redis({ host: 'localhost', port: 6379 });

// Global rate limiter — applied to all routes
app.use(createRateLimiterMiddleware(redis, {
  failOpen: true,   // allow requests if Redis is down
}));

// Route-specific stricter limit (applied before route handler)
const strictLimiter = createRateLimiterMiddleware(redis, {
  rulesConfig: {
    rules: [{ limit: 5, windowSeconds: 60, algorithm: 'token_bucket' }]
  }
});

app.post('/api/v1/login', strictLimiter, loginHandler);
app.get('/api/v1/search', searchHandler);
*/

module.exports = { createRateLimiterMiddleware, extractIdentifier };
```

---

### 5.8 Rule Engine & Configuration

```javascript
// RuleEngine.js
// Matches incoming requests to rate limit rules based on priority

class RuleEngine {
  /**
   * @param {object} config
   * @param {Array}  config.rules   - Array of rule objects
   */
  constructor(config = {}) {
    // Sort rules by priority descending (highest priority = most specific = wins)
    this.rules = (config.rules || []).sort((a, b) => b.priority - a.priority);

    // Local cache: precompile endpoint matchers
    this._compiledRules = this.rules.map(rule => ({
      ...rule,
      _endpointRegex: rule.endpointPattern
        ? new RegExp(rule.endpointPattern)
        : null,
    }));
  }

  /**
   * Find the highest-priority rule that matches the request context.
   *
   * @param {object} context
   * @param {string} context.identifier   - e.g. "user:123" or "ip:1.2.3.4"
   * @param {string} context.endpoint     - e.g. "GET:/api/v1/search"
   * @param {string} context.userTier     - "free" | "pro" | "enterprise"
   * @param {string} context.method       - HTTP method
   * @returns {object|null} matching rule or null
   */
  findRule(context) {
    for (const rule of this._compiledRules) {
      if (this._matches(rule, context)) {
        return rule;
      }
    }
    return this._getDefaultRule();
  }

  _matches(rule, context) {
    const { matcher } = rule;
    if (!matcher) return true; // wildcard rule

    // Check user tier
    if (matcher.userTier && matcher.userTier !== context.userTier) {
      return false;
    }

    // Check endpoint (exact or regex)
    if (matcher.endpoint && !context.endpoint.includes(matcher.endpoint)) {
      return false;
    }
    if (rule._endpointRegex && !rule._endpointRegex.test(context.endpoint)) {
      return false;
    }

    // Check identifier prefix (e.g., only apply to "ip:" identifiers)
    if (matcher.identifierPrefix && !context.identifier.startsWith(matcher.identifierPrefix)) {
      return false;
    }

    // Check HTTP method
    if (matcher.method && matcher.method !== context.method) {
      return false;
    }

    return true;
  }

  _getDefaultRule() {
    return {
      limit: 1000,
      windowSeconds: 3600,
      algorithm: 'token_bucket',
      burstMultiplier: 1.5,
      priority: 0,
      name: 'default',
    };
  }

  /**
   * Reload rules without restarting the service.
   * Called by config service when rules are updated.
   */
  reloadRules(newRules) {
    this.rules = newRules.sort((a, b) => b.priority - a.priority);
    this._compiledRules = this.rules.map(rule => ({
      ...rule,
      _endpointRegex: rule.endpointPattern
        ? new RegExp(rule.endpointPattern)
        : null,
    }));
    console.log(`[RuleEngine] Reloaded ${this.rules.length} rules`);
  }
}


// ─── ConfigService ────────────────────────────────────────────────────────────
// Polls for rule updates and keeps RuleEngine in sync

class ConfigService {
  /**
   * @param {object} db          - Database client (e.g., Prisma, pg)
   * @param {RuleEngine} engine  - RuleEngine instance to update
   * @param {number} pollMs      - Polling interval in ms (default: 30s)
   */
  constructor(db, engine, pollMs = 30000) {
    this.db = db;
    this.engine = engine;
    this.pollMs = pollMs;
    this._interval = null;
  }

  async start() {
    await this._fetchAndReload();
    this._interval = setInterval(() => this._fetchAndReload(), this.pollMs);
    console.log('[ConfigService] Started rule polling');
  }

  async _fetchAndReload() {
    try {
      const rules = await this.db.rateLimitRule.findMany({
        where: { enabled: true },
        orderBy: { priority: 'desc' },
      });
      this.engine.reloadRules(rules);
    } catch (err) {
      console.error('[ConfigService] Failed to fetch rules:', err.message);
      // Keep existing rules on DB failure
    }
  }

  stop() {
    if (this._interval) clearInterval(this._interval);
  }
}

module.exports = { RuleEngine, ConfigService };
```

---

### 5.9 Class Diagram & Data Models

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           CLASS DIAGRAM                                      │
└─────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────┐
│    RateLimiterMiddleware         │
├──────────────────────────────────┤
│ - limiter: DistributedRateLimiter│
│ - ruleEngine: RuleEngine         │
│ - identifierFn: Function         │
├──────────────────────────────────┤
│ + handle(req, res, next): void   │
│ + extractIdentifier(req): string │
└──────────────┬───────────────────┘
               │ uses
               ▼
┌──────────────────────────────────┐        ┌────────────────────────────────┐
│    DistributedRateLimiter        │        │         RuleEngine             │
├──────────────────────────────────┤        ├────────────────────────────────┤
│ - redis: RedisClient             │        │ - rules: Rule[]                │
│ - failOpen: boolean              │        │ - compiledRules: CompiledRule[]│
│ - keyPrefix: string              │        ├────────────────────────────────┤
│ - scriptSHAs: Map<string,string> │        │ + findRule(context): Rule|null │
├──────────────────────────────────┤        │ + reloadRules(rules): void     │
│ + check(id,ep,rule): Result      │        │ - matches(rule,ctx): boolean   │
│ + checkTokenBucket(...): Result  │        └────────────────────────────────┘
│ + checkFixedWindow(...): Result  │
│ - evalScript(...): any[]         │
│ - buildKey(id,ep,sfx): string    │
│ - handleRedisError(...): Result  │
└──────────────────────────────────┘
               │ returns
               ▼
┌──────────────────────────────────┐
│       RateLimitResult            │
├──────────────────────────────────┤
│ + allowed: boolean               │
│ + limit: number                  │
│ + remaining: number              │
│ + resetAt: number (Unix epoch)   │
│ + retryAfterMs: number           │
│ + retryAfterSeconds: number      │
├──────────────────────────────────┤
│ + toHeaders(): object            │
└──────────────────────────────────┘


┌──────────────────────────────────────────────────────────────────────────────┐
│                           DATA MODELS                                        │
└──────────────────────────────────────────────────────────────────────────────┘

// Rule (stored in PostgreSQL)
{
  id:               UUID PRIMARY KEY,
  name:             VARCHAR(100),
  priority:         INTEGER,         -- higher = more specific
  enabled:          BOOLEAN,
  algorithm:        ENUM('token_bucket', 'sliding_window_counter',
                         'sliding_window_log', 'fixed_window', 'leaky_bucket'),
  limit:            INTEGER,         -- max requests
  windowSeconds:    INTEGER,         -- time window
  burstMultiplier:  FLOAT,           -- burst = limit × burstMultiplier
  matcher: {
    userTier:         VARCHAR(20),   -- 'free' | 'pro' | 'enterprise'
    endpoint:         VARCHAR(200),  -- exact match or null
    endpointPattern:  VARCHAR(200),  -- regex pattern or null
    identifierPrefix: VARCHAR(50),   -- 'ip:' | 'user:' | 'apikey:'
    method:           VARCHAR(10),   -- HTTP method or null (wildcard)
  },
  createdAt:        TIMESTAMP,
  updatedAt:        TIMESTAMP,
}

// Redis key pattern examples:
// Token Bucket:       "rl:user:12345:/api/v1/search:tb"         → HASH
// Fixed Window:       "rl:ip:1.2.3.4:/api/v1/data:fw:1719235200" → STRING
// Sliding Window Log: "rl:apikey:sk-abc:/api/v1/msg:swl"         → ZSET
```

---

## 6. Edge Cases & Failure Scenarios

### Edge Case 1: Window Boundary Spike (Fixed Window Flaw)
```
Window size: 60s
Limit: 100 req/window

Timeline:
  T=59s: User sends 100 requests → window 1 exhausted
  T=60s: New window starts
  T=61s: User sends 100 requests → window 2 allows them

Result: 200 requests in 2 seconds — 2× the intended rate!

Fix: Use Sliding Window Counter or Sliding Window Log.
```

### Edge Case 2: Distributed Counter Inconsistency
```
Redis node A has count = 99. Network partition.
Requests arrive at nodes B and C simultaneously.
Both read count = 99 (stale replica). Both allow. Count = 101.

Fix: Use Lua scripts on PRIMARY node. Never read from replica for limit checks.
     Accept slight inconsistency (~1%) if using local counters for performance.
```

### Edge Case 3: IP Spoofing / X-Forwarded-For Abuse
```
Attacker sets: X-Forwarded-For: 8.8.8.8
Rate limiter counts against 8.8.8.8 instead of attacker's real IP.

Fix:
  1. Only trust X-Forwarded-For from known load balancer IP ranges
  2. Use PROXY protocol (TCP layer) instead of HTTP headers
  3. Rate limit on API key, not just IP, for authenticated endpoints
```

### Edge Case 4: Redis Key Expiry Race
```
Key expires between ZCARD check and ZADD in sliding window log.
TTL not refreshed properly → key disappears → window resets unexpectedly.

Fix: Use Lua scripts that atomically check-add-expire in one transaction.
     Set TTL = 2 × windowSize as safety margin.
```

### Edge Case 5: Clock Drift in Token Bucket
```
App server clock drifts by 2 seconds backward.
Token refill calculation uses stale time → tokens over-refilled.

Fix: Always use Redis server time (TIME command) as the clock source,
     not the application server's clock.
```

### Edge Case 6: Burst at Service Startup
```
Service restarts → in-memory buckets are empty → all users get burst limit.
Could overwhelm downstream services.

Fix: Persist state in Redis (not in-memory). On restart, Redis already has counters.
     For in-memory: warm up buckets by fetching last known state from Redis.
```

### Edge Case 7: Rate Limiter Adds Too Much Latency
```
Redis round-trip adds 5ms+ to every request.

Fix:
  1. Use pipelining: batch multiple operations
  2. Keep Redis co-located with API gateway (same AZ/region)
  3. Use local cache for read-heavy rule lookups
  4. Async logging: don't wait for counter writes to complete
```

---

## 7. Performance Optimizations

### Optimization 1: Lua Scripts for Atomicity
Combine read + write into a single Lua script. Eliminates race conditions AND reduces round trips from 2+ to 1.

### Optimization 2: Connection Pooling
```javascript
const redis = new Redis.Cluster([...], {
  redisOptions: {
    maxRetriesPerRequest: 3,
    enableReadyCheck: true,
    connectTimeout: 2000,
    lazyConnect: true,
  },
  // Connection pool
  scaleReads: 'slave',        // read-heavy traffic hits replicas
  natMap: { ... },
});
```

### Optimization 3: Local Counter Aggregation
For extreme throughput (millions req/sec per node):
```
1. Maintain in-memory atomic counter per key
2. Every 100ms, flush increments to Redis via pipeline
3. Tradeoff: up to 100ms of over-counting (acceptable for most use cases)
4. Reduces Redis ops from 1M/sec → 10K/sec
```

### Optimization 4: Rule Caching
```javascript
class CachedRuleEngine extends RuleEngine {
  constructor(config, ttlMs = 60000) {
    super(config);
    // LRU cache: avoid regex matching for hot keys
    this._cache = new LRUCache({ max: 10000, ttl: ttlMs });
  }

  findRule(context) {
    const cacheKey = `${context.identifier}:${context.endpoint}:${context.userTier}`;
    if (this._cache.has(cacheKey)) return this._cache.get(cacheKey);

    const rule = super.findRule(context);
    this._cache.set(cacheKey, rule);
    return rule;
  }
}
```

### Optimization 5: Adaptive Throttling (Google SRE Pattern)
Instead of hard limits, use probability-based throttling:
```
throttle_probability = max(0, (requests - limit × 0.9) / (requests + 1))
```
This smoothly rejects requests as load increases, avoiding cliff-edge behavior.

### Optimization 6: Redis Pipelining
```javascript
const pipeline = redis.pipeline();
pipeline.incr(key);
pipeline.expire(key, windowSeconds);
const results = await pipeline.exec();
// Sends both commands in one TCP round-trip
```

---

## 8. Trade-offs & Design Decisions

| Decision | Option A | Option B | Choice | Reason |
|---|---|---|---|---|
| **Algorithm** | Token Bucket | Sliding Window Counter | Token Bucket for user APIs | Allows burst, intuitive quota |
| **Storage** | In-memory | Redis | Redis | Distributed, persistent, fast |
| **Consistency** | Strong (Redis primary) | Eventual (local counters) | Strong by default, eventual opt-in | Accuracy matters for billing |
| **Failure mode** | Fail open | Fail closed | Fail open | Availability > slight over-limit |
| **Identifier** | IP | User ID | User ID preferred, IP fallback | Users can share IPs (NAT) |
| **Placement** | Edge/Gateway | Per-service | Gateway + optional per-service | Centralized + fine-grained |
| **Clock** | App server time | Redis server time | Redis server time | Avoid clock drift across nodes |
| **Script execution** | EVAL | EVALSHA | EVALSHA with fallback | Bandwidth efficiency |
| **Config storage** | YAML/Env | Database | Database + memory cache | Dynamic updates without deploy |

---

## 9. Real-world References

| Company | Approach | Details |
|---|---|---|
| **GitHub** | Fixed window | 5000 req/hr for authenticated, 60 for unauthenticated |
| **Twitter/X** | Sliding window | Per-endpoint limits, per-app and per-user |
| **Stripe** | Token bucket | Burst allowance, per-key limits |
| **Cloudflare** | CRDT-based | Eventually consistent distributed counters at edge |
| **AWS API Gateway** | Token bucket | Per-stage, per-method, per-key configuration |
| **Google** | Adaptive throttling | Client-side + server-side, probabilistic rejection |
| **Uber** | Ratelimit library | Go library, Redis backend, multiple algorithms |
| **Kong** | Plugin-based | Sliding window, supports Redis + local |

---

## Summary

```
For FAANG-scale production:

Algorithm:   Token Bucket (user APIs) + Sliding Window Counter (service-to-service)
Storage:     Redis Cluster (6 shards, replication factor 2)
Placement:   API Gateway + optional per-service middleware
Atomicity:   Lua scripts (single round-trip, no race conditions)
Accuracy:    Strong consistency (Redis primary reads)
Failure:     Fail open + circuit breaker + alerting
Config:      Database with in-memory LRU cache (60s TTL)
Identity:    API Key > User ID > IP (in that priority order)
Headers:     X-RateLimit-Limit, Remaining, Reset + Retry-After on 429
Monitoring:  Prometheus metrics + Grafana dashboards + PagerDuty alerts
Latency:     <2ms p99 with co-located Redis (target: never exceed 5ms)
```

---

*Document version: 1.0 | Covers: Single-region and multi-region deployments | Algorithms: All 5 major variants | Implementation: Node.js / JavaScript*