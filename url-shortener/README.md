# 🔗 URL Shortener — Complete System Design
### FAANG-Level Interview Reference (HLD + LLD)

---

## Table of Contents

1. [Problem Statement & Requirements](#1-problem-statement--requirements)
2. [Capacity Estimation](#2-capacity-estimation)
3. [High-Level Design (HLD)](#3-high-level-design-hld)
   - 3.1 [Core Architecture Overview](#31-core-architecture-overview)
   - 3.2 [API Design](#32-api-design)
   - 3.3 [URL Shortening Algorithm](#33-url-shortening-algorithm)
   - 3.4 [Database Design](#34-database-design)
   - 3.5 [Caching Strategy](#35-caching-strategy)
   - 3.6 [CDN & Load Balancing](#36-cdn--load-balancing)
   - 3.7 [Data Partitioning & Sharding](#37-data-partitioning--sharding)
   - 3.8 [Replication & Consistency](#38-replication--consistency)
   - 3.9 [Analytics Pipeline](#39-analytics-pipeline)
   - 3.10 [Rate Limiting](#310-rate-limiting)
   - 3.11 [Security](#311-security)
4. [Low-Level Design (LLD) — JavaScript](#4-low-level-design-lld--javascript)
   - 4.1 [Project Structure](#41-project-structure)
   - 4.2 [Core URL Shortener Service](#42-core-url-shortener-service)
   - 4.3 [ID Generation — Snowflake](#43-id-generation--snowflake)
   - 4.4 [Base62 Encoder](#44-base62-encoder)
   - 4.5 [Cache Layer (Redis)](#45-cache-layer-redis)
   - 4.6 [Database Layer](#46-database-layer)
   - 4.7 [Rate Limiter (Token Bucket)](#47-rate-limiter-token-bucket)
   - 4.8 [Analytics Service](#48-analytics-service)
   - 4.9 [REST API Controllers](#49-rest-api-controllers)
   - 4.10 [Middleware](#410-middleware)
   - 4.11 [Bloom Filter (Duplicate Detection)](#411-bloom-filter-duplicate-detection)
5. [Failure Scenarios & Mitigations](#5-failure-scenarios--mitigations)
6. [Scalability Deep Dive](#6-scalability-deep-dive)
7. [Trade-offs Summary](#7-trade-offs-summary)

---

## 1. Problem Statement & Requirements

### Functional Requirements
- Given a long URL, generate a unique short URL (e.g., `https://short.ly/aB3xY9`)
- Redirect user to the original URL when the short URL is accessed
- Custom aliases — users can optionally specify their own short key
- URL expiration — TTL-based expiry, with default and user-specified TTL
- Analytics — track clicks, geolocation, device type, referrer
- User accounts — authenticated users can manage their links

### Non-Functional Requirements
- **High Availability**: 99.99% uptime (≈52 minutes downtime/year)
- **Low Latency**: Redirect in < 10ms (P99), creation in < 100ms
- **Durability**: No data loss — short URLs once created must always resolve
- **Scalability**: Handle 100K write RPS, 1M read RPS at peak
- **Idempotency**: Same long URL + user should optionally return same short URL
- **Security**: Prevent phishing/spam links, rate-limit abuse

### Out of Scope (for this design)
- Link preview / screenshot generation
- Browser extension integrations
- OAuth provider management

---

## 2. Capacity Estimation

### Traffic
| Metric | Value | Notes |
|--------|-------|-------|
| DAU | 100M users | Assumption |
| Write RPS (avg) | ~1,200 | 100M / 86400 |
| Write RPS (peak) | ~12,000 | 10x multiplier |
| Read:Write ratio | 100:1 | URL reads >> creations |
| Read RPS (avg) | ~120,000 | |
| Read RPS (peak) | ~1,200,000 | |

### Storage
| Item | Calculation | Total |
|------|-------------|-------|
| Short key (7 chars) | 7 bytes | |
| Long URL (avg 200 chars) | 200 bytes | |
| Metadata (user, TTL, created_at) | ~100 bytes | |
| Total per record | ~307 bytes | |
| New URLs/day | 100M | |
| Storage/day | 100M × 307B | ~30 GB/day |
| Storage/5 years | 30GB × 365 × 5 | ~55 TB |

### Cache
- 80/20 rule: 20% of URLs handle 80% of traffic
- Cache size: 20% of daily reads = 0.2 × 100M × 307B ≈ **~6 GB/day**

### Bandwidth
- **Inbound** (write): 12,000 RPS × 500B = **~6 MB/s**
- **Outbound** (redirect): 1,200,000 RPS × 307B = **~368 MB/s**

### Short Key Space
- Base62 (a-z, A-Z, 0-9): 62 characters
- 7 characters: `62^7 = ~3.5 trillion` unique URLs
- At 100M URLs/day → lasts **~95 years**

---

## 3. High-Level Design (HLD)

### 3.1 Core Architecture Overview

```
                        ┌─────────────────────────────────────────────────────────────┐
                        │                    INTERNET / CLIENTS                       │
                        └───────────────────┬─────────────────────────────────────────┘
                                            │
                                            ▼
                        ┌─────────────────────────────────────────┐
                        │              DNS / GeoDNS               │
                        │   Routes to nearest regional cluster     │
                        └───────────────────┬─────────────────────┘
                                            │
                                            ▼
                        ┌─────────────────────────────────────────┐
                        │          Global Load Balancer           │
                        │     (AWS ALB / Cloudflare / Nginx)      │
                        │   - SSL termination                      │
                        │   - DDoS protection                      │
                        └───────────┬──────────────┬──────────────┘
                                    │              │
                    ┌───────────────▼──┐        ┌──▼────────────────┐
                    │  Write Service   │        │   Read Service     │
                    │  (URL Creation)  │        │  (URL Resolution)  │
                    │  Stateless pods  │        │  Stateless pods    │
                    └───────────┬──────┘        └──────┬─────────────┘
                                │                       │
            ┌───────────────────▼───┐        ┌─────────▼──────────────┐
            │   ID Generator        │        │      Redis Cluster      │
            │  (Snowflake Service)  │        │   (Cache — L1/L2)       │
            └───────────────────────┘        └─────────┬──────────────┘
                                                        │ Cache Miss
                                            ┌───────────▼──────────────┐
                        ┌───────────────────►  Primary DB (PostgreSQL) ◄─────────────────┐
                        │                   │    + Read Replicas        │                 │
                        │                   └──────────────────────────┘                 │
                        │                                                                 │
              ┌─────────▼───────────────┐                               ┌────────────────▼──┐
              │  Analytics Queue        │                               │    Object Store    │
              │  (Kafka Topics)         │                               │  (S3 — archival)   │
              └─────────────────────────┘                               └───────────────────┘
                          │
              ┌───────────▼─────────────┐
              │  Analytics Consumers    │
              │  (Flink / Spark)        │
              └─────────────────────────┘
                          │
              ┌───────────▼─────────────┐
              │  Analytics Store        │
              │  (ClickHouse / BigQuery) │
              └─────────────────────────┘
```

### 3.2 API Design

#### REST Endpoints

**1. Create Short URL**
```
POST /api/v1/urls
Authorization: Bearer <token>   (optional for anonymous)
Content-Type: application/json

Request:
{
  "longUrl": "https://www.example.com/very/long/url?param=value",
  "customAlias": "my-link",          // optional
  "expiresAt": "2025-12-31T23:59:59Z", // optional
  "tags": ["marketing", "campaign1"]   // optional
}

Response 201 Created:
{
  "shortUrl": "https://short.ly/aB3xY9",
  "shortKey": "aB3xY9",
  "longUrl": "https://www.example.com/...",
  "expiresAt": "2025-12-31T23:59:59Z",
  "createdAt": "2024-01-01T00:00:00Z"
}

Errors:
  400 — Invalid URL format
  409 — Custom alias already taken
  429 — Rate limit exceeded
```

**2. Redirect (Core Path — must be fastest)**
```
GET /:shortKey
→ 301 Moved Permanently   (cacheable — for permanent URLs)
→ 302 Found               (not cached — for analytics/expiring URLs)

Location: https://www.example.com/very/long/url

Note: Use 302 when analytics tracking is required, so browsers re-request
each time. Use 301 for pure performance when tracking is not needed.
```

**3. Get URL Info**
```
GET /api/v1/urls/:shortKey
→ 200 OK
{
  "shortKey": "aB3xY9",
  "longUrl": "...",
  "clicks": 14230,
  "createdAt": "...",
  "expiresAt": "...",
  "isActive": true
}
```

**4. Delete URL**
```
DELETE /api/v1/urls/:shortKey
Authorization: Bearer <token>
→ 204 No Content
```

**5. Analytics**
```
GET /api/v1/urls/:shortKey/analytics?from=2024-01-01&to=2024-01-31
→ 200 OK
{
  "totalClicks": 14230,
  "uniqueClicks": 9871,
  "clicksByDay": [...],
  "topCountries": [...],
  "topDevices": [...],
  "topReferrers": [...]
}
```

### 3.3 URL Shortening Algorithm

Three approaches — trade-offs discussed:

#### Option A: Hash-Based (MD5/SHA256 → Base62)
```
longUrl → MD5 hash (128 bits) → take first 43 bits → Base62 encode → 7-char key

Pros:  Same long URL always produces same short key (natural deduplication)
Cons:  Hash collisions possible; must check DB on every write
       Hard to distribute (all nodes must coordinate on collision resolution)
```

#### Option B: Counter-Based (Auto-Increment → Base62)
```
global_counter++ → Base62 encode → 7-char key

Pros:  Simple, no collisions
Cons:  Single point of failure on counter; sequential keys leak info
       Counter service bottleneck at scale
```

#### Option C: Snowflake ID → Base62 ✅ (Recommended)
```
64-bit Snowflake ID → Base62 encode → 7-11 char key

Snowflake ID layout (64 bits):
┌──────────────────────────┬────────────┬──────────────┬──────────────────┐
│  41 bits timestamp (ms)  │  5 bits DC │  5 bits Mach │  12 bits seq     │
└──────────────────────────┴────────────┴──────────────┴──────────────────┘

Pros:  Distributed, no coordination needed; monotonically increasing
       Embeds time (useful for TTL cleanup); no collisions
Cons:  Clock skew requires NTP; slightly longer keys
```

**Why Snowflake wins at scale:**
- Each pod generates IDs independently — zero network calls
- Time-sortable — range scans by creation time are efficient
- 4096 IDs/ms/machine × 1024 machines = ~4B IDs/second capacity

### 3.4 Database Design

#### Primary Store: PostgreSQL (or CockroachDB for global distribution)

**Table: `urls`**
```sql
CREATE TABLE urls (
    id             BIGINT PRIMARY KEY,           -- Snowflake ID
    short_key      VARCHAR(16) NOT NULL UNIQUE,  -- e.g. "aB3xY9"
    long_url       TEXT NOT NULL,
    user_id        BIGINT REFERENCES users(id),  -- NULL for anonymous
    is_active      BOOLEAN NOT NULL DEFAULT TRUE,
    is_custom      BOOLEAN NOT NULL DEFAULT FALSE,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at     TIMESTAMPTZ,
    click_count    BIGINT NOT NULL DEFAULT 0,    -- Approximate counter
    tags           TEXT[],
    metadata       JSONB                         -- Extensible
);

CREATE INDEX idx_urls_short_key   ON urls (short_key);
CREATE INDEX idx_urls_user_id     ON urls (user_id) WHERE user_id IS NOT NULL;
CREATE INDEX idx_urls_expires_at  ON urls (expires_at) WHERE expires_at IS NOT NULL;
CREATE INDEX idx_urls_created_at  ON urls (created_at);
```

**Table: `users`**
```sql
CREATE TABLE users (
    id           BIGINT PRIMARY KEY,
    email        VARCHAR(320) UNIQUE NOT NULL,
    plan         VARCHAR(20) NOT NULL DEFAULT 'free',  -- free|pro|enterprise
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    rate_limit   INT NOT NULL DEFAULT 1000             -- URLs/day
);
```

**Table: `click_events`** (Write-optimized, partitioned by day)
```sql
CREATE TABLE click_events (
    id           BIGSERIAL,
    short_key    VARCHAR(16) NOT NULL,
    clicked_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ip_hash      VARCHAR(64),          -- hashed for privacy
    country      CHAR(2),
    device_type  VARCHAR(20),          -- mobile|desktop|tablet|bot
    browser      VARCHAR(50),
    referrer     TEXT,
    user_agent   TEXT
) PARTITION BY RANGE (clicked_at);

-- Monthly partitions
CREATE TABLE click_events_2024_01 PARTITION OF click_events
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
```

#### Why PostgreSQL over NoSQL here?
- URL records have strong relational needs (user ownership, tags, constraints)
- Read path is pure key lookup → same O(1) as Cassandra with proper indexing
- ACID guarantees prevent double-registration of custom aliases
- At our scale, sharded PostgreSQL outperforms Cassandra in P99 latency

#### For massive scale: Cassandra for `click_events`
```
Partition key: short_key
Clustering key: clicked_at DESC

→ Per-URL time-series reads are extremely fast
→ Write-optimized LSM tree handles 1M+ write RPS
```

### 3.5 Caching Strategy

```
Request → L1 (In-Memory, per-pod, 100ms TTL)
             ↓ miss
          L2 (Redis Cluster, 24h TTL)
             ↓ miss
          PostgreSQL read replica
             ↓
          Populate L2 → populate L1 → respond
```

#### Redis Key Structure
```
url:{shortKey}          → JSON: { longUrl, expiresAt, isActive }
rate:{userId}:{minute}  → integer counter
bloom:global            → serialized Bloom filter
user:{userId}:session   → session data
```

#### Cache Invalidation
- On URL deletion: `DEL url:{shortKey}` (write-through)
- On URL update: `SET url:{shortKey} <new> EX 86400` (write-through)
- On expiry: Redis TTL handles auto-eviction; background job marks DB records

#### Cache Eviction Policy
- **allkeys-lru** — evict least-recently-used keys when memory full
- 20% of URLs = 80% of traffic → hot URLs stay in cache naturally

### 3.6 CDN & Load Balancing

#### CDN (Cloudflare / AWS CloudFront)
- Cache 301 redirects at edge PoPs for permanent URLs
- Serve analytics dashboard static assets globally
- DDoS protection at layer 3/4/7

#### Load Balancing Strategy
```
Tier 1: Global — GeoDNS routes to nearest region (US-EAST, EU-WEST, APAC)
Tier 2: Regional — AWS ALB distributes across AZs
Tier 3: Service — Internal L7 LB routes to read/write pods separately
```

**Read/Write Separation at LB Layer:**
- `GET /:shortKey` → Read pods (horizontal scale to 1000s)
- `POST /api/v1/urls` → Write pods (moderate scale, limited by DB writes)

### 3.7 Data Partitioning & Sharding

#### Sharding Strategy: Key-Based (Short Key Hash)
```
shard_id = hash(shortKey) % NUM_SHARDS

Pros:  Even distribution; same key always routes to same shard
Cons:  Resharding is expensive (requires data migration)
```

#### Sharding by User ID (for user-owned URLs)
```
shard_id = userId % NUM_SHARDS

Pros:  All of a user's URLs on one shard → fast user dashboard queries
Cons:  Hot users cause uneven load ("celebrity problem")
       → Mitigated by caching celebrity URLs aggressively
```

#### Consistent Hashing (for Redis Cluster)
```
Virtual nodes on a ring → shard assignment survives node add/remove
Only K/N keys need remapping when a node is added (K=keys, N=nodes)
```

### 3.8 Replication & Consistency

#### PostgreSQL Replication
```
Primary (1) → Synchronous replica (1, same AZ) — for failover
           → Asynchronous replicas (2-3, other AZs/regions) — for reads
```

- **RPO** (Recovery Point Objective): < 1 second (synchronous replica)
- **RTO** (Recovery Time Objective): < 30 seconds (automated failover via Patroni)

#### Consistency Model
- URL creation: **Strong consistency** — must confirm write before returning
- URL resolution: **Eventual consistency** acceptable — cache may be slightly stale
- Click counts: **Eventually consistent** — approximate counts via Redis incr, periodic flush to DB

### 3.9 Analytics Pipeline

```
Click Event
    │
    ▼
Kafka Topic: "click-events"  (partitioned by shortKey)
    │
    ├──► Flink Job (real-time aggregation, 1-min windows)
    │         └──► Redis counters (live click counts per URL)
    │
    └──► Spark Batch Job (hourly/daily rollups)
              └──► ClickHouse (OLAP store for historical analytics)
```

#### Why Kafka?
- Decouples click recording from redirect path — redirect is instant
- Durable log — analytics can be reprocessed if consumer fails
- Partitioned by shortKey → same URL's events in order

#### ClickHouse Schema
```sql
CREATE TABLE url_analytics (
    short_key     String,
    event_date    Date,
    event_hour    UInt8,
    country       FixedString(2),
    device_type   LowCardinality(String),
    clicks        UInt64
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (short_key, event_date, event_hour, country, device_type);
```

### 3.10 Rate Limiting

#### Strategy: Token Bucket per User
```
Anonymous:    100 URL creations/day,  1,000 redirects/min
Free tier:   1,000 URL creations/day, 10,000 redirects/min
Pro tier:   10,000 URL creations/day, unlimited redirects
```

#### Implementation: Redis + Sliding Window
```
Key: rate:{userId}:{windowStart}
Value: count (INCR with TTL)
Algorithm: Sliding window log (precise) or fixed window (approximate, cheaper)
```

### 3.11 Security

| Threat | Mitigation |
|--------|-----------|
| Phishing/malware URLs | Integration with Google Safe Browsing API on creation |
| URL spam (mass creation) | Rate limiting + CAPTCHA for anonymous users |
| Enumeration attack (guessing short keys) | Random Base62 (not sequential), Bloom filter presence check |
| DDoS | Cloudflare, rate limiting at CDN layer |
| SQL injection | Parameterized queries everywhere |
| SSRF (redirect to internal IPs) | Block private IP ranges (10.x, 192.168.x, 127.x) on creation |
| Data privacy | Hash IPs in analytics (SHA256 + salt), GDPR compliance |
| Hotlink abuse | Per-IP rate limiting on redirect endpoint |

---

## 4. Low-Level Design (LLD) — JavaScript

### 4.1 Project Structure

```
url-shortener/
├── src/
│   ├── server.js                  # Express app entry point
│   ├── config/
│   │   ├── index.js               # Environment config
│   │   └── redis.js               # Redis client setup
│   ├── controllers/
│   │   ├── urlController.js       # HTTP handlers
│   │   └── analyticsController.js
│   ├── services/
│   │   ├── UrlShortenerService.js  # Core business logic
│   │   ├── CacheService.js         # Redis abstraction
│   │   ├── AnalyticsService.js     # Click tracking
│   │   └── BloomFilterService.js   # Duplicate detection
│   ├── utils/
│   │   ├── SnowflakeIdGenerator.js # Distributed ID generation
│   │   ├── Base62.js               # Encode/decode
│   │   └── UrlValidator.js         # URL validation & safety
│   ├── middleware/
│   │   ├── rateLimiter.js          # Token bucket rate limiter
│   │   ├── authMiddleware.js       # JWT auth
│   │   └── requestLogger.js        # Structured logging
│   ├── models/
│   │   ├── Url.js                  # DB model
│   │   └── User.js
│   └── database/
│       ├── connection.js           # PostgreSQL pool
│       └── migrations/
├── tests/
│   ├── unit/
│   └── integration/
├── package.json
└── docker-compose.yml
```

### 4.2 Core URL Shortener Service

```javascript
// src/services/UrlShortenerService.js

const { SnowflakeIdGenerator } = require('../utils/SnowflakeIdGenerator');
const { Base62 }               = require('../utils/Base62');
const { CacheService }         = require('./CacheService');
const { UrlValidator }         = require('../utils/UrlValidator');
const { BloomFilterService }   = require('./BloomFilterService');
const db                       = require('../database/connection');

class UrlShortenerService {
  constructor() {
    this.snowflake   = new SnowflakeIdGenerator({ machineId: process.env.MACHINE_ID || 1 });
    this.cache       = new CacheService();
    this.bloom       = new BloomFilterService();
    this.defaultTTL  = 365 * 24 * 60 * 60; // 1 year in seconds
  }

  /**
   * Creates a short URL for the given long URL.
   * Time complexity: O(1) amortized (rare collision retries)
   * @param {Object} params
   * @returns {Object} Created URL record
   */
  async createShortUrl({ longUrl, customAlias, expiresAt, userId, tags }) {
    // --- Step 1: Validate URL ---
    const validation = UrlValidator.validate(longUrl);
    if (!validation.isValid) {
      throw new ValidationError(validation.reason);
    }

    // --- Step 2: Safety check (Google Safe Browsing) ---
    const isSafe = await UrlValidator.checkSafeBrowsing(longUrl);
    if (!isSafe) {
      throw new SafetyError('URL flagged as unsafe by Safe Browsing API');
    }

    // --- Step 3: Handle custom alias ---
    if (customAlias) {
      return this._createWithCustomAlias({ longUrl, customAlias, expiresAt, userId, tags });
    }

    // --- Step 4: Check for existing short URL (deduplication) ---
    // Only for authenticated users — anonymous gets new key each time
    if (userId) {
      const existing = await this._findExisting(longUrl, userId);
      if (existing) return existing;
    }

    // --- Step 5: Generate unique short key ---
    const shortKey = await this._generateUniqueKey();

    // --- Step 6: Persist to database ---
    const record = await this._persistUrl({
      shortKey,
      longUrl,
      userId,
      expiresAt,
      tags,
      isCustom: false,
    });

    // --- Step 7: Warm the cache ---
    await this.cache.set(`url:${shortKey}`, {
      longUrl,
      expiresAt: record.expires_at,
      isActive: true,
    }, this.defaultTTL);

    return record;
  }

  /**
   * Resolves a short key to its long URL.
   * Hot path — must be as fast as possible.
   * @param {string} shortKey
   * @returns {string} longUrl
   */
  async resolveUrl(shortKey) {
    // --- L1: Check in-process LRU cache (sub-millisecond) ---
    const inMemory = this._localCache.get(shortKey);
    if (inMemory) return inMemory;

    // --- L2: Check Redis cache ---
    const cached = await this.cache.get(`url:${shortKey}`);
    if (cached) {
      if (!cached.isActive) throw new UrlNotFoundError(shortKey);
      if (cached.expiresAt && new Date(cached.expiresAt) < new Date()) {
        throw new UrlExpiredError(shortKey);
      }
      this._localCache.set(shortKey, cached.longUrl); // warm L1
      return cached.longUrl;
    }

    // --- L3: Database lookup (read replica) ---
    const row = await db.queryReadReplica(
      `SELECT long_url, expires_at, is_active
       FROM urls
       WHERE short_key = $1
       LIMIT 1`,
      [shortKey]
    );

    if (!row) throw new UrlNotFoundError(shortKey);
    if (!row.is_active) throw new UrlNotFoundError(shortKey);
    if (row.expires_at && new Date(row.expires_at) < new Date()) {
      throw new UrlExpiredError(shortKey);
    }

    const payload = {
      longUrl: row.long_url,
      expiresAt: row.expires_at,
      isActive: row.is_active,
    };

    // Back-fill caches
    await this.cache.set(`url:${shortKey}`, payload, this.defaultTTL);
    this._localCache.set(shortKey, row.long_url);

    return row.long_url;
  }

  /**
   * Deletes a short URL (soft delete).
   */
  async deleteUrl(shortKey, userId) {
    const row = await db.query(
      'SELECT user_id FROM urls WHERE short_key = $1',
      [shortKey]
    );

    if (!row) throw new UrlNotFoundError(shortKey);
    if (String(row.user_id) !== String(userId)) {
      throw new AuthorizationError('Not owner of this URL');
    }

    await db.query(
      'UPDATE urls SET is_active = FALSE WHERE short_key = $1',
      [shortKey]
    );

    // Invalidate cache immediately (write-through)
    await this.cache.del(`url:${shortKey}`);
    this._localCache.delete(shortKey);
  }

  // ─────────────────────────────────────────────────
  // Private helpers
  // ─────────────────────────────────────────────────

  async _generateUniqueKey(retries = 0) {
    if (retries > 5) throw new Error('Failed to generate unique key after 5 retries');

    const snowflakeId = this.snowflake.nextId();   // BigInt
    const shortKey    = Base62.encode(snowflakeId); // e.g. "aB3xY9"

    // Bloom filter fast-path: if not in filter, definitely new
    const mightExist = await this.bloom.mightContain(shortKey);
    if (!mightExist) {
      await this.bloom.add(shortKey);
      return shortKey;
    }

    // Rare: Bloom filter false positive — verify in DB
    const exists = await db.query(
      'SELECT 1 FROM urls WHERE short_key = $1',
      [shortKey]
    );

    if (!exists) {
      await this.bloom.add(shortKey);
      return shortKey;
    }

    // True collision (astronomically rare with Snowflake) — retry
    return this._generateUniqueKey(retries + 1);
  }

  async _createWithCustomAlias({ longUrl, customAlias, expiresAt, userId, tags }) {
    if (!/^[a-zA-Z0-9_-]{3,16}$/.test(customAlias)) {
      throw new ValidationError('Custom alias must be 3-16 alphanumeric characters');
    }

    // Atomic insert — DB unique constraint on short_key handles race conditions
    try {
      return await this._persistUrl({
        shortKey: customAlias,
        longUrl,
        userId,
        expiresAt,
        tags,
        isCustom: true,
      });
    } catch (err) {
      if (err.code === '23505') { // PostgreSQL unique violation
        throw new ConflictError(`Custom alias "${customAlias}" is already taken`);
      }
      throw err;
    }
  }

  async _findExisting(longUrl, userId) {
    return db.queryReadReplica(
      `SELECT short_key, long_url, expires_at, created_at
       FROM urls
       WHERE long_url = $1 AND user_id = $2 AND is_active = TRUE
       LIMIT 1`,
      [longUrl, userId]
    );
  }

  async _persistUrl({ shortKey, longUrl, userId, expiresAt, tags, isCustom }) {
    const snowflakeId = this.snowflake.nextId();

    const [row] = await db.query(
      `INSERT INTO urls (id, short_key, long_url, user_id, expires_at, tags, is_custom)
       VALUES ($1, $2, $3, $4, $5, $6, $7)
       RETURNING *`,
      [snowflakeId, shortKey, longUrl, userId || null,
       expiresAt || null, tags || [], isCustom]
    );

    return row;
  }

  // Simple in-process LRU cache (Node.js Map preserves insertion order)
  _localCache = (() => {
    const MAX   = 10_000;
    const store = new Map();
    return {
      get: (k)    => store.get(k),
      set: (k, v) => {
        if (store.size >= MAX) store.delete(store.keys().next().value); // evict LRU
        store.set(k, v);
      },
      delete: (k) => store.delete(k),
    };
  })();
}

// Custom error classes
class ValidationError   extends Error { constructor(m) { super(m); this.statusCode = 400; } }
class ConflictError     extends Error { constructor(m) { super(m); this.statusCode = 409; } }
class UrlNotFoundError  extends Error { constructor(m) { super(m); this.statusCode = 404; } }
class UrlExpiredError   extends Error { constructor(m) { super(m); this.statusCode = 410; } }
class AuthorizationError extends Error { constructor(m) { super(m); this.statusCode = 403; } }
class SafetyError       extends Error { constructor(m) { super(m); this.statusCode = 422; } }

module.exports = { UrlShortenerService };
```

### 4.3 ID Generation — Snowflake

```javascript
// src/utils/SnowflakeIdGenerator.js

/**
 * Distributed unique ID generator based on Twitter Snowflake.
 *
 * ID Layout (64 bits):
 * ┌──────────────────────────────────────────┬─────────┬─────────┬──────────────┐
 * │ 41 bits — timestamp (ms since EPOCH)     │ 5 bits  │ 5 bits  │ 12 bits seq  │
 * │                                          │ datacen │ machine │              │
 * └──────────────────────────────────────────┴─────────┴─────────┴──────────────┘
 *
 * Max IDs per machine per millisecond: 4096
 * Total unique IDs: ~4.6 × 10^18
 * Epoch exhaustion: ~139 years from CUSTOM_EPOCH
 */
class SnowflakeIdGenerator {
  static CUSTOM_EPOCH  = 1700000000000n; // Nov 2023 — reduces bit usage
  static MACHINE_BITS  = 10n;            // 5 DC + 5 machine
  static SEQUENCE_BITS = 12n;

  static MAX_MACHINE_ID = (1n << SnowflakeIdGenerator.MACHINE_BITS) - 1n; // 1023
  static MAX_SEQUENCE   = (1n << SnowflakeIdGenerator.SEQUENCE_BITS) - 1n; // 4095

  static MACHINE_SHIFT  = SnowflakeIdGenerator.SEQUENCE_BITS;
  static TIMESTAMP_SHIFT = SnowflakeIdGenerator.MACHINE_BITS + SnowflakeIdGenerator.SEQUENCE_BITS;

  #machineId;
  #sequence    = 0n;
  #lastTimestamp = -1n;

  constructor({ machineId = 1 } = {}) {
    const id = BigInt(machineId);
    if (id < 0n || id > SnowflakeIdGenerator.MAX_MACHINE_ID) {
      throw new RangeError(`machineId must be 0-${SnowflakeIdGenerator.MAX_MACHINE_ID}`);
    }
    this.#machineId = id;
  }

  /**
   * Generates the next unique Snowflake ID.
   * Thread-safe within a single Node.js event loop tick.
   * @returns {BigInt}
   */
  nextId() {
    let timestamp = this.#currentTimestamp();

    if (timestamp < this.#lastTimestamp) {
      // Clock moved backwards — wait for recovery
      const drift = this.#lastTimestamp - timestamp;
      if (drift > 10n) throw new Error(`Clock drift too large: ${drift}ms`);
      timestamp = this.#lastTimestamp; // Use last known good timestamp
    }

    if (timestamp === this.#lastTimestamp) {
      this.#sequence = (this.#sequence + 1n) & SnowflakeIdGenerator.MAX_SEQUENCE;
      if (this.#sequence === 0n) {
        // Sequence exhausted — spin until next millisecond
        timestamp = this.#waitNextMillis(this.#lastTimestamp);
      }
    } else {
      this.#sequence = 0n;
    }

    this.#lastTimestamp = timestamp;

    return (
      ((timestamp - SnowflakeIdGenerator.CUSTOM_EPOCH) << SnowflakeIdGenerator.TIMESTAMP_SHIFT) |
      (this.#machineId << SnowflakeIdGenerator.MACHINE_SHIFT) |
      this.#sequence
    );
  }

  /**
   * Extracts the creation timestamp from a Snowflake ID.
   * Useful for TTL calculations and debugging.
   */
  static extractTimestamp(id) {
    return Number((BigInt(id) >> SnowflakeIdGenerator.TIMESTAMP_SHIFT) + SnowflakeIdGenerator.CUSTOM_EPOCH);
  }

  #currentTimestamp() {
    return BigInt(Date.now());
  }

  #waitNextMillis(lastTs) {
    let ts = this.#currentTimestamp();
    while (ts <= lastTs) ts = this.#currentTimestamp();
    return ts;
  }
}

module.exports = { SnowflakeIdGenerator };
```

### 4.4 Base62 Encoder

```javascript
// src/utils/Base62.js

/**
 * Base62 encoding for URL-safe short keys.
 *
 * Character set: 0-9 (10) + a-z (26) + A-Z (26) = 62 chars
 * Key length for various ID spaces:
 *   62^6 = 56.8 billion
 *   62^7 = 3.52 trillion   ← our target (7 chars)
 *   62^8 = 218 trillion
 *
 * NOTE: BigInt required — Snowflake IDs exceed Number.MAX_SAFE_INTEGER
 */
class Base62 {
  static CHARSET = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
  static BASE     = BigInt(Base62.CHARSET.length); // 62n
  static MIN_LEN  = 7; // Pad to this length for consistent-length keys

  /**
   * Encodes a BigInt to a Base62 string.
   * @param {BigInt} num
   * @returns {string}
   */
  static encode(num) {
    if (typeof num !== 'bigint') num = BigInt(num);
    if (num < 0n) throw new RangeError('Cannot encode negative numbers');
    if (num === 0n) return Base62.CHARSET[0].padStart(Base62.MIN_LEN, '0');

    let result = '';
    while (num > 0n) {
      result = Base62.CHARSET[Number(num % Base62.BASE)] + result;
      num    = num / Base62.BASE;
    }

    return result.padStart(Base62.MIN_LEN, '0');
  }

  /**
   * Decodes a Base62 string back to a BigInt.
   * @param {string} str
   * @returns {BigInt}
   */
  static decode(str) {
    if (!str || typeof str !== 'string') throw new TypeError('Input must be a non-empty string');

    let result = 0n;
    for (const char of str) {
      const idx = Base62.CHARSET.indexOf(char);
      if (idx === -1) throw new Error(`Invalid Base62 character: '${char}'`);
      result = result * Base62.BASE + BigInt(idx);
    }
    return result;
  }

  /**
   * Validates that a string is a valid Base62 short key.
   */
  static isValid(str) {
    return typeof str === 'string' &&
           str.length >= 4 &&
           str.length <= 16 &&
           /^[0-9a-zA-Z]+$/.test(str);
  }
}

module.exports = { Base62 };
```

### 4.5 Cache Layer (Redis)

```javascript
// src/services/CacheService.js

const Redis = require('ioredis');

/**
 * Redis cache abstraction with:
 * - Cluster support
 * - Automatic serialization/deserialization
 * - Connection retry with exponential backoff
 * - Graceful degradation (cache miss on Redis failure)
 */
class CacheService {
  constructor() {
    this.client = this._createClient();
    this._stats = { hits: 0, misses: 0, errors: 0 };
  }

  _createClient() {
    const isCluster = process.env.REDIS_CLUSTER === 'true';

    if (isCluster) {
      return new Redis.Cluster(
        (process.env.REDIS_NODES || '').split(',').map(node => {
          const [host, port] = node.split(':');
          return { host, port: Number(port) };
        }),
        {
          scaleReads: 'slave',           // Read from replicas
          redisOptions: {
            password: process.env.REDIS_PASSWORD,
            connectTimeout: 5000,
            maxRetriesPerRequest: 2,
          },
          retryDelayOnFailover: 300,
          enableOfflineQueue: false,     // Fail fast — don't buffer
        }
      );
    }

    return new Redis({
      host:            process.env.REDIS_HOST || 'localhost',
      port:            Number(process.env.REDIS_PORT) || 6379,
      password:        process.env.REDIS_PASSWORD,
      connectTimeout:  5000,
      lazyConnect:     true,
      enableOfflineQueue: false,
    });
  }

  /**
   * Gets a value by key. Returns null on miss or error (graceful degradation).
   * @param {string} key
   * @returns {Object|null}
   */
  async get(key) {
    try {
      const raw = await this.client.get(key);
      if (raw === null) {
        this._stats.misses++;
        return null;
      }
      this._stats.hits++;
      return JSON.parse(raw);
    } catch (err) {
      this._stats.errors++;
      console.error('Cache GET error:', err.message);
      return null; // Graceful degradation — fall through to DB
    }
  }

  /**
   * Sets a value with optional TTL in seconds.
   */
  async set(key, value, ttlSeconds = 3600) {
    try {
      await this.client.setex(key, ttlSeconds, JSON.stringify(value));
    } catch (err) {
      this._stats.errors++;
      console.error('Cache SET error:', err.message);
      // Non-fatal — continue without cache
    }
  }

  /**
   * Deletes one or more keys.
   */
  async del(...keys) {
    try {
      if (keys.length > 0) await this.client.del(...keys);
    } catch (err) {
      this._stats.errors++;
      console.error('Cache DEL error:', err.message);
    }
  }

  /**
   * Atomic increment — used for click counts and rate limiting.
   * Returns the new value.
   */
  async increment(key, ttlSeconds) {
    try {
      const pipe  = this.client.pipeline();
      pipe.incr(key);
      if (ttlSeconds) pipe.expire(key, ttlSeconds);
      const [[, count]] = await pipe.exec();
      return count;
    } catch (err) {
      this._stats.errors++;
      return null;
    }
  }

  /**
   * Returns a snapshot of cache performance stats.
   */
  getStats() {
    const total      = this._stats.hits + this._stats.misses;
    const hitRate    = total > 0 ? (this._stats.hits / total * 100).toFixed(2) : '0.00';
    return { ...this._stats, hitRate: `${hitRate}%` };
  }
}

module.exports = { CacheService };
```

### 4.6 Database Layer

```javascript
// src/database/connection.js

const { Pool } = require('pg');

/**
 * PostgreSQL connection pool with:
 * - Separate primary (read-write) and replica (read-only) pools
 * - Connection health checks
 * - Query timing for observability
 */
class Database {
  constructor() {
    // Primary — all writes
    this.primaryPool = new Pool({
      host:              process.env.DB_PRIMARY_HOST,
      port:              Number(process.env.DB_PORT) || 5432,
      database:          process.env.DB_NAME,
      user:              process.env.DB_USER,
      password:          process.env.DB_PASSWORD,
      max:               20,          // Max connections in pool
      idleTimeoutMillis: 30_000,
      connectionTimeoutMillis: 5_000,
      ssl:               { rejectUnauthorized: false },
    });

    // Read replica — all read-only queries
    this.replicaPool = new Pool({
      host:     process.env.DB_REPLICA_HOST || process.env.DB_PRIMARY_HOST,
      port:     Number(process.env.DB_PORT) || 5432,
      database: process.env.DB_NAME,
      user:     process.env.DB_USER,
      password: process.env.DB_PASSWORD,
      max:      50, // Higher — reads are more frequent
      idleTimeoutMillis: 30_000,
      connectionTimeoutMillis: 5_000,
    });

    this._setupErrorHandlers();
  }

  /**
   * Executes a read-write query on the primary.
   */
  async query(sql, params = []) {
    const start  = Date.now();
    const client = await this.primaryPool.connect();
    try {
      const result = await client.query(sql, params);
      this._logQuery(sql, Date.now() - start);
      return result.rows[0] ?? null; // Return first row for convenience
    } finally {
      client.release();
    }
  }

  /**
   * Executes a read-only query on the replica.
   */
  async queryReadReplica(sql, params = []) {
    const start  = Date.now();
    const client = await this.replicaPool.connect();
    try {
      const result = await client.query(sql, params);
      this._logQuery(sql, Date.now() - start, 'replica');
      return result.rows[0] ?? null;
    } finally {
      client.release();
    }
  }

  /**
   * Runs multiple queries in a single transaction.
   * @param {Function} callback - receives a transaction client
   */
  async transaction(callback) {
    const client = await this.primaryPool.connect();
    try {
      await client.query('BEGIN');
      const result = await callback(client);
      await client.query('COMMIT');
      return result;
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }
  }

  _logQuery(sql, durationMs, source = 'primary') {
    if (durationMs > 100) {
      console.warn(`Slow query (${durationMs}ms) [${source}]: ${sql.slice(0, 80)}...`);
    }
  }

  _setupErrorHandlers() {
    this.primaryPool.on('error', (err) => console.error('DB primary pool error:', err));
    this.replicaPool.on('error', (err) => console.error('DB replica pool error:', err));
  }
}

module.exports = new Database(); // Singleton
```

### 4.7 Rate Limiter (Token Bucket)

```javascript
// src/middleware/rateLimiter.js

const { CacheService } = require('../services/CacheService');

/**
 * Token Bucket Rate Limiter implemented over Redis.
 *
 * Algorithm:
 *   - Each user has a bucket with a max capacity of tokens
 *   - Tokens refill at a fixed rate (e.g., 100 tokens/minute)
 *   - Each request consumes 1 token
 *   - If bucket empty → 429 Too Many Requests
 *
 * This is implemented as a sliding window counter in Redis
 * for simplicity at scale (avoids Lua scripts).
 */
class RateLimiter {
  constructor(cache = new CacheService()) {
    this.cache = cache;

    // Tier-based limits
    this.LIMITS = {
      anonymous: { windowMs: 60_000, maxRequests: 10  },
      free:      { windowMs: 60_000, maxRequests: 100 },
      pro:       { windowMs: 60_000, maxRequests: 1000 },
      enterprise:{ windowMs: 60_000, maxRequests: 10000 },
    };
  }

  /**
   * Returns Express middleware for rate limiting.
   * @param {string} limitType - Which limit config to use
   */
  middleware(limitType = 'anonymous') {
    return async (req, res, next) => {
      const identifier = this._getIdentifier(req);
      const limit      = this.LIMITS[req.user?.plan || limitType];
      const windowSec  = Math.floor(limit.windowMs / 1000);

      // Sliding window: key per (identifier, window_start)
      const windowStart = Math.floor(Date.now() / limit.windowMs);
      const key         = `rate:${limitType}:${identifier}:${windowStart}`;

      const count = await this.cache.increment(key, windowSec * 2);

      // Add rate limit headers (standard RFC)
      res.set({
        'X-RateLimit-Limit':     limit.maxRequests,
        'X-RateLimit-Remaining': Math.max(0, limit.maxRequests - count),
        'X-RateLimit-Reset':     new Date((windowStart + 1) * limit.windowMs).toISOString(),
      });

      if (count !== null && count > limit.maxRequests) {
        return res.status(429).json({
          error:   'Too Many Requests',
          message: `Rate limit exceeded. Try again after ${new Date((windowStart + 1) * limit.windowMs).toISOString()}`,
          retryAfter: Math.ceil((((windowStart + 1) * limit.windowMs) - Date.now()) / 1000),
        });
      }

      next();
    };
  }

  _getIdentifier(req) {
    // Authenticated users identified by userId
    if (req.user?.id) return `user:${req.user.id}`;
    // Anonymous users identified by IP (hashed)
    const ip = req.headers['x-forwarded-for']?.split(',')[0] || req.socket.remoteAddress;
    return `ip:${this._hashIp(ip)}`;
  }

  _hashIp(ip) {
    const crypto = require('crypto');
    return crypto.createHash('sha256').update(ip + process.env.IP_HASH_SALT).digest('hex').slice(0, 16);
  }
}

module.exports = { RateLimiter };
```

### 4.8 Analytics Service

```javascript
// src/services/AnalyticsService.js

const { Kafka }        = require('kafkajs');
const { CacheService } = require('./CacheService');
const db               = require('../database/connection');

/**
 * Analytics Service — records and queries click events.
 *
 * Write path: Click → Redis INCR (real-time counter) + Kafka (durable log)
 * Read path:  Redis counter (recent) → ClickHouse (historical aggregates)
 *
 * Design principle: Click recording must NEVER slow down the redirect.
 * All writes are fire-and-forget or async.
 */
class AnalyticsService {
  constructor() {
    this.cache = new CacheService();
    this.producer = this._createKafkaProducer();
  }

  /**
   * Records a single click event.
   * Non-blocking — called without await in redirect handler.
   * @param {Object} event
   */
  recordClick(event) {
    // Fire-and-forget — don't await, don't block redirect
    setImmediate(() => this._recordClickAsync(event).catch(err =>
      console.error('Analytics record error:', err)
    ));
  }

  async _recordClickAsync({ shortKey, ip, userAgent, referrer, country }) {
    // 1. Increment real-time counter in Redis
    await this.cache.increment(`clicks:${shortKey}`, 86400 * 7); // 7-day TTL

    // 2. Publish to Kafka for durable processing
    await this.producer.send({
      topic:    'click-events',
      messages: [{
        key:   shortKey,              // Partition by shortKey for ordering
        value: JSON.stringify({
          shortKey,
          clickedAt:  new Date().toISOString(),
          ipHash:     this._hashIp(ip),
          country:    country || 'XX',
          deviceType: this._parseDeviceType(userAgent),
          browser:    this._parseBrowser(userAgent),
          referrer:   referrer ? new URL(referrer).hostname : 'direct',
        }),
      }],
    });
  }

  /**
   * Gets aggregated analytics for a short URL.
   * @returns {Object} Analytics summary
   */
  async getAnalytics(shortKey, { from, to } = {}) {
    // Real-time click count from Redis
    const realtimeCount = await this.cache.get(`clicks:${shortKey}`) || 0;

    // Historical data from ClickHouse (or PostgreSQL fallback)
    const historicalData = await db.queryReadReplica(
      `SELECT
         DATE(clicked_at)       AS date,
         COUNT(*)               AS clicks,
         COUNT(DISTINCT ip_hash) AS unique_clicks,
         country,
         device_type
       FROM click_events
       WHERE short_key = $1
         AND clicked_at >= $2
         AND clicked_at <= $3
       GROUP BY DATE(clicked_at), country, device_type
       ORDER BY date DESC`,
      [shortKey, from || new Date(0), to || new Date()]
    );

    return {
      totalClicks:   Number(realtimeCount),
      clicksByDay:   this._aggregateByDay(historicalData),
      topCountries:  this._topN(historicalData, 'country', 10),
      topDevices:    this._topN(historicalData, 'device_type', 5),
    };
  }

  _parseDeviceType(ua = '') {
    if (/bot|crawler|spider/i.test(ua))       return 'bot';
    if (/mobile|android|iphone/i.test(ua))    return 'mobile';
    if (/tablet|ipad/i.test(ua))              return 'tablet';
    return 'desktop';
  }

  _parseBrowser(ua = '') {
    if (/chrome/i.test(ua))  return 'chrome';
    if (/firefox/i.test(ua)) return 'firefox';
    if (/safari/i.test(ua))  return 'safari';
    if (/edge/i.test(ua))    return 'edge';
    return 'other';
  }

  _hashIp(ip = '') {
    const crypto = require('crypto');
    return crypto.createHash('sha256').update(ip + process.env.IP_HASH_SALT).digest('hex');
  }

  _aggregateByDay(rows) {
    const map = {};
    for (const row of rows) {
      const d = row.date;
      map[d] = (map[d] || 0) + Number(row.clicks);
    }
    return Object.entries(map).map(([date, clicks]) => ({ date, clicks }));
  }

  _topN(rows, field, n) {
    const counts = {};
    for (const row of rows) {
      const k = row[field] || 'unknown';
      counts[k] = (counts[k] || 0) + Number(row.clicks);
    }
    return Object.entries(counts)
      .sort((a, b) => b[1] - a[1])
      .slice(0, n)
      .map(([value, clicks]) => ({ value, clicks }));
  }

  _createKafkaProducer() {
    const kafka    = new Kafka({
      clientId: 'url-shortener',
      brokers:   (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
      retry:     { retries: 3, initialRetryTime: 100 },
    });
    const producer = kafka.producer({
      allowAutoTopicCreation: false,
      idempotent:             true, // Exactly-once semantics
    });
    producer.connect().catch(console.error);
    return producer;
  }
}

module.exports = { AnalyticsService };
```

### 4.9 REST API Controllers

```javascript
// src/controllers/urlController.js

const { UrlShortenerService } = require('../services/UrlShortenerService');
const { AnalyticsService }    = require('../services/AnalyticsService');

const shortener  = new UrlShortenerService();
const analytics  = new AnalyticsService();
const BASE_URL   = process.env.BASE_URL || 'https://short.ly';

/**
 * POST /api/v1/urls
 * Creates a short URL.
 */
async function createUrl(req, res, next) {
  try {
    const { longUrl, customAlias, expiresAt, tags } = req.body;

    if (!longUrl) {
      return res.status(400).json({ error: 'longUrl is required' });
    }

    const record = await shortener.createShortUrl({
      longUrl,
      customAlias,
      expiresAt: expiresAt ? new Date(expiresAt) : null,
      userId:    req.user?.id || null,
      tags:      tags || [],
    });

    return res.status(201).json({
      shortUrl:  `${BASE_URL}/${record.short_key}`,
      shortKey:  record.short_key,
      longUrl:   record.long_url,
      expiresAt: record.expires_at,
      createdAt: record.created_at,
    });

  } catch (err) {
    // Pass to central error handler
    next(err);
  }
}

/**
 * GET /:shortKey
 * Resolves and redirects. Hot path — must be extremely fast.
 */
async function redirectUrl(req, res, next) {
  try {
    const { shortKey } = req.params;

    // Validate short key format before touching cache/DB
    const { Base62 } = require('../utils/Base62');
    if (!Base62.isValid(shortKey)) {
      return res.status(404).json({ error: 'URL not found' });
    }

    const longUrl = await shortener.resolveUrl(shortKey);

    // Fire analytics asynchronously — does not block redirect
    analytics.recordClick({
      shortKey,
      ip:        req.headers['x-forwarded-for'] || req.socket.remoteAddress,
      userAgent: req.headers['user-agent'] || '',
      referrer:  req.headers['referer'] || '',
      country:   req.headers['cf-ipcountry'] || // Cloudflare header
                 req.headers['x-country-code'] || 'XX',
    });

    // 302 for analytics (forces re-request); 301 for pure performance
    return res.redirect(302, longUrl);

  } catch (err) {
    if (err.statusCode === 404) return res.status(404).render('404');
    if (err.statusCode === 410) return res.status(410).render('expired');
    next(err);
  }
}

/**
 * GET /api/v1/urls/:shortKey
 * Returns URL metadata.
 */
async function getUrlInfo(req, res, next) {
  try {
    const { shortKey } = req.params;
    const db = require('../database/connection');

    const row = await db.queryReadReplica(
      'SELECT short_key, long_url, click_count, created_at, expires_at, is_active FROM urls WHERE short_key = $1',
      [shortKey]
    );

    if (!row) return res.status(404).json({ error: 'URL not found' });

    return res.json({
      shortKey:   row.short_key,
      longUrl:    row.long_url,
      clicks:     Number(row.click_count),
      createdAt:  row.created_at,
      expiresAt:  row.expires_at,
      isActive:   row.is_active,
    });

  } catch (err) { next(err); }
}

/**
 * DELETE /api/v1/urls/:shortKey
 * Soft-deletes a URL.
 */
async function deleteUrl(req, res, next) {
  try {
    if (!req.user) return res.status(401).json({ error: 'Authentication required' });

    await shortener.deleteUrl(req.params.shortKey, req.user.id);
    return res.status(204).send();

  } catch (err) { next(err); }
}

module.exports = { createUrl, redirectUrl, getUrlInfo, deleteUrl };
```

### 4.10 Middleware

```javascript
// src/server.js

const express    = require('express');
const helmet     = require('helmet');
const compression = require('compression');
const { RateLimiter } = require('./middleware/rateLimiter');

const {
  createUrl,
  redirectUrl,
  getUrlInfo,
  deleteUrl,
} = require('./controllers/urlController');

const app     = express();
const limiter = new RateLimiter();

// ─── Security headers ───────────────────────────────────────────────────────
app.use(helmet());
app.use(compression()); // Gzip responses

// ─── Body parsing ───────────────────────────────────────────────────────────
app.use(express.json({ limit: '10kb' })); // Limit body size

// ─── Trust proxy (needed for accurate IPs behind LB) ────────────────────────
app.set('trust proxy', 1);

// ─── Routes ─────────────────────────────────────────────────────────────────

// Redirect route — NO auth required, heavy rate limiting per IP
app.get('/:shortKey',
  limiter.middleware('anonymous'),
  redirectUrl
);

// Write route — optional auth, rate limit per user/plan
app.post('/api/v1/urls',
  optionalAuth,
  limiter.middleware('free'),
  createUrl
);

app.get('/api/v1/urls/:shortKey',
  getUrlInfo
);

app.delete('/api/v1/urls/:shortKey',
  requireAuth,
  deleteUrl
);

// ─── Central error handler ──────────────────────────────────────────────────
app.use((err, req, res, _next) => {
  const status  = err.statusCode || 500;
  const message = status < 500 ? err.message : 'Internal Server Error';

  if (status >= 500) {
    console.error('Unhandled error:', err); // Log to observability platform
  }

  res.status(status).json({ error: message });
});

// ─── Health check ───────────────────────────────────────────────────────────
app.get('/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

app.listen(process.env.PORT || 3000, () => {
  console.log(`URL Shortener running on port ${process.env.PORT || 3000}`);
});

function requireAuth(req, res, next) {
  const token = req.headers.authorization?.replace('Bearer ', '');
  if (!token) return res.status(401).json({ error: 'Authentication required' });
  // JWT verification — simplified
  try {
    const jwt = require('jsonwebtoken');
    req.user  = jwt.verify(token, process.env.JWT_SECRET);
    next();
  } catch {
    res.status(401).json({ error: 'Invalid token' });
  }
}

function optionalAuth(req, res, next) {
  const token = req.headers.authorization?.replace('Bearer ', '');
  if (token) {
    try {
      const jwt = require('jsonwebtoken');
      req.user  = jwt.verify(token, process.env.JWT_SECRET);
    } catch { /* anonymous request */ }
  }
  next();
}
```

### 4.11 Bloom Filter (Duplicate Detection)

```javascript
// src/services/BloomFilterService.js

/**
 * In-memory Bloom Filter for fast duplicate short key detection.
 *
 * A Bloom filter provides probabilistic set membership:
 *   - "definitely NOT in set" → 100% accurate (no false negatives)
 *   - "might be in set" → small false positive rate (~1%)
 *
 * This avoids a DB round-trip for the vast majority of key generations.
 * False positives trigger a DB verification (rare, cheap).
 *
 * Parameters for 100M items at 1% false positive rate:
 *   Bit array size:     ~958 MB  (too large for single node)
 *   Optimal hash funcs: 7
 *
 * Production: Use Redis with Bloom filter module (RedisBloom)
 * or partition across multiple Redis nodes.
 */
class BloomFilterService {
  constructor({ size = 10_000_000, hashCount = 7 } = {}) {
    this.size      = size;
    this.hashCount = hashCount;
    this.bitArray  = new Uint8Array(Math.ceil(size / 8));
    this.itemCount = 0;
  }

  /**
   * Adds an item to the filter.
   */
  add(item) {
    for (const position of this._hashPositions(item)) {
      const byte = Math.floor(position / 8);
      const bit  = position % 8;
      this.bitArray[byte] |= (1 << bit);
    }
    this.itemCount++;
  }

  /**
   * Tests whether an item might be in the set.
   * @returns {boolean} true = might exist; false = definitely not
   */
  mightContain(item) {
    for (const position of this._hashPositions(item)) {
      const byte = Math.floor(position / 8);
      const bit  = position % 8;
      if (!(this.bitArray[byte] & (1 << bit))) return false;
    }
    return true;
  }

  /**
   * Current false positive probability.
   */
  get falsePositiveRate() {
    return Math.pow(
      1 - Math.exp(-this.hashCount * this.itemCount / this.size),
      this.hashCount
    );
  }

  _hashPositions(item) {
    const positions = [];
    // Use two independent hashes to simulate k hash functions (Kirsch-Mitzenmacher)
    const h1 = this._fnv1a(item);
    const h2 = this._djb2(item);
    for (let i = 0; i < this.hashCount; i++) {
      positions.push(Math.abs((h1 + i * h2) % this.size));
    }
    return positions;
  }

  _fnv1a(str) {
    let hash = 0x811c9dc5;
    for (let i = 0; i < str.length; i++) {
      hash ^= str.charCodeAt(i);
      hash = (hash * 0x01000193) >>> 0;
    }
    return hash;
  }

  _djb2(str) {
    let hash = 5381;
    for (let i = 0; i < str.length; i++) {
      hash = (((hash << 5) + hash) + str.charCodeAt(i)) >>> 0;
    }
    return hash;
  }
}

module.exports = { BloomFilterService };
```

---

## 5. Failure Scenarios & Mitigations

| Scenario | Impact | Detection | Mitigation |
|----------|--------|-----------|------------|
| Redis cluster down | All reads hit DB — 10x latency spike | Cache error rate alert | Graceful degradation: bypass cache, read from DB replica |
| Primary DB down | Write path fails (reads still work via replica) | Health check fails | Automated failover via Patroni (30s RTO); queue writes in Redis |
| Snowflake clock skew | Duplicate IDs possible | NTP monitoring | Accept up to 10ms backward drift; reject > 10ms |
| Kafka broker down | Click events lost | Consumer lag alert | Producer retries (idempotent); fall back to Redis counter only |
| Single region outage | Full service outage for that region | Route53 health check fails | GeoDNS failover to another region (< 60s) |
| Hot key (viral URL) | Cache stampede on miss | Latency spike for specific keys | Lock-based cache warming; 301 redirect for static URLs |
| Hash collision | Wrong redirect | Error monitoring | Bloom filter + DB verification on write |
| Disk full (DB) | Writes fail | Disk usage alert | Partition archival: move old click_events to S3 |

### Cache Stampede Prevention

```javascript
// Mutex-based cache warming for hot keys
async function getWithMutex(shortKey) {
  const cached = await cache.get(`url:${shortKey}`);
  if (cached) return cached;

  // Try to acquire lock
  const lockAcquired = await cache.client.set(
    `lock:${shortKey}`, '1', 'NX', 'EX', 5 // 5 second TTL
  );

  if (lockAcquired) {
    // We hold the lock — fetch from DB and populate cache
    const data = await db.queryReadReplica('SELECT ...', [shortKey]);
    await cache.set(`url:${shortKey}`, data, 86400);
    await cache.del(`lock:${shortKey}`);
    return data;
  } else {
    // Another process is fetching — wait briefly and retry
    await new Promise(r => setTimeout(r, 50));
    return cache.get(`url:${shortKey}`);
  }
}
```

---

## 6. Scalability Deep Dive

### Horizontal Scaling — Read Service
The read path is **stateless** — every pod can handle every request.
Scale by adding pods; Redis cluster handles the shared state.

```
Pod count:          Auto-scaled based on CPU (target 60%)
Max pods per AZ:    500
Request capacity:   ~2,000 RPS/pod → 1M RPS with 500 pods
```

### Write Path Bottleneck Analysis
```
Bottleneck         Max throughput     Solution
─────────────────────────────────────────────────────
DB write (primary) ~10K TPS           Batch inserts + async write
Redis write        ~100K ops/sec       Redis cluster (16 shards)
ID generation      ~4M IDs/sec/machine Snowflake (per-pod, no coordination)
Kafka produce      ~1M messages/sec    Async + batching
```

### URL Expiry Cleanup
- Background cron job (every 5 minutes): marks expired URLs as `is_active = FALSE`
- Redis TTL handles cache auto-eviction
- Archival job (daily): moves expired records to S3 (cold storage)

### Click Count Accuracy
- Redis `INCR` for real-time approximate counts (per URL)
- Periodic flush (every 60s) updates `click_count` in PostgreSQL
- Trade-off: accepts up to 60 seconds of lag for DB accuracy
- Exact counts available from Kafka → ClickHouse pipeline

---

## 7. Trade-offs Summary

| Decision | Chosen | Alternative | Why |
|----------|--------|-------------|-----|
| ID generation | Snowflake | Hash (MD5) | No coordination, time-sortable, no collisions |
| Short key length | 7 chars | 6 or 8 | Balances key space (~3.5T) vs URL brevity |
| Primary DB | PostgreSQL | Cassandra | ACID for ownership/uniqueness; sharded Postgres matches Cassandra reads |
| Redirect code | 302 | 301 | 302 forces re-request → analytics captured; 301 faster but uncacheable at analytics level |
| Cache eviction | LRU | LFU | LRU simpler, performs well for recency-heavy URL traffic |
| Analytics write | Kafka | Direct DB | Decouples hot path from analytics; enables replay |
| Consistency (reads) | Eventual | Strong | Read-your-writes only needed for creator; replicas acceptable for public redirects |
| Encoding | Base62 | Base64 | Base62 avoids URL-unsafe chars (`+`, `/`) without encoding |
| Counter type | Approx. (Redis) | Exact (DB) | Exact counts under 1M RPS would require distributed locking; approx. is sufficient |

---

## References & Further Reading

- [System Design Interview Vol. 1 — Alex Xu](https://www.goodreads.com/book/show/54109255) (Chapter: Design a URL Shortener)
- [Twitter Snowflake Blog Post](https://blog.twitter.com/engineering/en_us/a/2010/announcing-snowflake)
- [Redis Documentation — Cluster](https://redis.io/docs/management/scaling/)
- [PostgreSQL Partitioning](https://www.postgresql.org/docs/current/ddl-partitioning.html)
- [Consistent Hashing — MIT Lecture Notes](https://ocw.mit.edu)
- [Bloom Filters — Wikipedia](https://en.wikipedia.org/wiki/Bloom_filter)
- [Kafka Design](https://kafka.apache.org/documentation/#design)

---

*Designed to handle 100M DAU, 1M+ read RPS, 99.99% availability.*
*Last updated: 2026*