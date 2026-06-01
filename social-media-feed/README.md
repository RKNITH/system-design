# Social Media Feed — System Design

> **Interview Level:** FAANG / Staff Engineer  
> **Scope:** High-Level Design (HLD) + Low-Level Design (LLD in JavaScript)  
> **Covers:** Functional requirements, non-functional requirements, capacity estimation, HLD architecture, database design, API design, feed generation strategies, caching, ranking, notifications, scalability, fault tolerance, and full LLD with JS code.

---

## Table of Contents

1. [Problem Statement](#1-problem-statement)
2. [Functional Requirements](#2-functional-requirements)
3. [Non-Functional Requirements](#3-non-functional-requirements)
4. [Capacity Estimation](#4-capacity-estimation)
5. [High-Level Design (HLD)](#5-high-level-design-hld)
   - 5.1 [System Architecture Overview](#51-system-architecture-overview)
   - 5.2 [Core Services](#52-core-services)
   - 5.3 [Feed Generation Strategies](#53-feed-generation-strategies)
   - 5.4 [Data Flow](#54-data-flow)
   - 5.5 [Database Design](#55-database-design)
   - 5.6 [Caching Strategy](#56-caching-strategy)
   - 5.7 [Message Queue & Event Streaming](#57-message-queue--event-streaming)
   - 5.8 [CDN & Media Storage](#58-cdn--media-storage)
   - 5.9 [Search & Discovery](#59-search--discovery)
   - 5.10 [Notification System](#510-notification-system)
   - 5.11 [Ranking & Personalization](#511-ranking--personalization)
   - 5.12 [Security & Privacy](#512-security--privacy)
   - 5.13 [Fault Tolerance & Disaster Recovery](#513-fault-tolerance--disaster-recovery)
6. [Low-Level Design (LLD)](#6-low-level-design-lld)
   - 6.1 [Post Service](#61-post-service)
   - 6.2 [Follow Graph Service](#62-follow-graph-service)
   - 6.3 [Feed Generation Service (Fan-out)](#63-feed-generation-service-fan-out)
   - 6.4 [Feed Retrieval Service](#64-feed-retrieval-service)
   - 6.5 [Ranking Engine](#65-ranking-engine)
   - 6.6 [Cache Layer (Redis)](#66-cache-layer-redis)
   - 6.7 [Notification Service](#67-notification-service)
   - 6.8 [Rate Limiter](#68-rate-limiter)
   - 6.9 [API Gateway / BFF](#69-api-gateway--bff)
   - 6.10 [Media Upload Service](#610-media-upload-service)
7. [API Contracts](#7-api-contracts)
8. [Trade-offs & Design Decisions](#8-trade-offs--design-decisions)
9. [Bottlenecks & Solutions](#9-bottlenecks--solutions)
10. [Monitoring & Observability](#10-monitoring--observability)

---

## 1. Problem Statement

Design a scalable social media feed system (similar to Twitter/Instagram/Facebook feed) where:

- Users can **create posts** (text, images, videos).
- Users can **follow/unfollow** other users.
- Users see a **personalized feed** of posts from people they follow.
- The feed is **ranked** by relevance, recency, and engagement.
- The system must handle **hundreds of millions of users** and **millions of posts per day**.

---

## 2. Functional Requirements

| # | Requirement |
|---|-------------|
| FR1 | Users can create, edit, delete posts (text, image, video, links) |
| FR2 | Users can follow and unfollow other users |
| FR3 | Users can view a paginated, personalized feed of posts from followees |
| FR4 | Users can like, comment, share/repost posts |
| FR5 | Feed is ranked — not purely chronological (relevance + recency + engagement) |
| FR6 | Users receive real-time notifications (new post, like, comment, follow) |
| FR7 | Users can search for posts, hashtags, and people |
| FR8 | Support for stories / ephemeral content (24h expiry) |
| FR9 | Feed supports infinite scroll / pagination via cursor |
| FR10 | Support for "verified" accounts and celebrity users (millions of followers) |

---

## 3. Non-Functional Requirements

| # | Requirement | Target |
|---|-------------|--------|
| NFR1 | Availability | 99.99% uptime (< 52 min downtime/year) |
| NFR2 | Feed read latency | p99 < 200ms |
| NFR3 | Post write latency | p99 < 500ms |
| NFR4 | Consistency | Eventual consistency acceptable for feed; strong consistency for user data |
| NFR5 | Scalability | Handle 500M DAU, 10M posts/day |
| NFR6 | Durability | Zero data loss for posts and user data |
| NFR7 | Partitioning | Horizontally scalable, no single point of failure |
| NFR8 | Security | Authentication, authorization, rate limiting, content moderation |

---

## 4. Capacity Estimation

### Users & Posts

```
DAU                     = 500 million
Posts per DAU per day   = 0.1  (on average, 1 post per 10 days)
Total posts/day         = 500M × 0.1 = 50 million posts/day
Posts/second (write QPS)= 50M / 86400 ≈ 580 writes/sec
Peak write QPS          = 580 × 5 = ~3,000 writes/sec (5x spike factor)
```

### Feed Reads

```
Feed reads per DAU/day  = 20 (user refreshes feed ~20 times)
Total feed reads/day    = 500M × 20 = 10 billion reads/day
Read QPS                = 10B / 86400 ≈ 115,000 reads/sec
Peak read QPS           = 115,000 × 5 = ~575,000 reads/sec
```

### Storage

```
Avg post size (text)    = 500 bytes
50M posts/day × 500B    = 25 GB/day (text data)
Media (images/videos)   = ~200 GB/day (estimated)
10 years retention      = 25 GB × 365 × 10 ≈ 91 TB (text)
With replication (3x)   = ~273 TB for text data
Media CDN storage       = ~730 TB over 10 years
```

### Follow Graph

```
Average follows/user    = 300
500M users × 300        = 150 billion follow edges
Each edge               = ~16 bytes (user_id: 8B × 2)
Total                   = ~2.4 TB for follow graph
```

### Feed Cache

```
Feed per user           = 500 post IDs × 8 bytes = 4 KB
Active users cached     = 50M (top 10% DAU)
Total cache size        = 50M × 4 KB = 200 GB Redis cluster
```

---

## 5. High-Level Design (HLD)

### 5.1 System Architecture Overview

```
                          ┌─────────────────────────────────────────────────────────────────┐
                          │                         CLIENT TIER                              │
                          │     Web App (React)   |   iOS App   |   Android App             │
                          └──────────────────────────────┬──────────────────────────────────┘
                                                         │ HTTPS
                          ┌──────────────────────────────▼──────────────────────────────────┐
                          │                       API GATEWAY / BFF                          │
                          │   Auth (JWT/OAuth2) | Rate Limiting | Request Routing            │
                          │   Load Balancer (L7) | TLS Termination | GraphQL/REST           │
                          └────┬───────────┬────────────┬──────────────┬────────────────────┘
                               │           │            │              │
               ┌───────────────▼─┐  ┌──────▼──────┐  ┌▼──────────┐  ┌▼──────────────────┐
               │   Post Service  │  │ Feed Service │  │  User/    │  │  Notification      │
               │                 │  │              │  │  Auth     │  │  Service           │
               │ Create/Edit/    │  │ Fan-out on   │  │  Service  │  │                    │
               │ Delete posts    │  │ Write/Read   │  │           │  │  WebSocket/SSE     │
               └───────┬─────────┘  └──────┬───────┘  └─────┬─────┘  └───────┬────────────┘
                       │                   │                 │                │
               ┌───────▼───────────────────▼─────────────────▼────────────────▼────────────┐
               │                    MESSAGE BROKER (Apache Kafka)                            │
               │   Topics: post.created | post.deleted | user.followed | feed.invalidate    │
               └───────┬────────────────────────────────────────────────────────────────────┘
                       │
        ┌──────────────┼────────────────────────────────────────┐
        │              │                                         │
┌───────▼────────┐ ┌───▼──────────────────┐           ┌────────▼───────┐
│  Fan-out       │ │  Search Indexer       │           │  Analytics     │
│  Worker        │ │  (Elasticsearch)      │           │  Service       │
│  (Consumers)   │ │                       │           │  (ClickHouse)  │
└───────┬────────┘ └──────────────────────┘           └────────────────┘
        │
┌───────▼──────────────────────────────────────────────────────────────────────┐
│                              STORAGE TIER                                     │
│                                                                               │
│  ┌─────────────────┐  ┌──────────────────┐  ┌─────────────────────────────┐ │
│  │   PostgreSQL     │  │   Apache Cassandra│  │       Redis Cluster         │ │
│  │  (User, Auth,    │  │   (Posts, Feed   │  │   (Feed cache, sessions,    │ │
│  │   Follow graph)  │  │    timelines)    │  │    rate limits, counters)   │ │
│  └─────────────────┘  └──────────────────┘  └─────────────────────────────┘ │
│                                                                               │
│  ┌─────────────────┐  ┌──────────────────┐  ┌─────────────────────────────┐ │
│  │   Neo4j /        │  │   AWS S3 /        │  │   Elasticsearch             │ │
│  │   Social Graph   │  │   Object Store   │  │   (Post search, hashtags)   │ │
│  │   (optional)     │  │   (Images/Video) │  │                             │ │
│  └─────────────────┘  └──────────────────┘  └─────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

### 5.2 Core Services

| Service | Responsibility | Tech Stack |
|---------|---------------|------------|
| **API Gateway** | Auth, rate limiting, routing, TLS termination | Kong / AWS API Gateway / Nginx |
| **User Service** | Registration, login, profile, follow/unfollow | Node.js + PostgreSQL |
| **Post Service** | CRUD for posts, media handling | Node.js + Cassandra + S3 |
| **Feed Service** | Generate and serve personalized feed | Node.js + Redis + Cassandra |
| **Fan-out Worker** | Distribute new posts to followers' feeds | Node.js + Kafka Consumer |
| **Ranking Service** | Score and rank feed posts | Python (ML) + Redis |
| **Notification Service** | Real-time push, email, in-app notifications | Node.js + WebSocket + Firebase |
| **Search Service** | Full-text search for posts, users, hashtags | Elasticsearch |
| **Media Service** | Upload, transcode, serve media files | Node.js + S3 + CloudFront CDN |
| **Analytics Service** | Track engagement, impressions, clicks | Kafka + ClickHouse |

---

### 5.3 Feed Generation Strategies

This is the **most critical design decision** in the system.

#### Strategy 1: Fan-out on Write (Push Model)

```
User A posts → immediately push post_id to all followers' feed timelines in Redis/Cassandra

Pros:
  ✅ Feed reads are O(1) — just read pre-computed list
  ✅ Very low read latency
  ✅ Simple read path

Cons:
  ❌ High write amplification: celebrity with 10M followers → 10M writes
  ❌ Wasted storage for inactive users
  ❌ Slow for celebrities / high-follower accounts
```

#### Strategy 2: Fan-out on Read (Pull Model)

```
User opens feed → Query all followees → Merge their posts → Rank and return

Pros:
  ✅ No write amplification
  ✅ Always fresh/consistent

Cons:
  ❌ High read latency (fan-out to hundreds of followees' timelines)
  ❌ Not scalable for users following 1000+ accounts
```

#### Strategy 3: Hybrid Model (Industry Standard — Twitter/Instagram approach)

```
  ┌─────────────────────────────────────────────────────────┐
  │                    HYBRID STRATEGY                       │
  │                                                         │
  │  Regular users (< 10K followers):  Fan-out on Write     │
  │    → Post pushed to all followers' feed cache           │
  │                                                         │
  │  Celebrity users (> 10K followers): Fan-out on Read     │
  │    → Post NOT pushed to feeds                           │
  │    → On feed read: merge celebrity posts inline         │
  │                                                         │
  │  Result: Optimal write AND read performance             │
  └─────────────────────────────────────────────────────────┘
```

**Decision: Use Hybrid Model** — This is what Twitter, Instagram, and Facebook use.

---

### 5.4 Data Flow

#### Write Path (User Creates Post)

```
1. Client → POST /posts → API Gateway
2. API Gateway authenticates JWT, rate-limits, routes to Post Service
3. Post Service:
   a. Validates content, detects spam/abuse (async)
   b. Stores post in Cassandra (post_id generated via Snowflake ID)
   c. Uploads media to S3 (pre-signed URL or direct upload)
   d. Publishes {post.created, post_id, author_id, timestamp} to Kafka
4. Fan-out Worker (Kafka consumer):
   a. Reads author's follower list from User Service / cache
   b. If author has < 10K followers: push post_id to each follower's feed list in Redis
   c. If celebrity: skip fan-out; feed readers will pull on read
5. Notification Worker:
   a. Notifies followers (push notification, in-app)
6. Search Indexer:
   a. Indexes post content, hashtags in Elasticsearch

Response to client: 200 OK with post_id (async fan-out in background)
```

#### Read Path (User Requests Feed)

```
1. Client → GET /feed?cursor=<cursor>&limit=20 → API Gateway
2. API Gateway routes to Feed Service
3. Feed Service:
   a. Check Redis for user's precomputed feed: ZRANGE user:{uid}:feed 0 19 WITHSCORES
   b. Cache HIT: Get post_ids from Redis
   c. Cache MISS: Rebuild feed from Cassandra (fan-out on read for this user)
4. For celebrity followees (not in cached feed):
   a. Fetch recent posts from each celebrity followee
   b. Merge into feed list
5. Hydrate post_ids: Fetch full post objects from Cassandra (batch get)
   a. Check post detail cache first (Redis)
   b. Fall back to Cassandra for misses
6. Apply ranking: Score posts by [recency × engagement × affinity]
7. Return paginated response with next_cursor
```

---

### 5.5 Database Design

#### PostgreSQL — Users & Follows

```sql
-- Users table
CREATE TABLE users (
    user_id       BIGINT PRIMARY KEY,           -- Snowflake ID
    username      VARCHAR(50) UNIQUE NOT NULL,
    email         VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    display_name  VARCHAR(100),
    bio           TEXT,
    avatar_url    VARCHAR(500),
    is_verified   BOOLEAN DEFAULT FALSE,
    is_celebrity  BOOLEAN DEFAULT FALSE,        -- > 10K followers threshold
    follower_count INT DEFAULT 0,
    following_count INT DEFAULT 0,
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    updated_at    TIMESTAMPTZ DEFAULT NOW()
);

-- Follows table
CREATE TABLE follows (
    follower_id   BIGINT NOT NULL REFERENCES users(user_id),
    followee_id   BIGINT NOT NULL REFERENCES users(user_id),
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (follower_id, followee_id)
);

CREATE INDEX idx_follows_followee ON follows(followee_id);   -- "who follows X?"
CREATE INDEX idx_follows_follower ON follows(follower_id);   -- "who does X follow?"
```

#### Cassandra — Posts & Feed Timelines

```cql
-- Posts table (partition by author_id for write locality)
CREATE TABLE posts (
    post_id       BIGINT,
    author_id     BIGINT,
    content       TEXT,
    media_urls    LIST<TEXT>,
    hashtags      LIST<TEXT>,
    like_count    COUNTER,
    comment_count COUNTER,
    repost_count  COUNTER,
    is_deleted    BOOLEAN,
    created_at    TIMESTAMP,
    PRIMARY KEY (author_id, post_id)
) WITH CLUSTERING ORDER BY (post_id DESC)
  AND default_time_to_live = 0;   -- posts live forever (use soft delete)

-- User feed timeline (pre-computed by fan-out worker)
CREATE TABLE user_feed (
    user_id       BIGINT,
    post_id       BIGINT,
    author_id     BIGINT,
    score         DOUBLE,          -- ranking score for ordering
    added_at      TIMESTAMP,
    PRIMARY KEY (user_id, score, post_id)
) WITH CLUSTERING ORDER BY (score DESC, post_id DESC)
  AND default_time_to_live = 604800;  -- 7 day TTL (feeds expire)

-- Post lookup by ID (for hydration)
CREATE TABLE posts_by_id (
    post_id       BIGINT PRIMARY KEY,
    author_id     BIGINT,
    content       TEXT,
    media_urls    LIST<TEXT>,
    hashtags      LIST<TEXT>,
    created_at    TIMESTAMP,
    is_deleted    BOOLEAN
);
```

#### Redis — Feed Cache & Counters

```
Key pattern: user:{user_id}:feed
Type: Sorted Set (ZSET)
Score: ranking_score (float, higher = newer/more relevant)
Value: post_id

TTL: 7 days
Max entries per user: 500 (LRU trim after 500)

Key pattern: post:{post_id}:counts
Type: Hash
Fields: likes, comments, reposts, views

Key pattern: post:{post_id}:detail
Type: String (JSON)
TTL: 1 hour

Key pattern: user:{user_id}:session
Type: String (JWT token or session data)
TTL: 24 hours

Key pattern: ratelimit:{user_id}:{endpoint}
Type: String (counter)
TTL: 60 seconds
```

---

### 5.6 Caching Strategy

```
┌──────────────────────────────────────────────────────────────────────┐
│                        CACHE LAYERS                                   │
│                                                                       │
│  L1: Client-side cache (browser/app)                                  │
│      - Cache feed response with ETags                                 │
│      - Stale-while-revalidate: serve cached, refresh in background   │
│      - TTL: 30 seconds                                               │
│                                                                       │
│  L2: CDN (CloudFront / Fastly)                                        │
│      - Static assets: images, videos, JS bundles                     │
│      - Edge caching for public feeds / trending posts                 │
│      - TTL: 24h for media, 5min for public feed endpoints            │
│                                                                       │
│  L3: Application cache (Redis Cluster)                                │
│      - User feed timelines (Sorted Set by score)                     │
│      - Post detail objects (JSON strings)                             │
│      - Engagement counters (Hash)                                     │
│      - User session/auth tokens                                       │
│      - Rate limit counters                                            │
│      - Hot user follower lists (for fast fan-out reads)              │
│                                                                       │
│  L4: Database (Cassandra + PostgreSQL)                                │
│      - Source of truth                                               │
└──────────────────────────────────────────────────────────────────────┘
```

**Cache Eviction Policy:**
- Feed lists: TTL 7 days + ZREMRANGEBYRANK to cap at 500 entries
- Post details: TTL 1 hour, LRU eviction
- Counters: No TTL (persistent counters, sync to DB every 5 min via batch job)

**Cache Invalidation:**
- Post deleted: `DEL post:{post_id}:detail` + remove from all cached feeds via Kafka event
- Post edited: `DEL post:{post_id}:detail` (re-fetch on next read)
- User unfollowed: Does NOT immediately purge feed (eventual consistency OK)

---

### 5.7 Message Queue & Event Streaming

**Apache Kafka** with the following topics:

```
Topic: post.created
  Partition key: author_id
  Consumers:
    - fan-out-worker (pushes to follower feeds)
    - notification-worker (notifies followers)
    - search-indexer (indexes post in Elasticsearch)
    - analytics-worker (tracks creation event)

Topic: post.deleted
  Partition key: post_id
  Consumers:
    - feed-cleanup-worker (removes from all feeds)
    - search-indexer (removes from index)

Topic: post.engaged (likes, comments, reposts)
  Partition key: post_id
  Consumers:
    - counter-aggregator (increments Redis counters)
    - ranking-worker (recalculates post score)
    - notification-worker (notify post author)

Topic: user.followed
  Partition key: follower_id
  Consumers:
    - feed-backfill-worker (add followee's recent posts to follower's feed)
    - graph-updater (update follow graph)

Topic: feed.invalidate
  Partition key: user_id
  Consumers:
    - cache-invalidator (purge Redis feed cache for user)
```

**Why Kafka over RabbitMQ?**
- Log retention enables replay (recalculate feeds after ranking model update)
- High throughput (millions of events/sec)
- Consumer groups allow independent scaling
- Partitioning ensures ordering per user/post

---

### 5.8 CDN & Media Storage

```
Upload Flow:
1. Client requests pre-signed S3 URL from Media Service
2. Client uploads directly to S3 (bypasses app servers)
3. S3 triggers Lambda → transcode video (different resolutions: 360p, 720p, 1080p)
4. Transcoded files stored back in S3
5. CloudFront CDN sits in front of S3 for reads

Media URL structure:
  https://cdn.example.com/{user_id}/{post_id}/{filename}_{quality}.{ext}
  Example: https://cdn.example.com/123/456/photo_1080.jpg

CDN Edge Caching:
  - Images/thumbnails: Cache-Control: max-age=31536000 (1 year, immutable)
  - Videos: Cache-Control: max-age=86400 (1 day)
  - Signed URLs for private/premium content
```

---

### 5.9 Search & Discovery

**Elasticsearch** for full-text search:

```
Index: posts
  Mapping:
    post_id:    keyword
    author_id:  keyword
    content:    text (analyzed, BM25 scoring)
    hashtags:   keyword (exact match)
    created_at: date
    like_count: integer (for boost)

Queries:
  - Full-text: { "match": { "content": "query" } }
  - Hashtag:   { "term": { "hashtags": "#trending" } }
  - Boosted:   function_score with like_count boost

Trending Hashtags:
  - Kafka consumer counts hashtag occurrences in sliding 1h window
  - Top 20 trending hashtags stored in Redis sorted set
  - Key: trending:hashtags, Score: count, TTL: 1 hour
```

---

### 5.10 Notification System

```
Real-time (WebSocket / SSE):
  - User connects → persistent WebSocket to Notification Service
  - Notification Service subscribes to user's Kafka partition
  - Push events: new post from followee, like, comment, new follower

Push Notifications (mobile):
  - Firebase Cloud Messaging (FCM) for Android
  - Apple Push Notification Service (APNs) for iOS
  - Fanout via Firebase Admin SDK

Notification types:
  - FOLLOW: "X started following you"
  - LIKE: "X liked your post"
  - COMMENT: "X commented on your post"
  - REPOST: "X reposted your post"
  - MENTION: "X mentioned you in a post"
  - NEW_POST: "X posted: [preview]" (for close friends / notifications enabled)

Storage (PostgreSQL):
  CREATE TABLE notifications (
    notification_id BIGINT PRIMARY KEY,
    recipient_id    BIGINT NOT NULL,
    type            VARCHAR(50),
    actor_id        BIGINT,
    entity_id       BIGINT,     -- post_id or user_id
    is_read         BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMPTZ DEFAULT NOW()
  );
  CREATE INDEX idx_notif_recipient ON notifications(recipient_id, is_read, created_at DESC);
```

---

### 5.11 Ranking & Personalization

**Ranking Score Formula (simplified EdgeRank-style):**

```
score(post) = affinity(u, author) × weight(post_type) × time_decay(post_age)

where:
  affinity(u, author) = interaction history (likes/comments/views with author)
                        Stored in Redis: user:{uid}:affinity:{author_id} → float
  weight(post_type)   = video: 1.5 | image: 1.2 | text: 1.0 | repost: 0.8
  time_decay(age)     = e^(-λ × hours_old)   where λ = 0.1 for 7-day half-life

Full ranking pipeline:
  1. Retrieve candidate set (feed cache: top 200 post_ids)
  2. Feature extraction: age, likes, comments, media_type, author_affinity
  3. Score each candidate (lightweight ML model or formula)
  4. Re-rank top 20 for current page
  5. Update affinity scores based on engagement (async)
```

---

### 5.12 Security & Privacy

```
Authentication:
  - JWT (short-lived access token: 15min) + Refresh token (30 days, httpOnly cookie)
  - OAuth2 (Google, Apple, Facebook login)

Authorization:
  - Role-based: user, moderator, admin
  - Privacy settings: public / followers-only / private posts

Rate Limiting:
  - Per user per endpoint (Redis token bucket)
  - POST /posts: 30 req/hour
  - GET /feed: 300 req/min
  - POST /likes: 100 req/min

Content Moderation:
  - Async: NLP classifier on post content (hate speech, spam)
  - Image: AWS Rekognition for NSFW detection
  - User reports pipeline

Data Privacy (GDPR/CCPA):
  - Right to deletion: cascade delete posts, feed entries, notifications
  - Data export: async job generating user's data ZIP
  - PII encryption at rest (AES-256)
```

---

### 5.13 Fault Tolerance & Disaster Recovery

```
Database:
  - PostgreSQL: Primary + 2 read replicas, automatic failover (Patroni)
  - Cassandra: Replication factor 3, quorum reads/writes
  - Redis: Cluster mode (16 shards × 3 replicas), Sentinel for failover

Kafka:
  - Replication factor 3, min.insync.replicas=2
  - Consumer offset checkpointing for exactly-once processing

Services:
  - Kubernetes deployments with 3+ replicas per service
  - Health checks + automatic pod restart
  - Circuit breaker pattern (Netflix Hystrix / resilience4js)
  - Bulkhead pattern: isolate celebrity fan-out from regular fan-out

Multi-region:
  - Active-active across 2 regions (US-East, US-West)
  - Cassandra multi-region replication
  - Route 53 latency-based routing
  - RTO: < 30 seconds, RPO: < 5 seconds

Backups:
  - PostgreSQL: Daily full backup to S3 + WAL streaming (PITR)
  - Cassandra: Daily snapshots to S3
  - Retention: 30 days
```

---

## 6. Low-Level Design (LLD)

> All code is in JavaScript (Node.js). These are production-grade implementations covering the critical paths.

---

### 6.1 Post Service

```javascript
// services/post-service/src/PostService.js

const { v4: uuidv4 } = require('uuid');
const cassandra = require('cassandra-driver');
const kafka = require('../lib/kafka');
const redis = require('../lib/redis');
const SnowflakeId = require('../lib/snowflake');
const MediaService = require('./MediaService');
const ContentModerator = require('./ContentModerator');

const snowflake = new SnowflakeId({ workerId: process.env.WORKER_ID || 1 });

class PostService {
  constructor() {
    this.cassandraClient = new cassandra.Client({
      contactPoints: process.env.CASSANDRA_HOSTS.split(','),
      localDataCenter: 'datacenter1',
      keyspace: 'social_feed',
      pooling: { coreConnectionsPerHost: { local: 4, remote: 2 } }
    });
    this.producer = kafka.createProducer();
  }

  /**
   * Create a new post
   * @param {Object} params
   * @param {string} params.authorId
   * @param {string} params.content
   * @param {string[]} params.mediaUrls
   * @returns {Object} Created post
   */
  async createPost({ authorId, content, mediaUrls = [] }) {
    // 1. Validate inputs
    if (!content && mediaUrls.length === 0) {
      throw new Error('Post must have content or media');
    }
    if (content && content.length > 2000) {
      throw new Error('Post content exceeds 2000 characters');
    }

    // 2. Generate unique post ID using Snowflake (time-sortable)
    const postId = snowflake.generate();
    const createdAt = new Date();

    // 3. Extract hashtags from content
    const hashtags = this._extractHashtags(content);

    // 4. Async content moderation (non-blocking)
    ContentModerator.checkAsync({ postId, content, authorId });

    // 5. Persist to Cassandra (dual write: posts_by_author + posts_by_id)
    const insertPostByAuthor = `
      INSERT INTO posts (author_id, post_id, content, media_urls, hashtags, is_deleted, created_at)
      VALUES (?, ?, ?, ?, ?, false, ?)
    `;
    const insertPostById = `
      INSERT INTO posts_by_id (post_id, author_id, content, media_urls, hashtags, created_at, is_deleted)
      VALUES (?, ?, ?, ?, ?, ?, false)
    `;

    // Batched write for atomicity within Cassandra limitations
    const batch = [
      {
        query: insertPostByAuthor,
        params: [authorId, postId, content, mediaUrls, hashtags, createdAt]
      },
      {
        query: insertPostById,
        params: [postId, authorId, content, mediaUrls, hashtags, createdAt]
      }
    ];

    await this.cassandraClient.batch(batch, { prepare: true });

    const post = { postId, authorId, content, mediaUrls, hashtags, createdAt };

    // 6. Cache post detail in Redis
    await redis.setex(
      `post:${postId}:detail`,
      3600,                          // 1 hour TTL
      JSON.stringify(post)
    );

    // 7. Publish to Kafka for async fan-out, notifications, search indexing
    await this.producer.send({
      topic: 'post.created',
      messages: [{
        key: authorId.toString(),   // Partition by authorId for ordering
        value: JSON.stringify({
          postId,
          authorId,
          content,
          mediaUrls,
          hashtags,
          createdAt: createdAt.toISOString()
        })
      }]
    });

    return post;
  }

  /**
   * Soft-delete a post
   */
  async deletePost({ postId, requesterId }) {
    // 1. Fetch post to verify ownership
    const post = await this.getPostById(postId);
    if (!post) throw new Error('Post not found');
    if (post.authorId !== requesterId) throw new Error('Unauthorized');

    // 2. Soft delete in Cassandra (mark is_deleted = true)
    await this.cassandraClient.execute(
      'UPDATE posts_by_id SET is_deleted = true WHERE post_id = ?',
      [postId],
      { prepare: true }
    );

    // 3. Invalidate Redis cache
    await redis.del(`post:${postId}:detail`);

    // 4. Publish delete event for feed cleanup + search removal
    await this.producer.send({
      topic: 'post.deleted',
      messages: [{
        key: postId.toString(),
        value: JSON.stringify({ postId, authorId: post.authorId })
      }]
    });

    return { success: true };
  }

  /**
   * Get post by ID with caching
   */
  async getPostById(postId) {
    // L1: Redis cache check
    const cached = await redis.get(`post:${postId}:detail`);
    if (cached) return JSON.parse(cached);

    // L2: Cassandra fallback
    const result = await this.cassandraClient.execute(
      'SELECT * FROM posts_by_id WHERE post_id = ?',
      [postId],
      { prepare: true }
    );

    if (result.rows.length === 0) return null;

    const row = result.rows[0];
    if (row.is_deleted) return null;

    const post = {
      postId: row.post_id.toString(),
      authorId: row.author_id.toString(),
      content: row.content,
      mediaUrls: row.media_urls || [],
      hashtags: row.hashtags || [],
      createdAt: row.created_at
    };

    // Re-populate cache
    await redis.setex(`post:${postId}:detail`, 3600, JSON.stringify(post));

    return post;
  }

  /**
   * Batch get posts (for feed hydration) — minimizes round trips
   */
  async getPostsByIds(postIds) {
    if (!postIds || postIds.length === 0) return [];

    // 1. Check Redis for all at once
    const cacheKeys = postIds.map(id => `post:${id}:detail`);
    const cached = await redis.mget(cacheKeys);

    const posts = [];
    const missingIds = [];

    cached.forEach((val, idx) => {
      if (val) {
        posts.push({ index: idx, data: JSON.parse(val) });
      } else {
        missingIds.push({ index: idx, id: postIds[idx] });
      }
    });

    // 2. Batch fetch misses from Cassandra using IN query
    if (missingIds.length > 0) {
      const ids = missingIds.map(m => m.id);
      const placeholders = ids.map(() => '?').join(', ');

      const result = await this.cassandraClient.execute(
        `SELECT * FROM posts_by_id WHERE post_id IN (${placeholders})`,
        ids,
        { prepare: true }
      );

      const pipeline = redis.pipeline();
      result.rows.forEach(row => {
        if (!row.is_deleted) {
          const post = {
            postId: row.post_id.toString(),
            authorId: row.author_id.toString(),
            content: row.content,
            mediaUrls: row.media_urls || [],
            hashtags: row.hashtags || [],
            createdAt: row.created_at
          };

          const idx = missingIds.find(m => m.id === post.postId)?.index;
          if (idx !== undefined) {
            posts.push({ index: idx, data: post });
          }

          pipeline.setex(`post:${post.postId}:detail`, 3600, JSON.stringify(post));
        }
      });
      await pipeline.exec();
    }

    // Restore original order
    return posts.sort((a, b) => a.index - b.index).map(p => p.data);
  }

  _extractHashtags(content = '') {
    const regex = /#(\w+)/g;
    const matches = content.match(regex);
    return matches ? [...new Set(matches.map(t => t.toLowerCase()))] : [];
  }
}

module.exports = new PostService();
```

---

### 6.2 Follow Graph Service

```javascript
// services/user-service/src/FollowService.js

const { Pool } = require('pg');
const redis = require('../lib/redis');
const kafka = require('../lib/kafka');

// PostgreSQL connection pool
const pgPool = new Pool({
  connectionString: process.env.POSTGRES_URL,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000
});

const CELEBRITY_THRESHOLD = 10000;
const FOLLOWER_LIST_CACHE_TTL = 3600;       // 1 hour
const FOLLOWER_LIST_CACHE_LIMIT = 5000;     // Max cached followers per user

class FollowService {
  constructor() {
    this.producer = kafka.createProducer();
  }

  /**
   * Follow a user
   */
  async follow(followerId, followeeId) {
    if (followerId === followeeId) throw new Error('Cannot follow yourself');

    const client = await pgPool.connect();
    try {
      await client.query('BEGIN');

      // Insert follow edge (idempotent with ON CONFLICT DO NOTHING)
      const result = await client.query(
        `INSERT INTO follows (follower_id, followee_id, created_at)
         VALUES ($1, $2, NOW())
         ON CONFLICT (follower_id, followee_id) DO NOTHING
         RETURNING *`,
        [followerId, followeeId]
      );

      if (result.rowCount === 0) {
        // Already following
        await client.query('ROLLBACK');
        return { success: true, alreadyFollowing: true };
      }

      // Update follower/following counts atomically
      await client.query(
        `UPDATE users SET follower_count = follower_count + 1 WHERE user_id = $1`,
        [followeeId]
      );
      await client.query(
        `UPDATE users SET following_count = following_count + 1 WHERE user_id = $1`,
        [followerId]
      );

      // Check if followee crossed celebrity threshold
      const { rows: [followeeData] } = await client.query(
        'SELECT follower_count, is_celebrity FROM users WHERE user_id = $1',
        [followeeId]
      );

      if (!followeeData.is_celebrity && followeeData.follower_count >= CELEBRITY_THRESHOLD) {
        await client.query(
          'UPDATE users SET is_celebrity = true WHERE user_id = $1',
          [followeeId]
        );
      }

      await client.query('COMMIT');

      // Invalidate cached follower lists
      await Promise.all([
        redis.del(`user:${followeeId}:followers`),
        redis.del(`user:${followerId}:following`)
      ]);

      // Publish follow event for feed backfill and notifications
      await this.producer.send({
        topic: 'user.followed',
        messages: [{
          key: followerId.toString(),
          value: JSON.stringify({
            followerId,
            followeeId,
            timestamp: new Date().toISOString()
          })
        }]
      });

      return { success: true };
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }
  }

  /**
   * Unfollow a user
   */
  async unfollow(followerId, followeeId) {
    const client = await pgPool.connect();
    try {
      await client.query('BEGIN');

      const result = await client.query(
        `DELETE FROM follows WHERE follower_id = $1 AND followee_id = $2`,
        [followerId, followeeId]
      );

      if (result.rowCount === 0) {
        await client.query('ROLLBACK');
        return { success: false, reason: 'Not following' };
      }

      await client.query(
        'UPDATE users SET follower_count = GREATEST(follower_count - 1, 0) WHERE user_id = $1',
        [followeeId]
      );
      await client.query(
        'UPDATE users SET following_count = GREATEST(following_count - 1, 0) WHERE user_id = $1',
        [followerId]
      );

      await client.query('COMMIT');

      await Promise.all([
        redis.del(`user:${followeeId}:followers`),
        redis.del(`user:${followerId}:following`)
      ]);

      // Feed is eventually consistent — old posts stay until they expire
      return { success: true };
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }
  }

  /**
   * Get follower IDs for a user — used by fan-out worker
   * Returns array of follower user IDs (up to FOLLOWER_LIST_CACHE_LIMIT)
   */
  async getFollowers(userId, { limit = 5000, offset = 0 } = {}) {
    // Check cache for small/medium accounts
    const cacheKey = `user:${userId}:followers`;
    const cached = await redis.lrange(cacheKey, 0, -1);
    if (cached && cached.length > 0) {
      return cached.map(id => BigInt(id));
    }

    // Fetch from PostgreSQL
    const { rows } = await pgPool.query(
      `SELECT follower_id FROM follows
       WHERE followee_id = $1
       ORDER BY created_at DESC
       LIMIT $2 OFFSET $3`,
      [userId, limit, offset]
    );

    const followerIds = rows.map(r => r.follower_id);

    // Cache if follower list is small enough
    if (followerIds.length <= FOLLOWER_LIST_CACHE_LIMIT && followerIds.length > 0) {
      const pipeline = redis.pipeline();
      pipeline.rpush(cacheKey, ...followerIds.map(String));
      pipeline.expire(cacheKey, FOLLOWER_LIST_CACHE_TTL);
      await pipeline.exec();
    }

    return followerIds;
  }

  /**
   * Get user IDs that a user is following
   */
  async getFollowing(userId) {
    const cacheKey = `user:${userId}:following`;
    const cached = await redis.lrange(cacheKey, 0, -1);
    if (cached && cached.length > 0) return cached.map(id => BigInt(id));

    const { rows } = await pgPool.query(
      'SELECT followee_id FROM follows WHERE follower_id = $1 ORDER BY created_at DESC',
      [userId]
    );

    const followingIds = rows.map(r => r.followee_id);

    if (followingIds.length > 0) {
      const pipeline = redis.pipeline();
      pipeline.rpush(cacheKey, ...followingIds.map(String));
      pipeline.expire(cacheKey, FOLLOWER_LIST_CACHE_TTL);
      await pipeline.exec();
    }

    return followingIds;
  }
}

module.exports = new FollowService();
```

---

### 6.3 Feed Generation Service (Fan-out)

```javascript
// services/fan-out-worker/src/FanOutWorker.js

const { Kafka } = require('kafkajs');
const redis = require('../lib/redis');
const FollowService = require('../lib/followService');
const UserService = require('../lib/userService');
const RankingEngine = require('./RankingEngine');

const FEED_MAX_SIZE = 500;           // Max posts kept in user feed cache
const CELEBRITY_FOLLOWER_THRESHOLD = 10000;
const BATCH_SIZE = 100;              // Process followers in batches to avoid Redis overload

class FanOutWorker {
  constructor() {
    const kafka = new Kafka({
      clientId: 'fan-out-worker',
      brokers: process.env.KAFKA_BROKERS.split(','),
      retry: { retries: 5 }
    });

    this.consumer = kafka.consumer({
      groupId: 'fan-out-group',
      maxBytesPerPartition: 1048576  // 1 MB
    });
  }

  async start() {
    await this.consumer.connect();
    await this.consumer.subscribe({ topic: 'post.created', fromBeginning: false });

    await this.consumer.run({
      eachMessage: async ({ message }) => {
        try {
          const event = JSON.parse(message.value.toString());
          await this.handlePostCreated(event);
        } catch (err) {
          console.error('FanOutWorker error:', err);
          // Send to DLQ (Dead Letter Queue) for retry
          await this.sendToDLQ(message, err);
        }
      }
    });
  }

  /**
   * Main fan-out handler
   * Decides whether to do fan-out-on-write (regular users) or skip (celebrities)
   */
  async handlePostCreated({ postId, authorId, createdAt, content, hashtags }) {
    // 1. Check if author is a celebrity
    const author = await UserService.getUserById(authorId);

    if (author.is_celebrity) {
      // Skip fan-out for celebrities — feed readers will pull their posts inline
      console.log(`Skipping fan-out for celebrity ${authorId} (${author.follower_count} followers)`);
      return;
    }

    // 2. Get all followers of this author
    const followerIds = await FollowService.getFollowers(authorId);
    if (followerIds.length === 0) return;

    // 3. Calculate initial ranking score for this post
    const score = RankingEngine.calculateInitialScore({
      postId,
      createdAt: new Date(createdAt),
      hasMedia: false, // Could pass media_urls here for boost
      authorEngagementRate: author.avgEngagementRate || 1.0
    });

    // 4. Fan-out to all followers' feed timelines in Redis
    // Process in batches to avoid blocking Redis for too long
    for (let i = 0; i < followerIds.length; i += BATCH_SIZE) {
      const batch = followerIds.slice(i, i + BATCH_SIZE);
      await this._pushToFollowerFeeds(batch, postId, score);
    }

    console.log(`Fan-out complete: post ${postId} pushed to ${followerIds.length} feeds`);
  }

  /**
   * Push a post to a batch of followers' feed caches (Redis Sorted Sets)
   * Uses pipelining for performance
   */
  async _pushToFollowerFeeds(followerIds, postId, score) {
    const pipeline = redis.pipeline();

    followerIds.forEach(userId => {
      const feedKey = `user:${userId}:feed`;

      // ZADD with score (higher score = shown first in feed)
      pipeline.zadd(feedKey, score, postId.toString());

      // Trim feed to max size (remove lowest-scored posts beyond limit)
      // ZREMRANGEBYRANK key 0 -(FEED_MAX_SIZE+1) removes all but top FEED_MAX_SIZE
      pipeline.zremrangebyrank(feedKey, 0, -(FEED_MAX_SIZE + 1));

      // Refresh TTL on every write (keep active feeds alive)
      pipeline.expire(feedKey, 7 * 24 * 3600); // 7 days
    });

    await pipeline.exec();
  }
}

module.exports = new FanOutWorker();
```

---

### 6.4 Feed Retrieval Service

```javascript
// services/feed-service/src/FeedService.js

const redis = require('../lib/redis');
const cassandra = require('../lib/cassandra');
const PostService = require('../lib/postService');
const UserService = require('../lib/userService');
const FollowService = require('../lib/followService');
const RankingEngine = require('./RankingEngine');
const CursorEncoder = require('../lib/cursor');

const FEED_PAGE_SIZE = 20;
const CELEBRITY_POSTS_LIMIT = 5;     // Max celebrity posts fetched per feed refresh
const CELEBRITY_POSTS_MAX_AGE_HOURS = 48;

class FeedService {

  /**
   * Get paginated feed for a user
   * Implements hybrid pull (celebrity posts) + push (regular posts from cache)
   */
  async getFeed(userId, cursor = null) {
    const startTime = Date.now();

    // 1. Decode cursor to get the last seen score/post_id
    const { minScore, lastPostId } = cursor
      ? CursorEncoder.decode(cursor)
      : { minScore: 0, lastPostId: null };

    // 2. Get regular followees' post IDs from Redis ZSET (pre-computed by fan-out)
    const regularPostIds = await this._getFromFeedCache(userId, minScore, FEED_PAGE_SIZE * 2);

    // 3. Get celebrity followees' recent posts (fan-out on read)
    const celebrityPosts = await this._getCelebrityPosts(userId);

    // 4. Merge both sets of post IDs
    const allPostIds = this._mergePostIds(regularPostIds, celebrityPosts.map(p => p.postId));

    // 5. Hydrate post details (batch fetch with caching)
    const posts = await PostService.getPostsByIds(allPostIds);

    // 6. Filter deleted posts, apply privacy rules
    const visiblePosts = posts.filter(post => post && !post.isDeleted);

    // 7. Enrich with engagement counts from Redis
    const enrichedPosts = await this._enrichWithCounts(visiblePosts);

    // 8. Apply ranking (re-rank based on ML scores / formula)
    const rankedPosts = await RankingEngine.rankPosts(enrichedPosts, userId);

    // 9. Paginate: take first FEED_PAGE_SIZE
    const pagePosts = rankedPosts.slice(0, FEED_PAGE_SIZE);

    // 10. Build next cursor from last item's score
    const hasMore = rankedPosts.length > FEED_PAGE_SIZE;
    const nextCursor = hasMore && pagePosts.length > 0
      ? CursorEncoder.encode({
          minScore: pagePosts[pagePosts.length - 1]._score,
          lastPostId: pagePosts[pagePosts.length - 1].postId
        })
      : null;

    console.log(`Feed for user ${userId} built in ${Date.now() - startTime}ms`);

    return {
      posts: pagePosts,
      nextCursor,
      hasMore
    };
  }

  /**
   * Fetch post IDs from user's Redis feed sorted set
   * Score = ranking_score (pre-computed at write time)
   */
  async _getFromFeedCache(userId, minScore, limit) {
    const feedKey = `user:${userId}:feed`;

    // ZREVRANGEBYSCORE: get post IDs with score < minScore (for pagination)
    // First load: minScore = 0, so use +inf
    const maxScore = minScore === 0 ? '+inf' : `(${minScore}`;
    const results = await redis.zrevrangebyscore(
      feedKey,
      maxScore,
      '-inf',
      'LIMIT', 0, limit
    );

    if (results && results.length > 0) {
      return results; // Array of post_id strings
    }

    // Cache miss: rebuild feed from Cassandra (cold start)
    return await this._rebuildFeedFromDB(userId, limit);
  }

  /**
   * Rebuild feed from Cassandra when Redis cache is cold (e.g., new user, cache evicted)
   */
  async _rebuildFeedFromDB(userId, limit) {
    // Get who user follows
    const followingIds = await FollowService.getFollowing(userId);
    if (followingIds.length === 0) return [];

    // Fetch recent posts from each non-celebrity followee
    const users = await UserService.getUsersByIds(followingIds);
    const regularFollowees = users.filter(u => !u.is_celebrity);

    // Fan-out on read (expensive, but only for cold starts)
    const postPromises = regularFollowees.slice(0, 50).map(user =>
      cassandra.execute(
        'SELECT post_id FROM posts WHERE author_id = ? ORDER BY post_id DESC LIMIT 20',
        [user.user_id],
        { prepare: true }
      )
    );

    const results = await Promise.all(postPromises);
    const allPostIds = results.flatMap(r => r.rows.map(row => row.post_id.toString()));

    // Populate Redis cache asynchronously
    this._populateFeedCache(userId, allPostIds).catch(console.error);

    return allPostIds.slice(0, limit);
  }

  /**
   * Get recent posts from celebrity followees (fan-out on read)
   * These are NOT in the pre-computed feed cache
   */
  async _getCelebrityPosts(userId) {
    const cacheKey = `user:${userId}:celebrity_followees`;
    let celebrityIds = await redis.smembers(cacheKey);

    if (!celebrityIds || celebrityIds.length === 0) {
      // Fetch celebrity followees from DB
      const followingIds = await FollowService.getFollowing(userId);
      const users = await UserService.getUsersByIds(followingIds);
      celebrityIds = users.filter(u => u.is_celebrity).map(u => u.user_id.toString());

      if (celebrityIds.length > 0) {
        await redis.sadd(cacheKey, ...celebrityIds);
        await redis.expire(cacheKey, 3600); // Cache for 1 hour
      }
    }

    if (celebrityIds.length === 0) return [];

    // Fetch recent posts from each celebrity (last 48h)
    const cutoffTime = new Date(Date.now() - CELEBRITY_POSTS_MAX_AGE_HOURS * 3600 * 1000);

    const postPromises = celebrityIds.slice(0, 20).map(celebId =>
      cassandra.execute(
        `SELECT post_id, author_id, created_at FROM posts
         WHERE author_id = ? AND post_id > minTimeuuid(?)
         ORDER BY post_id DESC LIMIT ?`,
        [celebId, cutoffTime, CELEBRITY_POSTS_LIMIT],
        { prepare: true }
      )
    );

    const results = await Promise.all(postPromises);
    return results.flatMap(r =>
      r.rows.map(row => ({
        postId: row.post_id.toString(),
        authorId: row.author_id.toString()
      }))
    );
  }

  /**
   * Merge regular and celebrity post IDs (interleave celebrities naturally)
   */
  _mergePostIds(regularIds, celebrityIds) {
    // Simple merge: deduplicate and return combined list
    const seen = new Set();
    const merged = [];

    [...regularIds, ...celebrityIds].forEach(id => {
      if (!seen.has(id)) {
        seen.add(id);
        merged.push(id);
      }
    });

    return merged;
  }

  /**
   * Enrich post objects with real-time engagement counts from Redis
   */
  async _enrichWithCounts(posts) {
    if (posts.length === 0) return [];

    const pipeline = redis.pipeline();
    posts.forEach(post => {
      pipeline.hgetall(`post:${post.postId}:counts`);
    });

    const countResults = await pipeline.exec();

    return posts.map((post, idx) => {
      const counts = countResults[idx][1] || {};
      return {
        ...post,
        likeCount: parseInt(counts.likes) || 0,
        commentCount: parseInt(counts.comments) || 0,
        repostCount: parseInt(counts.reposts) || 0,
        viewCount: parseInt(counts.views) || 0
      };
    });
  }

  async _populateFeedCache(userId, postIds) {
    if (postIds.length === 0) return;
    const pipeline = redis.pipeline();
    const feedKey = `user:${userId}:feed`;

    postIds.forEach((postId, idx) => {
      // Score by recency (newer = higher index = lower score in desc order)
      const score = postIds.length - idx;
      pipeline.zadd(feedKey, score, postId);
    });

    pipeline.expire(feedKey, 7 * 24 * 3600);
    await pipeline.exec();
  }
}

module.exports = new FeedService();
```

---

### 6.5 Ranking Engine

```javascript
// services/feed-service/src/RankingEngine.js

const redis = require('../lib/redis');

// Decay constant: half-life of ~7 hours for time decay
const LAMBDA = Math.log(2) / 7;

// Post type engagement weights
const POST_TYPE_WEIGHT = {
  video: 1.5,
  image: 1.2,
  text: 1.0,
  repost: 0.8,
  link: 0.9
};

class RankingEngine {
  /**
   * Calculate the initial ranking score for a post at write time.
   * This score is stored in Redis ZSET and used to order the feed.
   *
   * Formula: score = affinity × type_weight × engagement_boost × time_decay
   *
   * At write time we don't have engagement data, so we use time-based score.
   * Engagement-based re-ranking happens at read time.
   */
  calculateInitialScore({ postId, createdAt, hasMedia = false, authorEngagementRate = 1.0 }) {
    const ageHours = (Date.now() - createdAt.getTime()) / 3600000;
    const timeDecay = Math.exp(-LAMBDA * ageHours);
    const typeWeight = hasMedia ? POST_TYPE_WEIGHT.image : POST_TYPE_WEIGHT.text;

    // Score range: 0 to ~10 (ZSET score)
    const score = typeWeight * authorEngagementRate * timeDecay * 10;
    return parseFloat(score.toFixed(6));
  }

  /**
   * Re-rank posts at read time using full feature set.
   * This is called in the feed read path before returning to the client.
   *
   * For a real ML model, this would call a ranking microservice.
   * Here we implement the formula-based version.
   */
  async rankPosts(posts, userId) {
    if (posts.length === 0) return [];

    // Fetch user affinity scores for each post's author
    const authorIds = [...new Set(posts.map(p => p.authorId))];
    const affinityScores = await this._getAffinityScores(userId, authorIds);

    const scoredPosts = posts.map(post => {
      const ageHours = (Date.now() - new Date(post.createdAt).getTime()) / 3600000;
      const timeDecay = Math.exp(-LAMBDA * ageHours);

      const affinity = affinityScores[post.authorId] || 0.1;

      const typeWeight = post.mediaUrls?.length > 0
        ? (post.mediaUrls.some(u => u.includes('.mp4')) ? POST_TYPE_WEIGHT.video : POST_TYPE_WEIGHT.image)
        : POST_TYPE_WEIGHT.text;

      // Engagement boost: log-scaled to prevent viral posts from completely dominating
      const engagementBoost = 1 + Math.log1p(
        (post.likeCount || 0) * 2 +
        (post.commentCount || 0) * 3 +
        (post.repostCount || 0) * 4
      ) / 10;

      const score = affinity * typeWeight * engagementBoost * timeDecay;

      return { ...post, _score: parseFloat(score.toFixed(6)) };
    });

    // Sort by score descending
    return scoredPosts.sort((a, b) => b._score - a._score);
  }

  /**
   * Get affinity scores between a user and a list of authors
   * Affinity = how much the user interacts with each author
   * Stored in Redis as: user:{userId}:affinity:{authorId} → float
   */
  async _getAffinityScores(userId, authorIds) {
    const pipeline = redis.pipeline();
    authorIds.forEach(authorId => {
      pipeline.get(`user:${userId}:affinity:${authorId}`);
    });

    const results = await pipeline.exec();
    const scores = {};

    authorIds.forEach((authorId, idx) => {
      scores[authorId] = parseFloat(results[idx][1]) || 0.5; // Default affinity = 0.5
    });

    return scores;
  }

  /**
   * Update affinity score when user engages with a post
   * Called asynchronously by Kafka consumer when engagement events arrive
   *
   * Uses exponential moving average: new_score = (1-α) × old_score + α × reward
   */
  async updateAffinity(userId, authorId, engagementType) {
    const ALPHA = 0.2; // Learning rate for EMA
    const REWARD = {
      like: 1.0,
      comment: 2.0,
      repost: 3.0,
      view: 0.1,
      click: 0.5
    };

    const reward = REWARD[engagementType] || 0;
    if (reward === 0) return;

    const key = `user:${userId}:affinity:${authorId}`;
    const current = parseFloat(await redis.get(key)) || 0.5;
    const newScore = Math.min(5.0, (1 - ALPHA) * current + ALPHA * reward);

    await redis.setex(key, 30 * 24 * 3600, newScore.toFixed(6)); // 30-day TTL
  }
}

module.exports = new RankingEngine();
```

---

### 6.6 Cache Layer (Redis)

```javascript
// lib/redis.js

const Redis = require('ioredis');

// Redis Cluster configuration for production
const redisCluster = new Redis.Cluster(
  process.env.REDIS_CLUSTER_NODES.split(',').map(node => {
    const [host, port] = node.split(':');
    return { host, port: parseInt(port) };
  }),
  {
    redisOptions: {
      password: process.env.REDIS_PASSWORD,
      tls: process.env.NODE_ENV === 'production' ? {} : undefined,
      connectTimeout: 5000,
      commandTimeout: 3000,
      maxRetriesPerRequest: 3,
    },
    enableReadyCheck: true,
    scaleReads: 'slave',     // Read from replicas to distribute load
    clusterRetryStrategy: (times) => {
      if (times > 3) return null; // Stop retrying after 3 attempts
      return Math.min(times * 100, 3000);
    }
  }
);

redisCluster.on('error', (err) => {
  console.error('Redis Cluster error:', err);
});

redisCluster.on('ready', () => {
  console.log('Redis Cluster connected');
});

/**
 * Feed-specific Redis operations with error handling and fallback
 */
class FeedCache {
  /**
   * Get post IDs from user feed sorted set
   * Returns: [{ postId, score }]
   */
  static async getFeedPosts(userId, { maxScore = '+inf', limit = 20 } = {}) {
    try {
      const results = await redisCluster.zrevrangebyscore(
        `user:${userId}:feed`,
        maxScore,
        '-inf',
        'WITHSCORES',
        'LIMIT', 0, limit
      );

      // Parse results: [postId1, score1, postId2, score2, ...]
      const posts = [];
      for (let i = 0; i < results.length; i += 2) {
        posts.push({ postId: results[i], score: parseFloat(results[i + 1]) });
      }
      return posts;
    } catch (err) {
      console.error('FeedCache.getFeedPosts error:', err);
      return []; // Fallback to empty (DB will be queried)
    }
  }

  /**
   * Add post to feed with score
   */
  static async addToFeed(userId, postId, score) {
    const key = `user:${userId}:feed`;
    const pipeline = redisCluster.pipeline();
    pipeline.zadd(key, score, postId.toString());
    pipeline.zremrangebyrank(key, 0, -501); // Keep top 500 only
    pipeline.expire(key, 604800);           // 7 days
    return pipeline.exec();
  }

  /**
   * Remove a post from all users' feeds (used on post delete)
   * NOTE: This is expensive at scale — use with care.
   * In practice, do lazy deletion (filter at read time).
   */
  static async removePostFromFeeds(postId, userIds) {
    if (userIds.length === 0) return;

    const BATCH_SIZE = 200;
    for (let i = 0; i < userIds.length; i += BATCH_SIZE) {
      const batch = userIds.slice(i, i + BATCH_SIZE);
      const pipeline = redisCluster.pipeline();
      batch.forEach(userId => {
        pipeline.zrem(`user:${userId}:feed`, postId.toString());
      });
      await pipeline.exec();
    }
  }

  /**
   * Increment engagement counter for a post
   */
  static async incrementCounter(postId, field) {
    return redisCluster.hincrby(`post:${postId}:counts`, field, 1);
  }

  /**
   * Decrement engagement counter (for unliking, etc.)
   */
  static async decrementCounter(postId, field) {
    return redisCluster.hincrby(`post:${postId}:counts`, field, -1);
  }
}

module.exports = { redis: redisCluster, FeedCache };
```

---

### 6.7 Notification Service

```javascript
// services/notification-service/src/NotificationService.js

const { Kafka } = require('kafkajs');
const WebSocket = require('ws');
const firebaseAdmin = require('firebase-admin');
const { Pool } = require('pg');
const redis = require('../lib/redis');

const pgPool = new Pool({ connectionString: process.env.POSTGRES_URL });

class NotificationService {
  constructor() {
    // Map: userId → WebSocket connection(s)
    this.wsConnections = new Map();

    // Initialize Firebase Admin for push notifications
    firebaseAdmin.initializeApp({
      credential: firebaseAdmin.credential.cert(JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT))
    });

    this.kafka = new Kafka({
      clientId: 'notification-service',
      brokers: process.env.KAFKA_BROKERS.split(',')
    });

    this.consumer = this.kafka.consumer({ groupId: 'notification-group' });
  }

  /**
   * Register a WebSocket connection for a user (real-time notifications)
   */
  registerConnection(userId, ws) {
    if (!this.wsConnections.has(userId)) {
      this.wsConnections.set(userId, new Set());
    }
    this.wsConnections.get(userId).add(ws);

    ws.on('close', () => {
      const connections = this.wsConnections.get(userId);
      if (connections) {
        connections.delete(ws);
        if (connections.size === 0) {
          this.wsConnections.delete(userId);
        }
      }
    });
  }

  /**
   * Send real-time notification to user (if connected via WebSocket)
   * Fallback to push notification if not connected
   */
  async notify(recipientId, notification) {
    // 1. Persist notification to DB
    const { rows: [saved] } = await pgPool.query(
      `INSERT INTO notifications (recipient_id, type, actor_id, entity_id, created_at)
       VALUES ($1, $2, $3, $4, NOW())
       RETURNING notification_id`,
      [recipientId, notification.type, notification.actorId, notification.entityId]
    );

    const fullNotification = { ...notification, notificationId: saved.notification_id };

    // 2. Try real-time WebSocket delivery
    const wsConnections = this.wsConnections.get(recipientId);
    if (wsConnections && wsConnections.size > 0) {
      const payload = JSON.stringify({ type: 'NOTIFICATION', data: fullNotification });
      wsConnections.forEach(ws => {
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(payload);
        }
      });
      return; // Successfully delivered in real-time
    }

    // 3. User not connected — send push notification
    await this._sendPushNotification(recipientId, notification);
  }

  /**
   * Kafka consumer for notification events
   */
  async start() {
    await this.consumer.connect();
    await this.consumer.subscribe({
      topics: ['post.engaged', 'user.followed'],
      fromBeginning: false
    });

    await this.consumer.run({
      eachMessage: async ({ topic, message }) => {
        const event = JSON.parse(message.value.toString());

        if (topic === 'post.engaged') {
          await this._handleEngagementEvent(event);
        } else if (topic === 'user.followed') {
          await this._handleFollowEvent(event);
        }
      }
    });
  }

  async _handleEngagementEvent({ postId, actorId, authorId, type }) {
    // Don't notify if user engages with their own post
    if (actorId === authorId) return;

    const notifTypes = {
      like: 'LIKE',
      comment: 'COMMENT',
      repost: 'REPOST'
    };

    await this.notify(authorId, {
      type: notifTypes[type] || 'ENGAGEMENT',
      actorId,
      entityId: postId
    });
  }

  async _handleFollowEvent({ followerId, followeeId }) {
    await this.notify(followeeId, {
      type: 'FOLLOW',
      actorId: followerId,
      entityId: followerId
    });
  }

  async _sendPushNotification(userId, notification) {
    const fcmToken = await redis.get(`user:${userId}:fcm_token`);
    if (!fcmToken) return;

    const messages = {
      LIKE: 'liked your post',
      COMMENT: 'commented on your post',
      FOLLOW: 'started following you',
      REPOST: 'reposted your post'
    };

    try {
      await firebaseAdmin.messaging().send({
        token: fcmToken,
        notification: {
          title: 'New notification',
          body: messages[notification.type] || 'New activity'
        },
        data: {
          type: notification.type,
          entityId: notification.entityId?.toString() || '',
          actorId: notification.actorId?.toString() || ''
        }
      });
    } catch (err) {
      console.error('Push notification failed:', err);
    }
  }
}

module.exports = new NotificationService();
```

---

### 6.8 Rate Limiter

```javascript
// middleware/rateLimiter.js

const redis = require('../lib/redis');

/**
 * Token Bucket Rate Limiter implemented in Redis using Lua script
 * for atomic operations (no race conditions).
 *
 * Algorithm: Sliding Window Log (accurate, works across multiple instances)
 */

// Lua script for atomic sliding window rate limiting
const RATE_LIMIT_SCRIPT = `
  local key = KEYS[1]
  local now = tonumber(ARGV[1])
  local window = tonumber(ARGV[2])
  local limit = tonumber(ARGV[3])
  local request_id = ARGV[4]

  -- Remove expired entries (outside the window)
  redis.call('ZREMRANGEBYSCORE', key, '-inf', now - window)

  -- Count current requests in window
  local count = redis.call('ZCARD', key)

  if count >= limit then
    return {0, count, limit}  -- Denied
  end

  -- Add this request with current timestamp as score
  redis.call('ZADD', key, now, request_id)
  redis.call('EXPIRE', key, math.ceil(window / 1000))

  return {1, count + 1, limit}  -- Allowed
`;

const RATE_LIMITS = {
  'POST:/posts':         { limit: 30,   windowMs: 3600000 }, // 30/hour
  'GET:/feed':           { limit: 300,  windowMs: 60000   }, // 300/min
  'POST:/likes':         { limit: 100,  windowMs: 60000   }, // 100/min
  'POST:/follows':       { limit: 20,   windowMs: 3600000 }, // 20/hour
  'POST:/comments':      { limit: 50,   windowMs: 3600000 }, // 50/hour
  DEFAULT:               { limit: 1000, windowMs: 60000   }
};

/**
 * Express middleware for rate limiting
 */
function rateLimiter(options = {}) {
  return async (req, res, next) => {
    const userId = req.user?.userId || req.ip;
    const endpoint = `${req.method}:${req.route?.path || req.path}`;
    const config = RATE_LIMITS[endpoint] || RATE_LIMITS.DEFAULT;

    const key = `ratelimit:${userId}:${endpoint}`;
    const now = Date.now();
    const requestId = `${now}-${Math.random()}`;

    try {
      const [allowed, current, limit] = await redis.eval(
        RATE_LIMIT_SCRIPT,
        1,
        key,
        now,
        config.windowMs,
        config.limit,
        requestId
      );

      // Set rate limit headers (standard X-RateLimit-*)
      res.set({
        'X-RateLimit-Limit': limit,
        'X-RateLimit-Remaining': Math.max(0, limit - current),
        'X-RateLimit-Reset': Math.ceil((now + config.windowMs) / 1000)
      });

      if (!allowed) {
        return res.status(429).json({
          error: 'Too Many Requests',
          message: `Rate limit exceeded. Try again in ${Math.ceil(config.windowMs / 1000)} seconds.`,
          retryAfter: Math.ceil(config.windowMs / 1000)
        });
      }

      next();
    } catch (err) {
      console.error('Rate limiter error:', err);
      // Fail open: allow request if rate limiter is down (availability > strict limiting)
      next();
    }
  };
}

module.exports = { rateLimiter };
```

---

### 6.9 API Gateway / BFF

```javascript
// gateway/src/server.js

const express = require('express');
const helmet = require('helmet');
const compression = require('compression');
const jwt = require('jsonwebtoken');
const { createProxyMiddleware } = require('http-proxy-middleware');
const { rateLimiter } = require('./middleware/rateLimiter');

const app = express();

// Security headers
app.use(helmet());
app.use(compression());
app.use(express.json({ limit: '1mb' }));

// ─── JWT Authentication Middleware ──────────────────────────────────────────
function authenticate(options = { required: true }) {
  return (req, res, next) => {
    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      if (options.required) {
        return res.status(401).json({ error: 'Authentication required' });
      }
      return next();
    }

    try {
      const token = authHeader.split(' ')[1];
      const decoded = jwt.verify(token, process.env.JWT_SECRET);
      req.user = decoded;    // { userId, username, roles }
      next();
    } catch (err) {
      if (err.name === 'TokenExpiredError') {
        return res.status(401).json({ error: 'Token expired', code: 'TOKEN_EXPIRED' });
      }
      return res.status(401).json({ error: 'Invalid token' });
    }
  };
}

// ─── Request ID & Tracing ────────────────────────────────────────────────────
app.use((req, res, next) => {
  req.requestId = `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
  res.set('X-Request-Id', req.requestId);
  next();
});

// ─── Routes ──────────────────────────────────────────────────────────────────

// Public routes (no auth required)
app.use('/api/v1/auth', createProxyMiddleware({
  target: process.env.USER_SERVICE_URL,
  changeOrigin: true,
  pathRewrite: { '^/api/v1/auth': '/auth' }
}));

// Feed routes (auth required)
app.use('/api/v1/feed',
  authenticate(),
  rateLimiter(),
  createProxyMiddleware({
    target: process.env.FEED_SERVICE_URL,
    changeOrigin: true,
    pathRewrite: { '^/api/v1/feed': '/feed' },
    on: {
      proxyReq: (proxyReq, req) => {
        // Forward user context to downstream services
        proxyReq.setHeader('X-User-Id', req.user.userId);
        proxyReq.setHeader('X-Request-Id', req.requestId);
      }
    }
  })
);

// Post routes (auth required)
app.use('/api/v1/posts',
  authenticate(),
  rateLimiter(),
  createProxyMiddleware({
    target: process.env.POST_SERVICE_URL,
    changeOrigin: true,
    pathRewrite: { '^/api/v1/posts': '/posts' },
    on: {
      proxyReq: (proxyReq, req) => {
        proxyReq.setHeader('X-User-Id', req.user.userId);
        proxyReq.setHeader('X-Request-Id', req.requestId);
      }
    }
  })
);

// User/follow routes (auth required)
app.use('/api/v1/users',
  authenticate({ required: false }),  // Some user endpoints are public (profiles)
  rateLimiter(),
  createProxyMiddleware({
    target: process.env.USER_SERVICE_URL,
    changeOrigin: true,
    pathRewrite: { '^/api/v1/users': '/users' }
  })
);

// ─── Health Check ────────────────────────────────────────────────────────────
app.get('/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// ─── Error Handler ───────────────────────────────────────────────────────────
app.use((err, req, res, next) => {
  console.error('Gateway error:', { requestId: req.requestId, error: err.message });
  res.status(500).json({ error: 'Internal server error', requestId: req.requestId });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`API Gateway running on port ${PORT}`));
```

---

### 6.10 Media Upload Service

```javascript
// services/media-service/src/MediaService.js

const AWS = require('aws-sdk');
const { v4: uuidv4 } = require('uuid');
const sharp = require('sharp');  // Image processing
const redis = require('../lib/redis');

const s3 = new AWS.S3({
  region: process.env.AWS_REGION,
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
});

const ALLOWED_IMAGE_TYPES = ['image/jpeg', 'image/png', 'image/webp', 'image/gif'];
const ALLOWED_VIDEO_TYPES = ['video/mp4', 'video/mov', 'video/avi'];
const MAX_IMAGE_SIZE = 10 * 1024 * 1024;  // 10 MB
const MAX_VIDEO_SIZE = 200 * 1024 * 1024; // 200 MB

const IMAGE_SIZES = [
  { suffix: 'thumb', width: 150, height: 150 },
  { suffix: 'medium', width: 600, height: 600 },
  { suffix: 'large', width: 1200, height: 1200 }
];

class MediaService {
  /**
   * Generate a pre-signed S3 URL for direct client upload.
   * This avoids routing the upload through our servers.
   */
  async generateUploadUrl(userId, { contentType, fileSize }) {
    // Validate
    const isImage = ALLOWED_IMAGE_TYPES.includes(contentType);
    const isVideo = ALLOWED_VIDEO_TYPES.includes(contentType);

    if (!isImage && !isVideo) {
      throw new Error(`Unsupported media type: ${contentType}`);
    }

    const maxSize = isImage ? MAX_IMAGE_SIZE : MAX_VIDEO_SIZE;
    if (fileSize > maxSize) {
      throw new Error(`File size exceeds limit of ${maxSize / 1024 / 1024}MB`);
    }

    // Generate unique media ID
    const mediaId = uuidv4();
    const extension = contentType.split('/')[1];
    const s3Key = `uploads/${userId}/${mediaId}/original.${extension}`;

    // Generate pre-signed URL (valid for 15 minutes)
    const uploadUrl = s3.getSignedUrl('putObject', {
      Bucket: process.env.S3_BUCKET,
      Key: s3Key,
      ContentType: contentType,
      ContentLength: fileSize,
      Expires: 900,  // 15 minutes
      Conditions: [
        ['content-length-range', 0, maxSize]
      ]
    });

    // Store pending upload metadata in Redis for verification
    await redis.setex(
      `media:pending:${mediaId}`,
      1800,  // 30 min TTL
      JSON.stringify({ userId, s3Key, contentType, mediaId })
    );

    return {
      uploadUrl,
      mediaId,
      s3Key,
      cdnUrl: `${process.env.CDN_BASE_URL}/${s3Key}`
    };
  }

  /**
   * Called after S3 upload completes (triggered by S3 event via Lambda or webhook).
   * Processes the image: resize, optimize, generate thumbnails.
   */
  async processUploadedImage(s3Key, mediaId) {
    // Download original from S3
    const { Body: imageBuffer } = await s3.getObject({
      Bucket: process.env.S3_BUCKET,
      Key: s3Key
    }).promise();

    // Generate multiple sizes using Sharp
    const uploadPromises = IMAGE_SIZES.map(async ({ suffix, width, height }) => {
      const processedBuffer = await sharp(imageBuffer)
        .resize(width, height, {
          fit: 'inside',          // Maintain aspect ratio
          withoutEnlargement: true  // Don't upscale small images
        })
        .webp({ quality: 85 })   // Convert to WebP for smaller size
        .toBuffer();

      const resizedKey = s3Key.replace('original', suffix).replace(/\.\w+$/, '.webp');

      await s3.putObject({
        Bucket: process.env.S3_BUCKET,
        Key: resizedKey,
        Body: processedBuffer,
        ContentType: 'image/webp',
        CacheControl: 'max-age=31536000',  // 1 year (immutable)
        Metadata: { mediaId }
      }).promise();

      return {
        suffix,
        key: resizedKey,
        url: `${process.env.CDN_BASE_URL}/${resizedKey}`
      };
    });

    const processedImages = await Promise.all(uploadPromises);

    // Mark upload as processed
    await redis.setex(
      `media:processed:${mediaId}`,
      86400,  // 24 hours
      JSON.stringify({ mediaId, variants: processedImages })
    );

    return processedImages;
  }

  /**
   * Delete media from S3 (called when post is deleted)
   */
  async deleteMedia(s3Keys) {
    if (!s3Keys || s3Keys.length === 0) return;

    const objects = s3Keys.map(Key => ({ Key }));

    await s3.deleteObjects({
      Bucket: process.env.S3_BUCKET,
      Delete: { Objects: objects }
    }).promise();
  }
}

module.exports = new MediaService();
```

---

## 7. API Contracts

### Feed Endpoints

```
GET /api/v1/feed
  Auth: Bearer token (required)
  Query params:
    cursor  : string (optional, for pagination)
    limit   : int    (default: 20, max: 50)
  
  Response 200:
  {
    "posts": [
      {
        "postId": "1234567890",
        "author": {
          "userId": "987",
          "username": "johndoe",
          "displayName": "John Doe",
          "avatarUrl": "https://cdn.example.com/...",
          "isVerified": false
        },
        "content": "Hello world! #firstpost",
        "mediaUrls": ["https://cdn.example.com/..."],
        "hashtags": ["#firstpost"],
        "likeCount": 42,
        "commentCount": 7,
        "repostCount": 3,
        "hasLiked": true,
        "createdAt": "2024-01-15T10:30:00Z"
      }
    ],
    "nextCursor": "eyJtaW5TY29yZSI6MS41LCJsYXN0UG9zdElkIjoiMTIzIn0=",
    "hasMore": true
  }

POST /api/v1/posts
  Auth: Bearer token (required)
  Body:
  {
    "content": "Post text (max 2000 chars)",
    "mediaUrls": ["https://cdn.example.com/..."],
    "hashtags": ["#optional"]
  }
  
  Response 201:
  {
    "postId": "1234567890",
    "createdAt": "2024-01-15T10:30:00Z"
  }

DELETE /api/v1/posts/:postId
  Auth: Bearer token (required, must be post owner)
  Response 200: { "success": true }

POST /api/v1/posts/:postId/like
  Auth: Bearer token (required)
  Response 200: { "likeCount": 43 }

DELETE /api/v1/posts/:postId/like
  Auth: Bearer token (required)
  Response 200: { "likeCount": 42 }

POST /api/v1/users/:userId/follow
  Auth: Bearer token (required)
  Response 200: { "success": true }

DELETE /api/v1/users/:userId/follow
  Auth: Bearer token (required)
  Response 200: { "success": true }

GET /api/v1/media/upload-url
  Auth: Bearer token (required)
  Query: contentType=image/jpeg&fileSize=1048576
  Response 200:
  {
    "uploadUrl": "https://s3.amazonaws.com/...",
    "mediaId": "uuid-v4",
    "cdnUrl": "https://cdn.example.com/..."
  }
```

---

## 8. Trade-offs & Design Decisions

| Decision | Chosen | Alternative | Reason |
|----------|--------|-------------|--------|
| Feed generation | Hybrid (push+pull) | Pure push or pure pull | Balances write amplification vs read latency for all user types |
| Primary DB for posts | Cassandra | PostgreSQL, DynamoDB | Write-heavy, time-series nature of posts; partition by user_id |
| Primary DB for users | PostgreSQL | MongoDB | Relational data (users, follows) benefits from ACID transactions |
| Cache | Redis Cluster | Memcached | Supports Sorted Sets (critical for feed ordering), Hashes, TTL |
| Message broker | Kafka | RabbitMQ, SQS | Log retention for replay, high throughput, partitioned ordering |
| Post ID generation | Snowflake | UUID v4, auto-increment | Time-sortable, globally unique, no coordination needed |
| Search | Elasticsearch | Solr, Algolia | Full-text search with scoring, hashtag aggregations |
| Media storage | S3 + CloudFront | Self-hosted | Durability, global CDN, zero-ops |
| Consistency model | Eventual for feed | Strong consistency | Feed staleness by seconds is acceptable; availability > consistency |
| Fan-out threshold | 10K followers | 1K, 100K | Balance: most influencers have ~10K; tested at Twitter/Instagram |

---

## 9. Bottlenecks & Solutions

### Bottleneck 1: Celebrity Post Fan-out

```
Problem: User with 50M followers posts → 50M Redis writes → fan-out takes minutes
Solution: Hybrid model — celebrities are NOT fanned out; pulled at read time
          Celebrity feed is merged inline when user requests feed
          Celebrity threshold: is_celebrity = true when follower_count > 10,000
```

### Bottleneck 2: Hot User Feed Cache Miss (Cold Start)

```
Problem: User hasn't visited in 7 days → Redis feed expired → expensive DB rebuild
Solution:
  1. Increase TTL for active users (refresh TTL on every access)
  2. Pre-warm feeds for users likely to return (ML-predicted active users)
  3. Lazy rebuild: serve from DB on miss, populate Redis async
  4. For new users: show trending posts until enough follows to build a real feed
```

### Bottleneck 3: Like/Comment Counter Thundering Herd

```
Problem: Viral post gets 1M likes in 10 minutes → 1M writes/min to DB
Solution:
  1. Buffer in Redis (HINCRBY is atomic, O(1))
  2. Batch sync to Cassandra every 30 seconds via scheduled job
  3. Read counters always from Redis (eventually consistent with DB)
  4. Counter schema: post:{post_id}:counts → { likes: N, comments: N, reposts: N }
```

### Bottleneck 4: Feed Read at Scale (575K reads/sec)

```
Problem: 575K feed reads/sec — can't hit DB on every request
Solution:
  1. Redis Cluster: 16 shards, each handling ~36K reads/sec (well within Redis limits)
  2. Read replicas in Redis Cluster (scaleReads: 'slave')
  3. Application-level cache: in-memory LRU for hottest users' feeds (celebrity profiles)
  4. CDN caching for public/anonymous feeds (trending, explore)
```

### Bottleneck 5: Follow Graph Traversal

```
Problem: User follows 2000 people → N+1 queries to get their recent posts
Solution:
  1. Store follower/following lists in Redis (SET or LIST)
  2. For cold-start feed builds: batch Cassandra IN queries (not N individual queries)
  3. For large follow lists (>1000): cap fan-in at 50 most interacted-with accounts
     and surface the rest via separate "More from people you follow" section
```

---

## 10. Monitoring & Observability

### Key Metrics (Prometheus + Grafana)

```
Infrastructure:
  - redis_connected_clients           (alert if > 80% max connections)
  - cassandra_write_latency_p99       (alert if > 100ms)
  - kafka_consumer_lag                (alert if > 10,000 messages per partition)
  - node_memory_usage_percent         (alert if > 85%)

Application (custom metrics):
  - feed_read_latency_p50/p95/p99     (alert p99 > 200ms)
  - post_write_latency_p99            (alert > 500ms)
  - fan_out_duration_seconds          (track per-post fan-out time)
  - cache_hit_rate_feed               (alert if < 80%)
  - posts_per_second                  (track write QPS)
  - notification_delivery_success_rate

Business metrics:
  - daily_active_users
  - feed_scroll_depth (avg posts seen per session)
  - post_creation_rate
  - engagement_rate (likes+comments+reposts / impressions)
```

### Distributed Tracing (Jaeger / OpenTelemetry)

```javascript
// Every service injects trace context
const { trace, context, propagation } = require('@opentelemetry/api');

app.use((req, res, next) => {
  const tracer = trace.getTracer('feed-service');
  const span = tracer.startSpan('http.request', {
    attributes: {
      'http.method': req.method,
      'http.url': req.url,
      'user.id': req.user?.userId
    }
  });

  req.span = span;
  res.on('finish', () => {
    span.setAttribute('http.status_code', res.statusCode);
    span.end();
  });

  next();
});
```

### Alerting (PagerDuty)

| Alert | Threshold | Severity |
|-------|-----------|----------|
| Feed p99 latency > 500ms | 5 min sustained | P1 |
| Cassandra write failure rate > 1% | 2 min | P1 |
| Kafka consumer lag > 100K | 10 min | P2 |
| Redis memory > 90% | 5 min | P1 |
| Post creation errors > 0.1% | 2 min | P2 |
| Fan-out worker down | 1 min | P1 |

---

## Summary

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    SYSTEM DESIGN SUMMARY                                 │
│                                                                         │
│  Scale: 500M DAU | 50M posts/day | 575K feed reads/sec                  │
│                                                                         │
│  Feed Strategy:  Hybrid (fan-out on write for regulars,                 │
│                          fan-out on read for celebrities > 10K)         │
│                                                                         │
│  Storage:        Cassandra (posts, timelines) — write-heavy NoSQL      │
│                  PostgreSQL (users, follows) — relational, ACID         │
│                  Redis Cluster (feed cache, counters, sessions)          │
│                  S3 + CloudFront (media)                                │
│                  Elasticsearch (search)                                  │
│                                                                         │
│  Async Pipeline: Kafka → Fan-out Worker → Redis feed ZSET               │
│                  Kafka → Notification Worker → WebSocket / FCM          │
│                  Kafka → Search Indexer → Elasticsearch                 │
│                                                                         │
│  Ranking:        EdgeRank-style: affinity × type_weight × time_decay   │
│                  × engagement_boost, updated per engagement event       │
│                                                                         │
│  Key Trade-offs:                                                        │
│    • Eventual consistency on feed (ok) vs strong (too expensive)        │
│    • Pre-computed feeds (fast reads) vs real-time (less storage)        │
│    • Celebrity fan-out on read (avoids 50M writes per post)             │
└─────────────────────────────────────────────────────────────────────────┘
```

---

*This document covers FAANG-level system design for a Social Media Feed.*
*All LLD code is in JavaScript (Node.js) and represents production-grade patterns.*