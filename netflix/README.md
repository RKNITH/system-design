# 🎬 Netflix System Design — FAANG Interview Level

> **Complete System Design Document covering High-Level Design (HLD) and Low-Level Design (LLD)**
> LLD implementations are in **JavaScript (Node.js)**

---

## 📋 Table of Contents

1. [Problem Statement & Requirements](#1-problem-statement--requirements)
2. [Capacity Estimation & Scale](#2-capacity-estimation--scale)
3. [High-Level Design (HLD)](#3-high-level-design-hld)
   - [Core Architecture](#31-core-architecture)
   - [Microservices Breakdown](#32-microservices-breakdown)
   - [Data Flow — Video Upload](#33-data-flow--video-upload)
   - [Data Flow — Video Streaming](#34-data-flow--video-streaming)
   - [CDN & Edge Architecture](#35-cdn--edge-architecture)
4. [Database Design](#4-database-design)
   - [SQL Schemas](#41-sql-schemas)
   - [NoSQL Schemas](#42-nosql-schemas)
5. [Low-Level Design (LLD)](#5-low-level-design-lld)
   - [Video Upload Service](#51-video-upload-service)
   - [Video Processing & Transcoding Pipeline](#52-video-processing--transcoding-pipeline)
   - [Streaming Service](#53-streaming-service)
   - [Recommendation Engine](#54-recommendation-engine)
   - [User Service & Auth](#55-user-service--auth)
   - [Search Service](#56-search-service)
   - [Notification Service](#57-notification-service)
   - [Billing Service](#58-billing-service)
   - [Analytics & Telemetry](#59-analytics--telemetry)
6. [Caching Strategy](#6-caching-strategy)
7. [Message Queue & Event Streaming](#7-message-queue--event-streaming)
8. [Fault Tolerance & Resilience Patterns](#8-fault-tolerance--resilience-patterns)
9. [Security Design](#9-security-design)
10. [Monitoring & Observability](#10-monitoring--observability)
11. [API Design](#11-api-design)
12. [Trade-offs & Interview Discussion Points](#12-trade-offs--interview-discussion-points)

---

## 1. Problem Statement & Requirements

### Functional Requirements
- Users can **upload** movies/shows (content creators / Netflix internal)
- Users can **stream** video content at various qualities (4K, 1080p, 720p, 480p, 360p)
- Users can **search** for content by title, genre, actor, director
- System provides **personalized recommendations**
- Users can **resume** watching from where they left off
- Users can **download** content for offline viewing
- Multiple **user profiles** per account (up to 5)
- **Subtitles / Audio tracks** in multiple languages
- Support **10+ concurrent streams** per account based on plan
- **Content DRM** (Digital Rights Management) protection

### Non-Functional Requirements
- **High Availability**: 99.99% uptime (< 53 minutes downtime/year)
- **Low Latency**: Video should start within **< 2 seconds** (Time-to-First-Frame)
- **High Throughput**: Serve **250 million+** concurrent users
- **Scalability**: Handle **10 PB/day** of video data served
- **Durability**: Video content must never be lost
- **Consistency**: Watch history, profiles — eventual consistency acceptable
- **Global Distribution**: 190+ countries, edge within 50ms of 95% of users

### Out of Scope (for this design)
- Live streaming (Netflix Live)
- Social features (friend activity)
- Creator portal UI

---

## 2. Capacity Estimation & Scale

```
Users:
  Total registered users         : 300 million
  Daily Active Users (DAU)       : 150 million
  Peak concurrent streamers      : 50 million

Video:
  Avg video size (before encode) : 500 GB per movie (RAW 4K)
  After adaptive encoding        : ~30 GB per title (all bitrates)
  Total catalog                  : ~15,000 titles
  Total storage (catalog)        : 15,000 × 30 GB = 450 TB
  New content ingested/day       : ~50 titles × 500 GB = 25 TB/day

Bandwidth:
  Avg bitrate per stream         : 5 Mbps (HD)
  Peak concurrent streams        : 50M
  Peak egress bandwidth          : 50M × 5 Mbps = 250 Tbps
  Daily data served              : 150M users × 2 hrs × 5 Mbps = ~675 PB/day

Storage:
  Video storage (raw + encoded)  : ~10 PB
  User data (profiles, history)  : ~200 TB
  Logs & telemetry               : ~5 PB/month

Requests per second:
  Video playback requests        : ~5 million RPS (peak)
  Search requests                : ~500K RPS
  Auth/profile requests          : ~2 million RPS
  Recommendation fetches         : ~1 million RPS
```

---

## 3. High-Level Design (HLD)

### 3.1 Core Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                            CLIENT LAYER                                   │
│   [Smart TV]  [iOS/Android]  [Web Browser]  [Game Console]  [Chromecast] │
└──────────────────────────┬───────────────────────────────────────────────┘
                           │ HTTPS / HLS / DASH
┌──────────────────────────▼───────────────────────────────────────────────┐
│                         API GATEWAY LAYER                                 │
│   ┌──────────────┐  ┌──────────────┐  ┌──────────────────────────────┐  │
│   │  Global LB   │  │  Rate Limiter│  │  Auth (JWT Validation)       │  │
│   │  (Anycast)   │  │  (per user)  │  │  DRM Token Validation        │  │
│   └──────┬───────┘  └──────────────┘  └──────────────────────────────┘  │
└──────────┼───────────────────────────────────────────────────────────────┘
           │ Routes to microservices via Service Mesh (Envoy/Istio)
┌──────────▼───────────────────────────────────────────────────────────────┐
│                        MICROSERVICES LAYER                                │
│                                                                           │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────────┐  │
│  │  User    │ │ Catalog  │ │ Stream   │ │  Search  │ │  Recommend   │  │
│  │ Service  │ │ Service  │ │ Service  │ │ Service  │ │  Service     │  │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘ └──────────────┘  │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────────┐  │
│  │  Upload  │ │Transcode │ │ Billing  │ │ Notif.   │ │  Analytics   │  │
│  │ Service  │ │ Service  │ │ Service  │ │ Service  │ │  Service     │  │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘ └──────────────┘  │
└──────────────────────────────────────────────────────────────────────────┘
           │
┌──────────▼───────────────────────────────────────────────────────────────┐
│                         DATA LAYER                                        │
│                                                                           │
│  ┌────────────────┐  ┌─────────────────┐  ┌──────────────────────────┐  │
│  │  PostgreSQL    │  │  Apache Cassandra│  │  Redis Cluster           │  │
│  │  (Users,       │  │  (Watch History, │  │  (Sessions, Catalog      │  │
│  │   Billing)     │  │   User Profiles) │  │   Cache, Rate Limiting)  │  │
│  └────────────────┘  └─────────────────┘  └──────────────────────────┘  │
│  ┌────────────────┐  ┌─────────────────┐  ┌──────────────────────────┐  │
│  │  Elasticsearch │  │   Amazon S3 /   │  │  Apache Kafka            │  │
│  │  (Search,      │  │   Object Store  │  │  (Event Streaming)       │  │
│  │   Catalog idx) │  │   (Video Files) │  └──────────────────────────┘  │
│  └────────────────┘  └─────────────────┘                                 │
└──────────────────────────────────────────────────────────────────────────┘
           │
┌──────────▼───────────────────────────────────────────────────────────────┐
│                      CDN / EDGE LAYER                                     │
│  [Open Connect Appliances — Netflix's own CDN]                            │
│  [Edge PoPs: 1000+ globally, co-located with ISPs]                        │
│  [Proactive content pre-positioning based on popularity]                  │
└──────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Microservices Breakdown

| Service | Responsibility | Tech Stack | DB |
|---|---|---|---|
| **User Service** | Auth, profiles, preferences | Node.js | PostgreSQL + Redis |
| **Catalog Service** | Metadata, genres, cast | Node.js | PostgreSQL + Elasticsearch |
| **Stream Service** | Manifest gen, DRM tokens, resume | Node.js | Cassandra + Redis |
| **Upload Service** | Chunked upload, S3 management | Node.js | S3 + DynamoDB |
| **Transcode Service** | FFmpeg pipeline, ABR encoding | Python/Go | S3 + SQS |
| **Recommendation** | ML ranking, personalization | Python | Cassandra + Redis |
| **Search Service** | Full-text, fuzzy, faceted search | Node.js | Elasticsearch |
| **Billing Service** | Subscriptions, payments | Node.js | PostgreSQL |
| **Notification Service** | Email, push, in-app alerts | Node.js | MongoDB |
| **Analytics Service** | Telemetry, playback events | Node.js | Kafka → ClickHouse |

### 3.3 Data Flow — Video Upload

```
Content Creator / Internal Team
         │
         ▼
  [Upload Service]
  ① Validate content metadata
  ② Issue pre-signed S3 URL for chunked upload
  ③ Client uploads chunks directly to S3
  ④ Upload Service confirms completion
         │
         ▼ (S3 event triggers)
  [Kafka Topic: video.uploaded]
         │
         ▼
  [Transcode Orchestrator]
  ① Pull raw video from S3
  ② Dispatch parallel jobs per resolution:
     ┌──────┬──────┬──────┬──────┬──────┐
     │ 4K   │1080p │ 720p │ 480p │ 360p │
     └──────┴──────┴──────┴──────┴──────┘
  ③ Each job uses FFmpeg with H.264 / H.265 / AV1
  ④ Generate HLS (.m3u8) & DASH (.mpd) manifests
  ⑤ Apply DRM (Widevine, FairPlay, PlayReady)
  ⑥ Generate thumbnail sprites & preview clips
  ⑦ Upload all encoded segments to S3
         │
         ▼ (Kafka: video.processed)
  [Catalog Service]  → Updates metadata, marks content READY
  [CDN Preposition]  → Pushes popular segments to edge nodes
```

### 3.4 Data Flow — Video Streaming

```
User clicks PLAY
      │
      ▼
[API Gateway]
      │ → Auth check (JWT + DRM entitlement)
      │ → Rate limiting check
      ▼
[Stream Service]
  ① Determine user's device capability & network speed
  ② Fetch available qualities for content_id
  ③ Generate DRM license token (Widevine / FairPlay)
  ④ Build manifest URL (HLS or DASH based on device)
  ⑤ Return manifest URL + DRM token to client
      │
      ▼
[Client player — e.g., Netflix SDK]
  ① Fetch .m3u8 / .mpd manifest from CDN
  ② Parse available quality levels
  ③ ABR (Adaptive Bitrate) algorithm selects starting quality
  ④ Download video segments (e.g., 2-second chunks) from CDN
  ⑤ Continuously monitors buffer, switches quality up/down
      │
      ▼ (every 30s)
[Analytics Service]
  Receives playback heartbeat: { userId, contentId, position,
    quality, bufferHealth, bandwidth, errors }
      │
      ▼
[Stream Service — Resume Point]
  Updates Cassandra: { userId, contentId, watchedSeconds }
```

### 3.5 CDN & Edge Architecture

```
Netflix Open Connect Architecture:
─────────────────────────────────
• Netflix operates its OWN CDN called Open Connect
• 1000+ Open Connect Appliances (OCAs) worldwide
• Co-located inside ISP data centers (not Netflix DCs)
• ISPs get free bandwidth; Netflix gets ultra-low latency

Content Placement Strategy:
  ┌─────────────────────────────────────────────────┐
  │  Every night, Netflix runs a "fill" operation:  │
  │  1. Analyze regional popularity of content      │
  │  2. Pre-push top N titles to regional OCAs      │
  │  3. Long-tail titles served from origin (S3)    │
  │  4. Fallback chain: Local OCA → Regional OCA    │
  │     → Netflix Origin (AWS S3)                   │
  └─────────────────────────────────────────────────┘

Client-OCA Selection (Steering Service):
  Client → DNS query for open.netflix.net
         → Netflix Steering Service resolves to
           nearest healthy OCA IP
         → Client fetches manifests + segments from that OCA
```

---

## 4. Database Design

### 4.1 SQL Schemas

```sql
-- ─────────────────────────────────────
-- PostgreSQL: Users & Billing
-- ─────────────────────────────────────

CREATE TABLE users (
    user_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email           VARCHAR(255) UNIQUE NOT NULL,
    password_hash   VARCHAR(255) NOT NULL,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW(),
    country_code    CHAR(2) NOT NULL,
    is_active       BOOLEAN DEFAULT TRUE
);

CREATE INDEX idx_users_email ON users(email);

CREATE TABLE profiles (
    profile_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         UUID REFERENCES users(user_id) ON DELETE CASCADE,
    name            VARCHAR(50) NOT NULL,
    avatar_url      TEXT,
    is_kids         BOOLEAN DEFAULT FALSE,
    language        CHAR(5) DEFAULT 'en-US',
    maturity_level  SMALLINT DEFAULT 3,       -- 1=Kids, 2=Teen, 3=Adult
    created_at      TIMESTAMP DEFAULT NOW(),
    CONSTRAINT max_profiles CHECK (
        (SELECT COUNT(*) FROM profiles p WHERE p.user_id = profiles.user_id) <= 5
    )
);

CREATE TABLE subscriptions (
    subscription_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         UUID REFERENCES users(user_id) ON DELETE CASCADE,
    plan_id         VARCHAR(50) NOT NULL,     -- 'basic', 'standard', 'premium'
    status          VARCHAR(20) NOT NULL,     -- 'active', 'cancelled', 'past_due'
    started_at      TIMESTAMP NOT NULL,
    expires_at      TIMESTAMP NOT NULL,
    max_streams     SMALLINT NOT NULL,
    max_downloads   SMALLINT NOT NULL,
    price_cents     INT NOT NULL,
    currency        CHAR(3) NOT NULL
);

CREATE TABLE payment_methods (
    payment_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         UUID REFERENCES users(user_id),
    type            VARCHAR(20),              -- 'card', 'paypal', 'upi'
    provider_token  TEXT NOT NULL,            -- stripe/braintree token
    last_four       CHAR(4),
    expiry_month    SMALLINT,
    expiry_year     SMALLINT,
    is_default      BOOLEAN DEFAULT FALSE
);

CREATE TABLE invoices (
    invoice_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         UUID REFERENCES users(user_id),
    amount_cents    INT NOT NULL,
    currency        CHAR(3) NOT NULL,
    status          VARCHAR(20),              -- 'paid', 'failed', 'pending'
    billing_date    DATE NOT NULL,
    pdf_url         TEXT
);

-- ─────────────────────────────────────
-- PostgreSQL: Content Catalog
-- ─────────────────────────────────────

CREATE TABLE content (
    content_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title           VARCHAR(255) NOT NULL,
    type            VARCHAR(20) NOT NULL,     -- 'movie', 'series', 'documentary'
    description     TEXT,
    release_year    SMALLINT,
    duration_mins   INT,                      -- null for series
    rating          VARCHAR(10),              -- 'PG', 'PG-13', 'R', 'TV-MA'
    imdb_rating     DECIMAL(3,1),
    language        CHAR(5),
    country         CHAR(2),
    created_at      TIMESTAMP DEFAULT NOW(),
    is_original     BOOLEAN DEFAULT FALSE,    -- Netflix Original
    status          VARCHAR(20) DEFAULT 'processing'  -- 'processing', 'ready', 'unavailable'
);

CREATE TABLE seasons (
    season_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    content_id      UUID REFERENCES content(content_id),
    season_number   SMALLINT NOT NULL,
    title           VARCHAR(255),
    release_year    SMALLINT
);

CREATE TABLE episodes (
    episode_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    season_id       UUID REFERENCES seasons(season_id),
    episode_number  SMALLINT NOT NULL,
    title           VARCHAR(255) NOT NULL,
    duration_mins   INT NOT NULL,
    description     TEXT
);

CREATE TABLE genres (
    genre_id        SERIAL PRIMARY KEY,
    name            VARCHAR(50) UNIQUE NOT NULL
);

CREATE TABLE content_genres (
    content_id      UUID REFERENCES content(content_id),
    genre_id        INT REFERENCES genres(genre_id),
    PRIMARY KEY(content_id, genre_id)
);

CREATE TABLE persons (
    person_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            VARCHAR(255) NOT NULL,
    bio             TEXT,
    photo_url       TEXT
);

CREATE TABLE content_persons (
    content_id      UUID REFERENCES content(content_id),
    person_id       UUID REFERENCES persons(person_id),
    role            VARCHAR(20),              -- 'director', 'actor', 'writer'
    character_name  VARCHAR(255),
    PRIMARY KEY(content_id, person_id, role)
);

-- Video assets (one content may have many encoded versions)
CREATE TABLE video_assets (
    asset_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    content_id      UUID,                     -- or episode_id for series
    episode_id      UUID,
    resolution      VARCHAR(10),              -- '4K', '1080p', '720p', '480p', '360p'
    codec           VARCHAR(20),              -- 'h264', 'h265', 'av1'
    format          VARCHAR(10),              -- 'hls', 'dash'
    s3_manifest_key TEXT NOT NULL,
    s3_segment_prefix TEXT NOT NULL,
    bitrate_kbps    INT,
    file_size_bytes BIGINT,
    duration_secs   INT,
    drm_type        VARCHAR(20),              -- 'widevine', 'fairplay', 'playready'
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE subtitles (
    subtitle_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    content_id      UUID,
    episode_id      UUID,
    language        CHAR(5) NOT NULL,
    format          VARCHAR(10),              -- 'srt', 'vtt', 'ttml'
    s3_key          TEXT NOT NULL,
    is_sdh          BOOLEAN DEFAULT FALSE     -- for hearing impaired
);
```

### 4.2 NoSQL Schemas

```javascript
// ─────────────────────────────────────
// Apache Cassandra: Watch History
// Optimized for writes and time-series reads
// ─────────────────────────────────────

/*
  Table: watch_history
  Partition Key: profile_id (ensures all history for a user on one node)
  Clustering Key: watched_at DESC (most recent first)
*/

CREATE TABLE watch_history (
    profile_id      UUID,
    content_id      UUID,
    episode_id      UUID,
    watched_at      TIMESTAMP,
    position_secs   INT,          -- resume position
    total_secs      INT,
    is_completed    BOOLEAN,
    device_type     TEXT,
    PRIMARY KEY ((profile_id), watched_at, content_id)
) WITH CLUSTERING ORDER BY (watched_at DESC)
  AND default_time_to_live = 7776000;   -- 90 days TTL

/*
  Table: user_ratings
*/
CREATE TABLE user_ratings (
    profile_id      UUID,
    content_id      UUID,
    rating          TINYINT,      -- thumbs_up=1, thumbs_down=-1
    rated_at        TIMESTAMP,
    PRIMARY KEY ((profile_id), content_id)
);

/*
  Table: my_list  (user's saved content)
*/
CREATE TABLE my_list (
    profile_id      UUID,
    added_at        TIMESTAMP,
    content_id      UUID,
    PRIMARY KEY ((profile_id), added_at, content_id)
) WITH CLUSTERING ORDER BY (added_at DESC);

// ─────────────────────────────────────
// Redis Data Structures
// ─────────────────────────────────────

/*
  Session Store:
  Key:   session:{sessionId}
  Type:  Hash
  TTL:   30 days
*/
session:{uuid} → {
  userId, profileId, deviceId, ip, createdAt, lastActive
}

/*
  Catalog Cache:
  Key:   catalog:content:{contentId}
  Type:  String (JSON)
  TTL:   1 hour
*/

/*
  Trending Content (by region):
  Key:   trending:{countryCode}:{date}
  Type:  Sorted Set (score = view_count)
  TTL:   24 hours
*/
ZADD trending:IN:2024-01-15 980000 "content-uuid-1"
ZADD trending:IN:2024-01-15 750000 "content-uuid-2"

/*
  Rate Limiting:
  Key:   ratelimit:{userId}:{endpoint}:{minute}
  Type:  String (counter)
  TTL:   60 seconds
*/

/*
  Active Stream Tracking (concurrent stream enforcement):
  Key:   active_streams:{userId}
  Type:  Set
  TTL:   5 minutes (heartbeat renewed)
*/
SADD active_streams:{userId} {streamToken}

/*
  Recommendation Cache:
  Key:   reco:{profileId}:{algorithm}
  Type:  String (JSON array of contentIds)
  TTL:   1 hour
*/
```

---

## 5. Low-Level Design (LLD)

### 5.1 Video Upload Service

```javascript
// services/upload/UploadService.js
const { S3Client, CreateMultipartUploadCommand,
        UploadPartCommand, CompleteMultipartUploadCommand,
        AbortMultipartUploadCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');
const { v4: uuidv4 } = require('uuid');
const EventEmitter = require('events');
const kafka = require('../shared/kafka');
const db = require('../shared/db');

const CHUNK_SIZE = 50 * 1024 * 1024; // 50 MB per part (S3 minimum = 5 MB)
const MAX_UPLOAD_SIZE = 500 * 1024 * 1024 * 1024; // 500 GB

class UploadService extends EventEmitter {
  constructor() {
    super();
    this.s3 = new S3Client({
      region: process.env.AWS_REGION,
      credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
      },
    });
    this.bucket = process.env.RAW_VIDEO_BUCKET;
  }

  /**
   * Step 1: Initiate a multipart upload session
   * Returns uploadId + presigned URLs for each part
   */
  async initiateUpload({ contentId, fileName, fileSize, mimeType, uploadedBy }) {
    if (fileSize > MAX_UPLOAD_SIZE) {
      throw new Error('File size exceeds maximum allowed (500 GB)');
    }

    const totalParts = Math.ceil(fileSize / CHUNK_SIZE);
    const s3Key = `raw/${contentId}/${uuidv4()}_${fileName}`;

    // Create multipart upload session in S3
    const createCmd = new CreateMultipartUploadCommand({
      Bucket: this.bucket,
      Key: s3Key,
      ContentType: mimeType,
      Metadata: {
        contentId,
        uploadedBy,
        originalName: fileName,
      },
      ServerSideEncryption: 'AES256',
    });

    const { UploadId } = await this.s3.send(createCmd);

    // Generate presigned URLs for each part (valid 4 hours)
    const partUrls = await Promise.all(
      Array.from({ length: totalParts }, (_, i) =>
        getSignedUrl(
          this.s3,
          new UploadPartCommand({
            Bucket: this.bucket,
            Key: s3Key,
            UploadId,
            PartNumber: i + 1,
          }),
          { expiresIn: 14400 }  // 4 hours
        )
      )
    );

    // Persist upload session to DB
    const uploadSession = await db.uploadSessions.create({
      uploadId: UploadId,
      contentId,
      s3Key,
      totalParts,
      completedParts: [],
      status: 'initiated',
      fileSize,
      uploadedBy,
      expiresAt: new Date(Date.now() + 4 * 60 * 60 * 1000),
    });

    return {
      uploadId: UploadId,
      s3Key,
      totalParts,
      chunkSize: CHUNK_SIZE,
      partUrls,           // Client uploads directly to S3 using these
      sessionId: uploadSession.id,
    };
  }

  /**
   * Step 2: Client notifies us which parts are done
   * We track ETags for final assembly
   */
  async acknowledgeChunk({ uploadId, s3Key, partNumber, eTag }) {
    await db.uploadSessions.updateOne(
      { uploadId },
      {
        $push: { completedParts: { PartNumber: partNumber, ETag: eTag } },
        $set: { updatedAt: new Date() },
      }
    );

    return { acknowledged: true, partNumber };
  }

  /**
   * Step 3: Client signals all parts uploaded — complete the multipart upload
   */
  async completeUpload({ uploadId, s3Key, contentId }) {
    const session = await db.uploadSessions.findOne({ uploadId });
    if (!session) throw new Error('Upload session not found');

    if (session.completedParts.length !== session.totalParts) {
      throw new Error(
        `Missing parts: expected ${session.totalParts}, got ${session.completedParts.length}`
      );
    }

    // Sort parts by PartNumber (required by S3)
    const sortedParts = session.completedParts.sort(
      (a, b) => a.PartNumber - b.PartNumber
    );

    await this.s3.send(
      new CompleteMultipartUploadCommand({
        Bucket: this.bucket,
        Key: s3Key,
        UploadId: uploadId,
        MultipartUpload: { Parts: sortedParts },
      })
    );

    // Update session status
    await db.uploadSessions.updateOne(
      { uploadId },
      { $set: { status: 'completed', completedAt: new Date() } }
    );

    // Publish event to Kafka → triggers transcoding pipeline
    await kafka.publish('video.uploaded', {
      contentId,
      s3Key,
      bucket: this.bucket,
      uploadId,
      timestamp: Date.now(),
    });

    // Update content status in catalog
    await db.content.updateOne(
      { contentId },
      { $set: { status: 'processing', uploadedAt: new Date() } }
    );

    return { status: 'completed', s3Key, message: 'Processing started' };
  }

  /**
   * Abort a failed or stale upload
   */
  async abortUpload({ uploadId, s3Key }) {
    await this.s3.send(
      new AbortMultipartUploadCommand({
        Bucket: this.bucket,
        Key: s3Key,
        UploadId: uploadId,
      })
    );
    await db.uploadSessions.updateOne(
      { uploadId },
      { $set: { status: 'aborted' } }
    );
    return { aborted: true };
  }
}

// ─────────────────────────────────────
// Upload Route Handler
// ─────────────────────────────────────

// routes/upload.js
const express = require('express');
const router = express.Router();
const uploadService = new UploadService();
const { authenticate, authorize } = require('../middleware/auth');

// Only content admins can upload
router.post('/initiate', authenticate, authorize('content:upload'), async (req, res) => {
  try {
    const { contentId, fileName, fileSize, mimeType } = req.body;
    const result = await uploadService.initiateUpload({
      contentId, fileName, fileSize, mimeType,
      uploadedBy: req.user.userId,
    });
    res.json({ success: true, data: result });
  } catch (err) {
    res.status(400).json({ success: false, error: err.message });
  }
});

router.post('/chunk/ack', authenticate, async (req, res) => {
  const result = await uploadService.acknowledgeChunk(req.body);
  res.json(result);
});

router.post('/complete', authenticate, async (req, res) => {
  const result = await uploadService.completeUpload(req.body);
  res.json(result);
});

module.exports = router;
```

---

### 5.2 Video Processing & Transcoding Pipeline

```javascript
// services/transcode/TranscodeOrchestrator.js
const { Worker, Queue } = require('bullmq');
const ffmpeg = require('fluent-ffmpeg');
const { S3Client, GetObjectCommand, PutObjectCommand } = require('@aws-sdk/client-s3');
const kafka = require('../shared/kafka');
const path = require('path');
const fs = require('fs-extra');

// ─── Encoding Profiles (Adaptive Bitrate Ladder) ───
const ENCODING_PROFILES = [
  {
    name: '4K',
    width: 3840, height: 2160,
    videoBitrate: '15000k', audioBitrate: '320k',
    codec: 'libx265',
    profile: 'main10',
    level: '5.1',
    segmentDuration: 4,          // seconds per HLS segment
  },
  {
    name: '1080p',
    width: 1920, height: 1080,
    videoBitrate: '8000k', audioBitrate: '192k',
    codec: 'libx264',
    profile: 'high',
    level: '4.0',
    segmentDuration: 4,
  },
  {
    name: '720p',
    width: 1280, height: 720,
    videoBitrate: '4000k', audioBitrate: '128k',
    codec: 'libx264',
    profile: 'main',
    level: '3.1',
    segmentDuration: 4,
  },
  {
    name: '480p',
    width: 854, height: 480,
    videoBitrate: '1500k', audioBitrate: '96k',
    codec: 'libx264',
    profile: 'main',
    level: '3.0',
    segmentDuration: 4,
  },
  {
    name: '360p',
    width: 640, height: 360,
    videoBitrate: '800k', audioBitrate: '64k',
    codec: 'libx264',
    profile: 'baseline',
    level: '3.0',
    segmentDuration: 4,
  },
];

class TranscodeOrchestrator {
  constructor() {
    this.s3 = new S3Client({ region: process.env.AWS_REGION });
    this.rawBucket = process.env.RAW_VIDEO_BUCKET;
    this.processedBucket = process.env.PROCESSED_VIDEO_BUCKET;
    this.queue = new Queue('transcoding', {
      connection: { host: process.env.REDIS_HOST, port: 6379 },
    });
  }

  /**
   * Listen for video.uploaded events and dispatch transcode jobs
   */
  async startConsuming() {
    await kafka.consume('video.uploaded', async (message) => {
      const { contentId, s3Key, bucket } = message;
      console.log(`Dispatching transcode jobs for contentId=${contentId}`);

      // Dispatch one job per encoding profile (parallel)
      const jobs = ENCODING_PROFILES.map((profile) =>
        this.queue.add(
          'transcode',
          { contentId, s3Key, sourceBucket: bucket, profile },
          {
            attempts: 3,
            backoff: { type: 'exponential', delay: 30000 },
            removeOnComplete: true,
          }
        )
      );

      await Promise.all(jobs);

      // Also dispatch a thumbnail extraction job
      await this.queue.add('thumbnail', { contentId, s3Key, sourceBucket: bucket });
    });
  }

  /**
   * Worker: Process a single transcode job
   */
  startWorker() {
    return new Worker(
      'transcoding',
      async (job) => {
        if (job.name === 'transcode') return this.processTranscodeJob(job);
        if (job.name === 'thumbnail') return this.processThumbnailJob(job);
      },
      {
        connection: { host: process.env.REDIS_HOST, port: 6379 },
        concurrency: 5,   // Process 5 jobs concurrently per worker node
      }
    );
  }

  async processTranscodeJob(job) {
    const { contentId, s3Key, sourceBucket, profile } = job.data;
    const tempDir = `/tmp/transcode/${contentId}/${profile.name}`;
    const inputPath = `${tempDir}/input.mp4`;
    const outputDir = `${tempDir}/output`;

    await fs.ensureDir(outputDir);

    try {
      // 1. Download raw video from S3
      await job.updateProgress(5);
      await this.downloadFromS3(sourceBucket, s3Key, inputPath);

      // 2. Transcode to HLS segments
      await job.updateProgress(20);
      const { duration } = await this.transcodeToHLS({
        inputPath,
        outputDir,
        profile,
        onProgress: (percent) => job.updateProgress(20 + percent * 0.6),
      });

      // 3. Upload segments to S3
      await job.updateProgress(80);
      const s3Prefix = `encoded/${contentId}/${profile.name}`;
      await this.uploadDirectoryToS3(outputDir, this.processedBucket, s3Prefix);

      // 4. Record asset in DB
      await job.updateProgress(95);
      await db.videoAssets.create({
        contentId,
        resolution: profile.name,
        codec: profile.codec === 'libx265' ? 'h265' : 'h264',
        format: 'hls',
        s3ManifestKey: `${s3Prefix}/index.m3u8`,
        s3SegmentPrefix: s3Prefix,
        bitrate_kbps: parseInt(profile.videoBitrate),
        durationSecs: duration,
      });

      await job.updateProgress(100);
      await this.checkAllProfilesDone(contentId);

    } finally {
      await fs.remove(tempDir);    // Always clean up temp files
    }
  }

  async transcodeToHLS({ inputPath, outputDir, profile, onProgress }) {
    return new Promise((resolve, reject) => {
      let duration = 0;

      ffmpeg(inputPath)
        .outputOptions([
          `-vf scale=${profile.width}:${profile.height}`,
          `-c:v ${profile.codec}`,
          `-b:v ${profile.videoBitrate}`,
          `-maxrate ${profile.videoBitrate}`,
          `-bufsize ${parseInt(profile.videoBitrate) * 2}k`,
          `-profile:v ${profile.profile}`,
          `-level ${profile.level}`,
          `-c:a aac`,
          `-b:a ${profile.audioBitrate}`,
          `-ar 48000`,
          `-ac 2`,
          `-f hls`,
          `-hls_time ${profile.segmentDuration}`,
          `-hls_list_size 0`,          // Keep all segments in manifest
          `-hls_segment_type mpegts`,
          `-hls_segment_filename ${outputDir}/seg_%06d.ts`,
          `-hls_flags independent_segments+split_by_time`,
          `-start_number 0`,
        ])
        .output(`${outputDir}/index.m3u8`)
        .on('codecData', (data) => {
          duration = this.parseDuration(data.duration);
        })
        .on('progress', (p) => {
          if (p.percent && onProgress) onProgress(p.percent);
        })
        .on('end', () => resolve({ duration }))
        .on('error', reject)
        .run();
    });
  }

  /**
   * Generate master HLS manifest referencing all quality levels
   * Called after all profile jobs complete
   */
  async generateMasterManifest(contentId) {
    const assets = await db.videoAssets.findAll({ contentId, format: 'hls' });

    // Sort by bitrate descending
    assets.sort((a, b) => b.bitrate_kbps - a.bitrate_kbps);

    let manifest = '#EXTM3U\n#EXT-X-VERSION:3\n\n';

    for (const asset of assets) {
      const bandwidth = asset.bitrate_kbps * 1000;
      const [width, height] = this.resolutionToDimensions(asset.resolution);
      manifest += `#EXT-X-STREAM-INF:BANDWIDTH=${bandwidth},RESOLUTION=${width}x${height},CODECS="avc1.640028,mp4a.40.2"\n`;
      manifest += `https://cdn.netflix.com/${asset.s3ManifestKey}\n\n`;
    }

    const masterKey = `encoded/${contentId}/master.m3u8`;
    await this.s3.send(new PutObjectCommand({
      Bucket: this.processedBucket,
      Key: masterKey,
      Body: manifest,
      ContentType: 'application/vnd.apple.mpegurl',
      CacheControl: 'max-age=86400',
    }));

    return masterKey;
  }

  async checkAllProfilesDone(contentId) {
    const assets = await db.videoAssets.count({ contentId, format: 'hls' });
    if (assets >= ENCODING_PROFILES.length) {
      const masterKey = await this.generateMasterManifest(contentId);
      await db.content.updateOne(
        { contentId },
        { $set: { status: 'ready', masterManifestKey: masterKey } }
      );
      await kafka.publish('video.processed', { contentId, masterKey });
    }
  }

  parseDuration(str) {
    const [h, m, s] = str.split(':').map(parseFloat);
    return h * 3600 + m * 60 + s;
  }

  resolutionToDimensions(name) {
    const map = { '4K': [3840, 2160], '1080p': [1920, 1080],
                  '720p': [1280, 720], '480p': [854, 480], '360p': [640, 360] };
    return map[name] || [1920, 1080];
  }

  async downloadFromS3(bucket, key, localPath) {
    const { Body } = await this.s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
    await fs.outputFile(localPath, await Body.transformToByteArray());
  }

  async uploadDirectoryToS3(dir, bucket, prefix) {
    const files = await fs.readdir(dir);
    await Promise.all(
      files.map((file) =>
        this.s3.send(new PutObjectCommand({
          Bucket: bucket,
          Key: `${prefix}/${file}`,
          Body: fs.createReadStream(path.join(dir, file)),
          ContentType: file.endsWith('.m3u8')
            ? 'application/vnd.apple.mpegurl'
            : 'video/mp2t',
        }))
      )
    );
  }
}

module.exports = { TranscodeOrchestrator, ENCODING_PROFILES };
```

---

### 5.3 Streaming Service

```javascript
// services/stream/StreamService.js
const jwt = require('jsonwebtoken');
const crypto = require('crypto');
const redis = require('../shared/redis');
const db = require('../shared/db');

const MAX_STREAMS_PER_PLAN = { basic: 1, standard: 2, premium: 4 };
const STREAM_HEARTBEAT_TTL = 300;    // 5 minutes
const RESUME_THRESHOLD_SECS = 30;    // Only save position after 30s watched

class StreamService {
  /**
   * Main entry point: client requests playback
   * Returns manifest URL + DRM token
   */
  async initiatePlayback({ userId, profileId, contentId, episodeId, deviceInfo }) {
    // 1. Verify subscription is active
    const subscription = await this.verifySubscription(userId);

    // 2. Enforce concurrent stream limit
    const streamToken = await this.acquireStreamSlot(userId, subscription.plan_id);

    // 3. Get content asset
    const assetKey = await this.resolveAsset(contentId, episodeId);
    if (!assetKey) throw new Error('Content not available');

    // 4. Get resume position
    const resumePosition = await this.getResumePosition(profileId, contentId, episodeId);

    // 5. Generate DRM license token
    const drmToken = this.generateDRMToken({
      userId, contentId, episodeId, deviceInfo, streamToken,
    });

    // 6. Generate CDN-signed manifest URL
    const manifestUrl = this.signManifestUrl(assetKey);

    // 7. Record stream start event
    await this.recordPlaybackEvent({
      userId, profileId, contentId, episodeId,
      eventType: 'PLAY_START',
      deviceInfo,
      streamToken,
    });

    return {
      manifestUrl,
      drmToken,
      streamToken,
      resumePositionSecs: resumePosition,
      heartbeatIntervalSecs: 30,
    };
  }

  /**
   * Concurrent stream enforcement using Redis Set
   */
  async acquireStreamSlot(userId, planId) {
    const maxStreams = MAX_STREAMS_PER_PLAN[planId] || 1;
    const streamSetKey = `active_streams:${userId}`;
    const streamToken = crypto.randomUUID();

    const pipeline = redis.pipeline();
    pipeline.smembers(streamSetKey);
    const [[, currentStreams]] = await pipeline.exec();

    if (currentStreams.length >= maxStreams) {
      throw new Error(
        `Concurrent stream limit reached (${maxStreams} for ${planId} plan)`
      );
    }

    // Add token with expiry (heartbeat must renew this)
    await redis.sadd(streamSetKey, streamToken);
    await redis.expire(streamSetKey, STREAM_HEARTBEAT_TTL);

    return streamToken;
  }

  /**
   * Client must call this every 30s to keep stream alive
   */
  async heartbeat({ userId, profileId, contentId, episodeId,
                    streamToken, positionSecs, qualityName, bufferHealth }) {
    const streamSetKey = `active_streams:${userId}`;

    // Verify stream token is valid
    const isValid = await redis.sismember(streamSetKey, streamToken);
    if (!isValid) throw new Error('Invalid or expired stream token');

    // Renew TTL
    await redis.expire(streamSetKey, STREAM_HEARTBEAT_TTL);

    // Save watch position (debounced — only if > threshold)
    if (positionSecs > RESUME_THRESHOLD_SECS) {
      await this.updateWatchPosition({ profileId, contentId, episodeId, positionSecs });
    }

    // Publish telemetry event to Kafka
    await kafka.publish('playback.heartbeat', {
      userId, profileId, contentId, episodeId,
      positionSecs, qualityName, bufferHealth,
      timestamp: Date.now(),
    });

    return { ok: true };
  }

  /**
   * Release stream slot when user stops watching
   */
  async stopPlayback({ userId, streamToken, profileId, contentId, episodeId, positionSecs }) {
    const streamSetKey = `active_streams:${userId}`;
    await redis.srem(streamSetKey, streamToken);

    // Final position save
    if (positionSecs > RESUME_THRESHOLD_SECS) {
      await this.updateWatchPosition({ profileId, contentId, episodeId, positionSecs });
    }

    await this.recordPlaybackEvent({
      userId, profileId, contentId, episodeId,
      eventType: 'PLAY_STOP', finalPositionSecs: positionSecs,
    });
  }

  /**
   * Save/update resume position in Cassandra
   */
  async updateWatchPosition({ profileId, contentId, episodeId, positionSecs }) {
    const key = `watchpos:${profileId}:${contentId}:${episodeId || 'movie'}`;
    // Use Redis for fast writes, async flush to Cassandra
    await redis.setex(key, 86400, positionSecs);
    // Async write to Cassandra via Kafka
    await kafka.publish('watch.position.updated', {
      profileId, contentId, episodeId, positionSecs, updatedAt: Date.now(),
    });
  }

  async getResumePosition(profileId, contentId, episodeId) {
    const key = `watchpos:${profileId}:${contentId}:${episodeId || 'movie'}`;
    const cached = await redis.get(key);
    if (cached) return parseInt(cached);
    // Fallback to Cassandra
    const row = await cassandra.execute(
      'SELECT position_secs FROM watch_history WHERE profile_id=? AND content_id=? LIMIT 1',
      [profileId, contentId], { prepare: true }
    );
    return row.rows[0]?.position_secs || 0;
  }

  /**
   * Generate a signed DRM token (Widevine-style)
   * Real implementation would call Widevine license server
   */
  generateDRMToken({ userId, contentId, episodeId, deviceInfo, streamToken }) {
    const payload = {
      sub: userId,
      contentId,
      episodeId: episodeId || null,
      deviceFingerprint: this.hashDevice(deviceInfo),
      streamToken,
      allowedDownload: false,
      iat: Math.floor(Date.now() / 1000),
      exp: Math.floor(Date.now() / 1000) + 7200,   // 2 hours
    };
    return jwt.sign(payload, process.env.DRM_SIGNING_KEY, { algorithm: 'RS256' });
  }

  /**
   * Sign CDN URL with HMAC so only our server can issue valid manifest URLs
   */
  signManifestUrl(assetKey) {
    const baseUrl = `https://cdn.netflix.com/${assetKey}`;
    const expires = Math.floor(Date.now() / 1000) + 7200;
    const signature = crypto
      .createHmac('sha256', process.env.CDN_SIGNING_KEY)
      .update(`${baseUrl}:${expires}`)
      .digest('hex');
    return `${baseUrl}?expires=${expires}&sig=${signature}`;
  }

  async verifySubscription(userId) {
    const cacheKey = `subscription:${userId}`;
    const cached = await redis.get(cacheKey);
    if (cached) return JSON.parse(cached);

    const sub = await db.subscriptions.findOne({
      userId, status: 'active',
      expires_at: { $gt: new Date() },
    });
    if (!sub) throw new Error('No active subscription');

    await redis.setex(cacheKey, 3600, JSON.stringify(sub));
    return sub;
  }

  async resolveAsset(contentId, episodeId) {
    const cacheKey = `asset:${contentId}:${episodeId || 'movie'}`;
    const cached = await redis.get(cacheKey);
    if (cached) return cached;

    const asset = await db.videoAssets.findOne({
      contentId, episodeId: episodeId || null,
      format: 'hls', resolution: 'master',
    });
    if (!asset) return null;

    await redis.setex(cacheKey, 3600, asset.s3ManifestKey);
    return asset.s3ManifestKey;
  }

  hashDevice({ userAgent, screenResolution, timezone }) {
    return crypto
      .createHash('sha256')
      .update(`${userAgent}:${screenResolution}:${timezone}`)
      .digest('hex')
      .substring(0, 16);
  }

  async recordPlaybackEvent(event) {
    await kafka.publish('playback.events', { ...event, timestamp: Date.now() });
  }
}

module.exports = StreamService;
```

---

### 5.4 Recommendation Engine

```javascript
// services/recommendation/RecommendationService.js
/**
 * Netflix's actual recommendation uses:
 * - Collaborative Filtering (CF): "Users like you also watched..."
 * - Content-Based Filtering: "Similar to what you watched..."
 * - Neural Networks (e.g., restricted Boltzmann machines)
 * - A/B-tested ranking algorithms
 * 
 * This LLD covers the service layer + candidate generation + ranking.
 */

const redis = require('../shared/redis');
const cassandra = require('../shared/cassandra');
const db = require('../shared/db');

const RECOMMENDATION_STRATEGIES = {
  COLLABORATIVE: 'collaborative_filtering',
  CONTENT_BASED: 'content_based',
  TRENDING: 'trending',
  BECAUSE_YOU_WATCHED: 'because_you_watched',
  NEW_RELEASES: 'new_releases',
  TOP_PICKS: 'top_picks',
};

class RecommendationService {
  constructor({ vectorStore, mlClient }) {
    this.vectorStore = vectorStore;     // e.g., Pinecone / Faiss for embeddings
    this.mlClient = mlClient;           // gRPC client to ML inference service
  }

  /**
   * Main entry: fetch personalized home page rows for a profile
   * Returns multiple "rows" like Netflix's home screen
   */
  async getHomePageRecommendations(profileId, { limit = 20, rows = 6 } = {}) {
    const cacheKey = `reco:home:${profileId}`;
    const cached = await redis.get(cacheKey);
    if (cached) return JSON.parse(cached);

    // Fetch user's watch history for context
    const watchHistory = await this.getWatchHistory(profileId, { limit: 50 });
    const userEmbedding = await this.getUserEmbedding(profileId);

    // Generate candidates from multiple strategies in parallel
    const [collaborative, contentBased, trending, becauseYouWatched, newReleases] =
      await Promise.allSettled([
        this.collaborativeFiltering(profileId, userEmbedding, { limit: 50 }),
        this.contentBasedFiltering(profileId, watchHistory, { limit: 50 }),
        this.getTrending(profileId, { limit: 20 }),
        this.becauseYouWatched(profileId, watchHistory, { limit: 30 }),
        this.getNewReleases(profileId, { limit: 20 }),
      ]);

    // Merge, de-duplicate, and rank candidates
    const allCandidates = this.mergeCandidates([
      ...(collaborative.value || []),
      ...(contentBased.value || []),
    ]);

    const ranked = await this.rankCandidates(allCandidates, profileId);

    const result = {
      rows: [
        {
          rowId: 'top_picks',
          title: 'Top Picks for You',
          strategy: RECOMMENDATION_STRATEGIES.TOP_PICKS,
          items: ranked.slice(0, limit),
        },
        {
          rowId: 'trending',
          title: 'Trending Now',
          strategy: RECOMMENDATION_STRATEGIES.TRENDING,
          items: (trending.value || []).slice(0, limit),
        },
        {
          rowId: 'because_you_watched',
          title: `Because You Watched: ${watchHistory[0]?.title || ''}`,
          strategy: RECOMMENDATION_STRATEGIES.BECAUSE_YOU_WATCHED,
          items: (becauseYouWatched.value || []).slice(0, limit),
        },
        {
          rowId: 'new_releases',
          title: 'New Releases',
          strategy: RECOMMENDATION_STRATEGIES.NEW_RELEASES,
          items: (newReleases.value || []).slice(0, limit),
        },
      ].slice(0, rows),
      generatedAt: Date.now(),
    };

    // Cache for 1 hour
    await redis.setex(cacheKey, 3600, JSON.stringify(result));
    return result;
  }

  /**
   * Collaborative Filtering using user embedding vectors
   * Find users similar to current user → recommend what they liked
   */
  async collaborativeFiltering(profileId, userEmbedding, { limit }) {
    // Find top-K similar users using vector similarity search
    const similarUsers = await this.vectorStore.query({
      vector: userEmbedding,
      topK: 20,
      filter: { type: 'user_embedding' },
    });

    const similarProfileIds = similarUsers.matches
      .filter(m => m.score > 0.7)
      .map(m => m.metadata.profileId);

    // Get content that similar users rated highly but current user hasn't seen
    const watchedByUser = new Set(
      (await this.getWatchHistory(profileId, { limit: 200 })).map(w => w.contentId)
    );

    const candidates = await cassandra.execute(
      `SELECT content_id, COUNT(*) as watch_count, AVG(rating) as avg_rating
       FROM user_ratings 
       WHERE profile_id IN ? 
       GROUP BY content_id 
       ORDER BY watch_count DESC 
       LIMIT ?`,
      [similarProfileIds, limit * 2], { prepare: true }
    );

    return candidates.rows
      .filter(c => !watchedByUser.has(c.content_id))
      .slice(0, limit)
      .map(c => ({
        contentId: c.content_id,
        score: (c.avg_rating || 3) * Math.log1p(c.watch_count),
        strategy: RECOMMENDATION_STRATEGIES.COLLABORATIVE,
      }));
  }

  /**
   * Content-Based Filtering: find content similar to what user watches
   */
  async contentBasedFiltering(profileId, watchHistory, { limit }) {
    if (!watchHistory.length) return [];

    // Get embeddings of recently watched + liked content
    const recentContent = watchHistory
      .filter(w => w.rating >= 0)    // not thumbs-down
      .slice(0, 10)
      .map(w => w.contentId);

    // Fetch content embeddings
    const contentEmbeddings = await this.vectorStore.fetch(recentContent);

    // Average the embeddings to get a "taste vector"
    const tasteVector = this.averageEmbeddings(
      Object.values(contentEmbeddings.vectors).map(v => v.values)
    );

    // Find similar content
    const similar = await this.vectorStore.query({
      vector: tasteVector,
      topK: limit * 2,
      filter: { type: 'content_embedding', status: 'ready' },
    });

    const watchedSet = new Set(watchHistory.map(w => w.contentId));

    return similar.matches
      .filter(m => !watchedSet.has(m.metadata.contentId))
      .slice(0, limit)
      .map(m => ({
        contentId: m.metadata.contentId,
        score: m.score * 10,
        strategy: RECOMMENDATION_STRATEGIES.CONTENT_BASED,
      }));
  }

  /**
   * Re-rank candidates using ML model (personalized ranking)
   */
  async rankCandidates(candidates, profileId) {
    if (!candidates.length) return [];

    // Call ML inference service via gRPC
    const features = candidates.map(c => ({
      contentId: c.contentId,
      baseScore: c.score,
      strategy: c.strategy,
    }));

    try {
      const ranked = await this.mlClient.rank({
        profileId,
        candidates: features,
      });
      return ranked.contentIds;
    } catch {
      // Fallback: sort by score
      return candidates.sort((a, b) => b.score - a.score).map(c => c.contentId);
    }
  }

  async getTrending(profileId, { limit }) {
    const subscription = await db.subscriptions.findOne({ profileId });
    const countryCode = subscription?.countryCode || 'US';
    const today = new Date().toISOString().split('T')[0];

    // Read from Redis sorted set (updated by analytics service)
    const trending = await redis.zrevrange(
      `trending:${countryCode}:${today}`, 0, limit - 1
    );
    return trending.map(contentId => ({ contentId, strategy: RECOMMENDATION_STRATEGIES.TRENDING }));
  }

  async becauseYouWatched(profileId, watchHistory, { limit }) {
    if (!watchHistory.length) return [];
    const lastWatched = watchHistory[0];

    // Find content similar to last watched (content-based)
    return this.contentBasedFiltering(profileId, [lastWatched], { limit });
  }

  async getNewReleases(profileId, { limit }) {
    const thirtyDaysAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);
    const cacheKey = `new_releases:${new Date().toISOString().split('T')[0]}`;
    const cached = await redis.get(cacheKey);
    if (cached) return JSON.parse(cached);

    const releases = await db.content.findAll({
      status: 'ready',
      created_at: { $gte: thirtyDaysAgo },
      orderBy: 'created_at DESC',
      limit: limit * 2,
    });

    const result = releases
      .slice(0, limit)
      .map(c => ({ contentId: c.contentId, strategy: RECOMMENDATION_STRATEGIES.NEW_RELEASES }));

    await redis.setex(cacheKey, 3600, JSON.stringify(result));
    return result;
  }

  mergeCandidates(candidates) {
    const map = new Map();
    for (const c of candidates) {
      if (map.has(c.contentId)) {
        // If same content from multiple strategies, boost score
        const existing = map.get(c.contentId);
        existing.score += c.score * 0.5;
      } else {
        map.set(c.contentId, { ...c });
      }
    }
    return [...map.values()];
  }

  averageEmbeddings(vectors) {
    if (!vectors.length) return [];
    const dim = vectors[0].length;
    const avg = new Array(dim).fill(0);
    for (const v of vectors) {
      for (let i = 0; i < dim; i++) avg[i] += v[i];
    }
    return avg.map(v => v / vectors.length);
  }

  async getWatchHistory(profileId, { limit = 50 }) {
    const cacheKey = `watch_history:${profileId}:${limit}`;
    const cached = await redis.get(cacheKey);
    if (cached) return JSON.parse(cached);

    const result = await cassandra.execute(
      'SELECT content_id, watched_at, position_secs, total_secs FROM watch_history WHERE profile_id=? LIMIT ?',
      [profileId, limit], { prepare: true }
    );

    const history = result.rows.map(r => ({
      contentId: r.content_id,
      watchedAt: r.watched_at,
      positionSecs: r.position_secs,
      totalSecs: r.total_secs,
    }));

    await redis.setex(cacheKey, 300, JSON.stringify(history));
    return history;
  }

  async getUserEmbedding(profileId) {
    const result = await this.vectorStore.fetch([`profile:${profileId}`]);
    return result.vectors[`profile:${profileId}`]?.values || new Array(256).fill(0);
  }
}

module.exports = RecommendationService;
```

---

### 5.5 User Service & Auth

```javascript
// services/user/UserService.js
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const crypto = require('crypto');
const redis = require('../shared/redis');
const db = require('../shared/db');
const { sendEmail } = require('../shared/mailer');

const SALT_ROUNDS = 12;
const ACCESS_TOKEN_TTL = '15m';
const REFRESH_TOKEN_TTL = '30d';
const MAX_LOGIN_ATTEMPTS = 5;
const LOCKOUT_DURATION = 900;   // 15 minutes

class UserService {
  async register({ email, password, countryCode }) {
    // Validate email format
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
      throw new Error('Invalid email format');
    }

    // Check duplicate
    const existing = await db.users.findOne({ email });
    if (existing) throw new Error('Email already registered');

    // Validate password strength
    this.validatePasswordStrength(password);

    const passwordHash = await bcrypt.hash(password, SALT_ROUNDS);
    const userId = crypto.randomUUID();

    const user = await db.users.create({
      userId,
      email,
      passwordHash,
      countryCode,
      isActive: false,    // Requires email verification
    });

    // Create default profile
    await db.profiles.create({
      profileId: crypto.randomUUID(),
      userId,
      name: email.split('@')[0],
      isKids: false,
    });

    // Send verification email
    const verificationToken = this.generateSecureToken();
    await redis.setex(`verify:${verificationToken}`, 86400, userId);
    await sendEmail({
      to: email,
      template: 'email-verification',
      data: { verificationToken },
    });

    return { userId, message: 'Verification email sent' };
  }

  async login({ email, password, deviceInfo }) {
    // Rate limiting / brute force protection
    const lockKey = `login_lock:${email}`;
    const attempts = await redis.get(lockKey);
    if (parseInt(attempts) >= MAX_LOGIN_ATTEMPTS) {
      throw new Error('Account temporarily locked. Try again in 15 minutes');
    }

    const user = await db.users.findOne({ email, isActive: true });

    if (!user || !(await bcrypt.compare(password, user.passwordHash))) {
      await redis.incr(lockKey);
      await redis.expire(lockKey, LOCKOUT_DURATION);
      throw new Error('Invalid credentials');
    }

    // Clear lockout on success
    await redis.del(lockKey);

    // Generate token pair
    const { accessToken, refreshToken, sessionId } =
      await this.generateTokens(user.userId, deviceInfo);

    return { accessToken, refreshToken, userId: user.userId };
  }

  async generateTokens(userId, deviceInfo = {}) {
    const sessionId = crypto.randomUUID();
    const deviceId = this.hashDevice(deviceInfo);

    const accessToken = jwt.sign(
      { sub: userId, sessionId, deviceId, type: 'access' },
      process.env.JWT_ACCESS_SECRET,
      { expiresIn: ACCESS_TOKEN_TTL, algorithm: 'RS256' }
    );

    const refreshToken = jwt.sign(
      { sub: userId, sessionId, type: 'refresh' },
      process.env.JWT_REFRESH_SECRET,
      { expiresIn: REFRESH_TOKEN_TTL, algorithm: 'RS256' }
    );

    // Store session in Redis
    await redis.setex(
      `session:${sessionId}`,
      30 * 24 * 3600,
      JSON.stringify({ userId, deviceId, createdAt: Date.now(), lastActive: Date.now() })
    );

    return { accessToken, refreshToken, sessionId };
  }

  async refreshTokens({ refreshToken }) {
    let payload;
    try {
      payload = jwt.verify(refreshToken, process.env.JWT_REFRESH_SECRET);
    } catch {
      throw new Error('Invalid or expired refresh token');
    }

    // Check session is still valid
    const session = await redis.get(`session:${payload.sessionId}`);
    if (!session) throw new Error('Session revoked');

    return this.generateTokens(payload.sub);
  }

  async logout({ sessionId }) {
    await redis.del(`session:${sessionId}`);
  }

  async logoutAllDevices({ userId }) {
    // Invalidate all sessions by bumping a version counter
    // Access tokens become invalid when version doesn't match
    await redis.incr(`user_token_version:${userId}`);
  }

  // ─── Profile Management ───

  async createProfile({ userId, name, isKids, language, maturityLevel }) {
    const existingCount = await db.profiles.count({ userId });
    if (existingCount >= 5) throw new Error('Maximum 5 profiles per account');

    return db.profiles.create({
      profileId: crypto.randomUUID(),
      userId, name, isKids,
      language: language || 'en-US',
      maturityLevel: isKids ? 1 : (maturityLevel || 3),
    });
  }

  async updateProfile({ profileId, userId, updates }) {
    const profile = await db.profiles.findOne({ profileId, userId });
    if (!profile) throw new Error('Profile not found');
    return db.profiles.updateOne({ profileId }, updates);
  }

  async deleteProfile({ profileId, userId }) {
    const profiles = await db.profiles.find({ userId });
    if (profiles.length <= 1) throw new Error('Cannot delete last profile');
    await db.profiles.deleteOne({ profileId, userId });
  }

  validatePasswordStrength(password) {
    if (password.length < 8) throw new Error('Password must be at least 8 characters');
    if (!/[A-Z]/.test(password)) throw new Error('Password must contain an uppercase letter');
    if (!/[0-9]/.test(password)) throw new Error('Password must contain a number');
  }

  generateSecureToken() {
    return crypto.randomBytes(32).toString('hex');
  }

  hashDevice({ userAgent, ip }) {
    return crypto.createHash('sha256').update(`${userAgent}:${ip}`).digest('hex').slice(0, 16);
  }
}

// ─── Auth Middleware ───
const authenticate = async (req, res, next) => {
  try {
    const token = req.headers.authorization?.split(' ')[1];
    if (!token) return res.status(401).json({ error: 'No token' });

    const payload = jwt.verify(token, process.env.JWT_ACCESS_SECRET);

    // Check token version (logout all devices support)
    const version = await redis.get(`user_token_version:${payload.sub}`);
    if (version && parseInt(version) > (payload.tokenVersion || 0)) {
      return res.status(401).json({ error: 'Token invalidated' });
    }

    // Validate session
    const session = await redis.get(`session:${payload.sessionId}`);
    if (!session) return res.status(401).json({ error: 'Session expired' });

    req.user = { userId: payload.sub, sessionId: payload.sessionId };
    next();
  } catch {
    res.status(401).json({ error: 'Invalid token' });
  }
};

const authorize = (...permissions) => (req, res, next) => {
  // Check if user has required permission (role-based)
  // Implementation varies — RBAC from DB/cache
  next();
};

module.exports = { UserService, authenticate, authorize };
```

---

### 5.6 Search Service

```javascript
// services/search/SearchService.js
const { Client } = require('@elastic/elasticsearch');
const redis = require('../shared/redis');

class SearchService {
  constructor() {
    this.es = new Client({ node: process.env.ELASTICSEARCH_URL });
    this.index = 'netflix_catalog';
  }

  /**
   * Elasticsearch index mapping for catalog
   * Set this up once on index creation
   */
  getIndexMapping() {
    return {
      mappings: {
        properties: {
          title: { type: 'text', analyzer: 'english', boost: 3 },
          title_suggest: { type: 'completion' },    // for autocomplete
          description: { type: 'text', analyzer: 'english' },
          genres: { type: 'keyword' },
          type: { type: 'keyword' },                // movie/series
          release_year: { type: 'integer' },
          rating: { type: 'keyword' },
          imdb_rating: { type: 'float' },
          cast: { type: 'text', boost: 2 },
          director: { type: 'text', boost: 2 },
          language: { type: 'keyword' },
          country: { type: 'keyword' },
          is_original: { type: 'boolean' },
          status: { type: 'keyword' },
          tags: { type: 'keyword' },
          popularity_score: { type: 'float' },
        },
      },
      settings: {
        number_of_shards: 5,
        number_of_replicas: 2,
        analysis: {
          analyzer: {
            english: {
              tokenizer: 'standard',
              filter: ['lowercase', 'english_stop', 'english_stemmer', 'english_possessive_stemmer'],
            },
          },
        },
      },
    };
  }

  /**
   * Full-text search with faceting, fuzzy matching, and personalization
   */
  async search({ query, filters = {}, page = 1, pageSize = 20, profileId }) {
    const cacheKey = `search:${query}:${JSON.stringify(filters)}:${page}`;
    const cached = await redis.get(cacheKey);
    if (cached) return JSON.parse(cached);

    const from = (page - 1) * pageSize;

    const esQuery = {
      index: this.index,
      from,
      size: pageSize,
      body: {
        query: {
          function_score: {
            query: {
              bool: {
                must: [
                  {
                    multi_match: {
                      query,
                      fields: ['title^3', 'cast^2', 'director^2', 'description', 'tags'],
                      type: 'best_fields',
                      fuzziness: 'AUTO',          // typo tolerance
                      prefix_length: 2,
                    },
                  },
                  { term: { status: 'ready' } },
                ],
                filter: this.buildFilters(filters),
              },
            },
            functions: [
              {
                field_value_factor: {
                  field: 'popularity_score',
                  factor: 1.2,
                  modifier: 'log1p',
                  missing: 1,
                },
              },
              {
                field_value_factor: {
                  field: 'imdb_rating',
                  factor: 0.1,
                  modifier: 'none',
                  missing: 5,
                },
              },
            ],
            score_mode: 'sum',
            boost_mode: 'multiply',
          },
        },
        highlight: {
          fields: {
            title: {},
            description: { fragment_size: 150, number_of_fragments: 1 },
          },
        },
        aggs: {
          genres: { terms: { field: 'genres', size: 20 } },
          type: { terms: { field: 'type', size: 5 } },
          release_years: { histogram: { field: 'release_year', interval: 5 } },
          ratings: { terms: { field: 'rating', size: 10 } },
        },
      },
    };

    const result = await this.es.search(esQuery);

    const response = {
      total: result.hits.total.value,
      page,
      pageSize,
      results: result.hits.hits.map(hit => ({
        contentId: hit._id,
        score: hit._score,
        highlight: hit.highlight,
        ...hit._source,
      })),
      facets: {
        genres: result.aggregations.genres.buckets,
        types: result.aggregations.type.buckets,
        releaseYears: result.aggregations.release_years.buckets,
        ratings: result.aggregations.ratings.buckets,
      },
    };

    // Cache popular search results for 5 minutes
    await redis.setex(cacheKey, 300, JSON.stringify(response));
    return response;
  }

  /**
   * Autocomplete / typeahead suggestions
   */
  async suggest({ prefix, limit = 10 }) {
    const result = await this.es.search({
      index: this.index,
      body: {
        suggest: {
          title_suggest: {
            prefix,
            completion: {
              field: 'title_suggest',
              size: limit,
              skip_duplicates: true,
              fuzzy: { fuzziness: 1 },
            },
          },
        },
      },
    });

    return result.suggest.title_suggest[0].options.map(opt => ({
      text: opt.text,
      contentId: opt._id,
      type: opt._source.type,
    }));
  }

  buildFilters(filters) {
    const clauses = [];
    if (filters.genre) clauses.push({ term: { genres: filters.genre } });
    if (filters.type) clauses.push({ term: { type: filters.type } });
    if (filters.language) clauses.push({ term: { language: filters.language } });
    if (filters.rating) clauses.push({ term: { rating: filters.rating } });
    if (filters.yearMin || filters.yearMax) {
      clauses.push({
        range: {
          release_year: {
            gte: filters.yearMin,
            lte: filters.yearMax,
          },
        },
      });
    }
    if (filters.isOriginal !== undefined) {
      clauses.push({ term: { is_original: filters.isOriginal } });
    }
    return clauses;
  }

  /**
   * Index a new content item (called by Catalog Service)
   */
  async indexContent(content) {
    await this.es.index({
      index: this.index,
      id: content.contentId,
      body: {
        title: content.title,
        title_suggest: {
          input: [content.title, ...content.title.split(' ')],
          weight: Math.round(content.popularityScore || 1),
        },
        description: content.description,
        genres: content.genres,
        type: content.type,
        release_year: content.releaseYear,
        rating: content.rating,
        imdb_rating: content.imdbRating,
        cast: content.cast?.join(' '),
        director: content.director,
        language: content.language,
        is_original: content.isOriginal,
        status: content.status,
        popularity_score: content.popularityScore || 1,
      },
    });
  }

  async deleteFromIndex(contentId) {
    await this.es.delete({ index: this.index, id: contentId });
  }
}

module.exports = SearchService;
```

---

### 5.7 Notification Service

```javascript
// services/notification/NotificationService.js
const kafka = require('../shared/kafka');
const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns');
const nodemailer = require('nodemailer');
const db = require('../shared/db');

class NotificationService {
  constructor() {
    this.sns = new SNSClient({ region: process.env.AWS_REGION });
    this.mailer = nodemailer.createTransport({
      host: process.env.SES_HOST,
      port: 587,
      auth: { user: process.env.SES_USER, pass: process.env.SES_PASS },
    });
    this.templates = new TemplateEngine();
  }

  async startConsuming() {
    // Subscribe to events that trigger notifications
    await kafka.consumeGroup('notification-service', [
      'content.new_release',
      'billing.payment_failed',
      'billing.subscription_expiring',
      'user.email_verification',
      'user.password_reset',
    ], this.handleEvent.bind(this));
  }

  async handleEvent({ topic, message }) {
    const handlers = {
      'content.new_release': this.notifyNewRelease.bind(this),
      'billing.payment_failed': this.notifyPaymentFailed.bind(this),
      'billing.subscription_expiring': this.notifyExpiringSubscription.bind(this),
      'user.email_verification': this.sendEmailVerification.bind(this),
      'user.password_reset': this.sendPasswordReset.bind(this),
    };
    const handler = handlers[topic];
    if (handler) await handler(message);
  }

  async notifyNewRelease({ contentId, title, genres, targetCountries }) {
    // Get users who have this genre in their preferences
    const interestedUsers = await db.users.findByGenrePreference(genres, targetCountries);
    const chunks = this.chunkArray(interestedUsers, 1000);

    for (const chunk of chunks) {
      await Promise.all(
        chunk.map(user =>
          this.sendNotification(user.userId, {
            type: 'NEW_RELEASE',
            title: `New on Netflix: ${title}`,
            body: 'A new title matching your interests is now available.',
            data: { contentId },
            channels: ['push', 'email'],
          })
        )
      );
    }
  }

  async sendNotification(userId, { type, title, body, data, channels }) {
    const user = await db.users.findById(userId);
    const prefs = await db.notificationPrefs.findOne({ userId });

    const promises = [];

    if (channels.includes('push') && prefs?.pushEnabled) {
      promises.push(this.sendPushNotification(user, { title, body, data }));
    }

    if (channels.includes('email') && prefs?.emailEnabled) {
      promises.push(this.sendEmail(user.email, type, { title, body, data }));
    }

    if (channels.includes('sms') && prefs?.smsEnabled && user.phone) {
      promises.push(this.sendSMS(user.phone, body));
    }

    await Promise.allSettled(promises);

    // Log notification
    await db.notificationLogs.create({
      userId, type, title, body,
      channels, sentAt: new Date(),
    });
  }

  async sendPushNotification(user, { title, body, data }) {
    const deviceTokens = await db.deviceTokens.find({ userId: user.userId });
    for (const device of deviceTokens) {
      try {
        await this.sns.send(new PublishCommand({
          TargetArn: device.snsEndpointArn,
          MessageStructure: 'json',
          Message: JSON.stringify({
            default: body,
            APNS: JSON.stringify({ aps: { alert: { title, body }, badge: 1, sound: 'default' }, data }),
            GCM: JSON.stringify({ notification: { title, body }, data }),
          }),
        }));
      } catch (err) {
        if (err.code === 'EndpointDisabled') {
          await db.deviceTokens.deleteOne({ id: device.id });
        }
      }
    }
  }

  async sendEmail(to, templateType, data) {
    const { subject, html } = await this.templates.render(templateType, data);
    await this.mailer.sendMail({
      from: '"Netflix" <noreply@netflix.com>',
      to,
      subject,
      html,
    });
  }

  async sendSMS(phone, message) {
    await this.sns.send(new PublishCommand({
      PhoneNumber: phone,
      Message: message,
      MessageAttributes: {
        'AWS.SNS.SMS.SenderID': { DataType: 'String', StringValue: 'Netflix' },
        'AWS.SNS.SMS.SMSType': { DataType: 'String', StringValue: 'Transactional' },
      },
    }));
  }

  chunkArray(arr, size) {
    const chunks = [];
    for (let i = 0; i < arr.length; i += size) chunks.push(arr.slice(i, i + size));
    return chunks;
  }
}

class TemplateEngine {
  async render(type, data) {
    const templates = {
      NEW_RELEASE: {
        subject: `New on Netflix: ${data.title}`,
        html: `<h1>${data.title} is now on Netflix!</h1><p>${data.body}</p>`,
      },
      EMAIL_VERIFICATION: {
        subject: 'Verify your Netflix account',
        html: `<p>Click <a href="${process.env.APP_URL}/verify/${data.token}">here</a> to verify.</p>`,
      },
    };
    return templates[type] || { subject: data.title, html: `<p>${data.body}</p>` };
  }
}

module.exports = NotificationService;
```

---

### 5.8 Billing Service

```javascript
// services/billing/BillingService.js
const stripe = require('stripe')(process.env.STRIPE_SECRET_KEY);
const kafka = require('../shared/kafka');
const db = require('../shared/db');

const PLANS = {
  basic:    { priceId: 'price_basic_monthly',    maxStreams: 1, maxDownloads: 1,  priceCents: 999  },
  standard: { priceId: 'price_standard_monthly', maxStreams: 2, maxDownloads: 2,  priceCents: 1549 },
  premium:  { priceId: 'price_premium_monthly',  maxStreams: 4, maxDownloads: 4,  priceCents: 2299 },
};

class BillingService {
  async subscribe({ userId, planId, paymentMethodId }) {
    const plan = PLANS[planId];
    if (!plan) throw new Error('Invalid plan');

    let stripeCustomerId = await this.getOrCreateStripeCustomer(userId);

    // Attach payment method to customer
    await stripe.paymentMethods.attach(paymentMethodId, { customer: stripeCustomerId });
    await stripe.customers.update(stripeCustomerId, {
      invoice_settings: { default_payment_method: paymentMethodId },
    });

    // Create subscription
    const stripeSubscription = await stripe.subscriptions.create({
      customer: stripeCustomerId,
      items: [{ price: plan.priceId }],
      payment_behavior: 'default_incomplete',
      expand: ['latest_invoice.payment_intent'],
    });

    // Save to DB
    await db.subscriptions.create({
      userId,
      planId,
      stripeSubscriptionId: stripeSubscription.id,
      status: stripeSubscription.status,
      startedAt: new Date(),
      expiresAt: new Date(stripeSubscription.current_period_end * 1000),
      maxStreams: plan.maxStreams,
      maxDownloads: plan.maxDownloads,
      priceCents: plan.priceCents,
      currency: 'usd',
    });

    await kafka.publish('billing.subscription_created', { userId, planId });

    return {
      subscriptionId: stripeSubscription.id,
      clientSecret: stripeSubscription.latest_invoice?.payment_intent?.client_secret,
      status: stripeSubscription.status,
    };
  }

  async changePlan({ userId, newPlanId }) {
    const plan = PLANS[newPlanId];
    if (!plan) throw new Error('Invalid plan');

    const subscription = await db.subscriptions.findOne({ userId, status: 'active' });
    if (!subscription) throw new Error('No active subscription');

    // Update Stripe subscription (prorated)
    await stripe.subscriptions.update(subscription.stripeSubscriptionId, {
      items: [{ id: subscription.stripeItemId, price: plan.priceId }],
      proration_behavior: 'create_prorations',
    });

    await db.subscriptions.updateOne(
      { userId, status: 'active' },
      { $set: { planId: newPlanId, maxStreams: plan.maxStreams, priceCents: plan.priceCents } }
    );

    // Invalidate subscription cache
    await redis.del(`subscription:${userId}`);
    await kafka.publish('billing.plan_changed', { userId, newPlanId });
  }

  async cancelSubscription({ userId, reason }) {
    const subscription = await db.subscriptions.findOne({ userId, status: 'active' });
    if (!subscription) throw new Error('No active subscription');

    // Cancel at period end (user keeps access until cycle ends)
    await stripe.subscriptions.update(subscription.stripeSubscriptionId, {
      cancel_at_period_end: true,
    });

    await db.subscriptions.updateOne(
      { userId },
      { $set: { status: 'cancelling', cancelReason: reason } }
    );

    await kafka.publish('billing.subscription_cancelled', { userId, reason });
  }

  /**
   * Stripe webhook handler for payment events
   */
  async handleWebhook(rawBody, signature) {
    const event = stripe.webhooks.constructEvent(
      rawBody,
      signature,
      process.env.STRIPE_WEBHOOK_SECRET
    );

    const handlers = {
      'invoice.paid': this.onInvoicePaid.bind(this),
      'invoice.payment_failed': this.onPaymentFailed.bind(this),
      'customer.subscription.deleted': this.onSubscriptionDeleted.bind(this),
    };

    const handler = handlers[event.type];
    if (handler) await handler(event.data.object);
  }

  async onInvoicePaid(invoice) {
    const sub = await db.subscriptions.findOne({ stripeSubscriptionId: invoice.subscription });
    if (!sub) return;

    await db.subscriptions.updateOne(
      { stripeSubscriptionId: invoice.subscription },
      { $set: { status: 'active', expiresAt: new Date(invoice.lines.data[0].period.end * 1000) } }
    );

    await db.invoices.create({
      userId: sub.userId,
      amountCents: invoice.amount_paid,
      currency: invoice.currency,
      status: 'paid',
      billingDate: new Date(),
      stripeInvoiceId: invoice.id,
      pdfUrl: invoice.invoice_pdf,
    });

    await kafka.publish('billing.payment_succeeded', { userId: sub.userId });
  }

  async onPaymentFailed(invoice) {
    const sub = await db.subscriptions.findOne({ stripeSubscriptionId: invoice.subscription });
    if (!sub) return;

    await db.subscriptions.updateOne(
      { stripeSubscriptionId: invoice.subscription },
      { $set: { status: 'past_due' } }
    );

    await kafka.publish('billing.payment_failed', { userId: sub.userId, attemptCount: invoice.attempt_count });
  }

  async onSubscriptionDeleted(stripeSubscription) {
    await db.subscriptions.updateOne(
      { stripeSubscriptionId: stripeSubscription.id },
      { $set: { status: 'cancelled' } }
    );
  }

  async getOrCreateStripeCustomer(userId) {
    const user = await db.users.findById(userId);
    if (user.stripeCustomerId) return user.stripeCustomerId;

    const customer = await stripe.customers.create({
      email: user.email,
      metadata: { userId },
    });

    await db.users.updateOne({ userId }, { $set: { stripeCustomerId: customer.id } });
    return customer.id;
  }
}

module.exports = BillingService;
```

---

### 5.9 Analytics & Telemetry

```javascript
// services/analytics/AnalyticsService.js
/**
 * Netflix Analytics Architecture:
 * Client → Kafka → Flink (real-time) → ClickHouse (OLAP)
 *                → Spark (batch)    → S3 Data Lake → Redshift
 *
 * This service handles ingestion and real-time aggregations.
 */

const kafka = require('../shared/kafka');
const { ClickHouseClient } = require('@clickhouse/client');
const redis = require('../shared/redis');

const EVENTS = {
  PLAY_START:      'play_start',
  PLAY_STOP:       'play_stop',
  PLAY_PAUSE:      'play_pause',
  QUALITY_CHANGE:  'quality_change',
  BUFFER_START:    'buffer_start',
  BUFFER_END:      'buffer_end',
  ERROR:           'playback_error',
  SEARCH:          'search',
  CONTENT_CLICK:   'content_click',
  IMPRESSION:      'impression',
};

class AnalyticsService {
  constructor() {
    this.clickhouse = new ClickHouseClient({
      url: process.env.CLICKHOUSE_URL,
      username: process.env.CLICKHOUSE_USER,
      password: process.env.CLICKHOUSE_PASS,
    });
  }

  /**
   * Receive event from client (batch endpoint for efficiency)
   */
  async ingestEvents(events) {
    // Validate and enrich events
    const enriched = events.map(event => ({
      ...event,
      serverId: process.env.SERVER_ID,
      ingestedAt: new Date().toISOString(),
      sessionMinute: Math.floor(Date.now() / 60000),
    }));

    // Publish to Kafka for downstream processing
    await Promise.all(
      enriched.map(event => kafka.publish(`analytics.${event.type}`, event))
    );

    return { accepted: enriched.length };
  }

  /**
   * Real-time trending counter (updated by heartbeat events)
   * Sliding window using Redis sorted sets
   */
  async updateTrendingScore(contentId, countryCode) {
    const now = Date.now();
    const windowMs = 3600000;   // 1 hour sliding window
    const today = new Date().toISOString().split('T')[0];
    const key = `trending:${countryCode}:${today}`;

    // Add current view to sorted set (score = timestamp)
    // Prune old entries outside window
    const pipeline = redis.pipeline();
    pipeline.zadd(`views:${contentId}:${countryCode}`, now, `${now}`);
    pipeline.zremrangebyscore(`views:${contentId}:${countryCode}`, 0, now - windowMs);
    pipeline.zcard(`views:${contentId}:${countryCode}`);
    const results = await pipeline.exec();
    const viewCount = results[2][1];

    // Update global trending sorted set
    await redis.zadd(key, viewCount, contentId);
    await redis.expire(key, 86400);
  }

  /**
   * Consumer: process heartbeat events for real-time analytics
   */
  async startRealTimeConsumer() {
    await kafka.consume('playback.heartbeat', async (event) => {
      await Promise.all([
        this.updateTrendingScore(event.contentId, event.countryCode),
        this.insertToClickHouse('playback_heartbeats', event),
        this.updateViewerCount(event.contentId),
      ]);
    });

    await kafka.consume('analytics.search', async (event) => {
      await this.insertToClickHouse('search_events', event);
      await this.updatePopularSearches(event.query);
    });
  }

  async insertToClickHouse(table, event) {
    await this.clickhouse.insert({
      table,
      values: [event],
      format: 'JSONEachRow',
    });
  }

  async updateViewerCount(contentId) {
    const key = `live_viewers:${contentId}`;
    await redis.incr(key);
    await redis.expire(key, 120);    // Decrement naturally via TTL expiry
  }

  async getLiveViewerCount(contentId) {
    return parseInt(await redis.get(`live_viewers:${contentId}`) || 0);
  }

  // ─── Dashboard Queries ───

  async getPlaybackMetrics({ contentId, startDate, endDate }) {
    const result = await this.clickhouse.query({
      query: `
        SELECT
          toDate(timestamp) as date,
          count() as total_views,
          countIf(event_type = 'buffer_start') as buffer_events,
          avg(watch_duration_secs) as avg_watch_duration,
          countDistinct(user_id) as unique_viewers,
          avg(quality_bitrate_kbps) as avg_quality
        FROM playback_heartbeats
        WHERE content_id = {content_id:String}
          AND timestamp BETWEEN {start:DateTime} AND {end:DateTime}
        GROUP BY date
        ORDER BY date
      `,
      query_params: { content_id: contentId, start: startDate, end: endDate },
      format: 'JSONEachRow',
    });

    return result.json();
  }
}

module.exports = { AnalyticsService, EVENTS };
```

---

## 6. Caching Strategy

```
┌──────────────────────────────────────────────────────────────────────┐
│                     CACHING LAYERS                                    │
│                                                                       │
│  L1: Client-Side Cache (Browser / Native App)                        │
│      • Manifests cached 1 hour                                       │
│      • Thumbnails cached 24 hours                                    │
│      • UI assets (JS, CSS) cached 1 year (with content hash)         │
│                                                                       │
│  L2: CDN Cache (Open Connect / CloudFront)                           │
│      • Video segments: immutable, long TTL (1 year)                  │
│      • Manifests: 5-minute TTL (to support quality changes)          │
│      • Thumbnails: 24 hours                                           │
│                                                                       │
│  L3: Redis Cache (Application Layer)                                 │
│      • Sessions: 30 days                                             │
│      • Subscriptions: 1 hour                                         │
│      • Catalog metadata: 1 hour                                      │
│      • Search results: 5 minutes                                     │
│      • Recommendations: 1 hour                                       │
│      • Trending: 24 hours (sorted set)                               │
│      • Rate limiting: 1 minute windows                               │
│      • Active streams: 5 min (heartbeat-renewed)                     │
│                                                                       │
│  L4: Database Query Cache (PostgreSQL)                               │
│      • pg_bouncer for connection pooling                             │
│      • Materialized views for expensive aggregations                 │
└──────────────────────────────────────────────────────────────────────┘

Cache Invalidation Strategy:
  • Write-through: session creation, profile updates
  • Cache-aside: catalog queries, user data
  • Event-driven: Kafka events trigger cache invalidation
    (e.g., video.processed → invalidate catalog cache for contentId)
  • TTL-based: most caches rely on TTL as primary invalidation

Cache Key Design Pattern:
  {service}:{entity}:{id}[:{sub-id}][:{variant}]
  Examples:
    catalog:content:uuid-1234
    stream:asset:uuid-1234:movie
    reco:home:profile-uuid:2024-01-15
    trending:US:2024-01-15
```

---

## 7. Message Queue & Event Streaming

```
Kafka Topic Design:
─────────────────────────────────────────────

Topic                    │ Partitions │ Retention │ Consumers
─────────────────────────┼────────────┼───────────┼──────────────────────
video.uploaded           │     10     │  7 days   │ Transcode Orchestrator
video.processed          │     10     │  7 days   │ Catalog, CDN Preposit.
playback.heartbeat       │    100     │  3 days   │ Analytics, Stream Svc
playback.events          │     50     │  7 days   │ Analytics, ML Training
analytics.search         │     50     │  3 days   │ Analytics, ML Training
watch.position.updated   │    100     │  1 day    │ Cassandra Writer
billing.payment_failed   │      5     │ 30 days   │ Notification Service
billing.subscription_*   │      5     │ 30 days   │ User Service, Notif.
content.new_release      │     10     │ 30 days   │ Notification, CDN

Kafka Consumer Groups:
  • Each microservice uses its own consumer group
  • Multiple instances of same service = shared group (load balanced)
  • Failed messages → Dead Letter Queue (DLQ) after 3 retries

Ordering Guarantee:
  • Partition by userId for user events (watch history, billing)
  • Partition by contentId for content events (upload, processing)
  • This ensures ordered processing per entity
```

---

## 8. Fault Tolerance & Resilience Patterns

```javascript
// shared/resilience/CircuitBreaker.js

class CircuitBreaker {
  constructor(name, { threshold = 5, timeout = 60000, halfOpenRequests = 3 } = {}) {
    this.name = name;
    this.threshold = threshold;      // failures before opening
    this.timeout = timeout;          // ms before half-open attempt
    this.halfOpenRequests = halfOpenRequests;
    this.state = 'CLOSED';           // CLOSED | OPEN | HALF_OPEN
    this.failures = 0;
    this.successes = 0;
    this.lastFailureTime = null;
  }

  async execute(fn) {
    if (this.state === 'OPEN') {
      if (Date.now() - this.lastFailureTime > this.timeout) {
        this.state = 'HALF_OPEN';
        this.successes = 0;
      } else {
        throw new Error(`Circuit breaker [${this.name}] is OPEN`);
      }
    }

    try {
      const result = await fn();
      this.onSuccess();
      return result;
    } catch (err) {
      this.onFailure();
      throw err;
    }
  }

  onSuccess() {
    this.failures = 0;
    if (this.state === 'HALF_OPEN') {
      this.successes++;
      if (this.successes >= this.halfOpenRequests) {
        this.state = 'CLOSED';
        console.log(`Circuit breaker [${this.name}] CLOSED`);
      }
    }
  }

  onFailure() {
    this.failures++;
    this.lastFailureTime = Date.now();
    if (this.failures >= this.threshold) {
      this.state = 'OPEN';
      console.error(`Circuit breaker [${this.name}] OPENED after ${this.failures} failures`);
    }
  }
}

// ─── Retry with Exponential Backoff ───
async function withRetry(fn, { maxAttempts = 3, baseDelayMs = 100 } = {}) {
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      return await fn();
    } catch (err) {
      if (attempt === maxAttempts) throw err;
      const delay = baseDelayMs * Math.pow(2, attempt - 1) + Math.random() * 100;
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
}

// ─── Bulkhead Pattern (limit concurrency per service) ───
class Bulkhead {
  constructor(maxConcurrent) {
    this.maxConcurrent = maxConcurrent;
    this.current = 0;
    this.queue = [];
  }

  async execute(fn) {
    if (this.current >= this.maxConcurrent) {
      await new Promise((resolve, reject) => {
        this.queue.push({ resolve, reject });
        setTimeout(() => reject(new Error('Bulkhead queue timeout')), 5000);
      });
    }
    this.current++;
    try {
      return await fn();
    } finally {
      this.current--;
      if (this.queue.length) {
        const { resolve } = this.queue.shift();
        resolve();
      }
    }
  }
}

module.exports = { CircuitBreaker, withRetry, Bulkhead };
```

```
Fault Tolerance Patterns Used:
──────────────────────────────
1. Circuit Breaker     — Prevents cascade failures between services
2. Retry + Backoff     — Handles transient failures (network blips)
3. Bulkhead            — Isolates failure domains (thread pool isolation)
4. Timeout             — Every external call has a hard timeout (2s API, 30s video)
5. Fallback            — Degraded responses (e.g., generic reco if ML fails)
6. Health Checks       — Kubernetes liveness + readiness probes
7. Graceful Degradation— Search returns cached results if ES is down
8. Rate Limiting       — Token bucket per user; leaky bucket globally
9. Dead Letter Queues  — Failed Kafka messages don't block consumer
10. Multi-AZ Deployment— Services deployed across 3 AZs minimum

Data Durability:
  • S3: 99.999999999% (11 nines) durability
  • Cassandra: Replication factor = 3, QUORUM reads/writes
  • PostgreSQL: Multi-AZ with synchronous standby
  • Redis: Sentinel mode with 3 nodes, AOF persistence
```

---

## 9. Security Design

```
Authentication & Authorization:
  ✓ JWT (RS256) — stateless access tokens (15 min TTL)
  ✓ Refresh tokens — stored in httpOnly cookies
  ✓ Session invalidation — Redis-based with version counter
  ✓ MFA support — TOTP (Google Authenticator compatible)
  ✓ OAuth 2.0 — SSO with Google/Apple
  ✓ Rate limiting — 5 failed logins → 15-minute lockout
  ✓ RBAC — roles: user, content_admin, billing_admin, super_admin

Content Security (DRM):
  ┌─────────────────────────────────────────────────────┐
  │  Platform   │  DRM System    │  Format              │
  ├─────────────┼────────────────┼──────────────────────┤
  │  Android    │  Widevine L1   │  DASH + CENC         │
  │  iOS/tvOS   │  FairPlay      │  HLS + AES-128       │
  │  Windows    │  PlayReady     │  DASH + CENC         │
  │  Web        │  Widevine      │  DASH (Encrypted MSE)│
  └─────────────┴────────────────┴──────────────────────┘
  
  DRM Flow:
  1. Client requests DRM license from Netflix License Server
  2. License Server validates JWT + entitlement
  3. License issued with content key (AES-128 or AES-256)
  4. Client decrypts video in secure enclave (TEE)
  5. License tied to device fingerprint (no sharing)

Network Security:
  ✓ TLS 1.3 everywhere (internal + external)
  ✓ mTLS between microservices (via Istio)
  ✓ WAF (Web Application Firewall) at edge
  ✓ DDoS protection (AWS Shield Advanced / Cloudflare)
  ✓ VPC with private subnets for all backend services
  ✓ Secrets Manager (AWS Secrets Manager / Vault)
  ✓ No credentials in code / environment variables

Data Protection:
  ✓ Encryption at rest — AES-256 (S3 SSE, RDS, DynamoDB)
  ✓ Field-level encryption — PII (email, phone, payment)
  ✓ Payment data — PCI DSS compliant (Stripe handles card data)
  ✓ GDPR compliance — right to erasure, data portability
  ✓ Data residency — content served from regional buckets
  ✓ Password hashing — bcrypt with salt rounds = 12

API Security:
  ✓ Input validation — Joi/Zod schema validation
  ✓ SQL injection prevention — parameterized queries only
  ✓ XSS protection — Content Security Policy headers
  ✓ CORS — whitelist of allowed origins
  ✓ API versioning — /api/v1/, /api/v2/
  ✓ Request signing — HMAC for CDN URLs
```

---

## 10. Monitoring & Observability

```
Observability Stack (The Three Pillars):

METRICS → Prometheus + Grafana
  • Service SLIs: latency (p50, p95, p99), error rate, throughput
  • Infrastructure: CPU, memory, disk, network
  • Business: concurrent streams, new signups, churn rate
  • Custom: TTFF (Time To First Frame), rebuffer ratio

  Key Dashboards:
  ┌─────────────────────────────────────────────────────────┐
  │  SLO Dashboard                                          │
  │  • Availability: 99.99% target                          │
  │  • API p99 latency: < 200ms                             │
  │  • Stream start latency: < 2 seconds (TTFF)             │
  │  • Rebuffering ratio: < 0.5%                            │
  │  • Error rate: < 0.1%                                   │
  └─────────────────────────────────────────────────────────┘

LOGS → ELK Stack (Elasticsearch + Logstash + Kibana)
  • Structured JSON logging (never raw strings)
  • Log levels: ERROR, WARN, INFO, DEBUG
  • Mandatory fields: traceId, spanId, userId, serviceVersion
  • Log sampling for high-volume INFO logs (1%)
  • Error logs: 100% captured

  Log Format:
  {
    "timestamp": "2024-01-15T10:30:00.123Z",
    "level": "ERROR",
    "service": "stream-service",
    "traceId": "abc123",
    "spanId": "def456",
    "userId": "uuid",
    "message": "DRM license generation failed",
    "error": { "code": "DRM_001", "message": "..." },
    "duration_ms": 234
  }

TRACES → Jaeger / AWS X-Ray
  • Distributed tracing across all microservices
  • 100% trace sampling for errors, 1% for success
  • P99 breakdown by service to pinpoint latency bottlenecks

ALERTS (PagerDuty Integration):
  • P0 (immediate): > 0.1% error rate, availability < 99.9%
  • P1 (15 min): TTFF > 3 seconds, payment failures spike
  • P2 (1 hour): disk usage > 80%, cache hit rate < 90%
  • P3 (business hours): recommendation CTR drop > 10%
```

---

## 11. API Design

```
REST API Design Principles:
  • Versioned: /api/v1/
  • Resource-based URLs, HTTP verbs for actions
  • JSON request/response
  • Cursor-based pagination (not offset)
  • Consistent error format

BASE URL: https://api.netflix.com/api/v1

─── Auth Endpoints ───────────────────────────────────────────
POST   /auth/register           Register new user
POST   /auth/login              Login, get tokens
POST   /auth/refresh            Refresh access token
POST   /auth/logout             Logout current session
DELETE /auth/sessions           Logout all devices

─── Profiles ─────────────────────────────────────────────────
GET    /profiles                List profiles for account
POST   /profiles                Create new profile
GET    /profiles/:profileId     Get profile details
PUT    /profiles/:profileId     Update profile
DELETE /profiles/:profileId     Delete profile

─── Catalog ──────────────────────────────────────────────────
GET    /catalog/content         List content (paginated)
GET    /catalog/content/:id     Get content details
GET    /catalog/content/:id/seasons          Series seasons
GET    /catalog/content/:id/seasons/:s/episodes  Episodes
GET    /catalog/genres          List all genres
GET    /catalog/trending        Trending content (by region)
GET    /catalog/new-releases    Recently added content

─── Streaming ────────────────────────────────────────────────
POST   /stream/initiate         Start streaming a title
POST   /stream/heartbeat        Keep stream alive + update position
POST   /stream/stop             Stop streaming
GET    /stream/resume/:contentId Get resume position

─── Search ───────────────────────────────────────────────────
GET    /search?q=&genre=&year=  Search with filters
GET    /search/suggest?prefix=  Autocomplete suggestions

─── Recommendations ──────────────────────────────────────────
GET    /recommendations/home    Home page rows
GET    /recommendations/similar/:contentId  Similar titles

─── My List ──────────────────────────────────────────────────
GET    /mylist                  Get saved titles
POST   /mylist                  Add to list
DELETE /mylist/:contentId       Remove from list

─── Ratings ──────────────────────────────────────────────────
POST   /ratings/:contentId      Rate content (thumb up/down)
DELETE /ratings/:contentId      Remove rating

─── Downloads ────────────────────────────────────────────────
POST   /downloads               Initiate download
GET    /downloads               List downloaded content
DELETE /downloads/:contentId    Remove download

─── Billing ──────────────────────────────────────────────────
GET    /billing/plans           Available plans
POST   /billing/subscribe       Subscribe to plan
PUT    /billing/plan            Change plan
POST   /billing/cancel          Cancel subscription
GET    /billing/invoices        Billing history

─── Analytics (Client events) ────────────────────────────────
POST   /analytics/events        Batch event ingestion

─── Error Response Format ────────────────────────────────────
{
  "success": false,
  "error": {
    "code": "STREAM_LIMIT_EXCEEDED",
    "message": "You have reached your concurrent stream limit",
    "retryable": false,
    "details": { "limit": 2, "current": 2 }
  },
  "traceId": "abc-123-def"
}

─── Pagination ───────────────────────────────────────────────
GET /catalog/content?cursor=eyJpZCI6MTIz&limit=20

Response:
{
  "data": [...],
  "pagination": {
    "nextCursor": "eyJpZCI6MTQ1",
    "hasMore": true,
    "totalCount": 15000
  }
}
```

---

## 12. Trade-offs & Interview Discussion Points

```
Q: Why Cassandra for watch history instead of PostgreSQL?
A: Watch history is write-heavy (every heartbeat writes a position update),
   time-series in nature, and doesn't require complex joins. Cassandra's
   partitioning by profile_id ensures all of a user's history sits on one
   node for fast reads, and its linear write scalability handles 150M+ DAU.
   The trade-off: no ACID transactions, eventual consistency. Acceptable
   because "last watched position" can tolerate 1-2 seconds of stale reads.

Q: Why HLS instead of MPEG-DASH?
A: Netflix uses both. HLS (with FairPlay DRM) for Apple ecosystem.
   DASH (with Widevine/PlayReady) for everything else. The choice is
   device-driven, not architecture-driven. Both support adaptive bitrate.

Q: How do you handle the "thundering herd" problem when a new popular title releases?
A: 
  1. Proactive CDN pre-positioning: push segments to edge nodes the night before
  2. Gradual rollout: soft launch to % of users (canary)
  3. Cache warming: pre-generate recommendations and catalog entries
  4. Rate limiting on playback initiation endpoint
  5. Horizontal scaling: auto-scale Stream Service based on queue depth

Q: How do you ensure a user can't watch on 3 devices with a 2-stream plan?
A: Redis Set per userId tracks active stream tokens. On PLAY:
   - Check SCARD(active_streams:{userId}) >= plan limit → reject
   - Otherwise, SADD the new stream token with 5-minute TTL
   Client heartbeats every 30s to EXPIRE renewal. If user closes app
   without calling /stop, token expires naturally after 5 minutes.
   Trade-off: user might briefly be unable to start new stream for
   up to 5 minutes after abnormal disconnect. Acceptable UX.

Q: How is the recommendation system kept fresh?
A:
  • Online (real-time): trending scores updated per heartbeat via Redis
  • Near-real-time: recommendation cache invalidated hourly
  • Offline: model retraining with Spark on S3 data lake, daily batch
  • A/B testing: 10% of users get new algorithm, metrics compared

Q: How do you handle video segment cache invalidation?
A: Video segments are immutable (content never changes once encoded).
   So they use content-addressable URLs (hash in filename) and
   max-age=31536000 (1 year). No invalidation needed.
   Manifests (.m3u8) use shorter TTL (5 minutes) since they reference
   available quality levels which can change.

Q: What happens if the Transcode Service crashes mid-job?
A:
  1. BullMQ persists jobs in Redis (survives restarts)
  2. Jobs are idempotent: re-processing same video with same profile
     produces same output (deterministic FFmpeg flags)
  3. S3 multipart upload can be resumed
  4. Partial S3 objects are cleaned up by lifecycle rules after 7 days
  5. Kafka topic video.uploaded has 7-day retention → can replay

Q: How do you scale the database?
A: 
  PostgreSQL (Users/Billing/Catalog):
    • Read replicas (5 read replicas, 1 primary)
    • Connection pooling via PgBouncer
    • Horizontal sharding by user_id range for Users table
    • Caching layer (Redis) absorbs 90%+ of reads
  
  Cassandra (Watch History):
    • Native horizontal scaling: add nodes → data rebalances automatically
    • Replication factor 3 across 3 AZs
    • Eventual consistency acceptable for watch history
  
  Elasticsearch (Search):
    • 5 shards, 2 replicas per shard
    • Index aliases for zero-downtime reindexing

Q: How do you handle GDPR right to erasure?
A:
  1. Soft delete user account (mark deleted, deactivate login)
  2. Kafka event: user.delete_requested
  3. Consumer removes from: PostgreSQL, Cassandra, Redis, Elasticsearch
  4. S3 user data (if any) deleted via lifecycle rule or lambda
  5. Retain billing records (legal requirement, 7 years) — anonymized
  6. Process completes within 30 days (GDPR requirement)
  7. Audit log of deletion maintained

Q: How do you achieve 2-second Time-To-First-Frame (TTFF)?
A:
  1. CDN edge within 20ms of user → segment fetch latency ~20ms
  2. Steering Service returns nearest OCA via anycast DNS
  3. Client starts downloading first segment BEFORE DRM license arrives
     (pre-fetches in parallel)
  4. ABR starts at lowest quality (360p, ~100KB segment) → playback
     begins immediately while buffer fills and quality ramps up
  5. Pre-signed manifest URL returned in < 50ms via cached metadata
  6. Adaptive startup: start at predicted quality based on historical
     bandwidth for that user's ISP/region
```

---

## 🏗️ Technology Stack Summary

| Layer | Technology | Why |
|---|---|---|
| **API Gateway** | AWS API Gateway + Kong | Rate limiting, auth, routing |
| **Service Mesh** | Istio + Envoy | mTLS, circuit breaking, observability |
| **Backend Services** | Node.js (Express/Fastify) | I/O bound, NPM ecosystem |
| **Transcoding** | FFmpeg + Python workers | Industry standard video encoding |
| **Primary DB** | PostgreSQL 16 | ACID for users/billing/catalog |
| **Time-Series / Wide-column** | Apache Cassandra 4 | Watch history, ratings |
| **Cache** | Redis 7 Cluster | Sessions, hot data, pub/sub |
| **Search** | Elasticsearch 8 | Full-text, faceted, fuzzy |
| **Message Queue** | Apache Kafka | Event streaming, decoupling |
| **Object Storage** | AWS S3 | Video files, durability |
| **CDN** | Netflix Open Connect | Own CDN, ISP co-location |
| **ML / Recommendations** | Python + TensorFlow | Model training + inference |
| **Vector Search** | Pinecone | Embedding similarity |
| **Analytics** | ClickHouse + Apache Spark | OLAP queries, batch jobs |
| **Container Orchestration** | Kubernetes (EKS) | Scaling, deployments |
| **CI/CD** | Spinnaker | Netflix's own CD platform |
| **Monitoring** | Prometheus + Grafana + Jaeger | Metrics, tracing |
| **Secrets** | AWS Secrets Manager | Credential management |
| **DRM** | Widevine + FairPlay + PlayReady | Content protection |

---

> **Interview Tip:** Always start with requirements → estimation → HLD → bottlenecks → LLD.
> Proactively call out trade-offs. Netflix prioritizes **availability** and **low latency** over
> **strong consistency**. Every design decision should reflect this.

---

*Document Version: 1.0 — FAANG Interview Level — JavaScript LLD*