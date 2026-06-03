# YouTube System Design — FAANG Level (HLD + LLD)

> **Scope:** End-to-end system design covering High-Level Design (HLD) and Low-Level Design (LLD) for a YouTube-scale video platform. LLD implementations are in JavaScript/Node.js.

---

## Table of Contents

1. [Requirements Clarification](#1-requirements-clarification)
2. [Capacity Estimation](#2-capacity-estimation)
3. [High-Level Design (HLD)](#3-high-level-design-hld)
   - 3.1 [Architecture Overview](#31-architecture-overview)
   - 3.2 [Core Services](#32-core-services)
   - 3.3 [Video Upload Pipeline](#33-video-upload-pipeline)
   - 3.4 [Video Streaming Pipeline](#34-video-streaming-pipeline)
   - 3.5 [CDN Strategy](#35-cdn-strategy)
   - 3.6 [Search & Recommendations](#36-search--recommendations)
   - 3.7 [Database Design](#37-database-design)
   - 3.8 [Caching Strategy](#38-caching-strategy)
   - 3.9 [Notification System](#39-notification-system)
   - 3.10 [Analytics Pipeline](#310-analytics-pipeline)
4. [Low-Level Design (LLD) in JavaScript](#4-low-level-design-lld-in-javascript)
   - 4.1 [Video Upload Service](#41-video-upload-service)
   - 4.2 [Video Transcoding Worker](#42-video-transcoding-worker)
   - 4.3 [Streaming Service with Adaptive Bitrate](#43-streaming-service-with-adaptive-bitrate)
   - 4.4 [Metadata Service](#44-metadata-service)
   - 4.5 [User & Auth Service](#45-user--auth-service)
   - 4.6 [Like / Dislike & Counter Service](#46-like--dislike--counter-service)
   - 4.7 [Comment Service](#47-comment-service)
   - 4.8 [Subscription & Notification Service](#48-subscription--notification-service)
   - 4.9 [Search Service](#49-search-service)
   - 4.10 [Recommendation Engine](#410-recommendation-engine)
   - 4.11 [Rate Limiter (Token Bucket)](#411-rate-limiter-token-bucket)
   - 4.12 [View Count (HyperLogLog + Redis)](#412-view-count-hyperloglog--redis)
5. [Data Models (Schemas)](#5-data-models-schemas)
6. [API Design (REST)](#6-api-design-rest)
7. [Fault Tolerance & Reliability](#7-fault-tolerance--reliability)
8. [Security Design](#8-security-design)
9. [Scalability Deep Dive](#9-scalability-deep-dive)
10. [Monitoring & Observability](#10-monitoring--observability)
11. [Trade-offs & Interview Tips](#11-trade-offs--interview-tips)

---

## 1. Requirements Clarification

### Functional Requirements

| Feature | Description |
|---|---|
| Video Upload | Users can upload videos up to 10 GB |
| Video Playback | Adaptive bitrate streaming (360p → 4K) |
| Search | Full-text search by title, description, tags |
| Recommendations | Personalized home feed & up-next |
| Like / Dislike | Atomic counters, idempotent per user |
| Comments | Threaded comments with replies |
| Subscriptions | Subscribe to channels, receive notifications |
| Trending | Real-time trending videos globally and by region |
| Live Streaming | Low-latency live video (out of scope for detailed LLD but mentioned in HLD) |
| Analytics | Views, watch time, demographics for creators |

### Non-Functional Requirements

| Property | Target |
|---|---|
| Availability | 99.99% uptime (≈ 52 min downtime/year) |
| Durability | No video data loss (3× replication minimum) |
| Read Latency | < 200ms for metadata; < 100ms first frame |
| Write Latency | Upload acknowledged within 2s; processing async |
| Consistency | Eventual consistency acceptable for counters |
| Strong Consistency | Required for auth, payments, subscriptions |
| Scalability | Handle 10× traffic spikes (Super Bowl, elections) |

---

## 2. Capacity Estimation

### Traffic Assumptions (YouTube scale, 2024)

```
Daily Active Users (DAU)          : 80 million
Videos uploaded per day           : 500,000 (~5.8 videos/second)
Average video size (raw)          : 500 MB
Average video size (post-encoded) : 150 MB per quality tier × 5 tiers = 750 MB total
Total video storage/day           : 500,000 × 750 MB = 375 TB/day
Video views per day               : 5 billion
Peak QPS (reads)                  : 5B / 86400 × 10 (peak factor) = ~578,000 QPS
Peak QPS (writes/uploads)         : 5.8 × 10 = ~60 QPS
```

### Storage Estimation

```
Raw upload retention (7 days)     : 375 TB × 7      = 2.6 PB
Processed video (permanent)       : 375 TB × 365    = 136 PB/year
Thumbnails (1 MB each, 5/video)   : 500,000 × 5 MB  = 2.5 TB/day
Metadata (DB)                     : ~200 bytes/video × 500K = 100 MB/day (trivial)
```

### Bandwidth Estimation

```
Avg video bitrate (adaptive avg)  : 2 Mbps
Concurrent viewers (peak)         : 5M
Total egress bandwidth            : 5M × 2 Mbps = 10 Tbps  → served 99% by CDN
Origin server bandwidth           : ~100 Gbps (1% CDN miss)
```

---

## 3. High-Level Design (HLD)

### 3.1 Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              CLIENT LAYER                                   │
│     Web (React)    iOS / Android App    Smart TV    Embedded Players        │
└────────────────────────────┬────────────────────────────────────────────────┘
                             │ HTTPS
┌────────────────────────────▼────────────────────────────────────────────────┐
│                         CDN  (CloudFront / Akamai / Fastly)                 │
│   Static Assets  │  Video Segments (HLS/DASH)  │  Thumbnails  │  API Cache  │
└──────────────────┬──────────────────────────────────────────────────────────┘
                   │ Cache Miss
┌──────────────────▼──────────────────────────────────────────────────────────┐
│                        API GATEWAY / Load Balancer                          │
│              Rate Limiting │ Auth (JWT verify) │ SSL Termination            │
└───┬──────────┬─────────────┬──────────┬────────┬────────────────────────────┘
    │          │             │          │        │
    ▼          ▼             ▼          ▼        ▼
[Upload]  [Stream]     [Metadata]  [Search]  [User/Auth]
Service   Service       Service    Service    Service
    │          │             │          │        │
    │          │        ┌────▼────┐      │        │
    │          │        │Postgres │      │        │
    │          │        │(Primary)│      │        │
    │          │        └────┬────┘      │        │
    │          │             │ Replica   │        │
    │     ┌────▼────┐   ┌────▼────┐      │        │
    │     │  Redis  │   │Postgres │      │        │
    │     │ Cluster │   │(Read)   │      │        │
    │     └─────────┘   └─────────┘      │        │
    │                                   ▼        ▼
    ▼                              [Elastic  [Redis
[Object                             Search]   Sessions]
 Storage]
[S3/GCS]
    │
    ▼
[Message Queue]
[Kafka / SQS]
    │
    ▼
[Transcoding
 Workers]
[FFmpeg Fleet]
    │
    ▼
[Object Storage]
[Video Segments]
[Manifests]
```

### 3.2 Core Services

| Service | Responsibility | Tech Stack |
|---|---|---|
| **Upload Service** | Chunked upload, virus scan, durability | Node.js, S3 multipart, Kafka |
| **Transcoding Service** | FFmpeg fleet, multi-resolution encoding | Python workers, Kubernetes Jobs |
| **Streaming Service** | Serve HLS/DASH manifests & segments | Nginx, Node.js, Redis |
| **Metadata Service** | CRUD for video info, tags, categories | Node.js, PostgreSQL, Redis |
| **User/Auth Service** | Registration, OAuth, JWT, sessions | Node.js, PostgreSQL, Redis |
| **Search Service** | Full-text search, autocomplete | Elasticsearch, Redis |
| **Recommendation Service** | Personalized feed, collaborative filtering | Python, TensorFlow, Kafka |
| **Comment Service** | Threaded comments, moderation | Node.js, PostgreSQL, Cassandra |
| **Like/Counter Service** | Atomic like/dislike, view counts | Node.js, Redis, Kafka |
| **Notification Service** | Push notifications, email, in-app | Node.js, Firebase FCM, SQS |
| **Analytics Service** | Watch time, demographics, creator studio | Kafka, Spark, ClickHouse |
| **CDN Manager** | Purge, prefetch, edge logic | Cloudflare Workers, Lambda@Edge |

### 3.3 Video Upload Pipeline

```
Client
  │
  │ 1. POST /upload/initiate  (filename, size, content-type)
  ▼
Upload Service
  │ Returns: { uploadId, presignedUrls[] }  ← S3 Multipart PresignedURL
  │
Client
  │ 2. PUT each 5-100MB chunk directly to S3 using presigned URLs
  │    (bypasses our servers → saves bandwidth)
  ▼
Amazon S3 (raw-uploads bucket)
  │
  │ 3. S3 Event → Kafka topic: "video.uploaded.raw"
  ▼
Upload Service
  │ 4. Validates: file type, MIME, max size, ownership
  │ 5. Virus scan (ClamAV async worker)
  │ 6. Persist metadata: status=PROCESSING
  │
  │ 7. Publishes to Kafka: "video.transcode.requested"
  ▼
Transcoding Workers (Kubernetes Job per video)
  │ 8. Pull raw video from S3
  │ 9. FFmpeg: encode to 360p, 480p, 720p, 1080p, 1440p, 2160p
  │ 10. Generate HLS segments (.ts files) + master manifest (.m3u8)
  │ 11. Generate DASH segments (.mp4 fragments) + MPD manifest
  │ 12. Generate animated thumbnail, static thumbnail
  │ 13. Extract audio track for captions (Whisper ASR)
  │ 14. Upload all artifacts to S3 (processed-videos bucket)
  │ 15. Publish to Kafka: "video.transcode.completed"
  ▼
Metadata Service
  │ 16. Update video status: READY
  │ 17. Invalidate CDN cache for this video
  │ 18. Trigger notification to channel subscribers
```

### 3.4 Video Streaming Pipeline

```
Client Request: GET /watch?v=abc123
  │
  ▼
CDN Edge Node (nearest PoP)
  │  Cache HIT?  → serve immediately (< 10ms)
  │  Cache MISS? → fetch from origin
  ▼
Streaming Service (Origin)
  │ 1. Authenticate (public vs private/age-restricted)
  │ 2. Fetch manifest location from Redis/DB
  │ 3. Return master manifest URL (signed, time-limited)
  ▼
Client (Video Player: hls.js / ExoPlayer / AVPlayer)
  │ 1. Download master.m3u8
  │ 2. Select quality tier based on bandwidth estimation
  │ 3. Download media playlist (e.g., 1080p.m3u8)
  │ 4. Download individual .ts segments (2-6 seconds each)
  │ 5. Adaptive: switch quality up/down based on buffer health
  │ 6. Report playback events → Analytics Service
```

### 3.5 CDN Strategy

**Cache Hierarchy:**
```
Browser Cache (1 min for manifests, 24h for segments)
   └─► CDN Edge PoP (geo-distributed, 200+ locations)
          └─► CDN Regional Cache
                 └─► Origin Servers
```

**Cache Keys:**
- Video segments: `{videoId}/{quality}/{segmentNumber}.ts` → Cache: 365 days (immutable)
- Master manifests: `{videoId}/master.m3u8` → Cache: 30 seconds (updates for live)
- Thumbnails: `{videoId}/thumb_{size}.jpg` → Cache: 7 days
- API responses: `GET /videos/{id}` → Cache: 60 seconds (via Surrogate-Key headers)

**CDN Invalidation:**
- On video update/delete: purge by Surrogate-Key tag `video:{id}`
- On bulk operation: tag-based batch purge (avoid URL-by-URL purge)

**Push vs Pull CDN:**
- Segments: Pull CDN (too many files to pre-push)
- Popular video prefetch: Predictive push based on trending signals

### 3.6 Search & Recommendations

**Search Stack:**
```
User types → Autocomplete Service (Redis Sorted Sets, prefix search)
                │
User submits → Elasticsearch Cluster
                │ Indices: videos (title, desc, tags, transcript)
                │          channels (name, description)
                │ Features: BM25 ranking, phrase matching, fuzzy search
                │           Personalization boost (watch history re-ranking)
                │
Results → Metadata Service (enrich with views, likes, thumbnails)
```

**Recommendation Stack:**
```
User opens app
  │
  ▼
Recommendation Service
  │
  ├─► Collaborative Filtering: "users like you watched X"
  │   (Matrix Factorization, ALS algorithm, offline batch)
  │
  ├─► Content-Based Filtering: video embedding similarity
  │   (Video2Vec, title/tag embeddings via BERT)
  │
  ├─► Session-Based: real-time signals from current session
  │   (Kafka streams, online learning)
  │
  ├─► Candidate Generation → Ranking → Re-ranking
  │   (100K candidates → 500 ranked → 50 shown)
  │
  └─► Diversity & Freshness enforcement
      (suppress already-watched, inject new content)
```

### 3.7 Database Design

**Primary Databases:**

| Data | Database | Reasoning |
|---|---|---|
| Users, Channels, Videos metadata | **PostgreSQL** | ACID, complex joins, strong consistency |
| Comments (high write, append-heavy) | **Cassandra** | Wide-column, partition by videoId |
| Sessions, hot cache | **Redis** | Sub-ms reads, TTL, atomic ops |
| Search index | **Elasticsearch** | Inverted index, full-text |
| Analytics events | **ClickHouse** | Columnar, OLAP, 10B+ rows |
| View counts (approximate) | **Redis HyperLogLog** | Memory-efficient cardinality |
| Subscriptions graph | **PostgreSQL** + **Graph DB (optional)** | Fan-out on write |

**Sharding Strategy:**

- Videos table: shard by `video_id` (hash-based)
- Comments: partition by `video_id` in Cassandra (natural partition key)
- Users: shard by `user_id` (hash-based)
- Cross-shard queries avoided by denormalization and read replicas

### 3.8 Caching Strategy

**Cache Layers:**

```
L1: Application-level in-memory cache (Node.js LRU, TTL 30s)
L2: Redis Cluster (distributed, TTL 300s for metadata)
L3: CDN (video segments, static assets)
L4: Database read replicas (PostgreSQL replicas for read queries)
```

**Cache Invalidation Patterns:**

- **Write-through**: metadata updates → write to DB AND Redis simultaneously
- **Cache-aside (Lazy)**: on cache miss, fetch from DB, store in Redis
- **TTL-based expiry**: counters (views, likes) have short TTL, re-fetched periodically
- **Pub/Sub invalidation**: on video update, publish to Redis channel, all instances drop cache

**Hot Key Problem (Celebrity/Viral videos):**
- Replicate hot keys across multiple Redis nodes (read from random replica)
- Local in-process LRU as L1 shield
- CDN absorbs 99% of video traffic anyway

### 3.9 Notification System

```
Event Sources (Kafka topics):
  - video.uploaded          → notify subscribers
  - video.liked             → notify uploader (batched)
  - comment.created         → notify video owner + mentioned users
  - channel.subscribed      → notify channel owner

Notification Service
  │
  ├─► Fan-out Worker
  │   (For channel with 10M subscribers: batch, paginate, async)
  │
  ├─► Delivery Channels:
  │   ├─ Push (Firebase FCM / APNs)
  │   ├─ Email (SendGrid, batched digests)
  │   ├─ In-App (WebSocket / SSE for real-time badge)
  │   └─ SMS (Twilio, only for critical security events)
  │
  └─► Preferences Service
      (User can mute channels, choose frequency)
```

**Fan-out Strategy:**
- **Fan-out on Write** (push model): pre-compute feeds for all subscribers. Fast reads, expensive writes. Good for users with < 10K subscribers.
- **Fan-out on Read** (pull model): compute feed at read time. Slow reads, cheap writes. Good for mega-channels (10M+ subscribers, celeb problem).
- **Hybrid**: push for normal users, pull for celebrity channels.

### 3.10 Analytics Pipeline

```
Client Player Events (every 30s heartbeat):
  { userId, videoId, watchedSeconds, quality, bufferingCount, exitPoint }
  │
  ▼
Analytics Ingest API (fire-and-forget, no auth)
  │
  ▼
Kafka (topic: analytics.playback.events)
  │
  ├─► Flink / Spark Streaming (real-time)
  │   ├─ Trending score update (every 5 min)
  │   ├─ Live concurrent viewers count
  │   └─ Anomaly detection (sudden spikes)
  │
  └─► Spark Batch (hourly / daily)
      ├─ Watch time reports
      ├─ Demographics breakdown
      ├─ Revenue attribution
      └─ Creator Studio dashboard
          │
          ▼
        ClickHouse (OLAP)
        Grafana / Tableau (visualization)
```

---

## 4. Low-Level Design (LLD) in JavaScript

### 4.1 Video Upload Service

```javascript
// upload-service/src/uploadController.js

const express = require('express');
const { S3Client, CreateMultipartUploadCommand, UploadPartCommand,
        CompleteMultipartUploadCommand, AbortMultipartUploadCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');
const { v4: uuidv4 } = require('uuid');
const kafka = require('./kafkaProducer');
const db = require('./db');
const redis = require('./redis');

const s3 = new S3Client({ region: 'us-east-1' });
const RAW_BUCKET = 'yt-raw-uploads';
const CHUNK_SIZE_MB = 50;
const MAX_FILE_SIZE_GB = 10;

class UploadController {
  /**
   * Step 1: Client calls this to initiate a multipart upload.
   * Returns uploadId and presigned URLs for each chunk.
   * Client uploads directly to S3 — our servers never touch the bytes.
   */
  async initiate(req, res) {
    const { filename, fileSize, contentType, title, description, tags } = req.body;
    const userId = req.user.id; // injected by auth middleware

    // Validation
    if (fileSize > MAX_FILE_SIZE_GB * 1024 * 1024 * 1024) {
      return res.status(400).json({ error: 'File exceeds 10 GB limit' });
    }

    const ALLOWED_TYPES = ['video/mp4', 'video/quicktime', 'video/x-msvideo', 'video/webm', 'video/x-matroska'];
    if (!ALLOWED_TYPES.includes(contentType)) {
      return res.status(400).json({ error: 'Unsupported video format' });
    }

    const videoId = uuidv4();
    const s3Key = `uploads/${userId}/${videoId}/raw`;

    // Create S3 multipart upload
    const createCmd = new CreateMultipartUploadCommand({
      Bucket: RAW_BUCKET,
      Key: s3Key,
      ContentType: contentType,
      Metadata: { videoId, userId },
    });
    const { UploadId } = await s3.send(createCmd);

    // Generate presigned URLs for each chunk
    const numChunks = Math.ceil(fileSize / (CHUNK_SIZE_MB * 1024 * 1024));
    const presignedUrls = await Promise.all(
      Array.from({ length: numChunks }, (_, i) =>
        getSignedUrl(s3, new UploadPartCommand({
          Bucket: RAW_BUCKET,
          Key: s3Key,
          UploadId,
          PartNumber: i + 1,
        }), { expiresIn: 3600 }) // 1 hour to upload each part
      )
    );

    // Save video record with PENDING status
    await db.videos.create({
      id: videoId,
      userId,
      title: title || filename,
      description: description || '',
      tags: tags || [],
      s3Key,
      s3UploadId: UploadId,
      status: 'PENDING',
      fileSize,
      createdAt: new Date(),
    });

    // Store uploadId in Redis for 24h (cleanup cron uses this)
    await redis.setex(`upload:${videoId}:uploadId`, 86400, UploadId);

    return res.status(200).json({ videoId, uploadId: UploadId, presignedUrls });
  }

  /**
   * Step 2: Called after client finishes uploading all chunks.
   * Completes the multipart upload and kicks off transcoding.
   */
  async complete(req, res) {
    const { videoId, parts } = req.body;
    // parts: [{ PartNumber: 1, ETag: '...' }, ...]
    const userId = req.user.id;

    const video = await db.videos.findOne({ where: { id: videoId, userId } });
    if (!video) return res.status(404).json({ error: 'Video not found' });
    if (video.status !== 'PENDING') return res.status(409).json({ error: 'Upload already completed' });

    // Complete multipart upload on S3
    await s3.send(new CompleteMultipartUploadCommand({
      Bucket: RAW_BUCKET,
      Key: video.s3Key,
      UploadId: video.s3UploadId,
      MultipartUpload: { Parts: parts },
    }));

    // Update status
    await db.videos.update({ status: 'UPLOADED' }, { where: { id: videoId } });

    // Publish transcoding job to Kafka
    await kafka.publish('video.transcode.requested', {
      videoId,
      userId,
      s3Key: video.s3Key,
      timestamp: Date.now(),
    });

    return res.status(200).json({ videoId, status: 'PROCESSING' });
  }

  /**
   * Abort an in-progress upload (e.g. user cancels)
   */
  async abort(req, res) {
    const { videoId } = req.params;
    const userId = req.user.id;

    const video = await db.videos.findOne({ where: { id: videoId, userId } });
    if (!video) return res.status(404).json({ error: 'Not found' });

    await s3.send(new AbortMultipartUploadCommand({
      Bucket: RAW_BUCKET,
      Key: video.s3Key,
      UploadId: video.s3UploadId,
    }));

    await db.videos.update({ status: 'ABORTED' }, { where: { id: videoId } });
    return res.status(200).json({ message: 'Upload aborted' });
  }
}

module.exports = new UploadController();
```

---

### 4.2 Video Transcoding Worker

```javascript
// transcoding-service/src/transcodingWorker.js

const { KafkaConsumer } = require('./kafkaConsumer');
const { S3Client, GetObjectCommand, PutObjectCommand } = require('@aws-sdk/client-s3');
const { spawn } = require('child_process');
const fs = require('fs').promises;
const path = require('path');
const db = require('./db');
const kafka = require('./kafkaProducer');

const s3 = new S3Client({ region: 'us-east-1' });
const PROCESSED_BUCKET = 'yt-processed-videos';

const QUALITY_PROFILES = [
  { name: '360p',  width: 640,  height: 360,  videoBitrate: '500k',  audioBitrate: '96k',  crf: 28 },
  { name: '480p',  width: 854,  height: 480,  videoBitrate: '1000k', audioBitrate: '128k', crf: 26 },
  { name: '720p',  width: 1280, height: 720,  videoBitrate: '2500k', audioBitrate: '128k', crf: 24 },
  { name: '1080p', width: 1920, height: 1080, videoBitrate: '5000k', audioBitrate: '192k', crf: 22 },
  { name: '1440p', width: 2560, height: 1440, videoBitrate: '10000k',audioBitrate: '192k', crf: 20 },
  { name: '2160p', width: 3840, height: 2160, videoBitrate: '20000k',audioBitrate: '256k', crf: 18 },
];

class TranscodingWorker {
  constructor() {
    this.consumer = new KafkaConsumer('video.transcode.requested', this.process.bind(this));
  }

  async process(message) {
    const { videoId, s3Key } = JSON.parse(message.value);
    const workDir = `/tmp/transcode/${videoId}`;

    try {
      await fs.mkdir(workDir, { recursive: true });

      // 1. Download raw video
      await this.downloadFromS3(s3Key, `${workDir}/input.mp4`);

      // 2. Probe video metadata
      const metadata = await this.probeVideo(`${workDir}/input.mp4`);

      // 3. Determine applicable quality profiles (don't upscale)
      const profiles = QUALITY_PROFILES.filter(p => p.height <= metadata.height);

      // 4. Encode each quality tier (in parallel with concurrency limit)
      await this.encodeAllQualities(workDir, videoId, profiles);

      // 5. Generate HLS master manifest
      await this.generateHLSManifest(workDir, videoId, profiles);

      // 6. Generate thumbnails (at 10%, 30%, 50%, 70%, 90% of duration)
      await this.generateThumbnails(workDir, videoId, metadata.duration);

      // 7. Upload all artifacts to processed S3 bucket
      await this.uploadArtifacts(workDir, videoId, profiles);

      // 8. Update DB: status = READY, store manifest path
      await db.videos.update({
        status: 'READY',
        manifestPath: `${videoId}/master.m3u8`,
        duration: metadata.duration,
        availableQualities: profiles.map(p => p.name),
        processedAt: new Date(),
      }, { where: { id: videoId } });

      // 9. Emit success event
      await kafka.publish('video.transcode.completed', { videoId, timestamp: Date.now() });

    } catch (err) {
      console.error(`Transcoding failed for ${videoId}:`, err);
      await db.videos.update({ status: 'FAILED', errorMessage: err.message }, { where: { id: videoId } });
      await kafka.publish('video.transcode.failed', { videoId, error: err.message });
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  }

  async probeVideo(inputPath) {
    return new Promise((resolve, reject) => {
      const probe = spawn('ffprobe', [
        '-v', 'quiet', '-print_format', 'json', '-show_streams', '-show_format', inputPath
      ]);
      let output = '';
      probe.stdout.on('data', d => output += d);
      probe.on('close', code => {
        if (code !== 0) return reject(new Error('ffprobe failed'));
        const data = JSON.parse(output);
        const video = data.streams.find(s => s.codec_type === 'video');
        resolve({
          width: video.width,
          height: video.height,
          duration: parseFloat(data.format.duration),
          codec: video.codec_name,
        });
      });
    });
  }

  async encodeAllQualities(workDir, videoId, profiles) {
    // Encode with concurrency limit of 2 (CPU-bound)
    const concurrency = 2;
    for (let i = 0; i < profiles.length; i += concurrency) {
      const batch = profiles.slice(i, i + concurrency);
      await Promise.all(batch.map(profile => this.encodeQuality(workDir, profile)));
    }
  }

  async encodeQuality(workDir, profile) {
    const outputDir = `${workDir}/hls_${profile.name}`;
    await fs.mkdir(outputDir, { recursive: true });

    return new Promise((resolve, reject) => {
      const ffmpeg = spawn('ffmpeg', [
        '-i', `${workDir}/input.mp4`,
        '-vf', `scale=${profile.width}:${profile.height}:force_original_aspect_ratio=decrease`,
        '-c:v', 'libx264',
        '-preset', 'fast',         // balance speed/compression
        '-crf', String(profile.crf),
        '-maxrate', profile.videoBitrate,
        '-bufsize', `${parseInt(profile.videoBitrate) * 2}k`,
        '-c:a', 'aac',
        '-b:a', profile.audioBitrate,
        '-hls_time', '6',          // 6-second segments
        '-hls_playlist_type', 'vod',
        '-hls_segment_filename', `${outputDir}/segment_%05d.ts`,
        `${outputDir}/playlist.m3u8`,
        '-y'
      ]);
      ffmpeg.on('close', code => code === 0 ? resolve() : reject(new Error(`FFmpeg failed for ${profile.name}`)));
    });
  }

  async generateHLSManifest(workDir, videoId, profiles) {
    const bandwidthMap = { '360p': 500000, '480p': 1000000, '720p': 2500000,
                            '1080p': 5000000, '1440p': 10000000, '2160p': 20000000 };
    const resolutionMap = { '360p': '640x360', '480p': '854x480', '720p': '1280x720',
                             '1080p': '1920x1080', '1440p': '2560x1440', '2160p': '3840x2160' };

    let manifest = '#EXTM3U\n#EXT-X-VERSION:3\n\n';
    for (const profile of profiles) {
      manifest += `#EXT-X-STREAM-INF:BANDWIDTH=${bandwidthMap[profile.name]},` +
                  `RESOLUTION=${resolutionMap[profile.name]},` +
                  `CODECS="avc1.42e01e,mp4a.40.2"\n`;
      manifest += `hls_${profile.name}/playlist.m3u8\n\n`;
    }
    await fs.writeFile(`${workDir}/master.m3u8`, manifest);
  }

  async generateThumbnails(workDir, videoId, duration) {
    const timestamps = [0.1, 0.3, 0.5, 0.7, 0.9].map(f => duration * f);
    await Promise.all(timestamps.map((ts, i) =>
      new Promise((resolve, reject) => {
        const ffmpeg = spawn('ffmpeg', [
          '-ss', String(ts), '-i', `${workDir}/input.mp4`,
          '-vframes', '1', '-vf', 'scale=1280:720',
          `${workDir}/thumb_${i}.jpg`, '-y'
        ]);
        ffmpeg.on('close', code => code === 0 ? resolve() : reject());
      })
    ));
  }

  async uploadArtifacts(workDir, videoId, profiles) {
    const uploads = [];

    // Upload master manifest
    uploads.push(this.uploadToS3(`${workDir}/master.m3u8`, `${videoId}/master.m3u8`, 'application/vnd.apple.mpegurl'));

    // Upload each quality tier
    for (const profile of profiles) {
      const dir = `${workDir}/hls_${profile.name}`;
      const files = await fs.readdir(dir);
      for (const file of files) {
        const contentType = file.endsWith('.m3u8') ? 'application/vnd.apple.mpegurl' : 'video/MP2T';
        uploads.push(this.uploadToS3(`${dir}/${file}`, `${videoId}/hls_${profile.name}/${file}`, contentType));
      }
    }

    // Upload thumbnails
    for (let i = 0; i < 5; i++) {
      uploads.push(this.uploadToS3(`${workDir}/thumb_${i}.jpg`, `${videoId}/thumbs/thumb_${i}.jpg`, 'image/jpeg'));
    }

    await Promise.all(uploads);
  }

  async downloadFromS3(key, destPath) {
    const { Body } = await s3.send(new GetObjectCommand({ Bucket: 'yt-raw-uploads', Key: key }));
    const fileHandle = await fs.open(destPath, 'w');
    await new Promise((resolve, reject) => {
      const writeStream = fileHandle.createWriteStream();
      Body.pipe(writeStream);
      writeStream.on('finish', resolve);
      writeStream.on('error', reject);
    });
    await fileHandle.close();
  }

  async uploadToS3(localPath, s3Key, contentType) {
    const body = await fs.readFile(localPath);
    await s3.send(new PutObjectCommand({
      Bucket: PROCESSED_BUCKET,
      Key: s3Key,
      Body: body,
      ContentType: contentType,
      CacheControl: s3Key.includes('.m3u8') ? 'max-age=30' : 'max-age=31536000, immutable',
    }));
  }
}

module.exports = TranscodingWorker;
```

---

### 4.3 Streaming Service with Adaptive Bitrate

```javascript
// streaming-service/src/streamController.js

const express = require('express');
const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');
const redis = require('./redis');
const db = require('./db');

const s3 = new S3Client({ region: 'us-east-1' });
const PROCESSED_BUCKET = 'yt-processed-videos';
const CDN_BASE = 'https://cdn.youtube-clone.com';
const SIGNED_URL_TTL = 3600; // 1 hour

class StreamController {
  /**
   * GET /videos/:videoId/manifest
   * Returns a redirect to the CDN-hosted master manifest.
   * For private/premium videos, returns a short-lived signed URL.
   */
  async getManifest(req, res) {
    const { videoId } = req.params;

    // Check Redis cache first (metadata cache)
    const cacheKey = `video:manifest:${videoId}`;
    let video = await redis.getJSON(cacheKey);

    if (!video) {
      video = await db.videos.findOne({
        where: { id: videoId, status: 'READY' },
        attributes: ['id', 'userId', 'visibility', 'manifestPath', 'availableQualities']
      });
      if (!video) return res.status(404).json({ error: 'Video not found' });
      await redis.setJSON(cacheKey, video, 300); // cache 5 minutes
    }

    // Access control
    if (video.visibility === 'PRIVATE') {
      const userId = req.user?.id;
      if (!userId || userId !== video.userId) {
        return res.status(403).json({ error: 'Access denied' });
      }
    }

    if (video.visibility === 'UNLISTED') {
      // Allow access via direct link only — no search indexing
      // No additional auth needed, link sharing is intentional
    }

    // For premium/DRM content: generate signed URL with time window
    if (video.visibility === 'PREMIUM') {
      const signedUrl = await getSignedUrl(s3, new GetObjectCommand({
        Bucket: PROCESSED_BUCKET,
        Key: video.manifestPath,
      }), { expiresIn: SIGNED_URL_TTL });
      return res.redirect(302, signedUrl);
    }

    // Public video: redirect to CDN
    const manifestUrl = `${CDN_BASE}/${video.manifestPath}`;
    res.set({
      'Cache-Control': 'no-cache',           // manifest changes for live; short TTL
      'Access-Control-Allow-Origin': '*',
      'X-Video-Id': videoId,
    });
    return res.redirect(302, manifestUrl);
  }

  /**
   * GET /videos/:videoId/segment/:quality/:segmentFile
   * Proxies segment requests for access-controlled content.
   * Public videos are served directly from CDN without hitting this endpoint.
   */
  async getSegment(req, res) {
    const { videoId, quality, segmentFile } = req.params;

    // Verify access
    const video = await db.videos.findOne({ where: { id: videoId } });
    if (!video || video.status !== 'READY') return res.status(404).send();
    if (video.visibility === 'PRIVATE' && req.user?.id !== video.userId) {
      return res.status(403).send();
    }

    const s3Key = `${videoId}/hls_${quality}/${segmentFile}`;
    const command = new GetObjectCommand({ Bucket: PROCESSED_BUCKET, Key: s3Key });

    try {
      const { Body, ContentType, ContentLength } = await s3.send(command);
      res.set({
        'Content-Type': ContentType || 'video/MP2T',
        'Content-Length': ContentLength,
        'Cache-Control': 'max-age=86400',
        'Accept-Ranges': 'bytes',
      });
      Body.pipe(res);
    } catch (err) {
      if (err.name === 'NoSuchKey') return res.status(404).send();
      throw err;
    }
  }

  /**
   * POST /videos/:videoId/playback-event
   * Heartbeat from player. Async — fire and forget, never block playback.
   */
  async recordPlaybackEvent(req, res) {
    const { videoId } = req.params;
    const { watchedSeconds, quality, bufferingCount, exitPoint, sessionId } = req.body;
    const userId = req.user?.id || 'anonymous';

    // Async publish to Kafka — never await in hot path
    setImmediate(async () => {
      await kafka.publish('analytics.playback.events', {
        videoId, userId, sessionId, watchedSeconds,
        quality, bufferingCount, exitPoint,
        timestamp: Date.now(),
      });
    });

    // Increment view count in Redis (approximate, batched flush to DB)
    await redis.incr(`views:${videoId}`);

    return res.status(204).send(); // No content — fast response
  }
}

module.exports = new StreamController();
```

---

### 4.4 Metadata Service

```javascript
// metadata-service/src/metadataService.js

const db = require('./db');
const redis = require('./redis');
const elasticsearch = require('./esClient');

class MetadataService {
  async getVideo(videoId) {
    const cacheKey = `video:meta:${videoId}`;
    const cached = await redis.getJSON(cacheKey);
    if (cached) return cached;

    const video = await db.videos.findOne({
      where: { id: videoId, status: 'READY' },
      include: [{ model: db.users, as: 'channel', attributes: ['id', 'username', 'avatarUrl', 'subscriberCount'] }]
    });
    if (!video) return null;

    const enriched = {
      ...video.toJSON(),
      viewCount: await this.getViewCount(videoId),
      likeCount: await this.getLikeCount(videoId),
    };

    await redis.setJSON(cacheKey, enriched, 60); // 1 min TTL
    return enriched;
  }

  async updateVideo(videoId, userId, updates) {
    const allowedFields = ['title', 'description', 'tags', 'categoryId', 'visibility', 'thumbnailIndex'];
    const sanitized = Object.fromEntries(
      Object.entries(updates).filter(([k]) => allowedFields.includes(k))
    );

    const [count] = await db.videos.update(sanitized, { where: { id: videoId, userId } });
    if (count === 0) throw new Error('Video not found or permission denied');

    // Invalidate caches
    await redis.del(`video:meta:${videoId}`);
    await redis.del(`video:manifest:${videoId}`);

    // Re-index in Elasticsearch
    await this.indexVideo(videoId);
  }

  async indexVideo(videoId) {
    const video = await db.videos.findOne({ where: { id: videoId } });
    if (!video) return;

    await elasticsearch.index({
      index: 'videos',
      id: videoId,
      body: {
        title: video.title,
        description: video.description,
        tags: video.tags,
        channelId: video.userId,
        publishedAt: video.processedAt,
        viewCount: video.viewCount,
        likeCount: video.likeCount,
        duration: video.duration,
        categoryId: video.categoryId,
        visibility: video.visibility,
        // Text for autocomplete
        suggest: {
          input: [video.title, ...video.tags],
          weight: Math.log10(video.viewCount + 1), // boost popular videos
        }
      }
    });
  }

  async getViewCount(videoId) {
    // Redis holds approximate in-memory count; DB holds exact persisted count
    const redisCount = await redis.get(`views:${videoId}`);
    if (redisCount !== null) return parseInt(redisCount);

    const video = await db.videos.findOne({ where: { id: videoId }, attributes: ['viewCount'] });
    const count = video?.viewCount || 0;
    await redis.set(`views:${videoId}`, count, 'EX', 3600);
    return count;
  }

  async getLikeCount(videoId) {
    const cached = await redis.get(`likes:${videoId}`);
    if (cached !== null) return parseInt(cached);

    const count = await db.videoLikes.count({ where: { videoId, type: 'LIKE' } });
    await redis.set(`likes:${videoId}`, count, 'EX', 300);
    return count;
  }
}

// Periodic job: flush Redis view counts to DB (runs every 5 minutes)
async function flushViewCounts() {
  const keys = await redis.keys('views:*');
  for (const key of keys) {
    const videoId = key.split(':')[1];
    const count = await redis.getdel(key);
    if (count) {
      await db.videos.increment({ viewCount: parseInt(count) }, { where: { id: videoId } });
    }
  }
}

module.exports = { MetadataService: new MetadataService(), flushViewCounts };
```

---

### 4.5 User & Auth Service

```javascript
// auth-service/src/authService.js

const bcrypt = require('bcrypt');
const jwt = require('jsonwebtoken');
const { v4: uuidv4 } = require('uuid');
const db = require('./db');
const redis = require('./redis');

const SALT_ROUNDS = 12;
const ACCESS_TOKEN_TTL = '15m';
const REFRESH_TOKEN_TTL = 60 * 60 * 24 * 30; // 30 days in seconds

class AuthService {
  async register(email, password, username) {
    // Uniqueness check
    const existing = await db.users.findOne({ where: { email } });
    if (existing) throw new Error('EMAIL_TAKEN');

    const usernameExists = await db.users.findOne({ where: { username } });
    if (usernameExists) throw new Error('USERNAME_TAKEN');

    const passwordHash = await bcrypt.hash(password, SALT_ROUNDS);
    const user = await db.users.create({
      id: uuidv4(),
      email,
      username,
      passwordHash,
      role: 'USER',
      createdAt: new Date(),
    });

    return this.issueTokens(user);
  }

  async login(email, password) {
    const user = await db.users.findOne({ where: { email } });
    if (!user) throw new Error('INVALID_CREDENTIALS');

    // Timing-safe compare (bcrypt handles this internally)
    const valid = await bcrypt.compare(password, user.passwordHash);
    if (!valid) throw new Error('INVALID_CREDENTIALS');

    return this.issueTokens(user);
  }

  async issueTokens(user) {
    const accessToken = jwt.sign(
      { sub: user.id, username: user.username, role: user.role },
      process.env.JWT_SECRET,
      { expiresIn: ACCESS_TOKEN_TTL, algorithm: 'HS256' }
    );

    const refreshToken = uuidv4();
    await redis.setex(
      `refresh:${refreshToken}`,
      REFRESH_TOKEN_TTL,
      JSON.stringify({ userId: user.id, issuedAt: Date.now() })
    );

    return { accessToken, refreshToken, expiresIn: 900 };
  }

  async refresh(refreshToken) {
    const data = await redis.get(`refresh:${refreshToken}`);
    if (!data) throw new Error('INVALID_REFRESH_TOKEN');

    const { userId } = JSON.parse(data);
    const user = await db.users.findByPk(userId);
    if (!user || user.isBanned) throw new Error('USER_NOT_FOUND');

    // Rotate refresh token (invalidate old, issue new)
    await redis.del(`refresh:${refreshToken}`);
    return this.issueTokens(user);
  }

  async logout(refreshToken) {
    await redis.del(`refresh:${refreshToken}`);
  }

  // Middleware for Express
  verifyAccessToken(req, res, next) {
    const authHeader = req.headers.authorization;
    if (!authHeader?.startsWith('Bearer ')) {
      return res.status(401).json({ error: 'Missing token' });
    }
    try {
      const payload = jwt.verify(authHeader.slice(7), process.env.JWT_SECRET);
      req.user = { id: payload.sub, username: payload.username, role: payload.role };
      next();
    } catch (e) {
      return res.status(401).json({ error: 'Invalid or expired token' });
    }
  }
}

module.exports = new AuthService();
```

---

### 4.6 Like / Dislike & Counter Service

```javascript
// engagement-service/src/likeService.js

/**
 * Like/Dislike system requirements:
 * - Idempotent: toggling like→like removes the like
 * - Mutual exclusion: can't like AND dislike the same video
 * - Atomic: no race conditions on counter updates
 * - Eventual consistency acceptable for public counts
 */

const db = require('./db');
const redis = require('./redis');
const kafka = require('./kafkaProducer');

class LikeService {
  /**
   * Toggle like. Returns new state.
   * Uses a DB transaction for correctness + Redis for hot counter.
   */
  async toggleLike(userId, videoId, type) {
    // type: 'LIKE' or 'DISLIKE'
    if (!['LIKE', 'DISLIKE'].includes(type)) throw new Error('Invalid type');

    return await db.transaction(async (tx) => {
      const existing = await db.videoLikes.findOne({
        where: { userId, videoId },
        lock: tx.LOCK.UPDATE, // row-level lock prevents race condition
        transaction: tx,
      });

      let action;

      if (!existing) {
        // No prior engagement → create
        await db.videoLikes.create({ userId, videoId, type }, { transaction: tx });
        action = `${type}_ADDED`;
      } else if (existing.type === type) {
        // Same type → toggle off (remove)
        await existing.destroy({ transaction: tx });
        action = `${type}_REMOVED`;
      } else {
        // Switching from like to dislike or vice versa
        await existing.update({ type }, { transaction: tx });
        action = `${type}_SWITCHED`;
      }

      return action;
    }).then(async (action) => {
      // Update Redis counters (best-effort, non-blocking)
      await this.updateRedisCounters(videoId, action);

      // Emit event for analytics and notifications
      await kafka.publish('video.engagement', { userId, videoId, action, timestamp: Date.now() });

      const [likes, dislikes] = await Promise.all([
        this.getCount(videoId, 'LIKE'),
        this.getCount(videoId, 'DISLIKE'),
      ]);

      return { action, likes, dislikes };
    });
  }

  async updateRedisCounters(videoId, action) {
    const pipeline = redis.pipeline();
    switch (action) {
      case 'LIKE_ADDED':
        pipeline.incr(`likes:${videoId}`);
        break;
      case 'LIKE_REMOVED':
        pipeline.decr(`likes:${videoId}`);
        break;
      case 'DISLIKE_ADDED':
        pipeline.incr(`dislikes:${videoId}`);
        break;
      case 'DISLIKE_REMOVED':
        pipeline.decr(`dislikes:${videoId}`);
        break;
      case 'LIKE_SWITCHED':
        pipeline.decr(`likes:${videoId}`);
        pipeline.incr(`dislikes:${videoId}`);
        break;
      case 'DISLIKE_SWITCHED':
        pipeline.decr(`dislikes:${videoId}`);
        pipeline.incr(`likes:${videoId}`);
        break;
    }
    await pipeline.exec();
  }

  async getCount(videoId, type) {
    const key = type === 'LIKE' ? `likes:${videoId}` : `dislikes:${videoId}`;
    const cached = await redis.get(key);
    if (cached !== null) return parseInt(cached);

    const count = await db.videoLikes.count({ where: { videoId, type } });
    await redis.set(key, count, 'EX', 300);
    return count;
  }

  async getUserEngagement(userId, videoId) {
    const engagement = await db.videoLikes.findOne({ where: { userId, videoId } });
    return { liked: engagement?.type === 'LIKE', disliked: engagement?.type === 'DISLIKE' };
  }
}

module.exports = new LikeService();
```

---

### 4.7 Comment Service

```javascript
// comment-service/src/commentService.js

const { v4: uuidv4 } = require('uuid');
const db = require('./db'); // Cassandra client wrapper
const redis = require('./redis');
const kafka = require('./kafkaProducer');

/**
 * Comments are stored in Cassandra for:
 * - High write throughput
 * - Natural partition by videoId (all comments for a video on same node)
 * - Efficient pagination by timestamp (no OFFSET needed)
 *
 * Cassandra Schema:
 * CREATE TABLE comments (
 *   video_id UUID,
 *   comment_id TIMEUUID,
 *   parent_id UUID,        -- NULL for top-level
 *   user_id UUID,
 *   content TEXT,
 *   like_count INT,
 *   is_pinned BOOLEAN,
 *   is_deleted BOOLEAN,
 *   created_at TIMESTAMP,
 *   PRIMARY KEY (video_id, comment_id)
 * ) WITH CLUSTERING ORDER BY (comment_id DESC);
 */

class CommentService {
  async addComment(videoId, userId, content, parentId = null) {
    if (!content?.trim()) throw new Error('Empty comment');
    if (content.length > 10000) throw new Error('Comment too long');

    // Check if video exists (from Redis cache)
    const videoExists = await redis.exists(`video:meta:${videoId}`);
    if (!videoExists) {
      const video = await db.pg.videos.findOne({ where: { id: videoId, status: 'READY' } });
      if (!video) throw new Error('Video not found');
    }

    const commentId = uuidv4();
    const comment = {
      video_id: videoId,
      comment_id: commentId,
      parent_id: parentId || null,
      user_id: userId,
      content: content.trim(),
      like_count: 0,
      is_pinned: false,
      is_deleted: false,
      created_at: new Date(),
    };

    await db.cassandra.execute(
      `INSERT INTO comments (video_id, comment_id, parent_id, user_id, content, like_count, is_pinned, is_deleted, created_at)
       VALUES (?, ?, ?, ?, ?, 0, false, false, ?)`,
      [videoId, commentId, parentId, userId, content.trim(), new Date()],
      { prepare: true }
    );

    // Increment comment count (Redis, flushed to DB periodically)
    await redis.incr(`comment_count:${videoId}`);

    // Emit event for notifications
    await kafka.publish('comment.created', { videoId, commentId, userId, parentId });

    return comment;
  }

  /**
   * Paginated comment fetch using TIMEUUID cursor (no OFFSET anti-pattern)
   */
  async getComments(videoId, { cursor, limit = 20, sort = 'top' } = {}) {
    if (sort === 'top') {
      // Top comments: fetch from DB with like_count sort
      // Note: Cassandra doesn't support arbitrary ORDER BY, so top comments
      // are maintained in a Redis sorted set (score = likeCount)
      const topCommentIds = await redis.zrevrange(`top_comments:${videoId}`, 0, limit - 1);
      if (topCommentIds.length > 0) {
        return this.fetchCommentsByIds(videoId, topCommentIds);
      }
    }

    // Default: newest first using TIMEUUID cursor pagination
    let query = 'SELECT * FROM comments WHERE video_id = ?';
    const params = [videoId];

    if (cursor) {
      query += ' AND comment_id < ?';
      params.push(cursor);
    }
    query += ' AND parent_id = null AND is_deleted = false LIMIT ?';
    params.push(limit + 1);

    const result = await db.cassandra.execute(query, params, { prepare: true });
    const rows = result.rows;
    const hasMore = rows.length > limit;
    const comments = rows.slice(0, limit);

    return {
      comments,
      nextCursor: hasMore ? comments[comments.length - 1].comment_id : null,
      hasMore,
    };
  }

  async getReplies(videoId, parentCommentId, { limit = 20, cursor } = {}) {
    let query = 'SELECT * FROM comments WHERE video_id = ? AND parent_id = ?';
    const params = [videoId, parentCommentId];

    if (cursor) {
      query += ' AND comment_id > ?';
      params.push(cursor);
    }
    query += ' LIMIT ?';
    params.push(limit);

    const result = await db.cassandra.execute(query, params, { prepare: true });
    return result.rows;
  }

  async deleteComment(videoId, commentId, requestingUserId, requestingUserRole) {
    // Only comment author or video owner or admin can delete
    const result = await db.cassandra.execute(
      'SELECT user_id FROM comments WHERE video_id = ? AND comment_id = ?',
      [videoId, commentId], { prepare: true }
    );
    const comment = result.rows[0];
    if (!comment) throw new Error('Comment not found');

    const canDelete = comment.user_id === requestingUserId || requestingUserRole === 'ADMIN';
    if (!canDelete) throw new Error('Permission denied');

    // Soft delete
    await db.cassandra.execute(
      'UPDATE comments SET is_deleted = true WHERE video_id = ? AND comment_id = ?',
      [videoId, commentId], { prepare: true }
    );

    await redis.decr(`comment_count:${videoId}`);
  }
}

module.exports = new CommentService();
```

---

### 4.8 Subscription & Notification Service

```javascript
// notification-service/src/subscriptionService.js

const db = require('./db');
const redis = require('./redis');
const kafka = require('./kafkaProducer');
const fcm = require('./fcmClient');    // Firebase Cloud Messaging
const mailer = require('./mailer');   // SendGrid

class SubscriptionService {
  async subscribe(subscriberId, channelId) {
    if (subscriberId === channelId) throw new Error('Cannot subscribe to yourself');

    const [sub, created] = await db.subscriptions.findOrCreate({
      where: { subscriberId, channelId },
      defaults: { notificationType: 'ALL', createdAt: new Date() }
    });

    if (created) {
      // Increment subscriber count
      await db.users.increment({ subscriberCount: 1 }, { where: { id: channelId } });
      await redis.del(`channel:${channelId}`); // invalidate channel cache

      await kafka.publish('channel.subscribed', { subscriberId, channelId, timestamp: Date.now() });
    }

    return { subscribed: true, notificationType: sub.notificationType };
  }

  async unsubscribe(subscriberId, channelId) {
    const deleted = await db.subscriptions.destroy({ where: { subscriberId, channelId } });
    if (deleted > 0) {
      await db.users.decrement({ subscriberCount: 1 }, { where: { id: channelId } });
      await redis.del(`channel:${channelId}`);
    }
    return { subscribed: false };
  }

  /**
   * Fan-out: Notify all subscribers when a channel uploads.
   * Uses cursor-based pagination to avoid loading all subscribers in memory.
   * For channels > 1M subscribers, this is processed by multiple workers.
   */
  async fanOutNewVideoNotification(channelId, videoId, videoTitle) {
    const channel = await db.users.findByPk(channelId);
    const PAGE_SIZE = 1000;
    let lastId = null;
    let processed = 0;

    while (true) {
      const whereClause = { channelId, notificationType: ['ALL', 'PERSONALIZED'] };
      if (lastId) whereClause.id = { [db.Op.gt]: lastId };

      const subs = await db.subscriptions.findAll({
        where: whereClause,
        order: [['id', 'ASC']],
        limit: PAGE_SIZE,
        include: [{ model: db.users, as: 'subscriber', attributes: ['id', 'fcmToken', 'email', 'notificationPrefs'] }]
      });

      if (subs.length === 0) break;

      // Batch notifications
      const pushTokens = subs.filter(s => s.subscriber.fcmToken).map(s => s.subscriber.fcmToken);
      const emailAddresses = subs.filter(s => s.subscriber.notificationPrefs?.email).map(s => s.subscriber.email);

      // Push notifications in batches of 500 (FCM limit)
      for (let i = 0; i < pushTokens.length; i += 500) {
        const batch = pushTokens.slice(i, i + 500);
        await fcm.sendMulticast({
          tokens: batch,
          notification: {
            title: `${channel.username} uploaded a new video`,
            body: videoTitle,
          },
          data: { videoId, channelId, type: 'NEW_VIDEO' },
        }).catch(console.error); // Non-critical — best effort
      }

      // Email: queue async batch (don't send inline)
      if (emailAddresses.length > 0) {
        await kafka.publish('notification.email.batch', {
          videoId, channelId, channelName: channel.username,
          videoTitle, recipients: emailAddresses,
        });
      }

      processed += subs.length;
      lastId = subs[subs.length - 1].id;

      if (subs.length < PAGE_SIZE) break;
    }

    console.log(`Notified ${processed} subscribers for video ${videoId}`);
  }
}

module.exports = new SubscriptionService();
```

---

### 4.9 Search Service

```javascript
// search-service/src/searchService.js

const esClient = require('./esClient');
const redis = require('./redis');

class SearchService {
  /**
   * Full-text search with personalization boost.
   * Uses Elasticsearch BM25 + function score for view count boost.
   */
  async search(query, { page = 1, limit = 20, filters = {}, userId } = {}) {
    if (!query?.trim()) return { results: [], total: 0 };

    const from = (page - 1) * limit;

    // Get user watch history for personalization (optional)
    let watchedChannels = [];
    if (userId) {
      const history = await redis.lrange(`watch_history:${userId}`, 0, 50);
      watchedChannels = history.map(h => JSON.parse(h).channelId);
    }

    const esQuery = {
      index: 'videos',
      body: {
        from,
        size: limit,
        query: {
          function_score: {
            query: {
              bool: {
                must: [
                  {
                    multi_match: {
                      query,
                      fields: ['title^3', 'description^1', 'tags^2', 'channelName^1.5'],
                      type: 'best_fields',
                      fuzziness: 'AUTO',
                      minimum_should_match: '75%',
                    }
                  },
                  { term: { visibility: 'PUBLIC' } },
                ],
                should: watchedChannels.length > 0 ? [
                  { terms: { channelId: watchedChannels, boost: 1.5 } } // personalization boost
                ] : [],
                filter: this.buildFilters(filters),
              }
            },
            functions: [
              { field_value_factor: { field: 'viewCount', factor: 0.001, modifier: 'log1p', missing: 0 } },
              { gauss: { publishedAt: { origin: 'now', scale: '30d', decay: 0.5 } } }, // freshness decay
            ],
            boost_mode: 'sum',
          }
        },
        highlight: {
          fields: { title: { number_of_fragments: 0 }, description: { fragment_size: 200 } }
        },
        aggregations: {
          by_category: { terms: { field: 'categoryId' } },
          duration_ranges: {
            range: { field: 'duration', ranges: [
              { to: 240, key: 'short' }, { from: 240, to: 1200, key: 'medium' }, { from: 1200, key: 'long' }
            ]}
          }
        }
      }
    };

    const response = await esClient.search(esQuery);
    return {
      results: response.hits.hits.map(hit => ({
        videoId: hit._id,
        score: hit._score,
        highlight: hit.highlight,
        ...hit._source,
      })),
      total: response.hits.total.value,
      aggregations: response.aggregations,
      page,
      hasMore: from + limit < response.hits.total.value,
    };
  }

  buildFilters(filters) {
    const filterClauses = [];
    if (filters.duration) {
      const ranges = { short: { to: 240 }, medium: { from: 240, to: 1200 }, long: { from: 1200 } };
      if (ranges[filters.duration]) filterClauses.push({ range: { duration: ranges[filters.duration] } });
    }
    if (filters.uploadDate) {
      const dates = { hour: 'now-1h/h', day: 'now-1d/d', week: 'now-7d/d', month: 'now-1M/M', year: 'now-1y/y' };
      if (dates[filters.uploadDate]) filterClauses.push({ range: { publishedAt: { gte: dates[filters.uploadDate] } } });
    }
    if (filters.type) filterClauses.push({ term: { videoType: filters.type } });
    return filterClauses;
  }

  /**
   * Autocomplete suggestions using Elasticsearch completion suggester
   */
  async autocomplete(prefix, limit = 8) {
    const cacheKey = `autocomplete:${prefix.toLowerCase().slice(0, 30)}`;
    const cached = await redis.getJSON(cacheKey);
    if (cached) return cached;

    const response = await esClient.search({
      index: 'videos',
      body: {
        suggest: {
          video_suggest: {
            prefix,
            completion: {
              field: 'suggest',
              size: limit,
              fuzzy: { fuzziness: 1 },
              contexts: { visibility: [{ context: 'PUBLIC' }] }
            }
          }
        }
      }
    });

    const suggestions = response.suggest.video_suggest[0].options.map(o => o.text);
    await redis.setJSON(cacheKey, suggestions, 60); // cache 1 min
    return suggestions;
  }
}

module.exports = new SearchService();
```

---

### 4.10 Recommendation Engine

```javascript
// recommendation-service/src/recommendationService.js

const redis = require('./redis');
const db = require('./db');

/**
 * Recommendation pipeline:
 * 1. Candidate generation: ~100K candidates from multiple sources
 * 2. Scoring: ML ranking model (offline-trained, served via TF Serving)
 * 3. Re-ranking: diversity, freshness, exclusion of watched content
 * 4. Caching: pre-computed feed cached per user
 */

class RecommendationService {
  /**
   * Get personalized homepage feed
   */
  async getHomeFeed(userId, { page = 1, limit = 20 } = {}) {
    const cacheKey = `feed:home:${userId}:${page}`;
    const cached = await redis.getJSON(cacheKey);
    if (cached) return cached;

    // Fallback to collaborative + content-based
    const [watchHistory, subscriptions, trending] = await Promise.all([
      this.getWatchHistory(userId, 50),
      this.getSubscriptionFeed(userId, 100),
      this.getTrending(50),
    ]);

    // Merge candidates, deduplicate
    const seen = new Set(watchHistory.map(v => v.videoId));
    const candidates = [
      ...subscriptions.filter(v => !seen.has(v.videoId)),
      ...trending.filter(v => !seen.has(v.videoId)),
    ];

    // Score and rank (simplified; real system calls TF Serving)
    const scored = await this.scoreVideos(userId, candidates);
    const ranked = scored.sort((a, b) => b.score - a.score);

    // Diversity injection: every 5th video from a different channel
    const diversified = this.applyDiversity(ranked, { channelFreq: 5 });
    const result = diversified.slice((page - 1) * limit, page * limit);

    await redis.setJSON(cacheKey, result, 300); // 5 min TTL
    return result;
  }

  async getUpNext(videoId, userId) {
    const cacheKey = `upnext:${videoId}:${userId || 'anon'}`;
    const cached = await redis.getJSON(cacheKey);
    if (cached) return cached;

    const [currentVideo, watchHistory] = await Promise.all([
      db.videos.findByPk(videoId, { attributes: ['tags', 'categoryId', 'userId'] }),
      userId ? this.getWatchHistory(userId, 100) : Promise.resolve([]),
    ]);

    if (!currentVideo) return [];

    const watchedIds = new Set(watchHistory.map(h => h.videoId));
    watchedIds.add(videoId);

    // Find similar videos by tag/category overlap
    const similar = await db.videos.findAll({
      where: {
        id: { [db.Op.notIn]: Array.from(watchedIds) },
        status: 'READY',
        visibility: 'PUBLIC',
        [db.Op.or]: [
          { tags: { [db.Op.overlap]: currentVideo.tags || [] } },
          { categoryId: currentVideo.categoryId },
          { userId: currentVideo.userId }, // same channel
        ]
      },
      order: [['viewCount', 'DESC']],
      limit: 30,
    });

    const result = similar.slice(0, 20).map(v => v.toJSON());
    await redis.setJSON(cacheKey, result, 600);
    return result;
  }

  async scoreVideos(userId, candidates) {
    // Simplified scoring — real system uses ML model
    // Features: match with user interests, recency, engagement rate
    const userInterests = await this.getUserInterests(userId);

    return candidates.map(video => {
      let score = video.viewCount ? Math.log10(video.viewCount + 1) : 0;

      // Recency boost (videos from last 48h get extra weight)
      const ageHours = (Date.now() - new Date(video.publishedAt)) / 3600000;
      if (ageHours < 48) score += 2;
      if (ageHours < 6) score += 3;

      // Tag overlap with user interests
      const tagOverlap = (video.tags || []).filter(t => userInterests.has(t)).length;
      score += tagOverlap * 1.5;

      // Engagement ratio (likes/views)
      if (video.viewCount > 100) {
        const engagementRatio = (video.likeCount || 0) / video.viewCount;
        score += engagementRatio * 10;
      }

      return { ...video, score };
    });
  }

  applyDiversity(ranked, { channelFreq }) {
    const channelCount = {};
    return ranked.filter(video => {
      const count = channelCount[video.userId] || 0;
      if (count >= channelFreq) return false;
      channelCount[video.userId] = count + 1;
      return true;
    });
  }

  async getUserInterests(userId) {
    const cacheKey = `user:interests:${userId}`;
    const cached = await redis.getJSON(cacheKey);
    if (cached) return new Set(cached);

    const history = await db.watchHistory.findAll({
      where: { userId },
      order: [['watchedAt', 'DESC']],
      limit: 200,
      include: [{ model: db.videos, attributes: ['tags', 'categoryId'] }]
    });

    const tagFreq = {};
    history.forEach(h => {
      (h.video?.tags || []).forEach(tag => { tagFreq[tag] = (tagFreq[tag] || 0) + 1; });
    });

    const topTags = Object.entries(tagFreq).sort((a, b) => b[1] - a[1]).slice(0, 30).map(([tag]) => tag);
    await redis.setJSON(cacheKey, topTags, 3600);
    return new Set(topTags);
  }

  async getTrending(limit = 50) {
    const cacheKey = 'trending:global';
    const cached = await redis.getJSON(cacheKey);
    if (cached) return cached.slice(0, limit);

    // Trending score = views_last_24h / (age_hours + 2)^1.5  (HackerNews algorithm variant)
    const trending = await db.sequelize.query(`
      SELECT v.*, 
        CAST(v.view_count_24h AS FLOAT) / POWER(EXTRACT(EPOCH FROM NOW() - v.published_at) / 3600 + 2, 1.5) AS trending_score
      FROM videos v
      WHERE v.status = 'READY' AND v.visibility = 'PUBLIC'
        AND v.published_at > NOW() - INTERVAL '7 days'
      ORDER BY trending_score DESC
      LIMIT 200
    `, { type: db.QueryTypes.SELECT });

    await redis.setJSON(cacheKey, trending, 300); // refresh every 5 min
    return trending.slice(0, limit);
  }

  async getWatchHistory(userId, limit) {
    return db.watchHistory.findAll({
      where: { userId },
      order: [['watchedAt', 'DESC']],
      limit,
      attributes: ['videoId', 'watchedSeconds', 'watchedAt'],
    });
  }

  async getSubscriptionFeed(userId, limit) {
    const subs = await db.subscriptions.findAll({ where: { userId }, attributes: ['channelId'] });
    const channelIds = subs.map(s => s.channelId);
    if (channelIds.length === 0) return [];

    return db.videos.findAll({
      where: {
        userId: { [db.Op.in]: channelIds },
        status: 'READY',
        visibility: 'PUBLIC',
        publishedAt: { [db.Op.gte]: new Date(Date.now() - 7 * 24 * 3600 * 1000) }, // last 7 days
      },
      order: [['publishedAt', 'DESC']],
      limit,
    });
  }
}

module.exports = new RecommendationService();
```

---

### 4.11 Rate Limiter (Token Bucket)

```javascript
// middleware/rateLimiter.js

/**
 * Token Bucket Rate Limiter using Redis
 *
 * Token Bucket algorithm:
 * - Each user has a bucket with capacity N tokens
 * - Tokens refill at rate R per second
 * - Each request costs 1 token
 * - If bucket empty: reject with 429
 *
 * Stored in Redis as: { tokens: float, lastRefill: timestamp }
 */

const redis = require('./redis');

class TokenBucketRateLimiter {
  constructor({ capacity, refillRate, windowMs = 1000 }) {
    this.capacity = capacity;       // max tokens (burst capacity)
    this.refillRate = refillRate;   // tokens per windowMs
    this.windowMs = windowMs;
  }

  /**
   * Returns middleware for Express
   * @param {Function} keyFn - extracts rate limit key from req (e.g. IP or userId)
   */
  middleware(keyFn) {
    return async (req, res, next) => {
      const key = `ratelimit:${keyFn(req)}`;

      try {
        const allowed = await this.consume(key);
        if (!allowed) {
          res.set({
            'Retry-After': Math.ceil(this.windowMs / 1000),
            'X-RateLimit-Limit': this.capacity,
            'X-RateLimit-Remaining': 0,
          });
          return res.status(429).json({ error: 'Rate limit exceeded. Please slow down.' });
        }
        next();
      } catch (err) {
        // Redis failure → fail open (allow request) to preserve availability
        console.error('Rate limiter error:', err);
        next();
      }
    };
  }

  async consume(key) {
    const now = Date.now();

    // Lua script ensures atomicity (no race conditions)
    const luaScript = `
      local key = KEYS[1]
      local capacity = tonumber(ARGV[1])
      local refill_rate = tonumber(ARGV[2])
      local window_ms = tonumber(ARGV[3])
      local now = tonumber(ARGV[4])
      local cost = tonumber(ARGV[5])

      local bucket = redis.call('HGETALL', key)
      local tokens = capacity
      local last_refill = now

      if #bucket > 0 then
        tokens = tonumber(bucket[2])
        last_refill = tonumber(bucket[4])
      end

      -- Refill tokens based on elapsed time
      local elapsed = now - last_refill
      local refill = (elapsed / window_ms) * refill_rate
      tokens = math.min(capacity, tokens + refill)

      if tokens < cost then
        -- Not enough tokens
        redis.call('HSET', key, 'tokens', tokens, 'last_refill', now)
        redis.call('PEXPIRE', key, window_ms * 2)
        return 0
      end

      -- Consume token
      tokens = tokens - cost
      redis.call('HSET', key, 'tokens', tokens, 'last_refill', now)
      redis.call('PEXPIRE', key, window_ms * 2)
      return 1
    `;

    const result = await redis.eval(luaScript, 1, key,
      this.capacity, this.refillRate, this.windowMs, now, 1
    );
    return result === 1;
  }
}

// Pre-configured limiters for different endpoints
const uploadRateLimit = new TokenBucketRateLimiter({ capacity: 5,   refillRate: 1,  windowMs: 60000 }); // 5/min
const apiRateLimit    = new TokenBucketRateLimiter({ capacity: 200, refillRate: 10, windowMs: 1000 });  // 200/s burst, 10/s sustained
const searchLimit     = new TokenBucketRateLimiter({ capacity: 30,  refillRate: 5,  windowMs: 1000 });  // 30 burst

module.exports = {
  uploadLimit: uploadRateLimit.middleware(req => req.user?.id || req.ip),
  apiLimit: apiRateLimit.middleware(req => req.user?.id || req.ip),
  searchLimit: searchLimit.middleware(req => req.ip),
};
```

---

### 4.12 View Count (HyperLogLog + Redis)

```javascript
// analytics-service/src/viewCountService.js

/**
 * YouTube-style view counting:
 *
 * Two problems to solve:
 * 1. Exact unique viewers (use HyperLogLog for memory-efficient cardinality)
 * 2. Total view increments (exact counter, but batched writes)
 *
 * HyperLogLog: ~0.8% error rate, uses only 12KB per key regardless of cardinality.
 * Perfect for "unique viewers in last 24h" at billion-user scale.
 */

const redis = require('./redis');
const kafka = require('./kafkaProducer');
const db = require('./db');

class ViewCountService {
  /**
   * Record a view event.
   * - Increments Redis counter (exact total views)
   * - Adds userId to HyperLogLog (approximate unique viewers)
   * - Publishes to Kafka for analytics pipeline
   */
  async recordView(videoId, userId, sessionId, watchedSeconds) {
    const pipeline = redis.pipeline();

    // 1. Increment total view counter
    pipeline.incr(`views:total:${videoId}`);

    // 2. Track unique viewers with HyperLogLog (memory-efficient)
    // Keyed by day for daily unique viewer stats
    const today = new Date().toISOString().slice(0, 10);
    const hlKey = `views:unique:${videoId}:${today}`;
    pipeline.pfadd(hlKey, userId || sessionId); // pfadd = HyperLogLog ADD
    pipeline.expire(hlKey, 86400 * 8); // keep 8 days

    // 3. Track views per hour for trending calculation
    const hourKey = `views:hourly:${videoId}:${new Date().toISOString().slice(0, 13)}`;
    pipeline.incr(hourKey);
    pipeline.expire(hourKey, 86400 * 2); // keep 2 days

    await pipeline.exec();

    // 4. Async: publish full event to Kafka (non-blocking)
    setImmediate(() => {
      kafka.publish('analytics.view.event', {
        videoId, userId, sessionId, watchedSeconds,
        timestamp: Date.now(),
      }).catch(console.error);
    });
  }

  /**
   * Get approximate unique viewers for today
   */
  async getUniqueViewers(videoId, date = null) {
    const day = date || new Date().toISOString().slice(0, 10);
    const count = await redis.pfcount(`views:unique:${videoId}:${day}`);
    return count;
  }

  /**
   * Trending score: views in last 24 hours (sum of hourly buckets)
   */
  async getTrending24hViews(videoId) {
    const now = new Date();
    const hourKeys = Array.from({ length: 24 }, (_, i) => {
      const d = new Date(now - i * 3600000);
      return `views:hourly:${videoId}:${d.toISOString().slice(0, 13)}`;
    });

    const counts = await redis.mget(...hourKeys);
    return counts.reduce((sum, c) => sum + (parseInt(c) || 0), 0);
  }

  /**
   * Batch flush Redis view counts to PostgreSQL
   * Run via cron every 5 minutes
   */
  async flushToDB() {
    const keys = await redis.keys('views:total:*');
    if (keys.length === 0) return;

    const pipeline = redis.pipeline();
    keys.forEach(key => pipeline.getdel(key));
    const results = await pipeline.exec();

    const updates = keys.map((key, i) => ({
      videoId: key.split(':')[2],
      incrementBy: parseInt(results[i][1]) || 0,
    })).filter(u => u.incrementBy > 0);

    // Batch update DB
    for (const { videoId, incrementBy } of updates) {
      await db.videos.increment({ viewCount: incrementBy }, { where: { id: videoId } });
    }

    console.log(`Flushed view counts for ${updates.length} videos`);
  }
}

module.exports = new ViewCountService();
```

---

## 5. Data Models (Schemas)

### PostgreSQL — Core Entities

```sql
-- Users / Channels (same entity on YouTube)
CREATE TABLE users (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email           VARCHAR(255) UNIQUE NOT NULL,
    username        VARCHAR(50) UNIQUE NOT NULL,
    display_name    VARCHAR(100),
    password_hash   VARCHAR(255),
    avatar_url      TEXT,
    banner_url      TEXT,
    description     TEXT,
    subscriber_count BIGINT DEFAULT 0,
    total_views     BIGINT DEFAULT 0,
    role            VARCHAR(20) DEFAULT 'USER',  -- USER, CREATOR, ADMIN
    is_verified     BOOLEAN DEFAULT FALSE,       -- blue checkmark
    is_banned       BOOLEAN DEFAULT FALSE,
    oauth_provider  VARCHAR(20),                 -- google, github
    oauth_id        VARCHAR(255),
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Videos
CREATE TABLE videos (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id             UUID REFERENCES users(id) ON DELETE CASCADE,
    title               VARCHAR(500) NOT NULL,
    description         TEXT,
    tags                TEXT[],
    category_id         SMALLINT,
    status              VARCHAR(20) DEFAULT 'PENDING',  -- PENDING, UPLOADED, PROCESSING, READY, FAILED, DELETED
    visibility          VARCHAR(20) DEFAULT 'PUBLIC',   -- PUBLIC, UNLISTED, PRIVATE, PREMIUM
    duration            FLOAT,                          -- seconds
    file_size           BIGINT,
    s3_key              TEXT,
    manifest_path       TEXT,
    thumbnail_url       TEXT,
    available_qualities TEXT[],
    view_count          BIGINT DEFAULT 0,
    like_count          BIGINT DEFAULT 0,
    dislike_count       BIGINT DEFAULT 0,
    comment_count       BIGINT DEFAULT 0,
    age_restricted      BOOLEAN DEFAULT FALSE,
    is_live             BOOLEAN DEFAULT FALSE,
    published_at        TIMESTAMPTZ,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for videos
CREATE INDEX idx_videos_user_id      ON videos(user_id);
CREATE INDEX idx_videos_status       ON videos(status);
CREATE INDEX idx_videos_published_at ON videos(published_at DESC);
CREATE INDEX idx_videos_view_count   ON videos(view_count DESC);
CREATE INDEX idx_videos_tags         ON videos USING GIN(tags);

-- Subscriptions
CREATE TABLE subscriptions (
    id                  BIGSERIAL PRIMARY KEY,
    subscriber_id       UUID REFERENCES users(id) ON DELETE CASCADE,
    channel_id          UUID REFERENCES users(id) ON DELETE CASCADE,
    notification_type   VARCHAR(20) DEFAULT 'ALL',  -- ALL, PERSONALIZED, NONE
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(subscriber_id, channel_id)
);

CREATE INDEX idx_subscriptions_subscriber ON subscriptions(subscriber_id);
CREATE INDEX idx_subscriptions_channel    ON subscriptions(channel_id);

-- Likes / Dislikes
CREATE TABLE video_likes (
    id          BIGSERIAL PRIMARY KEY,
    user_id     UUID REFERENCES users(id) ON DELETE CASCADE,
    video_id    UUID REFERENCES videos(id) ON DELETE CASCADE,
    type        VARCHAR(10) NOT NULL,  -- LIKE, DISLIKE
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(user_id, video_id)
);

-- Watch History
CREATE TABLE watch_history (
    id              BIGSERIAL PRIMARY KEY,
    user_id         UUID REFERENCES users(id) ON DELETE CASCADE,
    video_id        UUID REFERENCES videos(id) ON DELETE CASCADE,
    watched_seconds FLOAT DEFAULT 0,
    completion_pct  FLOAT DEFAULT 0,
    watched_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(user_id, video_id)
);

CREATE INDEX idx_watch_history_user ON watch_history(user_id, watched_at DESC);
```

### Cassandra — Comments

```cql
CREATE KEYSPACE youtube WITH replication = {
  'class': 'NetworkTopologyStrategy',
  'datacenter1': 3
};

CREATE TABLE youtube.comments (
    video_id    UUID,
    comment_id  TIMEUUID,           -- TIME-based UUID for natural sort
    parent_id   UUID,               -- null for top-level
    user_id     UUID,
    content     TEXT,
    like_count  COUNTER,
    is_pinned   BOOLEAN,
    is_deleted  BOOLEAN,
    created_at  TIMESTAMP,
    PRIMARY KEY (video_id, comment_id)
) WITH CLUSTERING ORDER BY (comment_id DESC)
  AND default_time_to_live = 0;

-- Secondary index for user's comments
CREATE TABLE youtube.comments_by_user (
    user_id     UUID,
    created_at  TIMESTAMP,
    video_id    UUID,
    comment_id  TIMEUUID,
    PRIMARY KEY (user_id, created_at, comment_id)
) WITH CLUSTERING ORDER BY (created_at DESC);
```

---

## 6. API Design (REST)

### Video APIs

```
POST   /api/v1/upload/initiate              Upload: Start multipart upload
POST   /api/v1/upload/:videoId/complete     Upload: Finalize multipart upload
DELETE /api/v1/upload/:videoId              Upload: Abort upload

GET    /api/v1/videos/:videoId              Get video metadata
PATCH  /api/v1/videos/:videoId              Update video (auth required)
DELETE /api/v1/videos/:videoId              Delete video (auth required)

GET    /api/v1/videos/:videoId/manifest     Get HLS/DASH manifest URL
GET    /api/v1/videos/:videoId/stream       Stream redirect

POST   /api/v1/videos/:videoId/like         Like a video (auth)
POST   /api/v1/videos/:videoId/dislike      Dislike a video (auth)
GET    /api/v1/videos/:videoId/engagement   Get like/dislike + user state

GET    /api/v1/videos/:videoId/comments     List comments (paginated)
POST   /api/v1/videos/:videoId/comments     Add comment (auth)
DELETE /api/v1/videos/:videoId/comments/:id Delete comment (auth)

POST   /api/v1/videos/:videoId/playback     Report playback event (analytics)
```

### User/Channel APIs

```
POST   /api/v1/auth/register                Create account
POST   /api/v1/auth/login                   Login
POST   /api/v1/auth/refresh                 Refresh access token
POST   /api/v1/auth/logout                  Logout

GET    /api/v1/channels/:channelId          Get channel info
GET    /api/v1/channels/:channelId/videos   List channel videos

POST   /api/v1/channels/:channelId/subscribe    Subscribe (auth)
DELETE /api/v1/channels/:channelId/subscribe    Unsubscribe (auth)
```

### Search & Feed APIs

```
GET    /api/v1/search?q=:query&page=1&limit=20&sort=relevance&duration=short
GET    /api/v1/search/autocomplete?q=:prefix
GET    /api/v1/feed/home                    Personalized homepage feed (auth)
GET    /api/v1/feed/trending                Trending videos (global)
GET    /api/v1/feed/trending?region=IN      Trending by region
GET    /api/v1/videos/:videoId/recommended  Up-next recommendations
```

### Example Response Format

```json
// GET /api/v1/videos/:videoId
{
  "videoId": "abc123",
  "title": "System Design Interview Guide",
  "description": "...",
  "channel": {
    "id": "ch456",
    "username": "techdojo",
    "displayName": "TechDojo",
    "avatarUrl": "https://cdn.yt.com/avatars/ch456.jpg",
    "subscriberCount": 1200000,
    "isVerified": true
  },
  "duration": 3720,
  "viewCount": 4820000,
  "likeCount": 183000,
  "publishedAt": "2024-11-10T14:30:00Z",
  "tags": ["system design", "FAANG", "backend"],
  "availableQualities": ["360p", "480p", "720p", "1080p"],
  "manifestUrl": "https://cdn.yt.com/abc123/master.m3u8",
  "thumbnailUrl": "https://cdn.yt.com/abc123/thumbs/thumb_2.jpg",
  "visibility": "PUBLIC"
}
```

---

## 7. Fault Tolerance & Reliability

### Failure Scenarios & Mitigations

| Scenario | Impact | Mitigation |
|---|---|---|
| Upload service down | Uploads fail | Multiple AZ deployment; pre-signed S3 URLs bypass service |
| Transcoding worker crash mid-job | Video stuck in PROCESSING | Kafka retry with `at-least-once`; idempotent worker checks existing output before re-encoding |
| CDN PoP failure | Increased latency for region | Automatic failover to next nearest PoP; origin fallback |
| Redis cluster node failure | Cache miss storm | Redis Sentinel/Cluster with replicas; circuit breaker on DB |
| Primary DB down | Write unavailability | Automatic failover to replica (promoted in < 30s) via PgBouncer + Patroni |
| Elasticsearch down | Search unavailable | Return cached results; fallback to DB LIKE queries for simple cases |
| Kafka broker failure | Event loss risk | Kafka replication factor = 3; `acks=all` for critical producers |
| S3 region outage | Video unavailable | Cross-region S3 replication to DR region |

### Circuit Breaker Pattern

```javascript
// circuit-breaker.js
class CircuitBreaker {
  constructor({ failureThreshold = 5, recoveryTimeout = 30000, successThreshold = 2 } = {}) {
    this.state = 'CLOSED';       // CLOSED: normal, OPEN: blocked, HALF_OPEN: testing
    this.failureCount = 0;
    this.successCount = 0;
    this.lastFailureTime = null;
    this.failureThreshold = failureThreshold;
    this.recoveryTimeout = recoveryTimeout;
    this.successThreshold = successThreshold;
  }

  async execute(fn) {
    if (this.state === 'OPEN') {
      if (Date.now() - this.lastFailureTime > this.recoveryTimeout) {
        this.state = 'HALF_OPEN';
      } else {
        throw new Error('Circuit OPEN — service unavailable');
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
    this.failureCount = 0;
    if (this.state === 'HALF_OPEN') {
      this.successCount++;
      if (this.successCount >= this.successThreshold) {
        this.state = 'CLOSED';
        this.successCount = 0;
      }
    }
  }

  onFailure() {
    this.failureCount++;
    this.lastFailureTime = Date.now();
    if (this.failureCount >= this.failureThreshold || this.state === 'HALF_OPEN') {
      this.state = 'OPEN';
      this.successCount = 0;
    }
  }
}

module.exports = CircuitBreaker;
```

---

## 8. Security Design

### Authentication & Authorization

```
JWT (short-lived 15min) + Refresh Token (30 days, stored in Redis)
OAuth 2.0 for Google/GitHub login
Role-Based Access Control (RBAC): USER, CREATOR, MODERATOR, ADMIN

Video Access Control:
  PUBLIC     → no auth required
  UNLISTED   → no auth, but not indexed (link-only)
  PRIVATE    → owner only
  PREMIUM    → verified subscribers only (signed URLs, time-limited)
```

### Content Security

- **Virus scanning**: ClamAV on all uploads before processing
- **CSAM detection**: PhotoDNA hash matching (Microsoft API) before any storage
- **Watermarking**: Invisible forensic watermark embedded during encoding (DRM for premium)
- **DRM**: Widevine (Android/Chrome), FairPlay (Apple), PlayReady (Windows) via DASH + EME
- **Rate limiting**: Token bucket per user/IP (implemented in LLD above)
- **CORS**: Strict origin whitelist; `Access-Control-Allow-Origin` not wildcard for API
- **CSP headers**: Prevent XSS on web app

### Data Protection

- Passwords: bcrypt with 12 salt rounds (not MD5, not SHA256)
- PII: encrypted at rest (AES-256); column-level encryption for SSN, payment info
- GDPR: Right to erasure implemented (soft-delete → hard-delete after 30 days)
- HTTPS everywhere: HSTS headers, TLS 1.3 minimum
- S3 presigned URLs: short-lived (1h), scoped to specific key and operation

---

## 9. Scalability Deep Dive

### Horizontal Scaling

| Service | Scaling Strategy |
|---|---|
| Upload Service | Stateless → auto-scale behind ALB; S3 handles actual storage |
| Transcoding Workers | Kubernetes HPA; scale based on Kafka consumer lag |
| Streaming Service | Stateless, CDN absorbs 99% → minimal origin scale needed |
| Metadata Service | Stateless → auto-scale; Redis handles hot reads |
| PostgreSQL | Read replicas for reads; PgBouncer for connection pooling |
| Elasticsearch | Horizontal shard scaling; dedicated master nodes |
| Redis | Redis Cluster (hash slots) for horizontal partitioning |
| Kafka | Add brokers + increase partition count for higher throughput |

### Database Sharding

```
Videos table → Sharded by hash(videoId) across 16 shards
  Shard 0:  videoId % 16 == 0
  Shard 1:  videoId % 16 == 1
  ...

Users table → Sharded by hash(userId)

Query routing via proxy (Vitess for MySQL / Citus for PostgreSQL):
  Application → Vitess/Citus → correct shard
  Cross-shard joins avoided by denormalization
```

### Read-Heavy Optimization

```
For a video page with 5B daily views:
  - 99%+ served by CDN (video segments, thumbnails)
  - Metadata served from Redis (L2 cache, 300s TTL)
  - Comment counts: Redis counter, eventual consistency
  - View counts: Redis HLL + counter, flushed to DB every 5min
  - Like counts: Redis counter
  → PostgreSQL read replica handles < 1% of traffic
```

### Write Optimization (Thundering Herd)

Problem: Viral video → millions of simultaneous "view" writes to DB

Solution:
1. Write to Redis counter (O(1), in-memory, atomic)
2. Batch flush to PostgreSQL every 5 minutes (single UPDATE per video)
3. Result: DB write rate = O(unique videos) not O(views)

---

## 10. Monitoring & Observability

### Three Pillars

**Metrics (Prometheus + Grafana):**
```
System metrics:   CPU, memory, disk, network per service
Business metrics: uploads/sec, transcoding queue depth, active streams, search QPS
SLA metrics:      p50/p95/p99 latency, error rate, CDN hit ratio, availability %
```

**Logs (ELK Stack / Loki):**
```
Structured JSON logs from all services
Correlation ID propagated via X-Request-ID header across all services
Log levels: ERROR, WARN, INFO, DEBUG (DEBUG off in production)
Centralized in Elasticsearch → searchable in Kibana
```

**Traces (OpenTelemetry + Jaeger):**
```
Distributed tracing across service calls
Example trace: Client → API Gateway → Metadata Service → Redis → DB
Identify bottlenecks (e.g. N+1 query, slow DB call)
Sample rate: 1% in production, 100% on errors
```

### Key Alerts

```yaml
- alert: VideoTranscodeQueueDepth
  expr: kafka_consumer_lag{topic="video.transcode.requested"} > 1000
  severity: warning
  action: Scale up transcoding worker fleet

- alert: OriginCacheMissRate
  expr: cdn_cache_hit_ratio < 0.90
  severity: critical
  action: Investigate CDN config / cache TTLs

- alert: UploadServiceErrorRate
  expr: rate(http_requests_total{status=~"5.."}[5m]) > 0.01
  severity: critical
  action: Page on-call, check upload service logs

- alert: DBReplicationLag
  expr: postgres_replication_lag_seconds > 10
  severity: warning
  action: Investigate replication lag, potential write storm
```

---

## 11. Trade-offs & Interview Tips

### Key Trade-offs to Discuss

| Decision | Option A | Option B | Chosen & Why |
|---|---|---|---|
| Upload flow | Server proxies bytes | Presigned S3 URL (client direct) | **Presigned URL** — saves bandwidth, reduces server load, S3 handles reliability |
| Video storage format | Single large file | HLS/DASH segmented | **Segmented** — enables ABR, seek without full download, CDN caching of segments |
| Comment storage | PostgreSQL | Cassandra | **Cassandra** — high write throughput, natural partition by videoId, no OFFSET pagination needed |
| View count | Synchronous DB write | Redis buffer + batch flush | **Buffer + flush** — prevents DB hot spot, 10000× lower write rate to DB |
| Notification fan-out | Fan-out on write | Fan-out on read | **Hybrid** — push for normal channels, pull for celebrities with 10M+ subs |
| Recommendation | Real-time ML | Pre-computed batch | **Hybrid** — batch for cold start, real-time for session signals |
| Search ranking | Pure BM25 | ML learning-to-rank | **ML re-ranking** after BM25 candidate retrieval — balance precision and scalability |

### Common Interview Questions & Answers

**Q: How do you handle a video going viral unexpectedly?**
A: CDN absorbs video traffic (segments are immutable, cached 1 year). Metadata service is stateless and auto-scales. Redis counters handle view count spikes without DB writes. The only bottleneck would be the metadata DB — mitigated by Redis L2 cache (300s TTL) and read replicas. CDN pre-warming can be triggered by trending detection pipeline.

**Q: How do you ensure exactly-once view counting?**
A: We don't need exactly-once — approximate is fine for views (HyperLogLog for uniques). For total count, Redis INCR is atomic, and batch DB flush is idempotent (uses INCREMENT not SET). Duplicate counts from network retries are absorbed into statistical noise at 5B views/day.

**Q: How do you handle a 10-hour 4K video upload from a mobile device?**
A: Chunked multipart upload (50MB chunks via presigned S3 URLs). Client can resume from last successful chunk if connection drops. Each chunk is independently uploaded and retried. `CompleteMultipartUpload` is atomic — either all chunks or none.

**Q: How does adaptive bitrate streaming work?**
A: HLS/DASH player monitors download speed and buffer fill level. If buffer is low (rebuffering risk) → switch to lower bitrate. If buffer is high and bandwidth estimate is good → switch to higher quality. Manifest lists all available streams; player switches by fetching from different variant playlist.

**Q: How would you design YouTube Live?**
A: Broadcaster → RTMP ingest server → segmenter (generates 2-second HLS segments) → S3 (origin) → CDN (pull). Latency: ~6-10 seconds for standard live, ~2-3 seconds for low-latency HLS with reduced segment size. Chat: WebSocket connection, Cassandra for chat message storage.

---

*This document covers YouTube System Design at FAANG interview level. All LLD code is in Node.js/JavaScript. For production use, add proper error handling, observability, and security hardening per your organization's standards.*