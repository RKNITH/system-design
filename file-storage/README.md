# Distributed File Storage System — System Design

> **Interview Level:** FAANG / Staff Engineer  
> **Scope:** High-Level Design (HLD) + Low-Level Design (LLD in JavaScript)  
> **Inspired by:** Google Drive, Amazon S3, Dropbox, HDFS

---

## Table of Contents

1. [Problem Statement & Requirements](#1-problem-statement--requirements)
2. [Capacity Estimation & Constraints](#2-capacity-estimation--constraints)
3. [High-Level Design (HLD)](#3-high-level-design-hld)
   - [Core Architecture Overview](#31-core-architecture-overview)
   - [Key Components](#32-key-components)
   - [Request Flow — Upload](#33-request-flow--upload)
   - [Request Flow — Download](#34-request-flow--download)
   - [Request Flow — Delete & Versioning](#35-request-flow--delete--versioning)
4. [Deep Dive: Individual Components](#4-deep-dive-individual-components)
   - [Client SDK](#41-client-sdk)
   - [API Gateway](#42-api-gateway)
   - [Metadata Service](#43-metadata-service)
   - [Chunk/Block Service](#44-chunkblock-service)
   - [Storage Nodes](#45-storage-nodes)
   - [Replication & Consistency](#46-replication--consistency)
   - [CDN & Edge Caching](#47-cdn--edge-caching)
   - [Sync Service & Notification](#48-sync-service--notification)
   - [Garbage Collector](#49-garbage-collector)
5. [Database Design](#5-database-design)
   - [Metadata DB Schema](#51-metadata-db-schema)
   - [Chunk Mapping DB Schema](#52-chunk-mapping-db-schema)
   - [User & Permission Schema](#53-user--permission-schema)
6. [Consistency, Availability & Partition Tolerance](#6-consistency-availability--partition-tolerance)
7. [Fault Tolerance & Disaster Recovery](#7-fault-tolerance--disaster-recovery)
8. [Security Design](#8-security-design)
9. [Low-Level Design (LLD) — JavaScript](#9-low-level-design-lld--javascript)
   - [File Chunker](#91-file-chunker)
   - [Deduplication Engine](#92-deduplication-engine)
   - [Chunk Upload Manager](#93-chunk-upload-manager)
   - [Metadata Service — Class Design](#94-metadata-service--class-design)
   - [Storage Node — Class Design](#95-storage-node--class-design)
   - [Replication Manager](#96-replication-manager)
   - [Consistent Hashing Ring](#97-consistent-hashing-ring)
   - [Sync Engine (Client-Side)](#98-sync-engine-client-side)
   - [Lock Manager (Distributed Locking)](#99-lock-manager-distributed-locking)
   - [Rate Limiter (Token Bucket)](#910-rate-limiter-token-bucket)
10. [API Design](#10-api-design)
11. [Scalability Patterns](#11-scalability-patterns)
12. [Monitoring & Observability](#12-monitoring--observability)
13. [Trade-offs & Alternatives](#13-trade-offs--alternatives)

---

## 1. Problem Statement & Requirements

Design a distributed file storage system that allows users to upload, download, share, and sync files across multiple devices, similar to Google Drive or Dropbox.

### Functional Requirements

- **Upload** files of any size (up to 50 GB per file)
- **Download** files — full file or byte-range requests
- **Delete** files (soft delete with retention window)
- **Share** files/folders with other users (read/write/admin permissions)
- **Sync** — changes on one device propagate to all devices in near real-time
- **Versioning** — keep N versions of every file
- **Search** — search files by name, type, metadata
- **Deduplication** — avoid storing identical content twice (content-addressable storage)
- **Folders** — hierarchical directory structure
- **Resumable uploads** — network interruption should not restart upload from scratch

### Non-Functional Requirements

- **Availability:** 99.99% (≤ 52 min downtime/year)
- **Durability:** 11 nines (10^-11 data loss probability) — S3-class
- **Consistency:** Eventual consistency for sync; strong consistency for metadata
- **Latency:** Upload initiation < 200 ms; Download TTFB < 100 ms for cached content
- **Throughput:** Support 10 million DAU, 1 billion total files
- **Scalability:** Horizontally scalable at every layer
- **Security:** Encryption at rest (AES-256) and in transit (TLS 1.3)

### Out of Scope (for this design)

- Real-time collaborative editing (Google Docs-style)
- Video transcoding / media processing pipelines
- Full-text search inside documents

---

## 2. Capacity Estimation & Constraints

### Scale Assumptions

```
DAU                    = 10 million
Average files per user = 200
Total files            = 10M × 200 = 2 billion
Average file size      = 500 KB
Total storage          = 2B × 500 KB = 1 PB raw
With 3× replication    = 3 PB physical storage
```

### Traffic Estimates

```
Uploads per day        = 10M users × 2 uploads/day   = 20M uploads/day
                       = 20M / 86,400                 ≈ 231 uploads/sec (avg)
                       ≈ 1,000 uploads/sec (peak 4×)

Downloads per day      = 10M × 5 downloads/day        = 50M/day
                       ≈ 578 downloads/sec (avg)
                       ≈ 2,500 downloads/sec (peak)

Upload bandwidth       = 231 × 500 KB                 ≈ 115 MB/s ≈ 1 Gbps (avg)
Download bandwidth     = 578 × 500 KB                 ≈ 289 MB/s ≈ 2.5 Gbps (avg)
Peak download          ≈ 10 Gbps (need CDN offload)
```

### Storage Growth

```
New data per day       = 20M uploads × 500 KB         = 10 TB/day
New data per year      = 10 TB × 365                  ≈ 3.65 PB/year
With 3× replication    ≈ 11 PB/year growth
```

### Chunk Size Decision

```
Chunk size             = 4 MB (industry standard)
Chunks per 500 KB file = 1 chunk (file < chunk size, stored whole)
Chunks per 1 GB file   = 256 chunks
Chunks per 50 GB file  = 12,800 chunks
```

Chunk size of 4 MB balances:
- Parallelism (more chunks = more parallel transfers)
- Overhead (fewer chunks = less metadata per file)
- Network efficiency (not too small for TCP overhead)

---

## 3. High-Level Design (HLD)

### 3.1 Core Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              CLIENT LAYER                                    │
│   ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│   │  Web Client  │  │ Desktop App  │  │ Mobile App   │  │  CLI / SDK   │  │
│   └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  │
└──────────┼────────────────┼────────────────┼────────────────┼─────────────┘
           │                │                │                │
           └────────────────┴────────┬───────┘                │
                                     │ HTTPS / WebSocket       │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           EDGE / CDN LAYER                                   │
│              Cloudflare / AWS CloudFront / Akamai                           │
│         (Static assets, hot file caching, DDoS protection)                  │
└───────────────────────────────────┬─────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          API GATEWAY LAYER                                   │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Load Balancer (L7 — NGINX / AWS ALB)                               │   │
│  │  • Rate Limiting    • Auth (JWT/OAuth2)    • Request Routing        │   │
│  │  • SSL Termination  • Request Tracing      • API Versioning         │   │
│  └──────┬─────────────────┬──────────────────┬──────────────────┬──────┘   │
└─────────┼─────────────────┼──────────────────┼──────────────────┼──────────┘
          │                 │                  │                  │
          ▼                 ▼                  ▼                  ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐
│   Metadata   │  │    Chunk /   │  │    Sync /    │  │   Auth / IAM     │
│   Service    │  │  Upload Svc  │  │  Notify Svc  │  │   Service        │
│  (file tree) │  │  (multipart) │  │  (WebSocket) │  │  (OAuth, tokens) │
└──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └──────────────────┘
       │                 │                  │
       ▼                 ▼                  ▼
┌──────────────┐  ┌──────────────────────────────────┐  ┌──────────────────┐
│  Metadata DB │  │        Storage Cluster            │  │  Message Queue   │
│  (PostgreSQL │  │  ┌──────────┐  ┌──────────┐      │  │  (Kafka)         │
│   + Redis)   │  │  │ Storage  │  │ Storage  │ ...  │  │  • file.uploaded │
│              │  │  │ Node 1   │  │ Node 2   │      │  │  • file.deleted  │
└──────────────┘  │  └──────────┘  └──────────┘      │  │  • file.synced   │
                  │  (Erasure coded + replicated)      │  └──────────────────┘
                  └──────────────────────────────────┘
                                    │
                                    ▼
                  ┌──────────────────────────────────┐
                  │          Object Storage           │
                  │  (Chunks stored as blob objects)  │
                  │  Primary: Local disk cluster      │
                  │  Tier 2: AWS S3 / GCS (cold)     │
                  └──────────────────────────────────┘
```

### 3.2 Key Components

| Component | Responsibility | Tech Options |
|---|---|---|
| **API Gateway** | Auth, rate limiting, routing | Kong, NGINX, AWS API GW |
| **Metadata Service** | File tree, permissions, versioning | Node.js, PostgreSQL, Redis |
| **Chunk Service** | Upload/download chunks, dedup | Go / Node.js |
| **Storage Nodes** | Persist chunk blobs on disk | Custom or S3-compatible |
| **Replication Manager** | 3× replication, erasure coding | Custom daemon |
| **Sync Service** | Push change notifications | WebSocket + Kafka |
| **CDN** | Cache popular files at edge | CloudFront, Cloudflare |
| **Message Queue** | Async event processing | Apache Kafka |
| **Metadata DB** | Strongly consistent file metadata | PostgreSQL (primary) |
| **Cache** | Hot metadata, chunk lookups | Redis Cluster |
| **Garbage Collector** | Reclaim orphaned chunks | Background worker |
| **Search** | File name / tag search | Elasticsearch |

### 3.3 Request Flow — Upload

```
Client                API GW        Metadata Svc     Chunk Svc       Storage Node
  │                     │                │               │                │
  │ POST /files/init    │                │               │                │
  │ {name, size, hash}  │                │               │                │
  │────────────────────▶│                │               │                │
  │                     │ Auth + Route   │               │                │
  │                     │───────────────▶│               │                │
  │                     │                │ Check dedup   │                │
  │                     │                │ (hash exists?)│                │
  │                     │                │ Create file   │                │
  │                     │                │ record + get  │                │
  │                     │                │ upload_id     │                │
  │                     │◀───────────────│               │                │
  │◀────────────────────│                │               │                │
  │ {upload_id,         │                │               │                │
  │  chunk_urls[]}      │                │               │                │
  │                     │                │               │                │
  │  [For each chunk]   │                │               │                │
  │ PUT /chunks/{id}    │                │               │                │
  │ chunk_data          │                │               │                │
  │────────────────────▶│               │               │                │
  │                     │               │  PUT chunk     │                │
  │                     │───────────────┼──────────────▶│                │
  │                     │               │               │ Persist chunk  │
  │                     │               │               │───────────────▶│
  │                     │               │               │ Replicate 2×   │
  │                     │               │               │ ACK            │
  │                     │               │               │◀───────────────│
  │                     │◀──────────────┼───────────────│                │
  │◀────────────────────│               │               │                │
  │ 200 {chunk_etag}    │               │               │                │
  │                     │               │               │                │
  │ POST /files/commit  │               │               │                │
  │ {upload_id, etags}  │               │               │                │
  │────────────────────▶│               │               │                │
  │                     │──────────────▶│               │                │
  │                     │               │ Verify etags  │                │
  │                     │               │ Mark file ACTIVE              │
  │                     │               │ Emit file.uploaded to Kafka   │
  │                     │◀──────────────│               │                │
  │◀────────────────────│               │               │                │
  │ 200 {file_id}       │               │               │                │
```

### 3.4 Request Flow — Download

```
Client                CDN           API GW        Metadata Svc    Storage Node
  │                    │               │               │               │
  │ GET /files/{id}    │               │               │               │
  │───────────────────▶│               │               │               │
  │                    │ Cache HIT?    │               │               │
  │                    │ ─── YES ──────────────────────────────────────│
  │◀───────────────────│ Serve cached  │               │               │
  │  file data         │               │               │               │
  │                    │               │               │               │
  │                    │ Cache MISS    │               │               │
  │                    │──────────────▶│               │               │
  │                    │               │──────────────▶│               │
  │                    │               │               │ Return chunk  │
  │                    │               │               │ locations []  │
  │                    │               │◀──────────────│               │
  │                    │               │ Generate      │               │
  │                    │               │ signed URLs   │               │
  │                    │◀──────────────│               │               │
  │◀───────────────────│               │               │               │
  │ {signed_chunk_urls}│               │               │               │
  │                    │               │               │               │
  │ [Parallel fetch all chunks]        │               │               │
  │─────────────────────────────────────────────────────────────────▶│
  │◀─────────────────────────────────────────────────────────────────│
  │ [Reassemble chunks in order]       │               │               │
  │ [Verify hash]                      │               │               │
```

### 3.5 Request Flow — Delete & Versioning

```
DELETE /files/{id}
  → Metadata Svc: Soft-delete (set status = DELETED, tombstone timestamp)
  → File record NOT immediately removed (retention = 30 days)
  → Kafka: emit file.deleted event
  → Garbage Collector (async): after retention period
      → Verify no other file version references same chunks
      → If chunk ref_count == 0: mark chunk for physical deletion
      → Remove from storage nodes
      → Remove metadata record

Versioning Flow:
  → On each upload of same path: create new version record
  → Metadata keeps version chain: v1 → v2 → v3 (current)
  → Old versions retained for N days or N versions (configurable per plan)
  → Restore: swap current pointer to desired version
```

---

## 4. Deep Dive: Individual Components

### 4.1 Client SDK

The client SDK is the most complex component — it handles:

**Chunking Strategy:**
- Split file into 4 MB chunks
- Compute SHA-256 hash of each chunk (for deduplication)
- Compute SHA-256 of full file (for integrity verification)

**Delta Sync (Rsync-inspired):**
- On file modification, don't re-upload unchanged chunks
- Compare local chunk hashes vs. server's stored chunk hashes
- Only upload differing chunks

**Upload Strategies:**
- **Sequential:** Simple, one chunk at a time
- **Parallel:** Upload N chunks concurrently (default: 4)
- **Resumable:** Checkpoint progress; on failure, skip already-uploaded chunks

**Conflict Resolution:**
- Last-write-wins (default, based on server timestamp)
- Fork on conflict (Dropbox model: create conflicted copy)

### 4.2 API Gateway

```
Responsibilities:
  1. TLS Termination (TLS 1.3)
  2. Authentication: Validate JWT tokens (RS256 signed)
  3. Authorization: Check user permissions on resource
  4. Rate Limiting: Per user, per IP, per endpoint
  5. Request ID injection (X-Request-ID header)
  6. Routing to upstream microservices
  7. Request/Response logging and tracing

Rate Limit Tiers:
  Free   : 100 uploads/hour,  10 GB/month
  Pro    : 1000 uploads/hour, 2 TB/month
  Business: Unlimited (fair use)
```

### 4.3 Metadata Service

The Metadata Service is the brain of the system. It maintains:

- **File tree:** Hierarchical folder/file relationships
- **File versions:** Chain of version records per logical file
- **Chunk manifest:** Ordered list of chunk IDs that make up each file version
- **Permissions:** ACL entries per file/folder (owner, shared users, public link settings)

**Consistency Guarantee:**
- Uses PostgreSQL with SERIALIZABLE isolation for critical operations
- Redis cache for hot reads (file lookups, permission checks)
- Cache invalidation: write-through for metadata updates

**Read Path (cached):**
```
Request → Redis (cache hit: return) 
       → PostgreSQL (cache miss: fetch, populate cache, return)
```

**Write Path:**
```
Request → PostgreSQL write (durable) → Invalidate Redis key → Return
```

### 4.4 Chunk/Block Service

Handles the actual data path for upload and download.

**Upload:**
1. Receive chunk (up to 4 MB) via HTTP PUT
2. Verify chunk SHA-256 matches declared hash (integrity check)
3. Check deduplication: if chunk hash already exists in chunk index → skip storage, just increment ref count
4. Write chunk to primary storage node
5. Trigger async replication to 2 additional nodes
6. Return chunk ETag (= chunk SHA-256)

**Download:**
1. Receive request with chunk hash or chunk ID
2. Locate chunk on storage node (via consistent hash ring)
3. Serve byte stream directly
4. If primary node unavailable → failover to replica

### 4.5 Storage Nodes

Each storage node is a simple, stateless blob store:

```
Storage Node responsibilities:
  1. Accept chunk writes (PUT /chunks/{hash})
  2. Serve chunk reads (GET /chunks/{hash})
  3. Delete chunks when garbage collected
  4. Report health metrics (disk usage, IOPS, latency)
  5. Participate in replication protocol

Storage Layout on disk:
  /data/chunks/{prefix2}/{prefix2}/{full_hash}
  Example: /data/chunks/ab/cd/abcdef1234567890...

  Two-level directory prefix based on hash avoids inode exhaustion
  on filesystems with large directories.
```

**Erasure Coding (for large files):**
- Instead of 3× replication (3× storage cost), use Reed-Solomon erasure coding
- RS(6,3): split chunk into 6 data shards + 3 parity shards, stored on 9 nodes
- Can recover from any 3 node failures
- Storage overhead: 9/6 = 1.5× (vs. 3× for replication)
- Tradeoff: higher read latency (must fetch from multiple nodes)
- Used for cold/infrequently accessed data; hot data uses replication

### 4.6 Replication & Consistency

**Replication Strategy: Leaderless (Dynamo-style)**

```
Write Quorum W = 2 (of 3 replicas must acknowledge)
Read Quorum  R = 2 (of 3 replicas must respond)
Replication factor N = 3

W + R > N  →  2 + 2 > 3  →  strong consistency guaranteed
```

**Conflict Resolution:**
- Vector clocks for detecting concurrent writes
- Last-write-wins using hybrid logical clocks (HLC) — handles clock skew

**Anti-Entropy:**
- Background Merkle tree reconciliation between replicas
- Detects and repairs diverged chunk data

### 4.7 CDN & Edge Caching

**What gets cached at CDN:**
- Shared/public files accessed by many users
- Recently downloaded files (LRU eviction)
- Profile pictures, thumbnails

**What does NOT get cached:**
- Private files (must be fetched via signed URL with short TTL)
- Files currently being uploaded / incomplete

**CDN Strategy:**
- Push CDN for static assets (JS, CSS bundles)
- Pull CDN for file content (origin = storage cluster)
- Signed URLs: 15-minute expiry, invalidated on delete

### 4.8 Sync Service & Notification

**Architecture:** Long-poll WebSocket connections per client device.

```
Device connects → Sync Service (WebSocket server)
              → Subscribe to user_id channel on Redis Pub/Sub

File uploaded on Device A:
  → Kafka topic: file.events
  → Sync Service consumer reads event
  → Publishes to Redis channel: user:{user_id}:events
  → All connected devices for that user receive the event
  → Devices fetch changed metadata → download new/modified chunks
```

**Change Journal:**
- Per-user ordered log of all file events (insert, update, delete)
- Clients store a local cursor (last seen sequence number)
- On reconnect: fetch all events after cursor → reconcile local state

### 4.9 Garbage Collector

Runs as a background cron job (every hour):

```
Phase 1 — Find candidates:
  SELECT chunk_id FROM chunks
  WHERE ref_count = 0
  AND last_ref_removed_at < NOW() - INTERVAL '7 days'

Phase 2 — Safety check:
  Re-verify ref_count (race condition protection)
  Skip chunks where any file version still references them

Phase 3 — Delete:
  DELETE from storage nodes (all replicas)
  DELETE metadata record

Phase 4 — Report:
  Log bytes reclaimed, chunks deleted, errors
```

---

## 5. Database Design

### 5.1 Metadata DB Schema

```sql
-- Users
CREATE TABLE users (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email         VARCHAR(255) UNIQUE NOT NULL,
    display_name  VARCHAR(255),
    plan          ENUM('free', 'pro', 'business') DEFAULT 'free',
    quota_bytes   BIGINT DEFAULT 15000000000,  -- 15 GB free
    used_bytes    BIGINT DEFAULT 0,
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    updated_at    TIMESTAMPTZ DEFAULT NOW()
);

-- Files & Folders (unified: is_folder flag differentiates them)
CREATE TABLE files (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    owner_id        UUID NOT NULL REFERENCES users(id),
    parent_id       UUID REFERENCES files(id) ON DELETE CASCADE,
    name            VARCHAR(1024) NOT NULL,
    is_folder       BOOLEAN NOT NULL DEFAULT FALSE,
    status          ENUM('active', 'deleted', 'uploading') DEFAULT 'uploading',
    current_version_id UUID,              -- FK to file_versions
    path_hash       VARCHAR(64),         -- SHA-256(owner_id + full_path), for fast lookups
    mime_type       VARCHAR(128),
    deleted_at      TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT unique_name_per_parent UNIQUE (owner_id, parent_id, name)
);

CREATE INDEX idx_files_owner_parent ON files(owner_id, parent_id) WHERE status = 'active';
CREATE INDEX idx_files_path_hash    ON files(path_hash);
CREATE INDEX idx_files_deleted_at   ON files(deleted_at) WHERE status = 'deleted';

-- File Versions (linked list: version chain)
CREATE TABLE file_versions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    file_id         UUID NOT NULL REFERENCES files(id),
    version_number  INT NOT NULL,
    size_bytes      BIGINT NOT NULL,
    file_hash       VARCHAR(64) NOT NULL,  -- SHA-256 of full file
    upload_id       VARCHAR(64),           -- Resumable upload session
    created_by      UUID NOT NULL REFERENCES users(id),
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT unique_version_per_file UNIQUE (file_id, version_number)
);

CREATE INDEX idx_file_versions_file_id ON file_versions(file_id);

-- Chunk Manifest (ordered list of chunks per file version)
CREATE TABLE version_chunks (
    version_id   UUID NOT NULL REFERENCES file_versions(id),
    chunk_index  INT NOT NULL,              -- 0-based position in file
    chunk_id     VARCHAR(64) NOT NULL,      -- chunk hash (content-addressable)
    PRIMARY KEY (version_id, chunk_index)
);

-- Chunks (content-addressable store index)
CREATE TABLE chunks (
    hash         VARCHAR(64) PRIMARY KEY,   -- SHA-256
    size_bytes   INT NOT NULL,
    ref_count    INT NOT NULL DEFAULT 0,   -- # of version_chunks pointing here
    created_at   TIMESTAMPTZ DEFAULT NOW(),
    last_accessed_at TIMESTAMPTZ DEFAULT NOW()
);
```

### 5.2 Chunk Mapping DB Schema

```sql
-- Where is each chunk physically stored?
-- Stored in Redis for speed, backed by PostgreSQL

CREATE TABLE chunk_locations (
    chunk_hash    VARCHAR(64) NOT NULL REFERENCES chunks(hash),
    node_id       VARCHAR(64) NOT NULL,   -- Storage node identifier
    replica_role  ENUM('primary', 'replica') DEFAULT 'primary',
    status        ENUM('active', 'degraded', 'deleted') DEFAULT 'active',
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (chunk_hash, node_id)
);

CREATE INDEX idx_chunk_locations_node ON chunk_locations(node_id);
```

### 5.3 User & Permission Schema

```sql
-- Permissions (shared files/folders)
CREATE TABLE permissions (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    resource_id UUID NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    grantee_id  UUID REFERENCES users(id),   -- NULL = public link
    role        ENUM('viewer', 'editor', 'admin') NOT NULL,
    share_token VARCHAR(64) UNIQUE,           -- For public link sharing
    expires_at  TIMESTAMPTZ,
    created_by  UUID NOT NULL REFERENCES users(id),
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_permissions_resource ON permissions(resource_id);
CREATE INDEX idx_permissions_grantee  ON permissions(grantee_id);
CREATE INDEX idx_permissions_token    ON permissions(share_token) WHERE share_token IS NOT NULL;

-- Change journal for sync
CREATE TABLE change_log (
    seq         BIGSERIAL PRIMARY KEY,
    user_id     UUID NOT NULL REFERENCES users(id),
    event_type  ENUM('created', 'modified', 'deleted', 'moved', 'shared') NOT NULL,
    file_id     UUID NOT NULL,
    payload     JSONB,                      -- Event-specific details
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_change_log_user_seq ON change_log(user_id, seq);
```

---

## 6. Consistency, Availability & Partition Tolerance

### CAP Theorem Position

This system prioritizes **AP (Availability + Partition Tolerance)** for data access, with **strong consistency** for metadata operations.

```
Metadata Service → CP (Consistency + Partition Tolerance)
  • PostgreSQL primary-replica with synchronous replication
  • On partition: reject writes rather than accept inconsistent data

Storage Cluster  → AP (Availability + Partition Tolerance)
  • Leaderless replication (Dynamo-style)
  • On partition: continue serving reads/writes with quorum
  • Reconcile during anti-entropy after partition heals

Sync Service     → Eventual Consistency
  • Devices may be temporarily out of sync
  • Guaranteed to converge when all devices reconnect
```

### Conflict Scenarios

| Scenario | Resolution |
|---|---|
| Two devices upload different content to same path simultaneously | Server-side: last write wins (by server timestamp). Client receives conflict notification, creates conflicted copy |
| Device offline, makes changes, reconnects | Client sends changes with local timestamp; server applies if no server-side change since device's last sync cursor |
| Deleted file vs. concurrent edit | Delete wins (tombstone has higher priority); client notified of loss |
| Network partition heals, replicas diverge | Merkle tree comparison detects divergence; newer version (vector clock) wins |

---

## 7. Fault Tolerance & Disaster Recovery

### Storage Node Failure

```
Detection: Health check ping every 5 seconds (3 failed → node marked DOWN)

Immediate response:
  • New requests routed away from failed node
  • Reads served from replica nodes (quorum still satisfied with R=2 of 2 remaining)

Recovery:
  • New node provisioned
  • Replication manager identifies chunks missing a replica on new node
  • Copies chunks from surviving replicas
  • Target: re-replicate to 3× within 1 hour of failure
```

### Data Center / Region Failure

```
Multi-region deployment:
  Region A (primary): US-East
  Region B (secondary): US-West
  Region C (DR): EU-West

Replication: Active-passive async replication A→B, A→C
Failover: DNS failover (Route 53 health checks) in < 60 seconds
RPO (Recovery Point Objective): < 5 minutes (async replication lag)
RTO (Recovery Time Objective): < 2 minutes (DNS propagation)
```

### Metadata DB Failure

```
PostgreSQL: 1 primary + 2 read replicas (streaming replication)
Failover: Patroni (auto-promotion of replica to primary in < 30s)
Backup: Daily base backup + continuous WAL archiving to S3
PITR: Point-in-time recovery to any second in last 30 days
```

### Corruption Detection

```
All chunks: SHA-256 hash verified on every read
Scrubbing: Background job reads and verifies every chunk weekly
If corruption detected:
  → Restore from replica
  → Re-verify restored chunk
  → Log audit event
  → Alert on-call engineer
```

---

## 8. Security Design

### Authentication & Authorization

```
Authentication:
  • OAuth 2.0 + OpenID Connect (Google, GitHub SSO)
  • Internal: JWT tokens (RS256, 1-hour expiry)
  • Refresh tokens: 30-day rotation, stored in HttpOnly cookie

Authorization (per-resource ACL):
  • Owner: full control
  • Admin (shared): read/write/share/delete
  • Editor (shared): read/write
  • Viewer (shared): read-only
  • Public link: read-only (with optional password)

Permission Check Order:
  1. Is resource public? → allow read
  2. Is user the owner? → allow all
  3. Does user have explicit ACL entry? → apply that role
  4. Does user have ACL entry on parent folder? → inherit role
  5. Deny
```

### Encryption

```
In Transit:
  • TLS 1.3 for all external connections
  • mTLS for internal service-to-service communication

At Rest:
  • Chunk-level AES-256-GCM encryption
  • Per-user encryption key derived from master key (envelope encryption)
  • Keys stored in AWS KMS / HashiCorp Vault
  • Key rotation: annually (old chunks re-encrypted lazily)

Signed URLs:
  • Time-limited (15 minutes default)
  • HMAC-SHA256 signature
  • Bound to requesting IP (optional, enterprise feature)
```

### Threat Model

| Threat | Mitigation |
|---|---|
| Unauthorized file access | ACL checks on every request; signed URLs for direct storage access |
| Man-in-the-middle | TLS 1.3 everywhere; HSTS headers |
| Brute force auth | Rate limiting + account lockout after 5 failed attempts |
| Storage node compromise | Encrypted chunks — node sees only ciphertext blobs |
| Insider threat | Audit logs for all admin operations; encryption key access logged |
| DDoS | CDN + rate limiting + IP reputation filtering at edge |
| Malicious file upload | Virus scanning (ClamAV / cloud AV) async post-upload |

---

## 9. Low-Level Design (LLD) — JavaScript

### 9.1 File Chunker

```javascript
// fileChunker.js
// Splits a file (Browser File/Blob or Node.js Buffer) into fixed-size chunks
// and computes SHA-256 hash for each chunk and the full file.

import crypto from 'crypto';

const DEFAULT_CHUNK_SIZE = 4 * 1024 * 1024; // 4 MB

/**
 * Represents a single chunk of a file.
 * @typedef {Object} FileChunk
 * @property {number} index       - 0-based position in the file
 * @property {Buffer} data        - Raw chunk bytes
 * @property {string} hash        - SHA-256 hex digest of chunk bytes
 * @property {number} size        - Byte length of this chunk
 * @property {number} offset      - Byte offset in the original file
 */

/**
 * Splits a Buffer into chunks and returns metadata for each.
 * @param {Buffer} fileBuffer         - Full file contents as a Buffer
 * @param {number} chunkSize          - Desired chunk size in bytes (default: 4 MB)
 * @returns {{ chunks: FileChunk[], fileHash: string }}
 */
export function chunkFile(fileBuffer, chunkSize = DEFAULT_CHUNK_SIZE) {
  if (!Buffer.isBuffer(fileBuffer)) {
    throw new TypeError('fileBuffer must be a Node.js Buffer');
  }

  const chunks = [];
  const fileHasher = crypto.createHash('sha256');
  fileHasher.update(fileBuffer);
  const fileHash = fileHasher.digest('hex');

  let offset = 0;
  let index = 0;

  while (offset < fileBuffer.length) {
    const end = Math.min(offset + chunkSize, fileBuffer.length);
    const data = fileBuffer.slice(offset, end); // Zero-copy slice

    const chunkHasher = crypto.createHash('sha256');
    chunkHasher.update(data);
    const hash = chunkHasher.digest('hex');

    chunks.push({
      index,
      data,
      hash,
      size: data.length,
      offset,
    });

    offset = end;
    index++;
  }

  return { chunks, fileHash };
}

/**
 * For large files: streaming chunker using Node.js streams.
 * Processes the file incrementally without loading it entirely into memory.
 *
 * @param {string} filePath     - Path to the file on disk
 * @param {number} chunkSize    - Chunk size in bytes
 * @param {Function} onChunk    - Async callback invoked per chunk: async (chunk: FileChunk) => void
 * @returns {Promise<{ fileHash: string, totalChunks: number, totalBytes: number }>}
 */
export async function chunkFileStream(filePath, chunkSize = DEFAULT_CHUNK_SIZE, onChunk) {
  const fs = await import('fs');
  const { pipeline } = await import('stream/promises');

  return new Promise((resolve, reject) => {
    const readStream = fs.createReadStream(filePath);
    const fileHasher = crypto.createHash('sha256');

    let buffer = Buffer.alloc(0);
    let index = 0;
    let offset = 0;
    let totalBytes = 0;

    readStream.on('data', async (data) => {
      fileHasher.update(data);
      totalBytes += data.length;
      buffer = Buffer.concat([buffer, data]);

      // Emit complete chunks as the buffer fills
      while (buffer.length >= chunkSize) {
        const chunkData = buffer.slice(0, chunkSize);
        buffer = buffer.slice(chunkSize); // Consume from buffer

        const hash = crypto.createHash('sha256').update(chunkData).digest('hex');
        await onChunk({ index, data: chunkData, hash, size: chunkData.length, offset });

        offset += chunkSize;
        index++;
      }
    });

    readStream.on('end', async () => {
      // Flush remaining bytes as the last (partial) chunk
      if (buffer.length > 0) {
        const hash = crypto.createHash('sha256').update(buffer).digest('hex');
        await onChunk({ index, data: buffer, hash, size: buffer.length, offset });
        index++;
      }

      resolve({
        fileHash: fileHasher.digest('hex'),
        totalChunks: index,
        totalBytes,
      });
    });

    readStream.on('error', reject);
  });
}
```

### 9.2 Deduplication Engine

```javascript
// deduplicationEngine.js
// Content-addressable storage: identical chunks are stored exactly once.
// Uses chunk SHA-256 as the key; ref_count tracks active references.

/**
 * DeduplicationEngine wraps the chunk index (Redis + PostgreSQL).
 * Before uploading a chunk, callers check if it already exists.
 * The engine also manages reference counting for garbage collection.
 */
export class DeduplicationEngine {
  /**
   * @param {object} redis      - ioredis client instance
   * @param {object} db         - pg (node-postgres) pool instance
   */
  constructor(redis, db) {
    this.redis = redis;
    this.db = db;
    this.CHUNK_TTL = 3600; // Redis cache TTL in seconds
  }

  /**
   * Check if a chunk already exists in the store.
   * Returns chunk metadata if found, null otherwise.
   *
   * Strategy: Check Redis cache first (L1), fall back to PostgreSQL (L2).
   *
   * @param {string} chunkHash - SHA-256 hex digest
   * @returns {Promise<{ hash: string, size: number, locations: string[] } | null>}
   */
  async checkChunkExists(chunkHash) {
    // L1: Redis cache
    const cacheKey = `chunk:${chunkHash}`;
    const cached = await this.redis.get(cacheKey);
    if (cached) {
      return JSON.parse(cached);
    }

    // L2: PostgreSQL
    const { rows } = await this.db.query(
      `SELECT c.hash, c.size_bytes, array_agg(cl.node_id) AS locations
       FROM chunks c
       LEFT JOIN chunk_locations cl ON cl.chunk_hash = c.hash AND cl.status = 'active'
       WHERE c.hash = $1
       GROUP BY c.hash, c.size_bytes`,
      [chunkHash]
    );

    if (rows.length === 0) return null;

    const result = {
      hash: rows[0].hash,
      size: rows[0].size_bytes,
      locations: rows[0].locations.filter(Boolean),
    };

    // Populate cache
    await this.redis.setex(cacheKey, this.CHUNK_TTL, JSON.stringify(result));
    return result;
  }

  /**
   * Register a newly uploaded chunk in the index.
   * Increments ref_count if it already exists (concurrent upload race).
   *
   * @param {string} chunkHash  - SHA-256 hex digest
   * @param {number} sizeBytes  - Byte size of chunk
   * @param {string} nodeId     - ID of storage node where chunk was written
   */
  async registerChunk(chunkHash, sizeBytes, nodeId) {
    const client = await this.db.connect();
    try {
      await client.query('BEGIN');

      // Upsert chunk record (INSERT OR INCREMENT ref_count)
      await client.query(
        `INSERT INTO chunks (hash, size_bytes, ref_count)
         VALUES ($1, $2, 1)
         ON CONFLICT (hash) DO UPDATE
           SET ref_count = chunks.ref_count + 1,
               last_accessed_at = NOW()`,
        [chunkHash, sizeBytes]
      );

      // Record location on storage node
      await client.query(
        `INSERT INTO chunk_locations (chunk_hash, node_id, replica_role, status)
         VALUES ($1, $2, 'primary', 'active')
         ON CONFLICT (chunk_hash, node_id) DO NOTHING`,
        [chunkHash, nodeId]
      );

      await client.query('COMMIT');

      // Invalidate cache so fresh data is loaded next read
      await this.redis.del(`chunk:${chunkHash}`);
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }
  }

  /**
   * Decrement ref_count when a file version is deleted.
   * If ref_count reaches 0, the chunk becomes eligible for GC.
   *
   * @param {string[]} chunkHashes - Array of chunk SHA-256 digests
   */
  async decrementRefs(chunkHashes) {
    if (chunkHashes.length === 0) return;

    await this.db.query(
      `UPDATE chunks
       SET ref_count = GREATEST(ref_count - 1, 0),
           last_ref_removed_at = CASE WHEN ref_count - 1 <= 0 THEN NOW() ELSE last_ref_removed_at END
       WHERE hash = ANY($1)`,
      [chunkHashes]
    );

    // Invalidate cache for all affected chunks
    const pipeline = this.redis.pipeline();
    for (const hash of chunkHashes) {
      pipeline.del(`chunk:${hash}`);
    }
    await pipeline.exec();
  }

  /**
   * Find chunks eligible for physical deletion (ref_count = 0, past retention period).
   * Called by the Garbage Collector.
   *
   * @param {number} retentionDays  - Days to retain unreferenced chunks before deleting
   * @param {number} limit          - Max chunks to return per GC cycle
   * @returns {Promise<string[]>}   - Array of chunk hashes to delete
   */
  async getOrphanedChunks(retentionDays = 7, limit = 10000) {
    const { rows } = await this.db.query(
      `SELECT hash FROM chunks
       WHERE ref_count = 0
         AND last_ref_removed_at < NOW() - INTERVAL '${retentionDays} days'
       ORDER BY last_ref_removed_at ASC
       LIMIT $1`,
      [limit]
    );
    return rows.map((r) => r.hash);
  }
}
```

### 9.3 Chunk Upload Manager

```javascript
// chunkUploadManager.js
// Manages parallel, resumable multipart uploads with retry logic.

import axios from 'axios';
import pLimit from 'p-limit'; // Concurrency limiter

const MAX_CONCURRENT_UPLOADS = 4;
const MAX_RETRIES = 3;
const RETRY_DELAY_BASE_MS = 1000; // Exponential backoff base

/**
 * Manages the upload of all chunks for a single file.
 */
export class ChunkUploadManager {
  /**
   * @param {string} uploadId     - Server-issued upload session ID
   * @param {string} apiBaseUrl   - Base URL of the Chunk Service
   * @param {string} authToken    - Bearer auth token
   */
  constructor(uploadId, apiBaseUrl, authToken) {
    this.uploadId = uploadId;
    this.apiBaseUrl = apiBaseUrl;
    this.authToken = authToken;
    this.uploadedChunks = new Map(); // chunkHash → etag (persisted progress)
    this.limit = pLimit(MAX_CONCURRENT_UPLOADS);
  }

  /**
   * Restore previously uploaded chunks (for resumable upload).
   * Call this before starting an upload if resuming a session.
   *
   * @param {Map<string, string>} progress - Map of chunkHash → etag from local storage
   */
  restoreProgress(progress) {
    this.uploadedChunks = new Map(progress);
  }

  /**
   * Upload all chunks for a file, with parallelism and retry.
   *
   * @param {import('./fileChunker.js').FileChunk[]} chunks
   * @param {Function} onProgress   - Progress callback: (uploaded: number, total: number) => void
   * @returns {Promise<{ chunkHash: string, etag: string }[]>} - Ordered array of chunk receipts
   */
  async uploadAll(chunks, onProgress) {
    let uploaded = this.uploadedChunks.size;

    const uploadTasks = chunks.map((chunk) =>
      this.limit(async () => {
        // Skip already-uploaded chunks (resumable support)
        if (this.uploadedChunks.has(chunk.hash)) {
          return { chunkHash: chunk.hash, etag: this.uploadedChunks.get(chunk.hash) };
        }

        const etag = await this.uploadChunkWithRetry(chunk);

        this.uploadedChunks.set(chunk.hash, etag);
        uploaded++;
        onProgress?.(uploaded, chunks.length);

        return { chunkHash: chunk.hash, etag };
      })
    );

    const results = await Promise.all(uploadTasks);

    // Return in original chunk order (not upload order)
    return chunks.map((chunk) => ({
      chunkHash: chunk.hash,
      etag: this.uploadedChunks.get(chunk.hash),
      index: chunk.index,
    }));
  }

  /**
   * Upload a single chunk with exponential backoff retry.
   *
   * @param {import('./fileChunker.js').FileChunk} chunk
   * @returns {Promise<string>} - ETag returned by server (= chunk hash)
   */
  async uploadChunkWithRetry(chunk) {
    let lastError;

    for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
      try {
        const response = await axios.put(
          `${this.apiBaseUrl}/uploads/${this.uploadId}/chunks/${chunk.index}`,
          chunk.data,
          {
            headers: {
              Authorization: `Bearer ${this.authToken}`,
              'Content-Type': 'application/octet-stream',
              'Content-Length': chunk.size,
              'X-Chunk-Hash': chunk.hash,    // Server verifies integrity
              'X-Chunk-Index': chunk.index,
            },
            maxBodyLength: Infinity,
            timeout: 60000, // 60 second timeout per chunk
          }
        );

        const etag = response.headers['etag'] || response.data.etag;
        if (!etag) throw new Error('Server did not return ETag for chunk');

        return etag;
      } catch (err) {
        lastError = err;

        // Don't retry on 4xx client errors (bad request, auth failure)
        if (err.response && err.response.status >= 400 && err.response.status < 500) {
          throw err;
        }

        // Exponential backoff: 1s, 2s, 4s
        const delay = RETRY_DELAY_BASE_MS * Math.pow(2, attempt);
        await new Promise((res) => setTimeout(res, delay));
      }
    }

    throw new Error(`Chunk ${chunk.index} failed after ${MAX_RETRIES} attempts: ${lastError.message}`);
  }

  /**
   * Serialize upload progress for persistence (localStorage, file, etc.)
   * Allows resuming after app restart.
   *
   * @returns {object}
   */
  serializeProgress() {
    return {
      uploadId: this.uploadId,
      uploadedChunks: Object.fromEntries(this.uploadedChunks),
    };
  }
}
```

### 9.4 Metadata Service — Class Design

```javascript
// metadataService.js
// Manages file records, versions, and permissions.

import crypto from 'crypto';

export class MetadataService {
  constructor(db, redis, eventBus) {
    this.db = db;           // PostgreSQL pool
    this.redis = redis;     // Redis client
    this.eventBus = eventBus; // Kafka producer abstraction
    this.CACHE_TTL = 300;   // 5 minutes
  }

  // ─── File Lookup ─────────────────────────────────────────────────────────

  /**
   * Get file metadata by ID, with Redis caching.
   * @param {string} fileId
   * @param {string} requestingUserId   - For permission check
   */
  async getFile(fileId, requestingUserId) {
    const cacheKey = `file:${fileId}`;
    const cached = await this.redis.get(cacheKey);

    let file;
    if (cached) {
      file = JSON.parse(cached);
    } else {
      const { rows } = await this.db.query(
        `SELECT f.*, fv.size_bytes, fv.file_hash, fv.version_number
         FROM files f
         LEFT JOIN file_versions fv ON fv.id = f.current_version_id
         WHERE f.id = $1 AND f.status = 'active'`,
        [fileId]
      );

      if (rows.length === 0) return null;
      file = rows[0];
      await this.redis.setex(cacheKey, this.CACHE_TTL, JSON.stringify(file));
    }

    // Permission check
    const canAccess = await this.checkPermission(fileId, requestingUserId, 'viewer');
    if (!canAccess) throw Object.assign(new Error('Forbidden'), { status: 403 });

    return file;
  }

  /**
   * List directory contents (one level, no recursion).
   * @param {string|null} parentId  - null means root directory
   * @param {string} userId
   */
  async listDirectory(parentId, userId) {
    const cacheKey = `dir:${userId}:${parentId || 'root'}`;
    const cached = await this.redis.get(cacheKey);
    if (cached) return JSON.parse(cached);

    const { rows } = await this.db.query(
      `SELECT f.id, f.name, f.is_folder, f.updated_at, fv.size_bytes, fv.mime_type
       FROM files f
       LEFT JOIN file_versions fv ON fv.id = f.current_version_id
       WHERE f.owner_id = $1
         AND f.parent_id IS NOT DISTINCT FROM $2
         AND f.status = 'active'
       ORDER BY f.is_folder DESC, f.name ASC`,
      [userId, parentId]
    );

    await this.redis.setex(cacheKey, 60, JSON.stringify(rows)); // Short TTL for directories
    return rows;
  }

  // ─── File Creation ────────────────────────────────────────────────────────

  /**
   * Initialize a new file upload session.
   * Returns upload_id and pre-authorized chunk upload URLs.
   *
   * @param {object} params
   * @param {string} params.userId
   * @param {string|null} params.parentId
   * @param {string} params.name
   * @param {number} params.sizeBytes
   * @param {string} params.fileHash       - Client-computed SHA-256 of full file
   * @param {string[]} params.chunkHashes  - Ordered array of chunk SHA-256 digests
   */
  async initUpload({ userId, parentId, name, sizeBytes, fileHash, chunkHashes }) {
    // Check storage quota
    const quotaOk = await this.checkQuota(userId, sizeBytes);
    if (!quotaOk) throw Object.assign(new Error('Quota exceeded'), { status: 402 });

    // Deduplication check: if full file hash exists as current version somewhere,
    // we can create a virtual copy without re-uploading (server-side copy)
    const existingVersion = await this.findExistingVersion(fileHash);

    const client = await this.db.connect();
    try {
      await client.query('BEGIN');

      // Create/update file record
      const pathHash = this.computePathHash(userId, parentId, name);

      const { rows: fileRows } = await client.query(
        `INSERT INTO files (owner_id, parent_id, name, status, path_hash)
         VALUES ($1, $2, $3, 'uploading', $4)
         ON CONFLICT (owner_id, parent_id, name) DO UPDATE
           SET status = 'uploading', updated_at = NOW()
         RETURNING id`,
        [userId, parentId, name, pathHash]
      );
      const fileId = fileRows[0].id;

      // Get next version number
      const { rows: versionRows } = await client.query(
        `SELECT COALESCE(MAX(version_number), 0) + 1 AS next_version
         FROM file_versions WHERE file_id = $1`,
        [fileId]
      );
      const versionNumber = versionRows[0].next_version;

      // Create version record
      const uploadId = crypto.randomBytes(16).toString('hex');
      const { rows: vRows } = await client.query(
        `INSERT INTO file_versions (file_id, version_number, size_bytes, file_hash, upload_id, created_by)
         VALUES ($1, $2, $3, $4, $5, $6) RETURNING id`,
        [fileId, versionNumber, sizeBytes, fileHash, uploadId, userId]
      );
      const versionId = vRows[0].id;

      await client.query('COMMIT');

      // Determine which chunks need uploading vs. already exist (dedup)
      const chunksToUpload = existingVersion
        ? [] // Server-side copy — no chunks need uploading
        : await this.filterNewChunks(chunkHashes);

      return {
        uploadId,
        fileId,
        versionId,
        chunksToUpload, // Hashes of chunks client must upload
        chunksSkipped: chunkHashes.length - chunksToUpload.length,
        isServerSideCopy: !!existingVersion,
      };
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }
  }

  /**
   * Commit an upload session: mark file as active, link version.
   * Called after client has successfully uploaded all required chunks.
   *
   * @param {string} uploadId
   * @param {string} userId
   * @param {{ index: number, hash: string }[]} chunkManifest  - Ordered chunk list
   */
  async commitUpload(uploadId, userId, chunkManifest) {
    const client = await this.db.connect();
    try {
      await client.query('BEGIN');

      // Fetch version record by upload_id
      const { rows } = await client.query(
        `SELECT fv.id AS version_id, fv.file_id, fv.size_bytes
         FROM file_versions fv
         JOIN files f ON f.id = fv.file_id
         WHERE fv.upload_id = $1 AND f.owner_id = $2`,
        [uploadId, userId]
      );
      if (rows.length === 0) throw Object.assign(new Error('Upload session not found'), { status: 404 });

      const { version_id, file_id, size_bytes } = rows[0];

      // Insert chunk manifest
      const chunkInserts = chunkManifest.map((c) =>
        client.query(
          `INSERT INTO version_chunks (version_id, chunk_index, chunk_id) VALUES ($1, $2, $3)`,
          [version_id, c.index, c.hash]
        )
      );
      await Promise.all(chunkInserts);

      // Activate file: point current_version_id to new version
      await client.query(
        `UPDATE files SET status = 'active', current_version_id = $1, updated_at = NOW()
         WHERE id = $2`,
        [version_id, file_id]
      );

      // Update used quota
      await client.query(
        `UPDATE users SET used_bytes = used_bytes + $1 WHERE id = $2`,
        [size_bytes, userId]
      );

      await client.query('COMMIT');

      // Invalidate caches
      await this.redis.del(`file:${file_id}`);

      // Emit event for sync notification
      await this.eventBus.emit('file.uploaded', {
        fileId: file_id,
        versionId: version_id,
        userId,
        timestamp: new Date().toISOString(),
      });

      return { fileId: file_id, versionId: version_id };
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }
  }

  // ─── Delete ───────────────────────────────────────────────────────────────

  /**
   * Soft-delete a file. Physical deletion is handled by the GC.
   */
  async deleteFile(fileId, userId) {
    await this.checkPermission(fileId, userId, 'admin', { throw: true });

    const { rows } = await this.db.query(
      `UPDATE files
       SET status = 'deleted', deleted_at = NOW(), updated_at = NOW()
       WHERE id = $1 AND owner_id = $2 RETURNING id`,
      [fileId, userId]
    );

    if (rows.length === 0) throw Object.assign(new Error('File not found'), { status: 404 });

    await this.redis.del(`file:${fileId}`);
    await this.eventBus.emit('file.deleted', { fileId, userId, timestamp: new Date().toISOString() });
  }

  // ─── Permissions ──────────────────────────────────────────────────────────

  /**
   * Hierarchical permission check.
   * Checks file ACL first, then walks up parent chain.
   */
  async checkPermission(fileId, userId, requiredRole, opts = {}) {
    const ROLE_LEVELS = { viewer: 1, editor: 2, admin: 3 };
    const required = ROLE_LEVELS[requiredRole] || 0;

    const cacheKey = `perm:${fileId}:${userId}`;
    const cached = await this.redis.get(cacheKey);

    let role;
    if (cached) {
      role = cached;
    } else {
      // Check owner first (fast path)
      const { rows: ownerRows } = await this.db.query(
        `SELECT 1 FROM files WHERE id = $1 AND owner_id = $2`,
        [fileId, userId]
      );
      if (ownerRows.length > 0) {
        role = 'admin';
      } else {
        // Check explicit ACL
        const { rows: aclRows } = await this.db.query(
          `SELECT role FROM permissions WHERE resource_id = $1 AND grantee_id = $2
           AND (expires_at IS NULL OR expires_at > NOW())`,
          [fileId, userId]
        );
        role = aclRows.length > 0 ? aclRows[0].role : 'none';
      }

      await this.redis.setex(cacheKey, 60, role);
    }

    const hasAccess = (ROLE_LEVELS[role] || 0) >= required;
    if (!hasAccess && opts.throw) {
      throw Object.assign(new Error('Forbidden'), { status: 403 });
    }
    return hasAccess;
  }

  // ─── Helpers ─────────────────────────────────────────────────────────────

  computePathHash(userId, parentId, name) {
    return crypto
      .createHash('sha256')
      .update(`${userId}:${parentId || ''}:${name}`)
      .digest('hex');
  }

  async checkQuota(userId, additionalBytes) {
    const { rows } = await this.db.query(
      `SELECT quota_bytes - used_bytes AS remaining FROM users WHERE id = $1`,
      [userId]
    );
    return rows.length > 0 && rows[0].remaining >= additionalBytes;
  }

  async findExistingVersion(fileHash) {
    const { rows } = await this.db.query(
      `SELECT id FROM file_versions WHERE file_hash = $1 LIMIT 1`,
      [fileHash]
    );
    return rows.length > 0 ? rows[0] : null;
  }

  async filterNewChunks(chunkHashes) {
    // Return only hashes that don't already exist in the chunk store
    const { rows } = await this.db.query(
      `SELECT hash FROM chunks WHERE hash = ANY($1)`,
      [chunkHashes]
    );
    const existing = new Set(rows.map((r) => r.hash));
    return chunkHashes.filter((h) => !existing.has(h));
  }
}
```

### 9.5 Storage Node — Class Design

```javascript
// storageNode.js
// A single storage node: reads and writes raw chunk blobs to local disk.
// In production, multiple instances run across a storage cluster.

import fs from 'fs/promises';
import path from 'path';
import crypto from 'crypto';
import express from 'express';

const CHUNK_BASE_DIR = process.env.CHUNK_DIR || '/data/chunks';

export class StorageNode {
  /**
   * @param {string} nodeId     - Unique node identifier (e.g., "node-us-east-1a-01")
   * @param {string} baseDir    - Root directory for chunk storage
   */
  constructor(nodeId, baseDir = CHUNK_BASE_DIR) {
    this.nodeId = nodeId;
    this.baseDir = baseDir;
    this.app = express();
    this._registerRoutes();
  }

  /**
   * Derive the filesystem path for a chunk.
   * Uses two-level directory prefix to avoid inode exhaustion.
   *
   * e.g., hash = "abcdef1234..." → /data/chunks/ab/cd/abcdef1234...
   */
  chunkPath(hash) {
    return path.join(this.baseDir, hash.slice(0, 2), hash.slice(2, 4), hash);
  }

  // ─── Write ────────────────────────────────────────────────────────────────

  /**
   * Write a chunk to disk.
   * Verifies integrity (declared hash must match data hash) before writing.
   *
   * @param {string} declaredHash   - SHA-256 hex digest declared by client
   * @param {Buffer} data           - Raw chunk bytes
   * @returns {Promise<string>}     - Confirmed chunk hash (= ETag)
   */
  async writeChunk(declaredHash, data) {
    // Integrity check: verify the data matches the declared hash
    const actualHash = crypto.createHash('sha256').update(data).digest('hex');
    if (actualHash !== declaredHash) {
      throw Object.assign(
        new Error(`Chunk integrity check failed: declared=${declaredHash} actual=${actualHash}`),
        { status: 400 }
      );
    }

    const filePath = this.chunkPath(declaredHash);
    const dir = path.dirname(filePath);

    // Ensure directory exists
    await fs.mkdir(dir, { recursive: true });

    // Atomic write: write to temp file first, then rename
    // This prevents partial writes from corrupting the chunk store.
    const tmpPath = `${filePath}.tmp.${Date.now()}`;
    try {
      await fs.writeFile(tmpPath, data, { flag: 'wx' }); // Exclusive create
      await fs.rename(tmpPath, filePath);                  // Atomic rename
    } catch (err) {
      // If file already exists, another concurrent writer won the race — that's fine
      if (err.code !== 'EEXIST') {
        await fs.unlink(tmpPath).catch(() => {});          // Cleanup temp file
        throw err;
      }
    }

    return declaredHash;
  }

  // ─── Read ─────────────────────────────────────────────────────────────────

  /**
   * Read a chunk from disk.
   * Verifies integrity on read (detects silent bit-rot).
   *
   * @param {string} hash           - SHA-256 hex digest
   * @param {object} range          - Optional byte range { start, end }
   * @returns {Promise<Buffer>}
   */
  async readChunk(hash, range = null) {
    const filePath = this.chunkPath(hash);

    let data;
    if (range) {
      // Byte-range read for streaming large downloads
      const { start, end } = range;
      const fd = await fs.open(filePath, 'r');
      try {
        const length = end - start + 1;
        const buf = Buffer.alloc(length);
        await fd.read(buf, 0, length, start);
        data = buf;
      } finally {
        await fd.close();
      }
    } else {
      data = await fs.readFile(filePath);
    }

    // Integrity check on read — detect bit-rot or corruption
    if (!range) {
      const actualHash = crypto.createHash('sha256').update(data).digest('hex');
      if (actualHash !== hash) {
        // This chunk is corrupt — signal replication manager to restore from replica
        throw Object.assign(
          new Error(`Chunk corruption detected: ${hash}`),
          { status: 500, code: 'CHUNK_CORRUPT' }
        );
      }
    }

    return data;
  }

  // ─── Delete ───────────────────────────────────────────────────────────────

  /**
   * Physically delete a chunk from disk. Only called by Garbage Collector.
   * @param {string} hash
   */
  async deleteChunk(hash) {
    const filePath = this.chunkPath(hash);
    await fs.unlink(filePath).catch((err) => {
      if (err.code !== 'ENOENT') throw err; // Ignore "file not found"
    });
  }

  // ─── Health & Stats ───────────────────────────────────────────────────────

  async getStats() {
    const { statvfs } = await import('fs');  // Available on Linux
    // Simplified: in production, use systeminformation or df command
    return {
      nodeId: this.nodeId,
      status: 'healthy',
      timestamp: new Date().toISOString(),
    };
  }

  // ─── HTTP API (Express) ───────────────────────────────────────────────────

  _registerRoutes() {
    // Raw body for chunk uploads
    this.app.use('/chunks', express.raw({ limit: '5mb' }));

    // PUT /chunks/:hash — Upload a chunk
    this.app.put('/chunks/:hash', async (req, res) => {
      try {
        const etag = await this.writeChunk(req.params.hash, req.body);
        res.setHeader('ETag', etag);
        res.status(200).json({ etag });
      } catch (err) {
        res.status(err.status || 500).json({ error: err.message });
      }
    });

    // GET /chunks/:hash — Download a chunk
    this.app.get('/chunks/:hash', async (req, res) => {
      try {
        const range = req.headers.range
          ? this._parseRange(req.headers.range)
          : null;

        const data = await this.readChunk(req.params.hash, range);

        if (range) {
          res.status(206); // Partial content
          res.setHeader('Content-Range', `bytes ${range.start}-${range.end}/*`);
        }
        res.setHeader('Content-Type', 'application/octet-stream');
        res.setHeader('ETag', req.params.hash);
        res.setHeader('Cache-Control', 'public, max-age=31536000, immutable'); // Chunks are immutable
        res.send(data);
      } catch (err) {
        if (err.code === 'CHUNK_CORRUPT') {
          // Trigger replica fetch (in production: return 503, let load balancer retry replica)
          res.status(503).json({ error: 'Chunk unavailable, try another replica' });
        } else {
          res.status(err.status || 500).json({ error: err.message });
        }
      }
    });

    // DELETE /chunks/:hash — GC deletion
    this.app.delete('/chunks/:hash', async (req, res) => {
      try {
        await this.deleteChunk(req.params.hash);
        res.status(204).send();
      } catch (err) {
        res.status(err.status || 500).json({ error: err.message });
      }
    });

    // GET /health
    this.app.get('/health', async (req, res) => {
      const stats = await this.getStats();
      res.json(stats);
    });
  }

  _parseRange(rangeHeader) {
    const match = rangeHeader.match(/bytes=(\d+)-(\d+)/);
    if (!match) return null;
    return { start: parseInt(match[1]), end: parseInt(match[2]) };
  }

  listen(port = 8080) {
    return this.app.listen(port, () => {
      console.log(`StorageNode ${this.nodeId} listening on :${port}`);
    });
  }
}
```

### 9.6 Replication Manager

```javascript
// replicationManager.js
// Ensures every chunk is replicated to exactly REPLICATION_FACTOR nodes.
// Runs as a background process alongside the primary Chunk Service.

import axios from 'axios';

const REPLICATION_FACTOR = 3;

export class ReplicationManager {
  /**
   * @param {object} db                   - PostgreSQL pool
   * @param {ConsistentHashRing} hashRing  - Node selection ring
   */
  constructor(db, hashRing) {
    this.db = db;
    this.hashRing = hashRing;
  }

  /**
   * After a chunk is written to a primary node, asynchronously replicate to N-1 more nodes.
   * Called non-blocking from the upload path.
   *
   * @param {string} chunkHash
   * @param {string} primaryNodeId      - Node where chunk was initially written
   * @param {Buffer} chunkData          - Chunk bytes (in memory from upload)
   */
  async replicateAsync(chunkHash, primaryNodeId, chunkData) {
    const targetNodes = this.hashRing
      .getNodes(chunkHash, REPLICATION_FACTOR)  // Get N preferred nodes for this hash
      .filter((n) => n.id !== primaryNodeId);    // Exclude primary (already written)

    const replicaTasks = targetNodes.map((node) =>
      this.replicateToNode(chunkHash, chunkData, node).catch((err) => {
        // Log failure but don't fail the upload — GC/anti-entropy will fix later
        console.error(`Replication to ${node.id} failed for chunk ${chunkHash}: ${err.message}`);
      })
    );

    // Fire and forget — caller doesn't wait for replication to complete
    Promise.allSettled(replicaTasks).then(() => {
      this.updateReplicaLocations(chunkHash, targetNodes.map((n) => n.id));
    });
  }

  /**
   * Synchronous replication path: wait for W=2 replicas before returning success.
   * Used when strong durability guarantee is required immediately.
   *
   * @param {string} chunkHash
   * @param {string} primaryNodeId
   * @param {Buffer} chunkData
   * @returns {Promise<string[]>}  - Node IDs where chunk is now stored
   */
  async replicateSync(chunkHash, primaryNodeId, chunkData) {
    const WRITE_QUORUM = 2; // Must succeed on 2 of 3 nodes (including primary)

    const targetNodes = this.hashRing
      .getNodes(chunkHash, REPLICATION_FACTOR)
      .filter((n) => n.id !== primaryNodeId);

    const results = await Promise.allSettled(
      targetNodes.map((node) => this.replicateToNode(chunkHash, chunkData, node))
    );

    const successfulNodes = results
      .filter((r) => r.status === 'fulfilled')
      .map((_, i) => targetNodes[i].id);

    // +1 for the primary node already written
    const totalSuccessful = successfulNodes.length + 1;

    if (totalSuccessful < WRITE_QUORUM) {
      throw new Error(
        `Failed to meet write quorum: ${totalSuccessful}/${WRITE_QUORUM} replicas written`
      );
    }

    await this.updateReplicaLocations(chunkHash, successfulNodes);
    return [primaryNodeId, ...successfulNodes];
  }

  async replicateToNode(chunkHash, chunkData, node) {
    await axios.put(`${node.url}/chunks/${chunkHash}`, chunkData, {
      headers: {
        'Content-Type': 'application/octet-stream',
        'Content-Length': chunkData.length,
        'X-Chunk-Hash': chunkHash,
        'X-Internal-Request': 'true', // Bypass auth for internal replication
      },
      timeout: 30000,
    });
  }

  async updateReplicaLocations(chunkHash, nodeIds) {
    if (nodeIds.length === 0) return;
    const values = nodeIds
      .map((_, i) => `($1, $${i + 2}, 'replica', 'active')`)
      .join(', ');
    await this.db.query(
      `INSERT INTO chunk_locations (chunk_hash, node_id, replica_role, status)
       VALUES ${values} ON CONFLICT DO NOTHING`,
      [chunkHash, ...nodeIds]
    );
  }

  /**
   * Anti-entropy: find under-replicated chunks and restore them.
   * Runs on a schedule (e.g., every 10 minutes).
   */
  async repairUnderReplicatedChunks() {
    // Find chunks with fewer locations than REPLICATION_FACTOR
    const { rows } = await this.db.query(
      `SELECT c.hash, array_agg(cl.node_id) AS locations
       FROM chunks c
       JOIN chunk_locations cl ON cl.chunk_hash = c.hash AND cl.status = 'active'
       GROUP BY c.hash
       HAVING COUNT(cl.node_id) < $1`,
      [REPLICATION_FACTOR]
    );

    console.log(`Anti-entropy: found ${rows.length} under-replicated chunks`);

    for (const chunk of rows) {
      const missingCount = REPLICATION_FACTOR - chunk.locations.length;
      if (missingCount <= 0) continue;

      // Fetch from existing location
      const sourceNode = this.hashRing.getNodeById(chunk.locations[0]);
      if (!sourceNode) continue;

      try {
        const response = await axios.get(`${sourceNode.url}/chunks/${chunk.hash}`, {
          responseType: 'arraybuffer',
          timeout: 30000,
        });
        const chunkData = Buffer.from(response.data);

        // Find new nodes to replicate to
        const allPreferredNodes = this.hashRing.getNodes(chunk.hash, REPLICATION_FACTOR);
        const newNodes = allPreferredNodes.filter((n) => !chunk.locations.includes(n.id));

        for (let i = 0; i < Math.min(missingCount, newNodes.length); i++) {
          await this.replicateToNode(chunk.hash, chunkData, newNodes[i]);
          await this.updateReplicaLocations(chunk.hash, [newNodes[i].id]);
        }
      } catch (err) {
        console.error(`Failed to repair chunk ${chunk.hash}: ${err.message}`);
      }
    }
  }
}
```

### 9.7 Consistent Hashing Ring

```javascript
// consistentHashRing.js
// Distributes chunks across storage nodes using consistent hashing.
// Minimizes data movement when nodes are added or removed.

import crypto from 'crypto';

const VIRTUAL_NODES = 150; // Virtual nodes per physical node (reduces hot spots)

export class ConsistentHashRing {
  constructor() {
    this.ring = new Map();       // sortedHash → node
    this.sortedKeys = [];        // Sorted list of hash positions
    this.nodes = new Map();      // nodeId → node metadata
  }

  /**
   * Add a node to the ring.
   * Creates VIRTUAL_NODES virtual points distributed around the ring.
   *
   * @param {{ id: string, url: string, weight?: number }} node
   */
  addNode(node) {
    this.nodes.set(node.id, node);
    const vnodeCount = VIRTUAL_NODES * (node.weight || 1);

    for (let i = 0; i < vnodeCount; i++) {
      const virtualKey = `${node.id}#${i}`;
      const hash = this._hash(virtualKey);
      this.ring.set(hash, node.id);
    }

    this._rebuildSortedKeys();
  }

  /**
   * Remove a node from the ring (on failure or decommission).
   * All virtual nodes for this node are removed.
   * Keys previously routed to this node will route to the next node clockwise.
   *
   * @param {string} nodeId
   */
  removeNode(nodeId) {
    this.nodes.delete(nodeId);

    for (const [hash, id] of this.ring) {
      if (id === nodeId) {
        this.ring.delete(hash);
      }
    }

    this._rebuildSortedKeys();
  }

  /**
   * Get the N preferred nodes for a given key.
   * Returns physically distinct nodes (not virtual node duplicates).
   *
   * @param {string} key        - Chunk hash or file ID
   * @param {number} n          - Number of nodes to return (= replication factor)
   * @returns {object[]}        - Array of node metadata objects
   */
  getNodes(key, n = 1) {
    if (this.nodes.size === 0) throw new Error('No nodes in ring');
    if (n > this.nodes.size) n = this.nodes.size;

    const keyHash = this._hash(key);
    const startIndex = this._findInsertionPoint(keyHash);

    const selectedNodes = [];
    const seenNodeIds = new Set();

    // Walk clockwise from the key's position
    for (let i = 0; i < this.sortedKeys.length && selectedNodes.length < n; i++) {
      const ringIndex = (startIndex + i) % this.sortedKeys.length;
      const ringHash = this.sortedKeys[ringIndex];
      const nodeId = this.ring.get(ringHash);

      if (!seenNodeIds.has(nodeId)) {
        seenNodeIds.add(nodeId);
        selectedNodes.push(this.nodes.get(nodeId));
      }
    }

    return selectedNodes;
  }

  /**
   * Get the primary node for a chunk hash.
   */
  getPrimaryNode(key) {
    return this.getNodes(key, 1)[0];
  }

  getNodeById(nodeId) {
    return this.nodes.get(nodeId) || null;
  }

  getAllNodes() {
    return Array.from(this.nodes.values());
  }

  // ─── Internal ─────────────────────────────────────────────────────────────

  _hash(key) {
    // MD5 gives good distribution and is fast (not used for security here)
    return parseInt(
      crypto.createHash('md5').update(key).digest('hex').slice(0, 8),
      16
    );
  }

  _rebuildSortedKeys() {
    this.sortedKeys = Array.from(this.ring.keys()).sort((a, b) => a - b);
  }

  _findInsertionPoint(hash) {
    // Binary search for the first ring position >= hash
    let lo = 0;
    let hi = this.sortedKeys.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (this.sortedKeys[mid] < hash) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    return lo % this.sortedKeys.length;
  }
}
```

### 9.8 Sync Engine (Client-Side)

```javascript
// syncEngine.js
// Client-side sync engine: detects local file changes and applies remote changes.
// Uses a local SQLite DB to track file state and sync cursor.

/**
 * The SyncEngine is responsible for:
 *  1. Watching local filesystem for changes
 *  2. Uploading changed files to the server
 *  3. Receiving remote change notifications via WebSocket
 *  4. Downloading and applying remote changes to local disk
 *  5. Handling conflicts between local and remote changes
 */
export class SyncEngine {
  /**
   * @param {object} options
   * @param {string} options.syncRoot       - Local directory to sync
   * @param {string} options.userId         - Authenticated user ID
   * @param {string} options.apiBaseUrl     - API base URL
   * @param {string} options.authToken      - Auth token
   * @param {object} options.localDb        - SQLite connection (better-sqlite3)
   */
  constructor({ syncRoot, userId, apiBaseUrl, authToken, localDb }) {
    this.syncRoot = syncRoot;
    this.userId = userId;
    this.apiBaseUrl = apiBaseUrl;
    this.authToken = authToken;
    this.localDb = localDb;
    this.ws = null;                   // WebSocket connection
    this.uploadQueue = [];            // Pending uploads
    this.isProcessing = false;
  }

  // ─── Bootstrap ───────────────────────────────────────────────────────────

  async start() {
    this._initLocalDb();
    await this._connectWebSocket();
    await this._initialSync();
    this._watchLocalFS();
  }

  _initLocalDb() {
    this.localDb.exec(`
      CREATE TABLE IF NOT EXISTS local_files (
        path        TEXT PRIMARY KEY,
        file_id     TEXT,
        version_id  TEXT,
        local_hash  TEXT,       -- SHA-256 of local file content
        local_mtime INTEGER,    -- Last modified timestamp (ms)
        sync_state  TEXT        -- 'synced' | 'pending_upload' | 'pending_download' | 'conflict'
      );

      CREATE TABLE IF NOT EXISTS sync_cursor (
        user_id  TEXT PRIMARY KEY,
        seq      INTEGER DEFAULT 0
      );
    `);
  }

  // ─── WebSocket Connection ────────────────────────────────────────────────

  async _connectWebSocket() {
    const { WebSocket } = await import('ws');
    const wsUrl = this.apiBaseUrl.replace(/^http/, 'ws') + `/sync?token=${this.authToken}`;

    this.ws = new WebSocket(wsUrl);

    this.ws.on('message', (data) => {
      const event = JSON.parse(data);
      this._handleRemoteEvent(event);
    });

    this.ws.on('close', () => {
      // Reconnect with exponential backoff
      setTimeout(() => this._connectWebSocket(), 5000);
    });

    return new Promise((resolve) => {
      this.ws.on('open', resolve);
    });
  }

  // ─── Initial Sync ─────────────────────────────────────────────────────────

  /**
   * On app start, fetch all changes since our last known cursor.
   * Applies remote changes and queues any local changes for upload.
   */
  async _initialSync() {
    const cursor = this._getCursor();

    const response = await fetch(
      `${this.apiBaseUrl}/sync/changes?since_seq=${cursor}`,
      { headers: { Authorization: `Bearer ${this.authToken}` } }
    );
    const { events, cursor: newCursor } = await response.json();

    for (const event of events) {
      await this._applyRemoteEvent(event);
    }

    this._saveCursor(newCursor);
  }

  // ─── Local Filesystem Watcher ─────────────────────────────────────────────

  async _watchLocalFS() {
    const chokidar = (await import('chokidar')).default;

    const watcher = chokidar.watch(this.syncRoot, {
      persistent: true,
      ignoreInitial: false,
      awaitWriteFinish: { stabilityThreshold: 500, pollInterval: 100 },
    });

    watcher.on('add', (path) => this._onLocalChange(path, 'add'));
    watcher.on('change', (path) => this._onLocalChange(path, 'change'));
    watcher.on('unlink', (path) => this._onLocalDelete(path));
  }

  async _onLocalChange(absolutePath, eventType) {
    const relativePath = absolutePath.replace(this.syncRoot + '/', '');
    const existing = this.localDb
      .prepare('SELECT * FROM local_files WHERE path = ?')
      .get(relativePath);

    // Compute new hash to detect if content actually changed
    const { chunkFile } = await import('./fileChunker.js');
    const { readFileSync } = await import('fs');
    const content = readFileSync(absolutePath);
    const { fileHash } = chunkFile(content);

    if (existing && existing.local_hash === fileHash) return; // No real change

    // Enqueue upload
    this.localDb
      .prepare(`INSERT OR REPLACE INTO local_files (path, local_hash, sync_state)
                VALUES (?, ?, 'pending_upload')`)
      .run(relativePath, fileHash);

    this.uploadQueue.push({ relativePath, absolutePath, fileHash });
    this._processQueue();
  }

  async _onLocalDelete(absolutePath) {
    const relativePath = absolutePath.replace(this.syncRoot + '/', '');
    const existing = this.localDb
      .prepare('SELECT file_id FROM local_files WHERE path = ?')
      .get(relativePath);

    if (existing?.file_id) {
      // Notify server of deletion
      await fetch(`${this.apiBaseUrl}/files/${existing.file_id}`, {
        method: 'DELETE',
        headers: { Authorization: `Bearer ${this.authToken}` },
      });
    }

    this.localDb.prepare('DELETE FROM local_files WHERE path = ?').run(relativePath);
  }

  // ─── Queue Processor ─────────────────────────────────────────────────────

  async _processQueue() {
    if (this.isProcessing || this.uploadQueue.length === 0) return;
    this.isProcessing = true;

    while (this.uploadQueue.length > 0) {
      const item = this.uploadQueue.shift();
      try {
        await this._uploadFile(item);
        this.localDb
          .prepare('UPDATE local_files SET sync_state = ? WHERE path = ?')
          .run('synced', item.relativePath);
      } catch (err) {
        console.error(`Upload failed for ${item.relativePath}:`, err.message);
        // Re-queue with back-off (simplified — production would use a proper job queue)
        setTimeout(() => this.uploadQueue.push(item), 10000);
      }
    }

    this.isProcessing = false;
  }

  async _uploadFile({ relativePath, absolutePath }) {
    const { readFileSync } = await import('fs');
    const { chunkFile } = await import('./fileChunker.js');
    const { ChunkUploadManager } = await import('./chunkUploadManager.js');

    const content = readFileSync(absolutePath);
    const { chunks, fileHash } = chunkFile(content);

    // Init upload
    const initResp = await fetch(`${this.apiBaseUrl}/files/init`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${this.authToken}`,
      },
      body: JSON.stringify({
        name: relativePath.split('/').pop(),
        path: relativePath,
        sizeBytes: content.length,
        fileHash,
        chunkHashes: chunks.map((c) => c.hash),
      }),
    });

    const { uploadId, chunksToUpload } = await initResp.json();

    // Upload only chunks the server doesn't already have (dedup)
    const chunksToSend = chunks.filter((c) => chunksToUpload.includes(c.hash));
    const manager = new ChunkUploadManager(uploadId, this.apiBaseUrl, this.authToken);
    const receipts = await manager.uploadAll(chunksToSend, (done, total) => {
      console.log(`Uploading ${relativePath}: ${done}/${total} chunks`);
    });

    // Commit
    await fetch(`${this.apiBaseUrl}/files/commit`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${this.authToken}`,
      },
      body: JSON.stringify({
        uploadId,
        chunkManifest: chunks.map((c, i) => ({ index: i, hash: c.hash })),
      }),
    });
  }

  // ─── Remote Event Handling ───────────────────────────────────────────────

  _handleRemoteEvent(event) {
    this._applyRemoteEvent(event);
    this._saveCursor(event.seq);
  }

  async _applyRemoteEvent(event) {
    const { event_type, file_id, payload } = event;

    switch (event_type) {
      case 'created':
      case 'modified': {
        const localRecord = this.localDb
          .prepare('SELECT * FROM local_files WHERE file_id = ?')
          .get(file_id);

        if (localRecord && localRecord.sync_state === 'pending_upload') {
          // Conflict: local changes + remote changes on same file
          await this._handleConflict(localRecord, event);
        } else {
          await this._downloadFile(file_id, payload.path);
        }
        break;
      }
      case 'deleted': {
        const record = this.localDb
          .prepare('SELECT path FROM local_files WHERE file_id = ?')
          .get(file_id);
        if (record) {
          const { unlink } = await import('fs/promises');
          await unlink(`${this.syncRoot}/${record.path}`).catch(() => {});
          this.localDb.prepare('DELETE FROM local_files WHERE file_id = ?').run(file_id);
        }
        break;
      }
    }
  }

  async _downloadFile(fileId, remotePath) {
    // Fetch chunk URLs from metadata service
    const resp = await fetch(`${this.apiBaseUrl}/files/${fileId}/download`, {
      headers: { Authorization: `Bearer ${this.authToken}` },
    });
    const { chunks } = await resp.json();

    // Download all chunks in parallel and reassemble
    const chunkBuffers = await Promise.all(
      chunks.map(async (chunk) => {
        const data = await fetch(chunk.url);
        return { index: chunk.index, buffer: Buffer.from(await data.arrayBuffer()) };
      })
    );

    chunkBuffers.sort((a, b) => a.index - b.index);
    const fileContent = Buffer.concat(chunkBuffers.map((c) => c.buffer));

    const localPath = `${this.syncRoot}/${remotePath}`;
    const { mkdir, writeFile } = await import('fs/promises');
    await mkdir(localPath.substring(0, localPath.lastIndexOf('/')), { recursive: true });
    await writeFile(localPath, fileContent);

    this.localDb
      .prepare(`INSERT OR REPLACE INTO local_files (path, file_id, sync_state) VALUES (?, ?, 'synced')`)
      .run(remotePath, fileId);
  }

  async _handleConflict(localRecord, remoteEvent) {
    // Conflict resolution: Dropbox-style — create a "conflicted copy" locally
    const conflictPath = localRecord.path.replace(
      /(\.[^.]+)?$/,
      ` (conflicted copy ${new Date().toISOString().slice(0, 10)})$1`
    );

    const { rename } = await import('fs/promises');
    await rename(
      `${this.syncRoot}/${localRecord.path}`,
      `${this.syncRoot}/${conflictPath}`
    );

    // Download the remote version to the original path
    await this._downloadFile(remoteEvent.file_id, localRecord.path);

    console.warn(`Conflict detected for ${localRecord.path}. Conflicted copy saved as ${conflictPath}`);
  }

  // ─── Cursor Management ────────────────────────────────────────────────────

  _getCursor() {
    const row = this.localDb
      .prepare('SELECT seq FROM sync_cursor WHERE user_id = ?')
      .get(this.userId);
    return row?.seq || 0;
  }

  _saveCursor(seq) {
    this.localDb
      .prepare('INSERT OR REPLACE INTO sync_cursor (user_id, seq) VALUES (?, ?)')
      .run(this.userId, seq);
  }
}
```

### 9.9 Lock Manager (Distributed Locking)

```javascript
// lockManager.js
// Distributed lock using Redis SETNX + expiry (Redlock-lite).
// Used to prevent concurrent operations on the same file (e.g., simultaneous uploads).

const DEFAULT_LOCK_TTL_MS = 30000;  // 30 seconds
const RETRY_COUNT = 3;
const RETRY_DELAY_MS = 200;

export class DistributedLockManager {
  /**
   * @param {object} redis  - ioredis client
   */
  constructor(redis) {
    this.redis = redis;
  }

  /**
   * Acquire a distributed lock for a resource.
   * Uses SET NX EX (atomic set-if-not-exists with expiry).
   *
   * @param {string} resource   - Lock key (e.g., `file-upload:${fileId}`)
   * @param {number} ttlMs      - Lock TTL in milliseconds (auto-release safety net)
   * @returns {Promise<string|null>}  - Lock token if acquired, null if failed
   */
  async acquire(resource, ttlMs = DEFAULT_LOCK_TTL_MS) {
    const lockKey = `lock:${resource}`;
    // Unique token: proves this caller owns the lock (prevents accidental release by others)
    const token = `${Date.now()}-${Math.random().toString(36).slice(2)}`;

    for (let attempt = 0; attempt < RETRY_COUNT; attempt++) {
      // Atomic: SET key value NX PX milliseconds
      const result = await this.redis.set(lockKey, token, 'PX', ttlMs, 'NX');

      if (result === 'OK') {
        return token; // Lock acquired
      }

      // Lock held by someone else — wait and retry
      await new Promise((res) => setTimeout(res, RETRY_DELAY_MS * (attempt + 1)));
    }

    return null; // Failed to acquire after retries
  }

  /**
   * Release a lock. Only releases if the caller owns it (token matches).
   * Uses a Lua script for atomic check-and-delete.
   *
   * @param {string} resource
   * @param {string} token      - Token returned by acquire()
   */
  async release(resource, token) {
    const lockKey = `lock:${resource}`;

    // Lua script: atomically check token and delete
    const luaScript = `
      if redis.call("GET", KEYS[1]) == ARGV[1] then
        return redis.call("DEL", KEYS[1])
      else
        return 0
      end
    `;

    const released = await this.redis.eval(luaScript, 1, lockKey, token);
    return released === 1;
  }

  /**
   * Extend a lock's TTL while work is still in progress.
   * Call this periodically from long-running operations.
   *
   * @param {string} resource
   * @param {string} token
   * @param {number} extendMs
   */
  async extend(resource, token, extendMs = DEFAULT_LOCK_TTL_MS) {
    const lockKey = `lock:${resource}`;

    const luaScript = `
      if redis.call("GET", KEYS[1]) == ARGV[1] then
        return redis.call("PEXPIRE", KEYS[1], ARGV[2])
      else
        return 0
      end
    `;

    return this.redis.eval(luaScript, 1, lockKey, token, extendMs);
  }

  /**
   * Helper: run a function while holding a lock.
   * Automatically releases lock when fn completes.
   *
   * @param {string} resource
   * @param {Function} fn
   * @param {number} ttlMs
   */
  async withLock(resource, fn, ttlMs = DEFAULT_LOCK_TTL_MS) {
    const token = await this.acquire(resource, ttlMs);
    if (!token) {
      throw new Error(`Could not acquire lock for resource: ${resource}`);
    }

    try {
      return await fn();
    } finally {
      await this.release(resource, token);
    }
  }
}
```

### 9.10 Rate Limiter (Token Bucket)

```javascript
// rateLimiter.js
// Token Bucket rate limiter using Redis for distributed enforcement.
// Each user/IP gets a bucket that refills at a constant rate.

export class TokenBucketRateLimiter {
  /**
   * @param {object} redis
   * @param {object} config
   * @param {number} config.capacity        - Max tokens in bucket
   * @param {number} config.refillRate      - Tokens added per second
   * @param {number} config.refillInterval  - How often to refill (ms)
   */
  constructor(redis, config = {}) {
    this.redis = redis;
    this.capacity = config.capacity || 100;           // Default: 100 tokens max
    this.refillRate = config.refillRate || 10;        // Default: 10 tokens/sec
    this.refillIntervalMs = config.refillInterval || 1000; // Refill every 1 second
  }

  /**
   * Check if a request is allowed and consume a token if so.
   * Uses a Lua script for atomic read-modify-write.
   *
   * @param {string} key          - Bucket key (e.g., `ratelimit:user:{userId}:upload`)
   * @param {number} tokensNeeded - Tokens to consume (default 1; large uploads may need more)
   * @returns {Promise<{ allowed: boolean, remaining: number, retryAfterMs: number }>}
   */
  async consume(key, tokensNeeded = 1) {
    const now = Date.now();

    // Lua script: atomic token bucket logic
    // KEYS[1] = bucket key
    // ARGV = [now_ms, capacity, refillRate, refillIntervalMs, tokensNeeded]
    const luaScript = `
      local key = KEYS[1]
      local now = tonumber(ARGV[1])
      local capacity = tonumber(ARGV[2])
      local refill_rate = tonumber(ARGV[3])
      local refill_interval = tonumber(ARGV[4])
      local tokens_needed = tonumber(ARGV[5])

      -- Get current bucket state
      local bucket = redis.call("HMGET", key, "tokens", "last_refill")
      local tokens = tonumber(bucket[1]) or capacity
      local last_refill = tonumber(bucket[2]) or now

      -- Calculate tokens to add since last refill
      local elapsed = math.max(0, now - last_refill)
      local new_tokens = math.floor(elapsed / refill_interval) * refill_rate
      tokens = math.min(capacity, tokens + new_tokens)

      -- Update last_refill only if we actually added tokens
      if new_tokens > 0 then
        last_refill = now
      end

      -- Check if enough tokens
      if tokens >= tokens_needed then
        tokens = tokens - tokens_needed
        redis.call("HMSET", key, "tokens", tokens, "last_refill", last_refill)
        redis.call("EXPIRE", key, 3600)  -- Auto-expire after 1 hour of inactivity
        return {1, tokens, 0}
      else
        -- Calculate wait time until enough tokens are available
        local deficit = tokens_needed - tokens
        local wait_ms = math.ceil(deficit / refill_rate) * refill_interval
        redis.call("HMSET", key, "tokens", tokens, "last_refill", last_refill)
        redis.call("EXPIRE", key, 3600)
        return {0, tokens, wait_ms}
      end
    `;

    const [allowed, remaining, retryAfterMs] = await this.redis.eval(
      luaScript,
      1,
      key,
      now,
      this.capacity,
      this.refillRate,
      this.refillIntervalMs,
      tokensNeeded
    );

    return {
      allowed: allowed === 1,
      remaining: Math.floor(remaining),
      retryAfterMs: Math.ceil(retryAfterMs),
    };
  }

  /**
   * Express middleware factory for route-level rate limiting.
   *
   * @param {string} scope              - Scope label (e.g., 'upload', 'download')
   * @param {object} options
   * @param {Function} options.keyFn    - Function to derive bucket key from request
   * @param {number} options.tokens     - Tokens consumed per request
   */
  middleware(scope, { keyFn, tokens = 1 } = {}) {
    return async (req, res, next) => {
      const userId = req.user?.id || req.ip;
      const bucketKey = keyFn
        ? keyFn(req)
        : `ratelimit:${scope}:${userId}`;

      const { allowed, remaining, retryAfterMs } = await this.consume(bucketKey, tokens);

      res.setHeader('X-RateLimit-Limit', this.capacity);
      res.setHeader('X-RateLimit-Remaining', remaining);

      if (!allowed) {
        res.setHeader('Retry-After', Math.ceil(retryAfterMs / 1000));
        return res.status(429).json({
          error: 'Rate limit exceeded',
          retryAfterMs,
        });
      }

      next();
    };
  }
}

// Usage example (in route setup):
// const limiter = new TokenBucketRateLimiter(redis, { capacity: 50, refillRate: 5 });
// app.post('/files/init', limiter.middleware('upload', { tokens: 1 }), uploadHandler);
// app.put('/chunks/:id', limiter.middleware('chunk', { tokens: 1,
//   keyFn: (req) => `ratelimit:chunk:${req.user.id}` }), chunkHandler);
```

---

## 10. API Design

### REST API Endpoints

```
─── Auth ───────────────────────────────────────────────────────────────────
POST   /auth/login                    → { access_token, refresh_token }
POST   /auth/refresh                  → { access_token }
DELETE /auth/logout                   → 204

─── Files ──────────────────────────────────────────────────────────────────
GET    /files                         → List root directory
GET    /files?parent_id={id}          → List directory contents
GET    /files/{id}                    → Get file metadata
GET    /files/{id}/download           → Get signed chunk download URLs
GET    /files/{id}/versions           → List versions

POST   /files/init                    → Initiate upload session
       Body: { name, parent_id, size_bytes, file_hash, chunk_hashes[] }
       Returns: { upload_id, chunks_to_upload[], is_server_side_copy }

POST   /files/commit                  → Commit upload
       Body: { upload_id, chunk_manifest[{ index, hash }] }
       Returns: { file_id, version_id }

PATCH  /files/{id}                    → Rename or move
       Body: { name?, parent_id? }

DELETE /files/{id}                    → Soft-delete file
POST   /files/{id}/restore            → Restore from trash

─── Chunks ─────────────────────────────────────────────────────────────────
PUT    /uploads/{upload_id}/chunks/{index}
       Body: raw bytes
       Headers: X-Chunk-Hash, Content-Length
       Returns: { etag }

─── Sharing ────────────────────────────────────────────────────────────────
POST   /files/{id}/permissions        → Share with user
       Body: { grantee_email, role }

GET    /files/{id}/permissions        → List ACL entries
DELETE /files/{id}/permissions/{pid} → Revoke access
POST   /files/{id}/share-link        → Create public link
DELETE /files/{id}/share-link        → Revoke public link

─── Sync ───────────────────────────────────────────────────────────────────
GET    /sync/changes?since_seq={n}   → Fetch change log since sequence
WS     /sync                          → WebSocket for real-time push

─── Search ─────────────────────────────────────────────────────────────────
GET    /search?q={query}&type={mime}&from={date}  → Search files
```

### Key HTTP Headers

```
Request:
  Authorization: Bearer {jwt}
  X-Request-ID: {uuid}          (idempotency / tracing)
  X-Chunk-Hash: {sha256}        (chunk integrity declaration)
  If-None-Match: {etag}         (conditional downloads)

Response:
  ETag: {sha256}                (chunk / file hash)
  Cache-Control: immutable      (chunks never change)
  X-RateLimit-Remaining: {n}
  X-Request-ID: {uuid}          (echoed for tracing)
```

---

## 11. Scalability Patterns

### Horizontal Scaling

| Layer | Scaling Strategy |
|---|---|
| API Gateway | Stateless; auto-scale behind L7 LB |
| Metadata Service | Stateless; scale horizontally; DB is the bottleneck |
| Metadata DB | Read replicas for reads; partition by user_id for writes |
| Chunk Service | Stateless; scale horizontally |
| Storage Nodes | Add nodes to consistent hash ring; data migrates automatically |
| Redis | Redis Cluster (sharding) + Redis Sentinel (HA) |
| Kafka | Partition by user_id; add partitions / brokers as throughput grows |
| Sync Service | Stateful (WebSocket); use sticky sessions or pub/sub for cross-instance delivery |

### Database Sharding Strategy

```
Metadata DB sharding key: user_id
Shard count: Start with 8 shards, double when any shard > 70% capacity

Shard routing:
  shard_id = hash(user_id) % num_shards
  Connection pool per shard

Cross-shard queries (shared files):
  Sharing creates a lightweight reference record on the recipient's shard
  pointing to the owner's file record (cross-shard foreign key via application layer)
```

### Caching Strategy (Layered)

```
L1 — In-process cache (LRU Map, 1000 entries)
     • Per-request cache of permission checks
     • TTL: request lifetime only

L2 — Redis (shared cache, per service)
     • File metadata: 5 min TTL
     • Permission checks: 60 sec TTL
     • Directory listings: 60 sec TTL
     • Chunk locations: 1 hour TTL

L3 — CDN
     • Public/shared files: 24 hour TTL
     • Static assets: 1 year TTL (versioned filenames)
```

---

## 12. Monitoring & Observability

### Key Metrics (Prometheus)

```
Upload:
  dfs_upload_duration_ms (histogram)         — p50, p95, p99 latency
  dfs_upload_chunk_size_bytes (histogram)     — Distribution of chunk sizes
  dfs_upload_dedup_ratio (gauge)             — % chunks deduplicated
  dfs_upload_failures_total (counter)        — By error type

Download:
  dfs_download_duration_ms (histogram)
  dfs_cdn_cache_hit_ratio (gauge)
  dfs_download_bytes_total (counter)

Storage:
  dfs_storage_used_bytes (gauge)             — Per node
  dfs_storage_chunk_count (gauge)            — Per node
  dfs_replication_lag_ms (histogram)         — Time to replicate after write
  dfs_under_replicated_chunks (gauge)        — Alert if > 0 for > 5 min

Metadata:
  dfs_metadata_query_duration_ms (histogram) — By query type
  dfs_cache_hit_ratio (gauge)                — Redis hit rate

Sync:
  dfs_websocket_connections (gauge)
  dfs_sync_event_lag_ms (histogram)         — Event delivery latency
```

### Alerts

```
P0 (Page immediately):
  • Under-replicated chunks > 0 for > 10 minutes
  • Storage node count < REPLICATION_FACTOR
  • Metadata DB primary unavailable
  • Upload error rate > 5% (5-minute window)

P1 (Page within 30 min):
  • Upload P99 latency > 10 seconds
  • CDN cache hit rate < 50%
  • Kafka consumer lag > 100,000 messages

P2 (Business hours):
  • Storage capacity > 80% on any node
  • GC running > 2 hours
  • Dedup ratio dropping (potential storage leak)
```

### Distributed Tracing

All requests carry a `X-Request-ID` header that propagates through every service. Each service emits spans to Jaeger/Zipkin:

```
Upload trace example:
  [API Gateway] auth + route        — 12ms
  [Metadata Svc] init upload        — 34ms
    [PostgreSQL] quota check        — 8ms
    [PostgreSQL] create file record — 15ms
    [Redis] cache write             — 3ms
  [Chunk Svc] receive chunk         — 145ms
    [StorageNode] write to disk     — 98ms
    [ReplicationMgr] replicate ×2  — 210ms (async, parallel)
  [Metadata Svc] commit upload      — 28ms
  [Kafka] emit file.uploaded        — 5ms
  Total (client perspective): ~230ms upload initiation
```

---

## 13. Trade-offs & Alternatives

### Architecture Decisions & Alternatives

| Decision | Chosen | Alternative | Trade-off |
|---|---|---|---|
| Chunk size | 4 MB | 1 MB / 16 MB | Smaller = more parallelism but more metadata overhead; larger = fewer HTTP calls but poor latency for small files |
| Storage | Custom node cluster | AWS S3 directly | Custom gives more control and lower cost at scale; S3 is simpler operationally but 10× more expensive per GB |
| Replication | Leaderless (Dynamo) | Leader-based (Raft) | Leaderless = higher availability; Raft = stronger consistency, simpler conflict model |
| Metadata DB | PostgreSQL | Cassandra / DynamoDB | PostgreSQL gives strong consistency and complex queries; Cassandra gives better write throughput at scale |
| Dedup | Chunk-level SHA-256 | Block-level variable chunking | Fixed chunks are simpler; variable chunking (rsync algorithm) gives better dedup for modified files |
| Sync protocol | Event log + cursor | Full state sync | Event log is efficient for incremental changes; full sync is simpler to implement but expensive at scale |
| Conflict resolution | Last-write-wins + conflicted copy | Operational Transform | LWW is simple and understood; OT is needed for real-time collaboration (out of scope) |
| Erasure coding | RS(6,3) for cold data | 3× replication everywhere | EC saves 1.5× vs 3× storage overhead; at cost of higher read latency (multi-node fetch) |

### System Limitations & Known Issues

1. **Large file latency:** A 50 GB file = 12,800 × 4MB chunks. Parallel uploads help, but initial metadata creation and final commit add overhead. Mitigation: batch chunk acknowledgment.

2. **Hot files:** A viral shared file can overload storage nodes. Mitigation: CDN caching + dynamic replication (increase replica count for hot chunks).

3. **Namespace collision:** Two users uploading the same filename to the same folder simultaneously is handled by DB unique constraint + retry. The second upload gets a suffixed name.

4. **Clock skew:** Last-write-wins relies on timestamps. Mitigation: use Hybrid Logical Clocks (HLC) that combine physical time + logical counter, bounding skew to ±10ms.

5. **Small files:** Files smaller than 4 MB become single-chunk files. This is fine but wastes potential dedup across multiple files with shared content. Mitigation: consider variable-size chunking for future versions.

---

## Summary

| Aspect | Decision |
|---|---|
| **Architecture** | Microservices with Kafka event bus |
| **Storage** | Content-addressable chunk store with consistent hashing |
| **Replication** | Leaderless, W=2, R=2, N=3 |
| **Deduplication** | SHA-256 chunk hashing, ref-counted |
| **Consistency** | Strong for metadata (PostgreSQL), eventual for sync |
| **Availability** | 99.99% target, multi-region with active-passive DR |
| **Durability** | 11 nines via 3× replication + erasure coding for cold |
| **Security** | AES-256 at rest, TLS 1.3 in transit, envelope key encryption |
| **Scalability** | Horizontal at every layer; consistent hashing for storage |
| **Sync** | WebSocket + Kafka event log + local cursor |

---

*Document version: 1.0 | Designed for FAANG-level system design interviews*