# Google Drive — System Design (FAANG Interview Level)

> **Scope:** Full HLD + LLD covering every layer — from client chunking to distributed metadata stores, from blob storage to real-time collaboration. LLD code samples are in JavaScript/Node.js.

---

## Table of Contents

1. [Requirements Clarification](#1-requirements-clarification)
2. [Capacity Estimation & Back-of-Envelope Math](#2-capacity-estimation--back-of-envelope-math)
3. [High-Level Design (HLD)](#3-high-level-design-hld)
   - 3.1 [Architecture Overview](#31-architecture-overview)
   - 3.2 [Core Components](#32-core-components)
   - 3.3 [Data Flow — Upload](#33-data-flow--upload)
   - 3.4 [Data Flow — Download](#34-data-flow--download)
   - 3.5 [Data Flow — Sync](#35-data-flow--sync)
4. [Deep Dive — Storage Layer](#4-deep-dive--storage-layer)
   - 4.1 [Chunking Strategy](#41-chunking-strategy)
   - 4.2 [Deduplication (Content-Addressable Storage)](#42-deduplication-content-addressable-storage)
   - 4.3 [Blob Store Architecture](#43-blob-store-architecture)
   - 4.4 [CDN & Edge Caching](#44-cdn--edge-caching)
5. [Deep Dive — Metadata Layer](#5-deep-dive--metadata-layer)
   - 5.1 [Schema Design](#51-schema-design)
   - 5.2 [Database Selection & Sharding](#52-database-selection--sharding)
   - 5.3 [Caching Strategy](#53-caching-strategy)
6. [Deep Dive — Sync Engine](#6-deep-dive--sync-engine)
   - 6.1 [Delta Sync Algorithm](#61-delta-sync-algorithm)
   - 6.2 [Conflict Resolution](#62-conflict-resolution)
   - 6.3 [Long-Poll / WebSocket Notification Service](#63-long-poll--websocket-notification-service)
7. [Deep Dive — Upload Service](#7-deep-dive--upload-service)
   - 7.1 [Resumable Upload Protocol](#71-resumable-upload-protocol)
   - 7.2 [Chunk Upload Manager](#72-chunk-upload-manager)
8. [Deep Dive — Sharing & Permissions](#8-deep-dive--sharing--permissions)
   - 8.1 [ACL Model](#81-acl-model)
   - 8.2 [Sharing Service LLD](#82-sharing-service-lld)
9. [Deep Dive — Search](#9-deep-dive--search)
10. [Deep Dive — Versioning & Trash](#10-deep-dive--versioning--trash)
11. [Deep Dive — Real-Time Collaboration (Google Docs inside Drive)](#11-deep-dive--real-time-collaboration)
12. [Security Design](#12-security-design)
13. [Reliability, Availability & Disaster Recovery](#13-reliability-availability--disaster-recovery)
14. [Observability (Metrics, Logging, Tracing)](#14-observability)
15. [API Design](#15-api-design)
16. [Low-Level Design (LLD) — Complete JS Implementations](#16-low-level-design-lld--complete-js-implementations)
    - 16.1 [ChunkManager](#161-chunkmanager)
    - 16.2 [UploadService](#162-uploadservice)
    - 16.3 [MetadataService](#163-metadataservice)
    - 16.4 [SyncEngine](#164-syncengine)
    - 16.5 [PermissionService (ACL)](#165-permissionservice-acl)
    - 16.6 [NotificationService (WebSocket)](#166-notificationservice-websocket)
    - 16.7 [VersioningService](#167-versioningservice)
    - 16.8 [SearchService](#168-searchservice)
    - 16.9 [DeduplicationService (CAS)](#169-deduplicationservice-cas)
    - 16.10 [RateLimiter](#1610-ratelimiter)
17. [Trade-offs & Alternatives](#17-trade-offs--alternatives)
18. [Interview Cheat Sheet](#18-interview-cheat-sheet)

---

## 1. Requirements Clarification

### Functional Requirements
| # | Requirement |
|---|-------------|
| F1 | Users can upload, download, and delete files of any type |
| F2 | Files are organized in a hierarchical folder structure |
| F3 | Files sync automatically across all user devices |
| F4 | Users can share files/folders with other users (view / comment / edit) |
| F5 | Public shareable links |
| F6 | Full version history; restore any version |
| F7 | Soft-delete (Trash) with 30-day auto-purge |
| F8 | Full-text search across file names and document content |
| F9 | Real-time collaborative editing (Docs, Sheets, Slides) |
| F10 | Offline access with eventual sync |
| F11 | Mobile and desktop native clients + web UI |
| F12 | Storage quota enforcement per user/plan |

### Non-Functional Requirements
| # | Requirement | Target |
|---|-------------|--------|
| N1 | Availability | 99.99% (≈52 min downtime/year) |
| N2 | Durability | 99.999999999% (11 nines) |
| N3 | Upload latency | < 200 ms TTFB for small files |
| N4 | Download latency | P99 < 500 ms globally via CDN |
| N5 | Consistency | Strong for metadata; eventual for blob delivery |
| N6 | Scalability | 1 billion users; 15 PB new data/day |
| N7 | Max file size | 5 TB per file |
| N8 | Security | Encryption at rest (AES-256) + in transit (TLS 1.3) |

### Out of Scope
- Billing/payment system
- Video transcoding (YouTube handles that)
- Full Docs editor internals (covered minimally under real-time collab)

---

## 2. Capacity Estimation & Back-of-Envelope Math

```
Users:            1 billion total, 100 million DAU
Avg storage/user: 15 GB → total raw: ~15 EB
Upload/day:       100M DAU × 2 uploads/day = 200M uploads/day
Avg file size:    500 KB (median), 5 MB (mean)
Upload bandwidth: 200M × 5 MB = 1 PB/day ≈ 11.5 GB/s sustained

Download/upload ratio: 10:1
Download bandwidth:    115 GB/s sustained → need aggressive CDN

QPS (metadata reads):  200M DAU × 100 reads/day / 86400 = ~230K RPS
QPS (metadata writes): 200M × 5 writes/day / 86400 = ~11.5K RPS
QPS (upload API):      200M uploads / 86400 = ~2300 RPS (peak 10×)

Deduplication savings: ~30% (industry avg)  → net storage ~10.5 EB
Replication factor:    3× → raw storage need: ~31 EB

Chunk size: 4 MB → avg file = 1.25 chunks
Chunk metadata records: 200M files/day × 1.25 = 250M rows/day

Servers needed (rough):
  Upload workers: 2300 RPS × 30s avg upload / 50 concurrent = ~1400 servers
  API servers:    230K RPS / 50K RPS per server = ~5 servers (need 20 for HA)
  Notification:   100M active connections / 100K conn per server = 1000 servers
```

---

## 3. High-Level Design (HLD)

### 3.1 Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────┐
│                            CLIENTS                                   │
│   Web Browser    Android App    iOS App    Desktop (Win/Mac/Linux)   │
└────────────┬─────────────┬──────────────┬──────────────┬────────────┘
             │             │              │              │
             └─────────────┴──────────────┴──────────────┘
                                    │
                            ┌───────▼────────┐
                            │   DNS / GeoDNS  │
                            └───────┬────────┘
                                    │
                     ┌──────────────▼──────────────┐
                     │    Global Load Balancer       │
                     │   (Anycast + BGP routing)     │
                     └──────────────┬──────────────┘
                                    │
          ┌─────────────────────────┼──────────────────────────┐
          │                         │                          │
   ┌──────▼──────┐          ┌───────▼──────┐          ┌───────▼──────┐
   │  Edge PoPs  │          │  Edge PoPs   │          │  Edge PoPs   │
   │  (CDN)      │          │  (CDN)       │          │  (CDN)       │
   └──────┬──────┘          └───────┬──────┘          └───────┬──────┘
          │                         │                          │
          └─────────────────────────┼──────────────────────────┘
                                    │
                     ┌──────────────▼──────────────┐
                     │        API Gateway            │
                     │  (Auth, Rate-limit, Routing)  │
                     └──────────────┬──────────────┘
                                    │
        ┌──────────┬────────────────┼────────────────┬──────────┐
        │          │                │                │          │
  ┌─────▼──┐  ┌────▼─────┐  ┌──────▼──────┐  ┌─────▼──┐ ┌─────▼──────┐
  │ Upload │  │ Download  │  │  Metadata   │  │ Sharing│ │Notification│
  │Service │  │ Service   │  │  Service    │  │Service │ │ Service    │
  └─────┬──┘  └────┬─────┘  └──────┬──────┘  └─────┬──┘ └─────┬──────┘
        │          │                │                │          │
        │          │        ┌───────▼───────┐        │          │
        │          │        │ Metadata DB   │        │          │
        │          │        │ (Spanner/CDB) │        │          │
        │          │        └───────────────┘        │          │
        │          │                                  │          │
        └──────────┴──────────┐           ┌───────────┘          │
                              │           │                       │
                     ┌────────▼───────────▼──┐          ┌────────▼──────┐
                     │    Blob Storage        │          │  Message Bus  │
                     │  (GCS / S3-compatible) │          │  (Kafka/Pub)  │
                     └───────────────────────┘          └───────────────┘
```

### 3.2 Core Components

| Component | Responsibility | Tech Choice |
|-----------|---------------|-------------|
| **API Gateway** | Auth (JWT/OAuth2), rate-limiting, SSL termination, routing | Envoy / Kong |
| **Upload Service** | Chunked / resumable uploads, dedup check, write to blob | Node.js + gRPC |
| **Download Service** | Auth, generate signed URLs, stream from CDN/blob | Node.js |
| **Metadata Service** | CRUD for files/folders, hierarchy, quota | Node.js + gRPC |
| **Sync Engine** | Delta calculation, conflict resolution, device state | Node.js |
| **Notification Service** | Push change events to connected clients | Node.js + WebSocket |
| **Sharing Service** | ACL management, share links, permission checks | Node.js |
| **Search Service** | Index file names + content, full-text queries | Elasticsearch |
| **Versioning Service** | Version chain management, restore, purge | Node.js |
| **Metadata DB** | Strongly consistent, globally distributed | Google Spanner / CockroachDB |
| **Blob Store** | Raw file chunks, immutable, content-addressed | GCS / S3 |
| **Cache Layer** | Metadata hot-path, session data | Redis Cluster |
| **Message Bus** | Async events (uploaded, deleted, shared) | Apache Kafka |
| **CDN** | Edge delivery of blobs globally | Cloudflare / Fastly |
| **Object Index DB** | Reverse lookup chunk_hash → file list | Cassandra |

### 3.3 Data Flow — Upload

```
Client                Upload Service         Blob Store         Metadata DB
  │                        │                    │                    │
  │── POST /upload/init ──►│                    │                    │
  │                        │── check quota ────►│                    │
  │◄── { uploadId, urls }──│                    │                    │
  │                        │                    │                    │
  │ (for each chunk)        │                    │                    │
  │── PUT chunk (binary) ──►│                    │                    │
  │                        │── hash chunk ───────────────────────────│
  │                        │── dedup check ─────►│ (exists? skip)    │
  │                        │── PUT /chunks/{hash}►│                  │
  │◄── { chunkId, offset }─│                    │                    │
  │                        │                    │                    │
  │── POST /upload/complete►│                    │                    │
  │                        │── write file meta ─────────────────────►│
  │                        │── publish event ──► Kafka               │
  │◄── { fileId, version }─│                    │                    │
```

### 3.4 Data Flow — Download

```
Client              API Gateway          Download Svc        CDN / Blob
  │                     │                    │                   │
  │── GET /files/{id} ─►│                    │                   │
  │                     │── verify ACL ─────►│                   │
  │                     │                    │── gen signed URL ─►│
  │◄── 302 signed URL ──│◄───────────────────│                   │
  │                     │                    │                   │
  │─────────────────────────────── GET signed URL ──────────────►│
  │◄─────────────────────────────── file bytes ─────────────────│
```

### 3.5 Data Flow — Sync

```
Device A            Sync Engine          Notification Svc      Device B
   │                    │                      │                   │
   │── upload file ────►│                      │                   │
   │                    │── store metadata ────►│                  │
   │                    │── publish delta ─────►│                  │
   │                    │                      │── WS push ───────►│
   │                    │                      │                   │
   │                    │◄── GET /sync/delta ──────────────────────│
   │                    │── return delta list ─────────────────────►│
   │                    │                      │                   │
   │                    │                      │     ┌─ download chunks
   │                    │                      │     │ (parallel)
   │                    │                      │     └─────────────►│
```

---

## 4. Deep Dive — Storage Layer

### 4.1 Chunking Strategy

Files are split into fixed-size chunks (4 MB default) on the **client** before upload. This enables:
- Parallel multi-part upload
- Resumability (only failed chunks re-upload)
- Deduplication at chunk granularity
- Efficient delta-sync (only changed chunks transferred)

```
File: video.mp4  (100 MB)
│
├─ Chunk 0 [0–4MB]    hash: sha256_0
├─ Chunk 1 [4–8MB]    hash: sha256_1
├─ Chunk 2 [8–12MB]   hash: sha256_2
│  ...
└─ Chunk 24 [96–100MB] hash: sha256_24

Manifest: { fileId, name, size, chunks: [{seq, hash, size}] }
```

**Variable-length chunking (Rabin fingerprinting)** is used for plain text and office documents where inserting a paragraph shifts all byte offsets — Rabin chunks on content boundaries so only truly-changed chunks are retransmitted.

### 4.2 Deduplication (Content-Addressable Storage)

```
SHA-256(chunk bytes) → hex key → used as blob store object key

Client computes hash → sends hash to Upload Service first
Upload Service checks: EXISTS /blobs/{hash}?
  YES → skip upload (reference already stored blob)
  NO  → upload bytes to /blobs/{hash}

Reference counting table (Cassandra):
  hash      | ref_count | size_bytes | created_at
  sha256_0  | 14923     | 4194304    | 2024-01-01

Garbage collection: ref_count → 0 triggers async blob deletion
```

Cross-user deduplication at the chunk level saves ~30% storage globally (documents, photos with identical raw blocks, OS images).

### 4.3 Blob Store Architecture

```
┌─────────────────────────────────────────────────────┐
│                  Blob Store (GCS)                    │
│                                                     │
│  Region US-EAST   Region EU-WEST   Region AP-SOUTH  │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  │
│  │ Primary     │  │ Replica     │  │ Replica     │  │
│  │ Bucket      │◄─► Bucket      │◄─► Bucket      │  │
│  └─────────────┘  └─────────────┘  └─────────────┘  │
│                                                     │
│  Storage Classes:                                   │
│  - Standard: last 30 days (hot)                    │
│  - Nearline: 30–365 days (warm)                    │
│  - Coldline: > 365 days (cold)                     │
│  - Archive: trash + old versions                   │
└─────────────────────────────────────────────────────┘
```

Object key format: `{region}/{year}/{month}/{sha256_hash[0:2]}/{sha256_hash[2:4]}/{sha256_hash}`

Prefix sharding on first 4 hex chars distributes load across GCS prefix buckets.

### 4.4 CDN & Edge Caching

- Blobs are **immutable** (content-addressed), so Cache-Control: `max-age=31536000, immutable`
- Signed URL contains expiry + HMAC — CDN validates and caches the object
- Cache hit rate target: 85%+ (documents, shared files)
- Large files (>100 MB) use **byte-range requests** from CDN edge

---

## 5. Deep Dive — Metadata Layer

### 5.1 Schema Design

```sql
-- Core file/folder entity
CREATE TABLE nodes (
  node_id       UUID         PRIMARY KEY,
  owner_id      UUID         NOT NULL,
  parent_id     UUID,                         -- NULL = root
  name          VARCHAR(1024) NOT NULL,
  node_type     ENUM('file','folder','shortcut'),
  mime_type     VARCHAR(256),
  size_bytes    BIGINT       DEFAULT 0,
  created_at    TIMESTAMP    NOT NULL,
  modified_at   TIMESTAMP    NOT NULL,
  trashed_at    TIMESTAMP,
  version       INT          DEFAULT 1,
  is_deleted    BOOLEAN      DEFAULT FALSE,
  INDEX (owner_id, parent_id),               -- list folder contents
  INDEX (owner_id, modified_at DESC)         -- recent files
);

-- Chunk manifest (one row per chunk in latest version)
CREATE TABLE file_chunks (
  node_id       UUID         NOT NULL,
  version       INT          NOT NULL,
  seq           INT          NOT NULL,        -- chunk order
  chunk_hash    CHAR(64)     NOT NULL,        -- SHA-256 hex
  chunk_size    INT          NOT NULL,
  PRIMARY KEY (node_id, version, seq),
  INDEX (chunk_hash)                          -- dedup lookup
);

-- Version history
CREATE TABLE file_versions (
  node_id       UUID         NOT NULL,
  version       INT          NOT NULL,
  size_bytes    BIGINT,
  created_at    TIMESTAMP    NOT NULL,
  created_by    UUID         NOT NULL,
  change_note   TEXT,
  PRIMARY KEY (node_id, version)
);

-- ACL / Permissions
CREATE TABLE permissions (
  perm_id       UUID         PRIMARY KEY,
  node_id       UUID         NOT NULL,
  grantee_id    UUID,                         -- NULL = public
  grantee_type  ENUM('user','group','anyone'),
  role          ENUM('viewer','commenter','editor','owner'),
  share_link    VARCHAR(128),                 -- random token for links
  expires_at    TIMESTAMP,
  created_at    TIMESTAMP    NOT NULL,
  INDEX (node_id),
  INDEX (grantee_id),
  INDEX (share_link)
);

-- User quota
CREATE TABLE user_quotas (
  user_id       UUID         PRIMARY KEY,
  quota_bytes   BIGINT       NOT NULL,
  used_bytes    BIGINT       NOT NULL DEFAULT 0,
  plan          ENUM('free','100gb','2tb','business'),
  updated_at    TIMESTAMP    NOT NULL
);

-- Device sync state (for delta sync)
CREATE TABLE device_sync_cursors (
  device_id     VARCHAR(64)  NOT NULL,
  user_id       UUID         NOT NULL,
  change_token  VARCHAR(128) NOT NULL,        -- monotonic cursor
  last_sync     TIMESTAMP    NOT NULL,
  PRIMARY KEY (device_id, user_id)
);
```

**Closure Table** for efficient subtree queries (avoid recursive CTEs):
```sql
CREATE TABLE tree_paths (
  ancestor_id   UUID NOT NULL,
  descendant_id UUID NOT NULL,
  depth         INT  NOT NULL,
  PRIMARY KEY (ancestor_id, descendant_id)
);
-- Delete folder subtree: DELETE WHERE ancestor_id = ?
-- Get full path: SELECT * WHERE descendant_id = ? ORDER BY depth DESC
```

### 5.2 Database Selection & Sharding

**Google Spanner** (or CockroachDB for open-source):
- External consistency (TrueTime-based) — critical for version conflicts
- Multi-region replication built-in
- SQL interface with horizontal scaling

**Sharding strategy:**
- Nodes table: shard by `owner_id` (user-scoped queries dominate)
- file_chunks: shard by `node_id`
- permissions: shard by `node_id` (permission check on open)

**Hot-path optimization:**
- Popular shared files create "hot nodes" — permission row cached in Redis with TTL 60s
- Write-through cache on `user_quotas` (every upload updates used_bytes)

### 5.3 Caching Strategy

```
L1: In-process LRU cache (per API server, 256 MB)
    - File metadata for recently accessed files
    - TTL: 30 seconds

L2: Redis Cluster (distributed)
    - Key: metadata:{node_id} → serialized node JSON
    - Key: perms:{node_id}:{user_id} → {role}
    - Key: quota:{user_id} → {used, total}
    - Key: tree:{parent_id} → list of child node_ids
    - TTL: metadata 5 min, perms 60 s, quota 10 s

Cache invalidation:
    - Upload/rename/delete → publish to Kafka → consumer deletes Redis key
    - Permission change → immediate Redis delete + Kafka event

Read strategy: Cache-Aside (lazy population)
Write strategy: Write-through on quota; Write-behind on metadata (batch flush)
```

---

## 6. Deep Dive — Sync Engine

### 6.1 Delta Sync Algorithm

Each device maintains a **change token** (monotonically increasing sequence number per user). The server maintains a global change log:

```
change_log table:
  seq          BIGINT       AUTO_INCREMENT
  user_id      UUID
  node_id      UUID
  change_type  ENUM('create','modify','delete','move','rename','share')
  version      INT
  timestamp    TIMESTAMP
  INDEX (user_id, seq)
```

**Sync protocol:**
```
Device → GET /changes?since={cursor}&limit=500
Server → returns list of changes since cursor + new cursor
Device → applies changes locally
Device → advances local cursor
Device → repeat if has_more=true
```

**Change token = Lamport clock** per user — guarantees causal ordering even across concurrent writes from multiple devices.

### 6.2 Conflict Resolution

Strategy: **Last-write-wins (LWW)** for binary files; **Operational Transform (OT)** for Docs.

```
Binary conflict scenario:
  Device A modifies file at t=100
  Device B modifies same file at t=105 (offline since t=90)
  Device B comes online

Resolution:
  1. Server sees version A (v2, t=100) and version B (v2, t=105)
  2. B's timestamp > A's → B becomes canonical v3
  3. A's version saved as "conflicted copy (Device A, 2024-01-15)"
  4. User sees two files; can manually merge

Conflict detection:
  Client sends: { node_id, base_version: 1, changes: [...] }
  Server checks: current_version == base_version?
    YES → apply, increment version
    NO  → return 409 Conflict + current version
```

### 6.3 Long-Poll / WebSocket Notification Service

```
Architecture:
  - Clients maintain persistent WebSocket connection to Notification Service
  - Notification Service subscribes to Kafka topic: user.{user_id}.changes
  - On Kafka message → fan-out to all connected devices of that user
  - Horizontal scaling: each server handles ~100K connections
  - Sticky routing via consistent hash on user_id → same server cluster

Message format:
{
  "type": "CHANGE",
  "changes": [
    { "nodeId": "uuid", "type": "modify", "version": 5, "changeToken": "8472" }
  ],
  "nextCursor": "8472"
}

Client heartbeat: ping every 30s; server closes after 90s silence
Reconnect: exponential backoff 1s → 2s → 4s → max 60s
```

---

## 7. Deep Dive — Upload Service

### 7.1 Resumable Upload Protocol

Modeled after Google's resumable upload API:

```
Phase 1: Initialize
  POST /upload/initiate
  Body: { name, mimeType, size, parentId }
  Response: { uploadId: "xyz", chunkSize: 4194304, totalChunks: 25 }

Phase 2: Upload chunks (can be parallel)
  PUT /upload/{uploadId}/chunk/{seq}
  Headers: Content-Length, Content-SHA256
  Body: raw bytes
  Response: { seq, status: "received" }

Phase 3: Complete
  POST /upload/{uploadId}/complete
  Body: { chunks: [{ seq, hash }] }
  Response: { fileId, version, createdAt }

Phase 4: Resume (on failure)
  GET /upload/{uploadId}/status
  Response: { received: [0,1,2,5], missing: [3,4,6..24] }
  → Client re-uploads only missing chunks
```

Upload session stored in Redis with 24-hour TTL:
```json
{
  "uploadId": "xyz",
  "userId": "uid",
  "parentId": "pid",
  "name": "video.mp4",
  "size": 104857600,
  "totalChunks": 25,
  "receivedChunks": { "0": "hash0", "1": "hash1" },
  "expiresAt": 1700000000
}
```

### 7.2 Chunk Upload Manager

See [LLD Section 16.2](#162-uploadservice) for full implementation.

Key design points:
- Each chunk upload is idempotent (PUT with hash verification)
- Virus scan runs async after blob write (ClamAV / cloud-native)
- Quota check happens at initiate (reserve quota); confirmed at complete
- Background job: incomplete sessions older than 24h → release reserved quota

---

## 8. Deep Dive — Sharing & Permissions

### 8.1 ACL Model

Google Drive uses a **hierarchical ACL** with inheritance:

```
Permission resolution order:
  1. Direct permission on node (highest priority)
  2. Permission inherited from parent folder
  3. No permission → deny

Roles (ordered by capability):
  owner > editor > commenter > viewer

Special cases:
  - Shared drives: membership overrides individual ACLs
  - "Anyone with link" = synthetic grantee_type='anyone'
  - Expiring shares: expires_at enforced at request time
```

**Permission check algorithm: O(depth) walk up tree**
```
function canAccess(userId, nodeId, requiredRole):
  node = getNode(nodeId)
  perm = getDirectPermission(userId, nodeId)
  if perm && perm.role >= requiredRole: return true
  if node.parent_id: return canAccess(userId, node.parent_id, requiredRole)
  return false
```

Optimized with **closure table** — precomputed ancestor chain eliminates recursive DB calls:
```sql
SELECT p.role FROM permissions p
JOIN tree_paths tp ON p.node_id = tp.ancestor_id
WHERE tp.descendant_id = ? AND p.grantee_id IN (?, 'anyone')
ORDER BY tp.depth ASC
LIMIT 1;
```

### 8.2 Sharing Service LLD

See [LLD Section 16.5](#165-permissionservice-acl).

---

## 9. Deep Dive — Search

### Architecture

```
Indexing pipeline (async):
  Upload complete → Kafka → Search Indexer Service → Elasticsearch

Index documents:
{
  "nodeId": "uuid",
  "ownerId": "uid",
  "name": "Q3 Report.docx",
  "mimeType": "application/vnd.openxmlformats...",
  "content": "extracted plain text from doc...",
  "tags": ["work", "finance"],
  "sharedWith": ["uid2", "uid3"],
  "modifiedAt": "2024-01-15T10:00:00Z",
  "size": 204800
}
```

**Security trimming:** Every search query adds `filter: { bool: { should: [{ term: { ownerId } }, { term: { sharedWith: userId } }] } }` — users only see files they can access.

**Search features:**
- Fuzzy matching on file names (Levenshtein distance 1-2)
- Full-text search on document content (PDF, Docx text extraction via Tika)
- Facets: file type, owner, date range
- Type-ahead suggestions from name n-gram index

---

## 10. Deep Dive — Versioning & Trash

### Versioning

```
Version storage:
  - Current version chunks: file_chunks table, version = N
  - Old version chunks: retained in blob store, ref_count maintained
  - file_versions table tracks each version + metadata

Retention policy:
  - Free users: 30 days or 100 versions (whichever first)
  - Paid users: 180 days / unlimited versions
  - Version purge: async background job; decrements ref_count; GC removes orphan blobs

Restore:
  POST /files/{nodeId}/versions/{version}/restore
  → Creates new version N+1 with chunk list copied from version V
  → Old chunk blobs not duplicated (content-addressed, same hashes)
```

### Trash

```
Soft delete:
  DELETE /files/{nodeId}
  → Sets trashed_at = now(), is_deleted = true
  → File disappears from directory listings
  → File remains accessible via /trash endpoint

Auto-purge:
  Background job runs daily:
    SELECT * FROM nodes WHERE is_deleted = true AND trashed_at < NOW() - INTERVAL 30 DAY
  → For each: decrement chunk ref_counts, delete metadata rows
  → Quota reclaimed only on hard delete

Restore:
  POST /trash/{nodeId}/restore
  → Clears trashed_at, is_deleted = false
  → Re-inserts into original parent (if parent still exists; else root)
```

---

## 11. Deep Dive — Real-Time Collaboration

### Operational Transformation (OT) Architecture

```
┌──────────┐         ┌───────────────────┐         ┌──────────┐
│ Client A │         │   OT Server       │         │ Client B │
└────┬─────┘         │  (per document)   │         └────┬─────┘
     │               └─────────┬─────────┘              │
     │── Op{type:insert,pos:5}─►│                        │
     │                         │── transform against ───►│
     │                         │   pending ops           │
     │◄── ack + server_rev ────│                        │
     │                         │── broadcast Op' ───────►│
```

**OT guarantees convergence:** `transform(A, B)` and `transform(B, A)` produce the same document state regardless of application order.

**CRDT alternative** (used by newer systems like Figma):
- No central server needed for ordering
- Last-write-wins per character with Lamport timestamps
- Better for offline-first; slightly higher storage overhead

**Architecture:**
- One OT server instance per active document (sticky session)
- Document server assigned via consistent hash on doc_id
- On server failure: new server reloads from last checkpoint in DB
- Checkpoint: full document snapshot every 100 operations

---

## 12. Security Design

| Layer | Control |
|-------|---------|
| **Transport** | TLS 1.3 everywhere; HSTS with preload |
| **Authentication** | OAuth 2.0 + OIDC; short-lived JWTs (1 hour); refresh tokens (30 days) |
| **Authorization** | ACL check on every API call; no ambient authority |
| **Encryption at rest** | AES-256-GCM per blob; key stored in Cloud KMS; per-user key rotation |
| **Signed URLs** | HMAC-SHA256, 1-hour expiry, IP-bound optional |
| **Virus scanning** | Async scan on every uploaded blob; quarantine on positive |
| **Data isolation** | Blob keys are SHA-256 hashes — non-enumerable; no sequential IDs |
| **Rate limiting** | Per-user: 100 uploads/min, 1000 reads/min; per-IP: 5000 req/min |
| **Audit log** | Immutable append-only log: every access, share, delete |
| **GDPR** | Data export API; deletion within 30 days of request |
| **Abuse** | Hash-based CSAM detection (PhotoDNA); automated takedown |

---

## 13. Reliability, Availability & Disaster Recovery

### Replication

```
Blob store: 3 copies across 3 AZs in same region + async cross-region (RPO: 1 hour)
Metadata (Spanner): synchronous 3-way Paxos within region; async to secondary region
Redis: Redis Cluster with 3 replicas; Sentinel for failover

Durability math:
  GCS standard: 99.999999999% (11 nines) durability
  Annual failure rate per disk: 1-5%
  With 3 copies across AZs: P(all 3 fail) ≈ (0.03)^3 = 0.000027% per year per object
```

### Circuit Breakers

Every service-to-service call wrapped in circuit breaker (Hystrix pattern):
- Closed → Open: 50% failure rate over 10 seconds
- Half-open: allow 1 request/5s to test recovery
- Open → Closed: successful test request

### Chaos Engineering

Weekly automated chaos experiments:
- Kill random blob store node → verify upload/download succeed from replica
- Kill metadata DB leader → verify Spanner election completes in < 10s
- Inject 2s latency on CDN → verify fallback to origin blob store
- Kill 50% notification servers → verify reconnection works

### Backup & Recovery

| Asset | RPO | RTO | Method |
|-------|-----|-----|--------|
| Metadata DB | 1 hour | 4 hours | Spanner PITR + export |
| Blob store | 0 (replicated) | Minutes | Cross-region replica |
| Redis | 5 min | 10 min | AOF + snapshot to GCS |
| Kafka | 0 (replicated) | 1 min | 3-broker cluster |

---

## 14. Observability

### Metrics (Prometheus + Grafana)

```
# Upload
upload_requests_total{status="success|error"} counter
upload_duration_seconds{quantile="0.5|0.95|0.99"} histogram
upload_chunk_size_bytes histogram
upload_dedup_ratio gauge

# Storage
storage_used_bytes{tier="hot|warm|cold"} gauge
blob_read_latency_seconds histogram
cdn_hit_ratio gauge

# API
api_request_latency_seconds{service, endpoint} histogram
api_error_rate{service, error_code} counter
quota_exceeded_total counter

# Sync
sync_lag_seconds{device_type} histogram
change_queue_depth gauge
conflict_resolutions_total counter
```

### Distributed Tracing (OpenTelemetry + Jaeger)

Every request carries a `trace-id` header. Spans created at:
- API Gateway entry
- Each downstream service call
- DB query (auto-instrumented)
- Blob store operation
- Cache hit/miss

### Logging (Structured JSON → Cloud Logging)

```json
{
  "timestamp": "2024-01-15T10:00:00.123Z",
  "level": "INFO",
  "service": "upload-service",
  "traceId": "abc123",
  "userId": "uid",
  "nodeId": "nid",
  "event": "chunk_uploaded",
  "chunkSeq": 3,
  "chunkHash": "sha256...",
  "durationMs": 45,
  "deduplicated": false
}
```

### Alerting

| Alert | Threshold | Severity |
|-------|-----------|----------|
| Upload error rate | > 1% for 5 min | P1 |
| Metadata DB latency P99 | > 200 ms | P1 |
| CDN hit rate | < 70% | P2 |
| Sync lag P95 | > 60 s | P2 |
| Disk usage | > 85% | P2 |
| Quota check failures | > 0.01% | P3 |

---

## 15. API Design

### RESTful API

```
# Files
POST   /v1/files                     # Upload (initiate resumable)
GET    /v1/files/{fileId}            # Get metadata
PATCH  /v1/files/{fileId}            # Update metadata (rename, move)
DELETE /v1/files/{fileId}            # Soft delete (trash)
GET    /v1/files/{fileId}/content    # Download (returns signed URL)

# Folders
POST   /v1/folders                   # Create folder
GET    /v1/folders/{folderId}/children?pageToken=&pageSize=  # List contents
DELETE /v1/folders/{folderId}        # Delete folder (recursive)

# Versions
GET    /v1/files/{fileId}/versions   # List versions
GET    /v1/files/{fileId}/versions/{version}/content
POST   /v1/files/{fileId}/versions/{version}/restore

# Permissions
POST   /v1/files/{fileId}/permissions
GET    /v1/files/{fileId}/permissions
PATCH  /v1/files/{fileId}/permissions/{permId}
DELETE /v1/files/{fileId}/permissions/{permId}

# Search
GET    /v1/search?q=&mimeType=&modifiedAfter=&pageToken=

# Sync
GET    /v1/changes?cursor=&limit=

# Trash
GET    /v1/trash
POST   /v1/trash/{fileId}/restore
DELETE /v1/trash/{fileId}            # Permanent delete
DELETE /v1/trash                     # Empty trash

# Quota
GET    /v1/about                     # User quota + plan info
```

### Error Responses

```json
{
  "error": {
    "code": 403,
    "message": "The caller does not have permission to access this resource.",
    "status": "PERMISSION_DENIED",
    "details": [{ "reason": "forbidden", "domain": "drive.googleapis.com" }]
  }
}
```

Standard HTTP status codes: 200, 201, 204, 206 (partial), 400, 401, 403, 404, 409 (conflict), 429 (rate limit), 500, 503.

---

## 16. Low-Level Design (LLD) — Complete JS Implementations

### 16.1 ChunkManager

```javascript
// chunkManager.js
// Splits a file into fixed-size chunks and computes SHA-256 hashes
// Runs in browser (Web Crypto API) or Node.js

const CHUNK_SIZE = 4 * 1024 * 1024; // 4 MB default

class ChunkManager {
  /**
   * @param {File | Buffer} file
   * @param {number} chunkSize
   */
  constructor(file, chunkSize = CHUNK_SIZE) {
    this.file = file;
    this.chunkSize = chunkSize;
    this.totalChunks = Math.ceil(file.size / chunkSize);
  }

  /**
   * Returns a single chunk's ArrayBuffer + SHA-256 hash
   * @param {number} seq - chunk sequence number (0-indexed)
   * @returns {Promise<{ seq, data: ArrayBuffer, hash: string, size: number }>}
   */
  async getChunk(seq) {
    const start = seq * this.chunkSize;
    const end = Math.min(start + this.chunkSize, this.file.size);
    const blob = this.file.slice(start, end);
    const data = await blob.arrayBuffer();
    const hashBuffer = await crypto.subtle.digest('SHA-256', data);
    const hash = this._toHex(hashBuffer);
    return { seq, data, hash, size: data.byteLength };
  }

  /**
   * Computes hashes for all chunks (lightweight — no ArrayBuffer retention)
   * Used for dedup pre-check before uploading
   */
  async computeManifest() {
    const chunks = [];
    for (let seq = 0; seq < this.totalChunks; seq++) {
      const { hash, size } = await this.getChunk(seq);
      chunks.push({ seq, hash, size });
    }
    return {
      name: this.file.name,
      mimeType: this.file.type,
      size: this.file.size,
      totalChunks: this.totalChunks,
      chunkSize: this.chunkSize,
      chunks,
    };
  }

  _toHex(buffer) {
    return Array.from(new Uint8Array(buffer))
      .map(b => b.toString(16).padStart(2, '0'))
      .join('');
  }
}

module.exports = { ChunkManager, CHUNK_SIZE };
```

### 16.2 UploadService

```javascript
// uploadService.js — Server-side upload orchestrator (Node.js + Express)

const express = require('express');
const crypto = require('crypto');
const { promisify } = require('util');
const router = express.Router();

// --- Dependencies (injected) ---
// blobStore: { exists(hash), putChunk(hash, stream), getSignedUrl(hash) }
// metadataDb: { createNode(node), updateNodeChunks(nodeId, version, chunks) }
// redis: ioredis client
// kafka: kafka-node producer

const UPLOAD_SESSION_TTL = 24 * 60 * 60; // 24 hours in seconds

class UploadService {
  constructor({ blobStore, metadataDb, redis, kafka, quotaService }) {
    this.blobStore = blobStore;
    this.metadataDb = metadataDb;
    this.redis = redis;
    this.kafka = kafka;
    this.quotaService = quotaService;
  }

  /**
   * Phase 1: Initiate a resumable upload session
   */
  async initiate(userId, { name, mimeType, size, parentId }) {
    // 1. Quota check (reserve)
    await this.quotaService.reserve(userId, size);

    // 2. Generate upload session
    const uploadId = crypto.randomBytes(16).toString('hex');
    const totalChunks = Math.ceil(size / (4 * 1024 * 1024));

    const session = {
      uploadId,
      userId,
      parentId,
      name,
      mimeType,
      size,
      totalChunks,
      receivedChunks: {}, // { seq: hash }
      createdAt: Date.now(),
    };

    await this.redis.setex(
      `upload:${uploadId}`,
      UPLOAD_SESSION_TTL,
      JSON.stringify(session)
    );

    return { uploadId, totalChunks, chunkSize: 4 * 1024 * 1024 };
  }

  /**
   * Phase 2: Upload a single chunk
   */
  async uploadChunk(uploadId, seq, chunkBuffer, clientHash) {
    const session = await this._getSession(uploadId);

    // 1. Verify hash integrity
    const serverHash = crypto
      .createHash('sha256')
      .update(chunkBuffer)
      .digest('hex');

    if (serverHash !== clientHash) {
      throw new Error(`HASH_MISMATCH: chunk ${seq} corrupted in transit`);
    }

    // 2. Deduplication check — skip blob write if already stored
    const alreadyExists = await this.blobStore.exists(serverHash);
    if (!alreadyExists) {
      await this.blobStore.putChunk(serverHash, chunkBuffer);
    }

    // 3. Record received chunk in session
    session.receivedChunks[seq] = serverHash;
    await this.redis.setex(
      `upload:${uploadId}`,
      UPLOAD_SESSION_TTL,
      JSON.stringify(session)
    );

    return { seq, hash: serverHash, deduplicated: alreadyExists };
  }

  /**
   * Phase 3: Complete the upload — assemble manifest, write metadata
   */
  async complete(uploadId, clientChunks) {
    const session = await this._getSession(uploadId);

    // 1. Verify all chunks received
    const missing = [];
    for (let i = 0; i < session.totalChunks; i++) {
      if (!session.receivedChunks[i]) missing.push(i);
    }
    if (missing.length > 0) {
      throw new Error(`INCOMPLETE_UPLOAD: missing chunks ${missing.join(',')}`);
    }

    // 2. Write metadata to DB
    const nodeId = crypto.randomUUID();
    const version = 1;
    const now = new Date().toISOString();

    await this.metadataDb.createNode({
      nodeId,
      ownerId: session.userId,
      parentId: session.parentId,
      name: session.name,
      nodeType: 'file',
      mimeType: session.mimeType,
      sizeBytes: session.size,
      createdAt: now,
      modifiedAt: now,
      version,
    });

    // 3. Write chunk manifest
    const chunks = Object.entries(session.receivedChunks)
      .sort(([a], [b]) => parseInt(a) - parseInt(b))
      .map(([seq, hash]) => ({
        nodeId,
        version,
        seq: parseInt(seq),
        chunkHash: hash,
        chunkSize: (session.receivedChunks[seq] ? 4 * 1024 * 1024 : session.size % (4 * 1024 * 1024)),
      }));

    await this.metadataDb.updateNodeChunks(nodeId, version, chunks);

    // 4. Confirm quota usage
    await this.quotaService.confirm(session.userId, session.size);

    // 5. Publish event to Kafka
    await this.kafka.send({
      topic: 'file.events',
      messages: [{
        key: session.userId,
        value: JSON.stringify({
          type: 'FILE_UPLOADED',
          nodeId,
          userId: session.userId,
          parentId: session.parentId,
          name: session.name,
          size: session.size,
          version,
          timestamp: now,
        }),
      }],
    });

    // 6. Cleanup session
    await this.redis.del(`upload:${uploadId}`);

    return { nodeId, version, createdAt: now };
  }

  /**
   * Resume: return list of received vs missing chunks
   */
  async getStatus(uploadId) {
    const session = await this._getSession(uploadId);
    const received = Object.keys(session.receivedChunks).map(Number);
    const missing = [];
    for (let i = 0; i < session.totalChunks; i++) {
      if (!session.receivedChunks[i]) missing.push(i);
    }
    return { uploadId, totalChunks: session.totalChunks, received, missing };
  }

  async _getSession(uploadId) {
    const raw = await this.redis.get(`upload:${uploadId}`);
    if (!raw) throw new Error('UPLOAD_SESSION_NOT_FOUND');
    return JSON.parse(raw);
  }
}

module.exports = { UploadService };
```

### 16.3 MetadataService

```javascript
// metadataService.js — File/Folder CRUD with quota enforcement

const crypto = require('crypto');

class MetadataService {
  constructor({ db, cache, quotaService }) {
    this.db = db;           // SQL client (e.g., pg)
    this.cache = cache;     // Redis
    this.quotaService = quotaService;
  }

  /**
   * Get node metadata (with caching)
   */
  async getNode(nodeId, requestingUserId) {
    const cacheKey = `meta:${nodeId}`;
    const cached = await this.cache.get(cacheKey);
    if (cached) return this._applyAccess(JSON.parse(cached), requestingUserId);

    const node = await this.db.query(
      `SELECT * FROM nodes WHERE node_id = $1 AND is_deleted = false`,
      [nodeId]
    );
    if (!node.rows[0]) throw new NotFoundError(nodeId);

    await this.cache.setex(cacheKey, 300, JSON.stringify(node.rows[0]));
    return this._applyAccess(node.rows[0], requestingUserId);
  }

  /**
   * List folder children (paginated)
   */
  async listFolder(folderId, userId, { pageSize = 100, pageToken } = {}) {
    const cursor = pageToken ? Buffer.from(pageToken, 'base64').toString() : null;

    const rows = await this.db.query(
      `SELECT n.* FROM nodes n
       WHERE n.parent_id = $1
         AND n.is_deleted = false
         AND (
           n.owner_id = $2
           OR EXISTS (
             SELECT 1 FROM permissions p
             JOIN tree_paths tp ON p.node_id = tp.ancestor_id
             WHERE tp.descendant_id = n.node_id
               AND (p.grantee_id = $2 OR p.grantee_type = 'anyone')
           )
         )
         AND ($3::text IS NULL OR n.name > $3)
       ORDER BY n.node_type DESC, n.name ASC
       LIMIT $4`,
      [folderId, userId, cursor, pageSize + 1]
    );

    const hasMore = rows.rows.length > pageSize;
    const items = rows.rows.slice(0, pageSize);
    const nextPageToken = hasMore
      ? Buffer.from(items[items.length - 1].name).toString('base64')
      : null;

    return { items, nextPageToken };
  }

  /**
   * Move node (update parent_id + update tree_paths)
   */
  async moveNode(nodeId, newParentId, userId) {
    await this._requireRole(userId, nodeId, 'editor');
    await this._requireRole(userId, newParentId, 'editor');

    await this.db.transaction(async (client) => {
      // Update parent
      await client.query(
        `UPDATE nodes SET parent_id = $1, modified_at = NOW() WHERE node_id = $2`,
        [newParentId, nodeId]
      );

      // Update closure table: delete old ancestor paths, insert new ones
      await client.query(
        `DELETE FROM tree_paths WHERE descendant_id IN (
           SELECT descendant_id FROM tree_paths WHERE ancestor_id = $1
         ) AND ancestor_id IN (
           SELECT ancestor_id FROM tree_paths WHERE descendant_id = $1
             AND ancestor_id != descendant_id
         )`,
        [nodeId]
      );

      await client.query(
        `INSERT INTO tree_paths (ancestor_id, descendant_id, depth)
         SELECT supertree.ancestor_id, subtree.descendant_id,
                supertree.depth + subtree.depth + 1
         FROM tree_paths supertree, tree_paths subtree
         WHERE supertree.descendant_id = $1 AND subtree.ancestor_id = $2`,
        [newParentId, nodeId]
      );
    });

    await this.cache.del(`meta:${nodeId}`);
    await this.cache.del(`tree:${nodeId}`);
  }

  /**
   * Soft-delete (move to trash)
   */
  async trashNode(nodeId, userId) {
    await this._requireRole(userId, nodeId, 'editor');

    await this.db.query(
      `UPDATE nodes SET is_deleted = true, trashed_at = NOW()
       WHERE node_id = $1`,
      [nodeId]
    );

    await this.cache.del(`meta:${nodeId}`);
  }

  async _requireRole(userId, nodeId, minRole) {
    const roleOrder = { viewer: 1, commenter: 2, editor: 3, owner: 4 };
    const cacheKey = `perm:${nodeId}:${userId}`;
    let role = await this.cache.get(cacheKey);

    if (!role) {
      const result = await this.db.query(
        `SELECT p.role FROM permissions p
         JOIN tree_paths tp ON p.node_id = tp.ancestor_id
         WHERE tp.descendant_id = $1
           AND (p.grantee_id = $2 OR p.grantee_type = 'anyone')
         ORDER BY tp.depth ASC LIMIT 1`,
        [nodeId, userId]
      );
      role = result.rows[0]?.role;
      if (role) await this.cache.setex(cacheKey, 60, role);
    }

    if (!role || roleOrder[role] < roleOrder[minRole]) {
      throw new ForbiddenError(`Requires ${minRole} on ${nodeId}`);
    }
  }

  _applyAccess(node, userId) {
    // Strip sensitive fields if not owner
    if (node.owner_id !== userId) delete node.internal_hash;
    return node;
  }
}

class NotFoundError extends Error { constructor(id) { super(`Node ${id} not found`); this.code = 404; } }
class ForbiddenError extends Error { constructor(m) { super(m); this.code = 403; } }

module.exports = { MetadataService };
```

### 16.4 SyncEngine

```javascript
// syncEngine.js — Delta sync: compute changes since a cursor

class SyncEngine {
  constructor({ db, cache }) {
    this.db = db;
    this.cache = cache;
  }

  /**
   * Get changes for a user since a given cursor (change sequence number)
   * Returns changes + new cursor
   */
  async getChanges(userId, cursor, limit = 500) {
    const cursorSeq = cursor ? parseInt(cursor) : 0;

    const rows = await this.db.query(
      `SELECT cl.seq, cl.node_id, cl.change_type, cl.version, cl.timestamp,
              n.name, n.parent_id, n.node_type, n.size_bytes, n.is_deleted
       FROM change_log cl
       LEFT JOIN nodes n ON cl.node_id = n.node_id
       WHERE cl.user_id = $1 AND cl.seq > $2
       ORDER BY cl.seq ASC
       LIMIT $3`,
      [userId, cursorSeq, limit + 1]
    );

    const hasMore = rows.rows.length > limit;
    const changes = rows.rows.slice(0, limit);
    const newCursor = changes.length > 0
      ? String(changes[changes.length - 1].seq)
      : cursor;

    return {
      changes: changes.map(r => ({
        seq: r.seq,
        nodeId: r.node_id,
        changeType: r.change_type,  // create|modify|delete|move|rename
        version: r.version,
        timestamp: r.timestamp,
        node: r.is_deleted ? null : {
          name: r.name,
          parentId: r.parent_id,
          nodeType: r.node_type,
          sizeBytes: r.size_bytes,
        },
      })),
      nextCursor: newCursor,
      hasMore,
    };
  }

  /**
   * Record a change in the log (called by Upload/Metadata services)
   */
  async recordChange(userId, nodeId, changeType, version) {
    await this.db.query(
      `INSERT INTO change_log (user_id, node_id, change_type, version, timestamp)
       VALUES ($1, $2, $3, $4, NOW())`,
      [userId, nodeId, changeType, version]
    );
  }

  /**
   * Resolve conflict: LWW with conflicted copy creation
   * Returns { winner: 'server'|'client', conflictNodeId? }
   */
  async resolveConflict(userId, nodeId, baseVersion, clientTimestamp) {
    const node = await this.db.query(
      `SELECT version, modified_at FROM nodes WHERE node_id = $1`,
      [nodeId]
    );

    if (!node.rows[0]) throw new Error('Node not found');
    const { version: serverVersion, modified_at: serverTimestamp } = node.rows[0];

    if (serverVersion === baseVersion) {
      // No conflict — client can proceed
      return { winner: 'client', conflictNodeId: null };
    }

    // Conflict: both server and client modified since base
    if (new Date(clientTimestamp) > new Date(serverTimestamp)) {
      // Client wins: save server version as conflicted copy
      const conflictId = await this._createConflictCopy(userId, nodeId, serverVersion);
      return { winner: 'client', conflictNodeId: conflictId };
    } else {
      // Server wins: client needs to create conflicted copy of its changes
      return { winner: 'server', conflictNodeId: null };
    }
  }

  async _createConflictCopy(userId, nodeId, version) {
    const conflictId = require('crypto').randomUUID();
    await this.db.query(
      `INSERT INTO nodes (node_id, owner_id, parent_id, name, node_type, mime_type,
                          size_bytes, created_at, modified_at, version)
       SELECT $1, owner_id, parent_id,
              name || ' (conflicted copy ' || $3 || ')' AS name,
              node_type, mime_type, size_bytes, NOW(), NOW(), $4
       FROM nodes WHERE node_id = $2`,
      [conflictId, nodeId, new Date().toLocaleDateString(), version]
    );
    return conflictId;
  }
}

module.exports = { SyncEngine };
```

### 16.5 PermissionService (ACL)

```javascript
// permissionService.js — ACL management and permission checks

const crypto = require('crypto');

const ROLE_ORDER = { viewer: 1, commenter: 2, editor: 3, owner: 4 };

class PermissionService {
  constructor({ db, cache, emailService }) {
    this.db = db;
    this.cache = cache;
    this.emailService = emailService;
  }

  /**
   * Grant permission to a user or make public
   */
  async grant(grantorId, nodeId, { granteeId, granteeType = 'user', role, expiresAt, sendEmail = true }) {
    // Only owner/editor can share
    await this._requireRole(grantorId, nodeId, 'editor');

    // Owners cannot be downgraded via share
    if (granteeId) {
      const existing = await this._getDirectPermission(granteeId, nodeId);
      if (existing?.role === 'owner') throw new Error('Cannot modify owner permission');
    }

    const permId = crypto.randomUUID();
    const shareLink = granteeType === 'anyone'
      ? crypto.randomBytes(12).toString('base64url')
      : null;

    await this.db.query(
      `INSERT INTO permissions (perm_id, node_id, grantee_id, grantee_type, role, share_link, expires_at, created_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
       ON CONFLICT (node_id, grantee_id)
       DO UPDATE SET role = $5, expires_at = $7`,
      [permId, nodeId, granteeId, granteeType, role, shareLink, expiresAt]
    );

    // Invalidate cache
    if (granteeId) await this.cache.del(`perm:${nodeId}:${granteeId}`);

    // Notify grantee
    if (sendEmail && granteeId && granteeType === 'user') {
      const node = await this.db.query(`SELECT name FROM nodes WHERE node_id = $1`, [nodeId]);
      await this.emailService.sendShareNotification(granteeId, node.rows[0]?.name, role);
    }

    return { permId, shareLink };
  }

  /**
   * Revoke a specific permission
   */
  async revoke(revokerId, nodeId, permId) {
    await this._requireRole(revokerId, nodeId, 'owner');

    const perm = await this.db.query(
      `SELECT * FROM permissions WHERE perm_id = $1 AND node_id = $2`,
      [permId, nodeId]
    );
    if (!perm.rows[0]) throw new Error('Permission not found');
    if (perm.rows[0].role === 'owner') throw new Error('Cannot revoke owner');

    await this.db.query(`DELETE FROM permissions WHERE perm_id = $1`, [permId]);

    if (perm.rows[0].grantee_id) {
      await this.cache.del(`perm:${nodeId}:${perm.rows[0].grantee_id}`);
    }
  }

  /**
   * Check if user has at least minRole on a node (walks up tree)
   */
  async check(userId, nodeId, minRole) {
    const cacheKey = `perm:${nodeId}:${userId}`;
    let cachedRole = await this.cache.get(cacheKey);

    if (!cachedRole) {
      // Walk up tree using closure table — single query
      const result = await this.db.query(
        `SELECT p.role, tp.depth FROM permissions p
         JOIN tree_paths tp ON p.node_id = tp.ancestor_id
         WHERE tp.descendant_id = $1
           AND (p.grantee_id = $2 OR p.grantee_type = 'anyone')
           AND (p.expires_at IS NULL OR p.expires_at > NOW())
         ORDER BY tp.depth ASC, (
           CASE p.role WHEN 'owner' THEN 4 WHEN 'editor' THEN 3
                       WHEN 'commenter' THEN 2 ELSE 1 END
         ) DESC
         LIMIT 1`,
        [nodeId, userId]
      );

      cachedRole = result.rows[0]?.role || 'none';
      await this.cache.setex(cacheKey, 60, cachedRole);
    }

    return ROLE_ORDER[cachedRole] >= ROLE_ORDER[minRole];
  }

  /**
   * Resolve share link → node access
   */
  async resolveShareLink(shareLink) {
    const result = await this.db.query(
      `SELECT p.*, n.name, n.node_type
       FROM permissions p JOIN nodes n ON p.node_id = n.node_id
       WHERE p.share_link = $1
         AND (p.expires_at IS NULL OR p.expires_at > NOW())`,
      [shareLink]
    );
    if (!result.rows[0]) throw new Error('INVALID_SHARE_LINK');
    return result.rows[0];
  }

  async _getDirectPermission(userId, nodeId) {
    const r = await this.db.query(
      `SELECT role FROM permissions WHERE node_id = $1 AND grantee_id = $2`,
      [nodeId, userId]
    );
    return r.rows[0] || null;
  }

  async _requireRole(userId, nodeId, minRole) {
    const ok = await this.check(userId, nodeId, minRole);
    if (!ok) throw Object.assign(new Error('PERMISSION_DENIED'), { code: 403 });
  }
}

module.exports = { PermissionService };
```

### 16.6 NotificationService (WebSocket)

```javascript
// notificationService.js — Real-time change push via WebSocket

const WebSocket = require('ws');
const { Kafka } = require('kafkajs');

class NotificationService {
  constructor({ kafkaBrokers, redisCluster }) {
    this.kafka = new Kafka({ brokers: kafkaBrokers });
    this.consumer = this.kafka.consumer({ groupId: 'notification-service' });
    this.redis = redisCluster;

    // Map: userId → Set<WebSocket>
    this.connections = new Map();
  }

  async start(httpServer) {
    this.wss = new WebSocket.Server({ server: httpServer, path: '/ws' });

    this.wss.on('connection', (ws, req) => this._onConnect(ws, req));

    // Subscribe to Kafka file events
    await this.consumer.connect();
    await this.consumer.subscribe({ topic: 'file.events', fromBeginning: false });

    await this.consumer.run({
      eachMessage: async ({ message }) => {
        const event = JSON.parse(message.value.toString());
        await this._fanout(event);
      },
    });

    // Heartbeat: close stale connections
    setInterval(() => this._pingAll(), 30000);
  }

  _onConnect(ws, req) {
    const userId = this._authenticateWS(req);
    if (!userId) { ws.close(4001, 'Unauthorized'); return; }

    // Register connection
    if (!this.connections.has(userId)) this.connections.set(userId, new Set());
    this.connections.get(userId).add(ws);

    // Send any missed events (device comes back online)
    this._sendMissedEvents(ws, userId, req.url);

    ws.on('pong', () => { ws.isAlive = true; });
    ws.on('message', (msg) => this._handleClientMessage(ws, userId, msg));
    ws.on('close', () => {
      this.connections.get(userId)?.delete(ws);
      if (this.connections.get(userId)?.size === 0) {
        this.connections.delete(userId);
      }
    });
  }

  async _fanout(event) {
    const { userId, type, nodeId, version, timestamp } = event;

    const message = JSON.stringify({
      type: 'CHANGE',
      changes: [{ nodeId, changeType: type, version }],
      timestamp,
    });

    // Persist in Redis stream for missed-event delivery (7-day retention)
    await this.redis.xadd(
      `changes:${userId}`, 'MAXLEN', '~', 10000, '*',
      'data', message
    );

    // Push to connected devices
    const sockets = this.connections.get(userId);
    if (!sockets) return;

    for (const ws of sockets) {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(message);
      }
    }
  }

  async _sendMissedEvents(ws, userId, url) {
    // Parse ?since= from URL
    const sinceId = new URLSearchParams(url.split('?')[1]).get('since') || '0-0';
    const messages = await this.redis.xrange(`changes:${userId}`, sinceId, '+', 'COUNT', 500);
    for (const [, fields] of messages) {
      const data = fields[fields.indexOf('data') + 1];
      if (ws.readyState === WebSocket.OPEN) ws.send(data);
    }
  }

  _pingAll() {
    this.wss.clients.forEach(ws => {
      if (!ws.isAlive) { ws.terminate(); return; }
      ws.isAlive = false;
      ws.ping();
    });
  }

  _authenticateWS(req) {
    // Extract JWT from ?token= query param or Authorization header
    const token = new URLSearchParams(req.url.split('?')[1]).get('token');
    try {
      const payload = require('jsonwebtoken').verify(token, process.env.JWT_SECRET);
      return payload.sub;
    } catch { return null; }
  }

  _handleClientMessage(ws, userId, msg) {
    // Clients can send { type: 'ACK', seq: '...' } to advance their cursor
    // Stored in Redis for the sync engine
    try {
      const { type, cursor } = JSON.parse(msg);
      if (type === 'ACK' && cursor) {
        this.redis.hset(`device:cursors:${userId}`, ws._deviceId, cursor);
      }
    } catch { /* ignore malformed */ }
  }
}

module.exports = { NotificationService };
```

### 16.7 VersioningService

```javascript
// versioningService.js

class VersioningService {
  constructor({ db, blobStore, cache }) {
    this.db = db;
    this.blobStore = blobStore;
    this.cache = cache;
  }

  /**
   * List all versions of a file
   */
  async listVersions(nodeId, userId) {
    await this._requireAccess(userId, nodeId, 'viewer');

    const rows = await this.db.query(
      `SELECT fv.*, u.email AS created_by_email
       FROM file_versions fv
       JOIN users u ON fv.created_by = u.user_id
       WHERE fv.node_id = $1
       ORDER BY fv.version DESC`,
      [nodeId]
    );

    return rows.rows;
  }

  /**
   * Restore a previous version → creates new version with old chunk list
   */
  async restore(nodeId, targetVersion, userId) {
    await this._requireAccess(userId, nodeId, 'editor');

    return await this.db.transaction(async (client) => {
      // Get current version
      const node = await client.query(
        `SELECT version FROM nodes WHERE node_id = $1 FOR UPDATE`,
        [nodeId]
      );
      const currentVersion = node.rows[0].version;
      const newVersion = currentVersion + 1;

      // Copy chunks from target version to new version
      await client.query(
        `INSERT INTO file_chunks (node_id, version, seq, chunk_hash, chunk_size)
         SELECT node_id, $1, seq, chunk_hash, chunk_size
         FROM file_chunks WHERE node_id = $2 AND version = $3`,
        [newVersion, nodeId, targetVersion]
      );

      // Get size of target version
      const sizeResult = await client.query(
        `SELECT SUM(chunk_size) AS total FROM file_chunks
         WHERE node_id = $1 AND version = $2`,
        [nodeId, targetVersion]
      );

      // Update node version
      await client.query(
        `UPDATE nodes SET version = $1, size_bytes = $2, modified_at = NOW()
         WHERE node_id = $3`,
        [newVersion, sizeResult.rows[0].total, nodeId]
      );

      // Record version metadata
      await client.query(
        `INSERT INTO file_versions (node_id, version, size_bytes, created_at, created_by, change_note)
         VALUES ($1, $2, $3, NOW(), $4, $5)`,
        [nodeId, newVersion, sizeResult.rows[0].total, userId, `Restored from version ${targetVersion}`]
      );

      await this.cache.del(`meta:${nodeId}`);
      return { nodeId, newVersion, restoredFrom: targetVersion };
    });
  }

  /**
   * Purge old versions (called by background GC job)
   * Respects retention policy: freeUsers = 30 days, paid = 180 days
   */
  async purgeOldVersions(nodeId, retentionDays) {
    const cutoff = new Date(Date.now() - retentionDays * 24 * 60 * 60 * 1000).toISOString();

    // Get chunks of versions to be purged
    const chunksToMaybeDelete = await this.db.query(
      `SELECT DISTINCT fc.chunk_hash
       FROM file_chunks fc
       JOIN file_versions fv ON fc.node_id = fv.node_id AND fc.version = fv.version
       WHERE fc.node_id = $1
         AND fv.created_at < $2
         AND fc.version != (SELECT MAX(version) FROM nodes WHERE node_id = $1)`,
      [nodeId, cutoff]
    );

    // Decrement ref counts; delete blobs if count reaches 0
    for (const { chunk_hash } of chunksToMaybeDelete.rows) {
      const result = await this.db.query(
        `UPDATE chunk_refs SET ref_count = ref_count - 1
         WHERE chunk_hash = $1 RETURNING ref_count`,
        [chunk_hash]
      );
      if (result.rows[0]?.ref_count <= 0) {
        await this.blobStore.deleteChunk(chunk_hash);
        await this.db.query(`DELETE FROM chunk_refs WHERE chunk_hash = $1`, [chunk_hash]);
      }
    }

    // Delete version records
    await this.db.query(
      `DELETE FROM file_versions WHERE node_id = $1 AND created_at < $2
         AND version != (SELECT MAX(version) FROM nodes WHERE node_id = $1)`,
      [nodeId, cutoff]
    );
  }

  async _requireAccess(userId, nodeId, role) {
    const r = await this.db.query(
      `SELECT p.role FROM permissions p JOIN tree_paths tp ON p.node_id = tp.ancestor_id
       WHERE tp.descendant_id = $1 AND (p.grantee_id = $2 OR p.grantee_type = 'anyone')
       ORDER BY tp.depth LIMIT 1`,
      [nodeId, userId]
    );
    const ROLES = { viewer: 1, commenter: 2, editor: 3, owner: 4 };
    if (!r.rows[0] || ROLES[r.rows[0].role] < ROLES[role]) {
      throw Object.assign(new Error('PERMISSION_DENIED'), { code: 403 });
    }
  }
}

module.exports = { VersioningService };
```

### 16.8 SearchService

```javascript
// searchService.js — Full-text search using Elasticsearch

const { Client } = require('@elastic/elasticsearch');

class SearchService {
  constructor({ esNodes, metadataDb }) {
    this.es = new Client({ nodes: esNodes });
    this.db = metadataDb;
  }

  /**
   * Index or update a file document
   */
  async indexFile(nodeId, extractedText = '') {
    const node = await this.db.query(
      `SELECT n.*, ARRAY_AGG(p.grantee_id) AS shared_with
       FROM nodes n
       LEFT JOIN permissions p ON p.node_id = n.node_id AND p.grantee_type = 'user'
       WHERE n.node_id = $1
       GROUP BY n.node_id`,
      [nodeId]
    );
    if (!node.rows[0]) return;
    const n = node.rows[0];

    await this.es.index({
      index: 'drive_files',
      id: nodeId,
      body: {
        nodeId,
        ownerId: n.owner_id,
        parentId: n.parent_id,
        name: n.name,
        mimeType: n.mime_type,
        sizeBytes: n.size_bytes,
        content: extractedText.slice(0, 100000), // 100KB text cap
        sharedWith: n.shared_with.filter(Boolean),
        hasPublicLink: false, // set separately
        modifiedAt: n.modified_at,
        createdAt: n.created_at,
      },
    });
  }

  /**
   * Full-text search with security trimming
   */
  async search(userId, query, { mimeType, modifiedAfter, pageSize = 20, from = 0 } = {}) {
    const filters = [
      // Security: only return files user can access
      {
        bool: {
          should: [
            { term: { ownerId: userId } },
            { term: { sharedWith: userId } },
            { term: { hasPublicLink: true } },
          ],
          minimum_should_match: 1,
        },
      },
    ];

    if (mimeType) filters.push({ term: { mimeType } });
    if (modifiedAfter) filters.push({ range: { modifiedAt: { gte: modifiedAfter } } });

    const result = await this.es.search({
      index: 'drive_files',
      body: {
        from,
        size: pageSize,
        query: {
          bool: {
            must: {
              multi_match: {
                query,
                fields: ['name^3', 'content'],   // name boosted 3×
                type: 'best_fields',
                fuzziness: 'AUTO',
              },
            },
            filter: filters,
          },
        },
        highlight: {
          fields: { content: { fragment_size: 150, number_of_fragments: 2 } },
        },
        sort: [{ _score: 'desc' }, { modifiedAt: 'desc' }],
      },
    });

    return {
      total: result.hits.total.value,
      hits: result.hits.hits.map(h => ({
        nodeId: h._id,
        name: h._source.name,
        mimeType: h._source.mimeType,
        modifiedAt: h._source.modifiedAt,
        score: h._score,
        snippet: h.highlight?.content?.[0],
      })),
    };
  }

  /**
   * Suggest completions for search bar type-ahead
   */
  async suggest(userId, prefix) {
    const result = await this.es.search({
      index: 'drive_files',
      body: {
        size: 8,
        query: {
          bool: {
            must: { prefix: { 'name.keyword': { value: prefix, case_insensitive: true } } },
            filter: [{ bool: { should: [{ term: { ownerId: userId } }, { term: { sharedWith: userId } }] } }],
          },
        },
        _source: ['name', 'mimeType', 'nodeId'],
      },
    });

    return result.hits.hits.map(h => h._source);
  }
}

module.exports = { SearchService };
```

### 16.9 DeduplicationService (CAS)

```javascript
// deduplicationService.js — Content-Addressable Storage dedup logic

class DeduplicationService {
  constructor({ db, blobStore }) {
    this.db = db;           // Cassandra or PostgreSQL for ref counts
    this.blobStore = blobStore;
  }

  /**
   * Check which chunks already exist in blob store (batch)
   * Returns { existing: Set<hash>, missing: Set<hash> }
   */
  async checkChunks(hashes) {
    const rows = await this.db.query(
      `SELECT chunk_hash FROM chunk_refs WHERE chunk_hash = ANY($1)`,
      [hashes]
    );
    const existing = new Set(rows.rows.map(r => r.chunk_hash));
    const missing = new Set(hashes.filter(h => !existing.has(h)));
    return { existing, missing };
  }

  /**
   * Increment reference count for a set of chunk hashes
   * Called when a new file version references these chunks
   */
  async addRefs(hashes, chunkSizes) {
    const sizeMap = new Map(hashes.map((h, i) => [h, chunkSizes[i]]));

    for (const hash of hashes) {
      await this.db.query(
        `INSERT INTO chunk_refs (chunk_hash, ref_count, size_bytes, created_at)
         VALUES ($1, 1, $2, NOW())
         ON CONFLICT (chunk_hash) DO UPDATE SET ref_count = chunk_refs.ref_count + 1`,
        [hash, sizeMap.get(hash)]
      );
    }
  }

  /**
   * Decrement reference counts; enqueue blobs for deletion if count hits 0
   */
  async removeRefs(hashes) {
    const toDelete = [];
    for (const hash of hashes) {
      const result = await this.db.query(
        `UPDATE chunk_refs SET ref_count = ref_count - 1
         WHERE chunk_hash = $1 RETURNING ref_count`,
        [hash]
      );
      if (result.rows[0]?.ref_count <= 0) {
        toDelete.push(hash);
      }
    }

    // Async blob deletion (non-blocking)
    if (toDelete.length > 0) {
      setImmediate(async () => {
        for (const hash of toDelete) {
          try {
            await this.blobStore.deleteChunk(hash);
            await this.db.query(`DELETE FROM chunk_refs WHERE chunk_hash = $1`, [hash]);
          } catch (err) {
            console.error(`GC failed for ${hash}:`, err);
          }
        }
      });
    }
  }

  /**
   * Compute storage savings from deduplication
   */
  async getDeduplicationStats() {
    const result = await this.db.query(
      `SELECT
         COUNT(*) AS unique_blobs,
         SUM(size_bytes) AS actual_stored,
         SUM(size_bytes * ref_count) AS logical_size,
         SUM(size_bytes * (ref_count - 1)) AS bytes_saved
       FROM chunk_refs`
    );
    const { unique_blobs, actual_stored, logical_size, bytes_saved } = result.rows[0];
    return {
      uniqueBlobs: parseInt(unique_blobs),
      actualStoredBytes: parseInt(actual_stored),
      logicalSizeBytes: parseInt(logical_size),
      bytesSaved: parseInt(bytes_saved),
      dedupRatio: (parseInt(logical_size) / parseInt(actual_stored)).toFixed(2),
    };
  }
}

module.exports = { DeduplicationService };
```

### 16.10 RateLimiter

```javascript
// rateLimiter.js — Token bucket rate limiter using Redis

class RateLimiter {
  constructor(redis) {
    this.redis = redis;
  }

  /**
   * Token bucket algorithm
   * @param {string} key - e.g. "user:uid:upload" or "ip:1.2.3.4:api"
   * @param {number} capacity - max tokens (burst limit)
   * @param {number} refillRate - tokens per second
   * @param {number} tokensRequested - cost of this request
   * @returns {{ allowed: boolean, remaining: number, retryAfter?: number }}
   */
  async check(key, capacity, refillRate, tokensRequested = 1) {
    const now = Date.now() / 1000;
    const bucketKey = `ratelimit:${key}`;

    const result = await this.redis.eval(
      RATE_LIMIT_SCRIPT,
      1,           // numkeys
      bucketKey,   // KEYS[1]
      capacity,    // ARGV[1]
      refillRate,  // ARGV[2]
      now,         // ARGV[3]
      tokensRequested // ARGV[4]
    );

    const [allowed, remaining, retryAfter] = result;
    return {
      allowed: allowed === 1,
      remaining: Math.floor(remaining),
      retryAfter: allowed ? undefined : Math.ceil(retryAfter),
    };
  }

  /**
   * Express middleware factory
   */
  middleware({ capacity = 100, refillRate = 10, keyFn = (req) => `user:${req.user?.id}:api` } = {}) {
    return async (req, res, next) => {
      const key = keyFn(req);
      const result = await this.check(key, capacity, refillRate);

      res.setHeader('X-RateLimit-Limit', capacity);
      res.setHeader('X-RateLimit-Remaining', result.remaining);

      if (!result.allowed) {
        res.setHeader('Retry-After', result.retryAfter);
        return res.status(429).json({
          error: { code: 429, message: 'Too Many Requests', retryAfter: result.retryAfter }
        });
      }
      next();
    };
  }
}

// Lua script: atomic token bucket in Redis
const RATE_LIMIT_SCRIPT = `
  local key = KEYS[1]
  local capacity = tonumber(ARGV[1])
  local refill_rate = tonumber(ARGV[2])
  local now = tonumber(ARGV[3])
  local requested = tonumber(ARGV[4])

  local bucket = redis.call('HMGET', key, 'tokens', 'last_refill')
  local tokens = tonumber(bucket[1]) or capacity
  local last_refill = tonumber(bucket[2]) or now

  -- Refill tokens based on elapsed time
  local elapsed = math.max(0, now - last_refill)
  tokens = math.min(capacity, tokens + elapsed * refill_rate)

  if tokens >= requested then
    tokens = tokens - requested
    redis.call('HMSET', key, 'tokens', tokens, 'last_refill', now)
    redis.call('EXPIRE', key, math.ceil(capacity / refill_rate) + 1)
    return {1, tokens, 0}
  else
    local retry_after = (requested - tokens) / refill_rate
    redis.call('HMSET', key, 'tokens', tokens, 'last_refill', now)
    redis.call('EXPIRE', key, math.ceil(capacity / refill_rate) + 1)
    return {0, tokens, retry_after}
  end
`;

module.exports = { RateLimiter };
```

---

## 17. Trade-offs & Alternatives

### Chunking: Fixed vs. Variable (Rabin)

| | Fixed (4 MB) | Variable (Rabin) |
|--|---|---|
| **Dedup rate** | Good (30%) | Better (45%+) for text |
| **Client CPU** | Low | Medium |
| **Implementation** | Simple | Complex |
| **Best for** | Binary blobs | Text/office docs |

→ **Decision:** Fixed for binary; Rabin for Docs/Sheets detected by MIME type.

### Metadata DB: Spanner vs. Cassandra

| | Spanner / CockroachDB | Cassandra |
|--|---|---|
| **Consistency** | External (strong) | Eventual / tunable |
| **Transactions** | Full ACID | Limited (LWT) |
| **Query flexibility** | Full SQL | CQL (limited joins) |
| **Cost** | Higher | Lower |
| **Best for** | Metadata (needs consistency) | Chunk ref counts (write-heavy) |

→ **Decision:** Spanner for `nodes`, `permissions`; Cassandra for `chunk_refs`, `change_log`.

### Sync: Polling vs. WebSocket vs. SSE

| | Polling | WebSocket | SSE |
|--|---|---|---|
| **Latency** | High | Low | Low |
| **Bidirectional** | Yes (req/res) | Yes | No (server→client only) |
| **Load balancer** | Easy | Sticky sessions needed | Easy |
| **Mobile battery** | Bad | Medium | Good |

→ **Decision:** WebSocket for desktop; SSE fallback for mobile; long-poll for corporate proxies that block WS.

### Conflict Resolution: LWW vs. OT vs. CRDT

| | LWW | OT | CRDT |
|--|---|---|---|
| **Complexity** | Low | High | Medium |
| **Consistency** | Weak | Strong | Strong |
| **Offline support** | Poor | Good | Excellent |
| **Use case** | Binary files | Real-time docs | Distributed editors |

→ **Decision:** LWW for files; OT for Docs; CRDT considered for next-gen offline-first Docs.

---

## 18. Interview Cheat Sheet

### Key Numbers to Memorize

```
Chunk size:         4 MB
Max file size:      5 TB
Storage per user:   15 GB (free)
Durability:         11 nines
Availability:       99.99%
CDN cache TTL:      1 year (immutable blobs)
Permission cache:   60 seconds
Metadata cache:     5 minutes
Upload session TTL: 24 hours
Trash retention:    30 days
Dedup savings:      ~30%
WS connections:     100K per server
Sync lag target:    < 30 seconds P95
```

### Key Algorithms

- **Deduplication:** SHA-256 content-addressed storage + ref counting
- **Delta sync:** Lamport clock change log + cursor-based pagination
- **Conflict resolution:** LWW (binary) + OT (collaborative docs)
- **Permission check:** Closure table traversal (single SQL query up tree)
- **Rate limiting:** Token bucket in Redis Lua script (atomic)
- **Chunking (text):** Rabin fingerprinting for content-defined boundaries

### Top 5 Things Interviewers Probe

1. **Resumable upload** — how do you handle network failures mid-upload?
   → Session in Redis; GET status returns missing chunks; idempotent PUT per chunk.

2. **Deduplication** — how do you avoid storing the same file twice?
   → CAS: hash before upload, check existence, skip blob write if present.

3. **Conflict resolution** — two devices edit same file offline, both sync.
   → LWW with conflicted copy; client sends base_version; server returns 409 on mismatch.

4. **Permission check performance** — file deep in folder hierarchy, inherited permissions.
   → Closure table precomputes all ancestor-descendant pairs; single query O(1).

5. **Sync at scale** — 100M users, 10 devices each — how does notification work?
   → Kafka per-user topics; consistent hash routes user to same WS server pool; Redis Streams buffer missed events.

### Common Follow-ups

- **How do you handle a 5 TB file upload?** → Multipart with parallel chunk workers, each chunk independently resumable, server assembles manifest at completion.
- **What if blob store goes down?** → Circuit breaker; queue uploads; retry with exponential backoff; multi-region replica for reads.
- **How do you enforce quota atomically?** → Optimistic: reserve at initiate, confirm at complete, rollback on abort. Use DB row lock on `user_quotas`.
- **How do search results stay secure?** → Per-request security trim filter in Elasticsearch query (never index-time only).
- **How do you scale the notification service?** → Consistent hash on `user_id` to route WS connections to same server cluster; use Redis Pub/Sub for cross-node fan-out.

---

*Authored for FAANG-level system design preparation. All capacity numbers are approximations for interview discussion purposes.*