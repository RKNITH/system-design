# Chat / Messaging System — System Design
> **Interview Level: FAANG / Staff Engineer**
> Covers: Requirements → Estimations → HLD → Deep Dives → LLD (JavaScript) → Trade-offs

---

## Table of Contents

1. [Problem Statement](#1-problem-statement)
2. [Functional Requirements](#2-functional-requirements)
3. [Non-Functional Requirements](#3-non-functional-requirements)
4. [Capacity Estimation](#4-capacity-estimation)
5. [High-Level Design (HLD)](#5-high-level-design-hld)
   - 5.1 [Core Architecture Overview](#51-core-architecture-overview)
   - 5.2 [Client ↔ Server Communication](#52-client--server-communication)
   - 5.3 [Service Decomposition](#53-service-decomposition)
   - 5.4 [Data Flow — Sending a Message](#54-data-flow--sending-a-message)
   - 5.5 [Group Messaging Flow](#55-group-messaging-flow)
   - 5.6 [Presence & Online Status](#56-presence--online-status)
   - 5.7 [Push Notifications (Offline Users)](#57-push-notifications-offline-users)
   - 5.8 [Media / File Sharing](#58-media--file-sharing)
   - 5.9 [Message Search](#59-message-search)
   - 5.10 [Read Receipts & Delivery Receipts](#510-read-receipts--delivery-receipts)
6. [Database Design](#6-database-design)
   - 6.1 [Storage Engine Choices](#61-storage-engine-choices)
   - 6.2 [Schema Design](#62-schema-design)
   - 6.3 [Sharding Strategy](#63-sharding-strategy)
   - 6.4 [Message ID Generation (Snowflake)](#64-message-id-generation-snowflake)
7. [Deep Dives](#7-deep-dives)
   - 7.1 [WebSocket Connection Management](#71-websocket-connection-management)
   - 7.2 [Message Ordering & Consistency](#72-message-ordering--consistency)
   - 7.3 [At-Least-Once vs Exactly-Once Delivery](#73-at-least-once-vs-exactly-once-delivery)
   - 7.4 [Fan-out Strategy for Group Chats](#74-fan-out-strategy-for-group-chats)
   - 7.5 [Caching Strategy](#75-caching-strategy)
   - 7.6 [Rate Limiting](#76-rate-limiting)
   - 7.7 [End-to-End Encryption (E2EE)](#77-end-to-end-encryption-e2ee)
   - 7.8 [CDN & Media Optimization](#78-cdn--media-optimization)
8. [Low-Level Design (LLD) — JavaScript](#8-low-level-design-lld--javascript)
   - 8.1 [Snowflake ID Generator](#81-snowflake-id-generator)
   - 8.2 [WebSocket Connection Manager](#82-websocket-connection-manager)
   - 8.3 [Message Service](#83-message-service)
   - 8.4 [Presence Service](#84-presence-service)
   - 8.5 [Fan-out Service (Group Messages)](#85-fan-out-service-group-messages)
   - 8.6 [Rate Limiter (Token Bucket)](#86-rate-limiter-token-bucket)
   - 8.7 [Message Queue Consumer](#87-message-queue-consumer)
   - 8.8 [Notification Service](#88-notification-service)
   - 8.9 [Read Receipt Tracker](#89-read-receipt-tracker)
   - 8.10 [Conversation Service](#810-conversation-service)
   - 8.11 [REST API Layer (Express)](#811-rest-api-layer-express)
   - 8.12 [Client-Side Message Sync](#812-client-side-message-sync)
9. [Failure Scenarios & Resilience](#9-failure-scenarios--resilience)
10. [Monitoring & Observability](#10-monitoring--observability)
11. [Trade-offs & Alternatives](#11-trade-offs--alternatives)
12. [Security Considerations](#12-security-considerations)
13. [Interview Cheat Sheet](#13-interview-cheat-sheet)

---

## 1. Problem Statement

Design a **WhatsApp / Slack-like chat system** that supports:
- 1-on-1 messaging
- Group messaging (up to 1000 members)
- Real-time message delivery
- Media sharing (images, videos, documents)
- Message history / offline sync
- Online presence indicators
- Read/delivery receipts
- Push notifications
- End-to-end encryption (optional layer)

Scale target: **500 million DAU**, sending **~100 billion messages/day** (WhatsApp scale).

---

## 2. Functional Requirements

### Must-Have (Core)
- **Send & Receive Messages** — text, emoji, reactions
- **1:1 Chat** — direct messaging between two users
- **Group Chat** — up to 1000 members per group
- **Delivery Status** — sent → delivered → read (double tick pattern)
- **Online Presence** — last seen, typing indicators
- **Message History** — persistent storage, loadable on new device
- **Push Notifications** — when user is offline
- **Media Sharing** — images, videos, audio, documents (up to 100 MB)

### Nice-to-Have (Extend in interview if time permits)
- Message reactions (emoji reactions per message)
- Message threads / replies
- Message editing and deletion
- Voice/Video calls (separate WebRTC system)
- Message forwarding
- Disappearing messages
- End-to-end encryption
- Message search
- Pinned messages, starred messages
- User mentions (@username)

### Out of Scope
- WebRTC-based audio/video calls (different system design)
- Payment in chat
- Story/Status features

---

## 3. Non-Functional Requirements

| Property | Target |
|---|---|
| **Latency** | < 100ms message delivery (P99) for online users |
| **Availability** | 99.99% uptime (≤ 52 min downtime/year) |
| **Consistency** | Eventual consistency acceptable; causal ordering required |
| **Durability** | Messages must never be lost after server ACK |
| **Scalability** | Horizontal scaling; handle 500M DAU |
| **Security** | TLS in transit; optional E2EE; no plaintext passwords |
| **Fault Tolerance** | Survive datacenter-level failures |

---

## 4. Capacity Estimation

### Users & Messages
```
DAU                       = 500 million
Avg messages/user/day     = 40
Total messages/day        = 500M × 40 = 20 billion
Messages/second (peak 3×) = (20B / 86400) × 3 ≈ 700,000 msg/sec
```

### Storage
```
Avg message size (text)   = 200 bytes
Media messages            = 10% of all messages, avg 500 KB each
Text storage/day          = 20B × 0.9 × 200B   ≈ 3.6 TB/day
Media storage/day         = 20B × 0.1 × 500KB  ≈ 1000 TB/day
Metadata/day              = 20B × 50B           ≈ 1 TB/day

Total storage/year        ≈ 1000 TB × 365       ≈ 365 PB/year
(Media on object storage like S3 — compressed + deduped, ~100 PB practical)
```

### Bandwidth
```
Ingress (upload)    ≈ 700,000 msg/sec × 200B = 140 MB/s
Egress (delivery)   ≈ 140 MB/s × avg 2 recipients = 280 MB/s
Peak egress (media) ≈ ~10 GB/s (bursty, handled by CDN)
```

### WebSocket Connections
```
DAU concurrent (30%)  = 150 million WebSocket connections
Connections/server    = ~65,000 (Node.js with libuv, tested ~100K)
Chat servers needed   = 150M / 65K ≈ 2,400 servers
(Add 2× headroom)     ≈ 5,000 WebSocket servers
```

---

## 5. High-Level Design (HLD)

### 5.1 Core Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                           CLIENTS                                    │
│          Mobile (iOS/Android)    Web Browser    Desktop App          │
└───────────────────────┬─────────────────────────────────────────────┘
                        │  HTTPS / WSS (TLS 1.3)
                        ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        API GATEWAY / LOAD BALANCER                   │
│              (Layer 7 — sticky sessions for WebSockets)              │
│                    nginx / AWS ALB / Cloudflare                      │
└──────┬──────────────────────────────────────────────────────────────┘
       │
       ├─────────────────────────────────────────────────┐
       ▼                                                 ▼
┌──────────────┐                               ┌──────────────────────┐
│  REST API    │                               │  WebSocket Servers   │
│  Servers     │                               │  (Chat Servers)      │
│  (Stateless) │                               │  (Stateful — conn)   │
└──────┬───────┘                               └──────────┬───────────┘
       │                                                  │
       └──────────────────┬───────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     MESSAGE BROKER (Kafka)                            │
│         Topics: messages, notifications, presence, receipts          │
└──────┬──────────────────────────────────────────────────────────────┘
       │
       ├────────────────┬──────────────────┬────────────────────┐
       ▼                ▼                  ▼                    ▼
┌────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐
│  Message   │  │  Fan-out     │  │  Presence    │  │  Notification    │
│  Storage   │  │  Service     │  │  Service     │  │  Service         │
│  Service   │  │  (Group)     │  │              │  │  (APNs/FCM)      │
└─────┬──────┘  └──────┬───────┘  └──────┬───────┘  └──────────────────┘
      │                │                  │
      ▼                ▼                  ▼
┌──────────┐  ┌──────────────┐  ┌──────────────────┐
│ Cassandra│  │   Redis      │  │     Redis        │
│ (messages│  │   (Pub/Sub   │  │  (Presence TTL)  │
│  history)│  │    routing)  │  │                  │
└──────────┘  └──────────────┘  └──────────────────┘
      │
      ▼
┌──────────────────────────────────┐
│     Object Storage (S3)          │
│     Media Files + CDN            │
└──────────────────────────────────┘
```

### 5.2 Client ↔ Server Communication

#### Protocol Choice Matrix

| Protocol | Use Case | Why |
|---|---|---|
| **WebSocket** | Real-time message delivery, typing indicators, presence | Full-duplex, low overhead |
| **HTTPS REST** | Login, fetch history, upload media, settings | Stateless, cacheable |
| **HTTP/2 SSE** | Fallback for WebSocket (corporate firewalls) | Server push, unidirectional |
| **QUIC/HTTP3** | Future mobile optimization | Handles packet loss better |

#### WebSocket Handshake Flow
```
Client                    Load Balancer           Chat Server
  │                            │                       │
  │──── HTTP Upgrade ─────────►│                       │
  │                            │──── Route (sticky) ──►│
  │◄─── 101 Switching ─────────│◄──── ACK ─────────────│
  │                            │                       │
  │══ Authenticated WSS Conn ══════════════════════════│
  │                            │                       │
  │──── {type: "auth", token} ────────────────────────►│
  │◄─── {type: "auth_ack", userId} ────────────────────│
  │                            │                       │
```

#### Message Envelope Format (Wire Protocol)
```json
{
  "type": "message",
  "id": "1749823456789012345",
  "conversationId": "conv_abc123",
  "senderId": "user_111",
  "recipientId": "user_222",
  "content": {
    "type": "text",
    "body": "Hello!"
  },
  "clientTimestamp": 1749823456789,
  "serverTimestamp": 1749823456801,
  "status": "sent"
}
```

### 5.3 Service Decomposition

```
┌─────────────────────────────────────────────────────────────┐
│                     MICROSERVICES                            │
├──────────────────────┬──────────────────────────────────────┤
│ Service              │ Responsibility                        │
├──────────────────────┼──────────────────────────────────────┤
│ Auth Service         │ JWT issuance, token refresh, OAuth   │
│ User Service         │ Profile, contacts, block list        │
│ Chat Server          │ WebSocket lifecycle, message routing │
│ Message Service      │ Persist, retrieve, paginate messages │
│ Conversation Service │ Create/list conversations, members   │
│ Presence Service     │ Online status, last seen, typing     │
│ Fan-out Service      │ Group message delivery               │
│ Notification Service │ Push (APNs, FCM), email alerts       │
│ Media Service        │ Upload, transcode, CDN URL signing   │
│ Search Service       │ Message indexing (Elasticsearch)     │
│ Receipt Service      │ Delivered/read receipt processing    │
└──────────────────────┴──────────────────────────────────────┘
```

### 5.4 Data Flow — Sending a Message

```
Step 1: Alice sends message via WebSocket to Chat Server A
Step 2: Chat Server A publishes to Kafka topic "messages"
Step 3: Chat Server A sends ACK back to Alice (status: "sent" ✓)
Step 4: Message Service consumer writes to Cassandra (durable)
Step 5: Fan-out Service routes to Bob's Chat Server (via Redis routing table)
Step 6: Bob's Chat Server pushes message to Bob over WebSocket
Step 7: Bob's client sends delivery receipt → status: "delivered" ✓✓
Step 8: Bob reads message → status: "read" ✓✓ (blue ticks)

Parallel: If Bob is OFFLINE:
Step 6b: Notification Service sends APNs/FCM push notification
Step 7b: On Bob's reconnect, Message Service serves unread messages (gap fill)
```

### 5.5 Group Messaging Flow

```
Alice sends to Group (1000 members)
         │
         ▼
   Chat Server A
   (validates, ACKs)
         │
         ▼
    Kafka: "messages"
         │
    Fan-out Service
    (reads group membership)
         │
    ┌────┼────┐
    │    │    │  ... (999 deliveries)
    ▼    ▼    ▼
  Bob  Carol Dave
  (online)   (offline → push)
```

**Fan-out strategies:**

| Strategy | When to Use |
|---|---|
| Fan-out on Write | Small groups (< 200 members) — push to each member queue |
| Fan-out on Read | Large groups / inactive members — store once, pull on connect |
| Hybrid | Groups < 200: write; ≥ 200: read. (Slack/WhatsApp approach) |

### 5.6 Presence & Online Status

```
User connects WebSocket
       │
       ▼
 Chat Server → Presence Service
       │           │
       │    SET user:{id}:presence "online" EX 30 (Redis TTL)
       │           │
       │    Heartbeat every 15s → refresh TTL
       │
User disconnects / tab closes
       │
       ▼
 Chat Server → Presence Service
              DEL user:{id}:presence
              SET user:{id}:lastSeen {timestamp}

Friends query presence:
  MGET user:111:presence user:222:presence ...
  → ["online", null, "online", ...]
```

**Typing Indicators** (ephemeral, not persisted):
- Client sends `{type: "typing_start", conversationId}` every 3s while typing
- Server relays to conversation members via WebSocket
- Auto-expires after 5s if no refresh

### 5.7 Push Notifications (Offline Users)

```
Notification Service receives event from Kafka
       │
       ├── Look up user device tokens (User Service / DB)
       │
       ├── iOS devices ──► Apple Push Notification Service (APNs)
       │
       └── Android devices ──► Firebase Cloud Messaging (FCM)

Payload:
{
  "title": "Alice",
  "body": "Hey, are you free tonight?",
  "badge": 3,                    // unread count
  "conversationId": "conv_abc",
  "senderId": "user_111",
  "sound": "default"
}

E2EE mode: payload is empty — "You have a new message"
(Content only decryptable on device)
```

### 5.8 Media / File Sharing

```
Upload Flow:
1. Client requests pre-signed S3 URL (from Media Service REST API)
2. Client uploads file directly to S3 (bypasses app servers)
3. Client sends message with {type: "media", mediaId: "s3_key", thumbnail: "..."}
4. Media Service triggers async transcoding (Lambda/worker)
5. CDN distributes media globally

Download Flow:
1. Client requests media URL → Media Service returns signed CDN URL (TTL 24h)
2. Client fetches from nearest CDN edge node

Optimizations:
- Deduplication: hash(file) → if exists, reuse S3 key (saves 30-40% storage)
- Progressive JPEG / WebP transcoding for images
- HLS streaming for long videos
- Thumbnail generation on upload
```

### 5.9 Message Search

```
Architecture:
  Cassandra (source of truth) → Kafka → Elasticsearch indexer

Elasticsearch Index per user (privacy isolation):
{
  "userId": "user_111",
  "messageId": "snowflake_id",
  "conversationId": "conv_abc",
  "body": "meeting tomorrow at 3pm",
  "timestamp": 1749823456789,
  "mediaType": null
}

Search API:
GET /search?q=meeting&conversationId=conv_abc&limit=20

Returns:
{ results: [{messageId, snippet, conversationId, timestamp}] }

Tradeoffs:
- E2EE conversations: search only possible client-side (cannot index server-side)
- Non-E2EE: full-text server-side search with Elasticsearch
```

### 5.10 Read Receipts & Delivery Receipts

```
Message Status State Machine:

  PENDING → SENT → DELIVERED → READ

PENDING:   Client assigned ID, not yet ACKed by server
SENT:      Server ACK received (stored in Cassandra)
DELIVERED: Recipient's device received over WebSocket
READ:      Recipient opened conversation / visible in viewport

Implementation:
- Receipts flow UPSTREAM: Bob → server → Alice's WebSocket
- Stored in Redis (fast) + eventually in Cassandra
- Group chats: "Delivered to 450/1000 members" (aggregated)
- Read by: list of userIds who have read (shown on long press)
```

---

## 6. Database Design

### 6.1 Storage Engine Choices

| Data | Store | Reason |
|---|---|---|
| Messages | **Apache Cassandra** | Write-heavy, time-series, horizontal scale, no joins needed |
| User profiles | **PostgreSQL** | ACID, relational, complex queries |
| Conversations metadata | **PostgreSQL** | Relational, low volume |
| Group membership | **PostgreSQL** | Transactional updates |
| Presence / sessions | **Redis** | TTL, in-memory speed, pub/sub |
| WebSocket routing | **Redis** | Map userId → serverId |
| Message queue | **Apache Kafka** | Durable, ordered, replay |
| Media files | **Amazon S3** | Object store, cheap, CDN-friendly |
| Search index | **Elasticsearch** | Full-text search |
| Rate limiting | **Redis** | Atomic counters, sliding window |

### 6.2 Schema Design

#### PostgreSQL — Users Table
```sql
CREATE TABLE users (
    user_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    username        VARCHAR(50) UNIQUE NOT NULL,
    phone_number    VARCHAR(20) UNIQUE,
    email           VARCHAR(255) UNIQUE,
    display_name    VARCHAR(100),
    avatar_url      TEXT,
    public_key      TEXT,              -- E2EE: user's public key
    status_message  VARCHAR(200),
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    last_seen       TIMESTAMPTZ,
    is_deleted      BOOLEAN DEFAULT FALSE
);

CREATE INDEX idx_users_phone ON users(phone_number);
CREATE INDEX idx_users_username ON users(username);
```

#### PostgreSQL — Conversations Table
```sql
CREATE TABLE conversations (
    conversation_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type            VARCHAR(10) NOT NULL CHECK (type IN ('direct', 'group')),
    name            VARCHAR(200),          -- NULL for direct chats
    avatar_url      TEXT,
    created_by      UUID REFERENCES users(user_id),
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    last_message_id BIGINT,               -- Snowflake ID
    last_message_at TIMESTAMPTZ,
    is_e2ee         BOOLEAN DEFAULT FALSE
);

CREATE INDEX idx_conv_last_msg ON conversations(last_message_at DESC);
```

#### PostgreSQL — Conversation Members Table
```sql
CREATE TABLE conversation_members (
    conversation_id UUID REFERENCES conversations(conversation_id),
    user_id         UUID REFERENCES users(user_id),
    role            VARCHAR(20) DEFAULT 'member' CHECK (role IN ('owner','admin','member')),
    joined_at       TIMESTAMPTZ DEFAULT NOW(),
    last_read_msg_id BIGINT,             -- Snowflake ID — for unread counts
    is_muted        BOOLEAN DEFAULT FALSE,
    is_archived     BOOLEAN DEFAULT FALSE,
    left_at         TIMESTAMPTZ,         -- NULL = still in group
    PRIMARY KEY (conversation_id, user_id)
);

CREATE INDEX idx_members_user ON conversation_members(user_id, last_message_at DESC);
```

#### Cassandra — Messages Table
```sql
CREATE TABLE messages (
    conversation_id  UUID,
    message_id       BIGINT,             -- Snowflake ID (time-sortable)
    sender_id        UUID,
    message_type     TEXT,               -- 'text', 'image', 'video', 'audio', 'file', 'system'
    content          TEXT,               -- JSON blob or encrypted blob
    media_url        TEXT,
    media_mime_type  TEXT,
    thumbnail_url    TEXT,
    reply_to_msg_id  BIGINT,
    is_edited        BOOLEAN DEFAULT FALSE,
    is_deleted       BOOLEAN DEFAULT FALSE,
    client_msg_id    TEXT,              -- Idempotency key from client
    server_timestamp TIMESTAMP,
    PRIMARY KEY ((conversation_id), message_id)
) WITH CLUSTERING ORDER BY (message_id DESC)
  AND compaction = {'class': 'TimeWindowCompactionStrategy',
                    'compaction_window_unit': 'DAYS',
                    'compaction_window_size': 7}
  AND gc_grace_seconds = 86400;

-- For sender's sent messages (secondary access pattern)
CREATE TABLE messages_by_sender (
    sender_id       UUID,
    message_id      BIGINT,
    conversation_id UUID,
    PRIMARY KEY ((sender_id), message_id)
) WITH CLUSTERING ORDER BY (message_id DESC);
```

#### Cassandra — Message Receipts Table
```sql
CREATE TABLE message_receipts (
    conversation_id UUID,
    message_id      BIGINT,
    user_id         UUID,
    status          TEXT,               -- 'delivered', 'read'
    timestamp       TIMESTAMP,
    PRIMARY KEY ((conversation_id, message_id), user_id)
);
```

#### Redis — Key Patterns
```
# WebSocket routing: which server holds user's connection
user:{userId}:server        → "chat-server-42"       TTL: session lifetime

# Presence
user:{userId}:presence      → "online"               TTL: 30s (heartbeat refreshes)
user:{userId}:lastSeen      → "1749823456789"        Persistent

# Typing indicator
typing:{conversationId}:{userId}  → "1"              TTL: 5s

# Unread count per user per conversation
unread:{userId}:{conversationId}  → "12"             Counter

# Rate limiting (token bucket)
ratelimit:{userId}:msg      → "{tokens, lastRefill}" TTL: 60s

# Session
session:{sessionToken}      → {userId, deviceId}     TTL: 30 days

# Group membership cache (avoid DB hits on every message)
group:{conversationId}:members → [userId1, userId2, ...]  TTL: 5 min
```

### 6.3 Sharding Strategy

#### Cassandra Sharding (Partition Key Design)
```
Messages partitioned by conversation_id

Hotspot problem: Very active group chats = hot partition
Solution: Use conversation_id as partition key (Cassandra distributes via consistent hashing)

Each partition holds all messages for one conversation.
Time-range queries within partition are O(log n) via clustering key.

Large groups (>10K messages/day per conversation):
  Consider: conversation_id + bucket (week number)
  Partition key: (conversation_id, week_bucket)
```

#### User Sharding (PostgreSQL with Citus or Vitess)
```
Shard key: user_id (UUID → consistent hash)
Conversations partitioned by conversation_id
Cross-shard joins avoided by denormalization
```

### 6.4 Message ID Generation (Snowflake)

```
64-bit Snowflake ID Layout:
┌────────────────────────────────────────────────────────────┐
│  1 bit  │  41 bits       │  10 bits    │  12 bits          │
│  unused │  timestamp ms  │  machine ID │  sequence number  │
│  (sign) │  (epoch-based) │             │  (per machine)    │
└────────────────────────────────────────────────────────────┘

Properties:
- Time-sortable (ascending order = message order)
- Globally unique across all servers
- No coordination needed (machine ID pre-assigned)
- Max throughput: 4096 IDs/ms per machine
- Epoch: custom (e.g., 2020-01-01) → valid until 2089

Why not UUID?
- UUID v4 is random → no ordering → bad for Cassandra clustering
- UUID is 128-bit → doubles storage vs 64-bit Snowflake
```

---

## 7. Deep Dives

### 7.1 WebSocket Connection Management

```
Problem: 150M concurrent connections → need efficient multiplexing

Solution Stack:
1. Node.js (libuv event loop) — handles 50-100K connections per process
2. Cluster mode — 1 process per CPU core (typically 32-64 cores/machine)
3. Horizontal scaling — 5000 chat servers

Connection lifecycle:
  1. Client connects → TLS handshake → HTTP Upgrade → WSS
  2. Auth: client sends JWT in first message (not in URL for security)
  3. Server validates JWT → registers userId → serverId in Redis
  4. Heartbeat: client pings every 25s, server pongs, drops conn after 60s no ping
  5. Reconnect: exponential backoff (1s, 2s, 4s, 8s... max 60s)
  6. Gap fill: on reconnect, client sends last known messageId
              → server sends all messages since that ID

Connection Draining (for deploys):
  1. Load balancer stops routing new conns to this server
  2. Server sends "drain" signal to all clients
  3. Clients reconnect to other servers
  4. Server shuts down after 30s drain window
```

### 7.2 Message Ordering & Consistency

```
Problem: Alice sends M1, M2. Server receives M2 before M1 (network reorder).

Solution: Client-side sequence numbers + server-side Snowflake IDs

Client assigns: clientMsgId (UUID) + clientSeq (monotonic per conversation)
Server assigns: Snowflake messageId (globally ordered by time)

Display ordering: by Snowflake messageId (server time)
Conflict resolution: if two messages have same ms timestamp → 
  order by (senderId + sequence) for deterministic ordering

Causal ordering for replies:
  reply.replyToMsgId must be visible before showing the reply
  If parent not yet received → queue reply until parent arrives
```

### 7.3 At-Least-Once vs Exactly-Once Delivery

```
Strategy: At-least-once delivery + idempotent deduplication

Client → Server:
  Client sends: {clientMsgId: "uuid-123", ...}
  Server checks: EXISTS messages WHERE client_msg_id = 'uuid-123' AND sender_id = userId
  If exists: return existing messageId (duplicate suppressed)
  If not: insert and return new messageId

Server → Client (WebSocket push):
  Server sends message with Snowflake messageId
  Client ACKs: {type: "ack", messageId: "..."}
  Server stores: SET msg:{messageId}:delivered:{userId} = 1 EX 86400
  On reconnect: server re-sends un-ACKed messages from last 24h

Kafka:
  Consumer group offset commit AFTER successful DB write + delivery
  Enables at-least-once consumption with idempotent writes
```

### 7.4 Fan-out Strategy for Group Chats

```
Small group (< 200 members) — Fan-out on Write:
  1. Fan-out Service reads member list from Redis cache
  2. For each online member: look up their chat server in Redis
     → publish to that server's Redis channel
  3. Each chat server delivers to connected members
  4. For offline members: enqueue push notification

Large group (≥ 200 members) — Fan-out on Read:
  1. Store message once in Cassandra
  2. Online members: notify "new message in conv_abc" → clients fetch
  3. Members query: GET /conversations/{id}/messages?since={lastId}
  4. Offline: single push notification per user regardless of message count

Hybrid Threshold: WhatsApp ~200, Slack ~100 (workspace-level fan-out)

Further Optimization:
  - Batch notifications: if user gets 50 messages while offline → 1 push
  - Priority queue: messages from contacts vs strangers
  - Read horizons: only fan-out to members who joined before the message
```

### 7.5 Caching Strategy

```
Multi-layer caching:

L1 — In-process cache (LRU, per chat server):
  - Active conversation metadata (TTL: 5 min)
  - Group membership for hot groups (TTL: 1 min)
  Cache size: ~1 GB per server process

L2 — Redis (shared across all servers):
  - User presence (TTL: 30s)
  - WebSocket routing table (TTL: session lifetime)
  - Recent messages cache: last 50 messages per active conversation (TTL: 10 min)
  - Unread counts per user (persistent until cleared)
  - Rate limiting counters (TTL: window size)

L3 — CDN (Cloudfront / Fastly):
  - Media files (images, videos) — long TTL (1 year with content-hash URL)
  - Profile avatars (TTL: 24h)

Cache Invalidation:
  - Message delivered → increment unread counter (Redis INCR, atomic)
  - User reads conversation → DEL unread:{userId}:{convId}
  - Group member added → DEL group:{convId}:members
  - Write-through for user profile (write to PostgreSQL + update Redis)

Cache Eviction: LRU + TTL combo. Redis maxmemory-policy: allkeys-lru
```

### 7.6 Rate Limiting

```
Layers of rate limiting:

1. Connection rate: Max 5 WebSocket upgrades per IP per second
2. Message rate per user: 60 messages/minute (Token Bucket)
3. Media upload: 10 uploads/minute per user
4. Group creation: 5 groups/day per user
5. API endpoints: per-user token bucket per endpoint

Token Bucket Algorithm (Redis atomic):
  - Bucket capacity: 60 tokens
  - Refill rate: 1 token/second
  - Each message costs 1 token
  - Implemented via Redis Lua script (atomic check + update)

Rate limit response:
  {
    "error": "RATE_LIMITED",
    "retryAfter": 15,          // seconds
    "limit": 60,
    "remaining": 0,
    "resetAt": 1749823470000
  }
```

### 7.7 End-to-End Encryption (E2EE)

```
Protocol: Signal Protocol (used by WhatsApp, Signal)
  - Double Ratchet Algorithm
  - Extended Triple Diffie-Hellman (X3DH) key exchange
  - Forward secrecy: new keys per message
  - Break-in recovery: keys rotate

Key Exchange Flow:
  1. Bob registers → uploads public keys (identity key, signed prekey, one-time prekeys)
  2. Alice fetches Bob's public keys from Key Service
  3. Alice computes shared secret via X3DH
  4. First message includes Alice's ephemeral public key
  5. Both derive symmetric keys → AES-256-GCM encryption
  6. Server stores only encrypted ciphertext — cannot read content

Group E2EE:
  - Sender Key: Alice creates group session key, distributes to each member
  - Each member can decrypt using distributed sender key
  - Key rotation when member leaves group

Tradeoffs:
  - Server-side search impossible (client-side search only)
  - Message backup must be client-side encrypted
  - Key management complexity (device linking, key backup)
  - Can't scan for CSAM/spam server-side (policy concern)
```

### 7.8 CDN & Media Optimization

```
Upload flow (client → S3 → CDN):
  1. Client: POST /media/upload-url → {uploadUrl, mediaId}
  2. Client: PUT {uploadUrl} with file bytes (direct to S3, no app server)
  3. S3 trigger → Lambda → generate thumbnail, transcode video
  4. Message references mediaId (not URL directly)
  5. Client: GET /media/{mediaId} → signed CDN URL (expires 24h)

Content deduplication:
  SHA-256 hash of file → check if already stored → reuse S3 key
  Saves ~30% storage (forwarded photos/memes)

Adaptive media quality:
  - Images: serve WebP (60% smaller than JPEG) with JPEG fallback
  - Videos: HLS with multiple bitrates (360p, 720p, 1080p)
  - Client requests quality based on network conditions

CDN strategy:
  - Static media: TTL = 1 year (immutable URLs with content hash)
  - Push to CDN: common memes/GIFs pre-positioned at edge
  - Pull CDN: origin → S3, edge caches on first request
```

---

## 8. Low-Level Design (LLD) — JavaScript

### 8.1 Snowflake ID Generator

```javascript
// snowflake.js
// Generates 64-bit time-sortable unique IDs (as BigInt)

const EPOCH = 1577836800000n; // 2020-01-01T00:00:00Z custom epoch

const MACHINE_ID_BITS = 10n;
const SEQUENCE_BITS = 12n;
const MAX_SEQUENCE = (1n << SEQUENCE_BITS) - 1n;       // 4095
const MAX_MACHINE_ID = (1n << MACHINE_ID_BITS) - 1n;   // 1023

class SnowflakeGenerator {
  #machineId;
  #sequence = 0n;
  #lastTimestamp = -1n;

  constructor(machineId) {
    if (machineId < 0 || machineId > Number(MAX_MACHINE_ID)) {
      throw new Error(`Machine ID must be 0-${MAX_MACHINE_ID}`);
    }
    this.#machineId = BigInt(machineId);
  }

  generate() {
    let now = BigInt(Date.now()) - EPOCH;

    if (now < this.#lastTimestamp) {
      // Clock moved backwards — wait it out
      const wait = Number(this.#lastTimestamp - now);
      throw new Error(`Clock moved backwards by ${wait}ms. Refusing to generate.`);
    }

    if (now === this.#lastTimestamp) {
      this.#sequence = (this.#sequence + 1n) & MAX_SEQUENCE;
      if (this.#sequence === 0n) {
        // Sequence overflow — spin until next millisecond
        while (BigInt(Date.now()) - EPOCH <= this.#lastTimestamp) {}
        now = BigInt(Date.now()) - EPOCH;
      }
    } else {
      this.#sequence = 0n;
    }

    this.#lastTimestamp = now;

    return (
      (now << (MACHINE_ID_BITS + SEQUENCE_BITS)) |
      (this.#machineId << SEQUENCE_BITS) |
      this.#sequence
    ).toString(); // Return as string to avoid JS precision loss
  }

  // Extract timestamp from a Snowflake ID
  static extractTimestamp(id) {
    const snowflake = BigInt(id);
    const ts = snowflake >> (MACHINE_ID_BITS + SEQUENCE_BITS);
    return new Date(Number(ts + EPOCH));
  }
}

module.exports = SnowflakeGenerator;

// Usage:
// const gen = new SnowflakeGenerator(machineId); // machineId from env/config
// const id = gen.generate(); // "1749823456789012345"
```

### 8.2 WebSocket Connection Manager

```javascript
// connectionManager.js
const WebSocket = require('ws');
const Redis = require('ioredis');
const jwt = require('jsonwebtoken');
const { EventEmitter } = require('events');

const redis = new Redis(process.env.REDIS_URL);
const redisPub = new Redis(process.env.REDIS_URL);
const redisSub = new Redis(process.env.REDIS_URL);

const SERVER_ID = process.env.SERVER_ID; // e.g. "chat-server-42"
const HEARTBEAT_INTERVAL = 25_000;        // 25 seconds
const CONNECTION_TIMEOUT = 60_000;        // 60 seconds
const PRESENCE_TTL = 30;                  // seconds

class ConnectionManager extends EventEmitter {
  // Map<userId, Set<WebSocket>> — one user may have multiple devices
  #connections = new Map();

  // Map<WebSocket, { userId, deviceId, pingTimeout, heartbeatInterval }>
  #socketMeta = new WeakMap();

  constructor() {
    super();
    // Subscribe to inbound messages addressed to this server
    redisSub.subscribe(`server:${SERVER_ID}:inbound`, (err) => {
      if (err) throw err;
    });

    redisSub.on('message', (channel, message) => {
      this.#handleInboundFromRedis(JSON.parse(message));
    });
  }

  // Called when a new WebSocket connection is established
  async handleConnection(ws, req) {
    // Start unauthenticated — wait for auth message
    const authTimeout = setTimeout(() => ws.close(4001, 'Auth timeout'), 10_000);

    ws.once('message', async (rawData) => {
      clearTimeout(authTimeout);

      let payload;
      try {
        payload = JSON.parse(rawData);
      } catch {
        return ws.close(4002, 'Invalid JSON');
      }

      if (payload.type !== 'auth') {
        return ws.close(4003, 'Expected auth message');
      }

      let decoded;
      try {
        decoded = jwt.verify(payload.token, process.env.JWT_SECRET);
      } catch {
        return ws.close(4004, 'Invalid token');
      }

      const { userId, deviceId } = decoded;
      await this.#registerConnection(ws, userId, deviceId);
    });
  }

  async #registerConnection(ws, userId, deviceId) {
    // Track connection in memory
    if (!this.#connections.has(userId)) {
      this.#connections.set(userId, new Set());
    }
    this.#connections.get(userId).add(ws);

    // Store in Redis: userId → serverId (for cross-server routing)
    await redis.setex(`user:${userId}:server`, 3600, SERVER_ID);

    // Set online presence with TTL
    await redis.setex(`user:${userId}:presence`, PRESENCE_TTL, 'online');

    const heartbeatInterval = setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.ping();
        redis.setex(`user:${userId}:presence`, PRESENCE_TTL, 'online');
      }
    }, HEARTBEAT_INTERVAL);

    const pingTimeout = setTimeout(() => {
      ws.terminate(); // Force close if pong not received
    }, CONNECTION_TIMEOUT);

    ws.on('pong', () => {
      clearTimeout(pingTimeout);
    });

    this.#socketMeta.set(ws, { userId, deviceId, heartbeatInterval, pingTimeout });

    // ACK authentication
    this.#send(ws, { type: 'auth_ack', userId, serverId: SERVER_ID });

    // Deliver any queued messages (gap fill)
    this.emit('user_connected', { userId, deviceId });

    // Listen for messages from this client
    ws.on('message', (data) => this.#handleClientMessage(ws, data));
    ws.on('close', () => this.#handleDisconnect(ws));
    ws.on('error', (err) => {
      console.error('WebSocket error', err);
      this.#handleDisconnect(ws);
    });
  }

  async #handleDisconnect(ws) {
    const meta = this.#socketMeta.get(ws);
    if (!meta) return;

    const { userId, deviceId, heartbeatInterval, pingTimeout } = meta;

    clearInterval(heartbeatInterval);
    clearTimeout(pingTimeout);

    const userConns = this.#connections.get(userId);
    if (userConns) {
      userConns.delete(ws);
      if (userConns.size === 0) {
        this.#connections.delete(userId);
        // Mark offline only when ALL devices disconnect
        await redis.del(`user:${userId}:presence`);
        await redis.set(`user:${userId}:lastSeen`, Date.now());
        await redis.del(`user:${userId}:server`);
        this.emit('user_disconnected', { userId });
      }
    }
  }

  async #handleClientMessage(ws, rawData) {
    const meta = this.#socketMeta.get(ws);
    if (!meta) return;

    let message;
    try {
      message = JSON.parse(rawData);
    } catch {
      return this.#send(ws, { type: 'error', code: 'INVALID_JSON' });
    }

    this.emit('client_message', { ...message, _senderId: meta.userId });
  }

  // Deliver a message to a user connected to THIS server
  deliverToUser(userId, message) {
    const conns = this.#connections.get(userId);
    if (!conns || conns.size === 0) return false;

    const payload = JSON.stringify(message);
    for (const ws of conns) {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(payload);
      }
    }
    return true;
  }

  // Route message to correct server via Redis pub/sub
  async routeToUser(userId, message) {
    // Check if user is on THIS server
    if (this.deliverToUser(userId, message)) return 'delivered_local';

    // Check which server holds the user's connection
    const targetServer = await redis.get(`user:${userId}:server`);
    if (!targetServer) return 'user_offline';

    // Publish to target server's inbound channel
    await redisPub.publish(
      `server:${targetServer}:inbound`,
      JSON.stringify({ targetUserId: userId, message })
    );
    return 'routed';
  }

  #handleInboundFromRedis({ targetUserId, message }) {
    this.deliverToUser(targetUserId, message);
  }

  #send(ws, data) {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(data));
    }
  }

  isUserOnline(userId) {
    return this.#connections.has(userId) && this.#connections.get(userId).size > 0;
  }

  getOnlineCount() {
    return this.#connections.size;
  }
}

module.exports = new ConnectionManager(); // Singleton
```

### 8.3 Message Service

```javascript
// messageService.js
const cassandra = require('./db/cassandra');
const redis = require('./db/redis');
const kafka = require('./kafka/producer');
const SnowflakeGenerator = require('./snowflake');

const snowflake = new SnowflakeGenerator(parseInt(process.env.MACHINE_ID));

class MessageService {
  // Persist a message and publish to Kafka for fan-out
  async sendMessage({ conversationId, senderId, content, clientMsgId, replyToMsgId }) {
    // 1. Idempotency check — avoid duplicate messages
    const dedupeKey = `dedup:${senderId}:${clientMsgId}`;
    const existingId = await redis.get(dedupeKey);
    if (existingId) {
      // Already processed — return existing message
      return this.getMessage(conversationId, existingId);
    }

    // 2. Generate Snowflake message ID
    const messageId = snowflake.generate();

    // 3. Persist to Cassandra
    const serverTimestamp = new Date();
    await cassandra.execute(
      `INSERT INTO messages
         (conversation_id, message_id, sender_id, message_type, content,
          reply_to_msg_id, client_msg_id, server_timestamp, is_deleted, is_edited)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, false, false)`,
      [conversationId, messageId, senderId, content.type,
       JSON.stringify(content), replyToMsgId || null, clientMsgId, serverTimestamp],
      { prepare: true }
    );

    // 4. Store idempotency key (expire after 24h)
    await redis.setex(dedupeKey, 86400, messageId);

    // 5. Update conversation last message (PostgreSQL)
    await this.#updateConversationLastMessage(conversationId, messageId, serverTimestamp);

    // 6. Publish to Kafka for async fan-out
    await kafka.send({
      topic: 'messages',
      messages: [{
        key: conversationId,           // ensures ordered delivery per conversation
        value: JSON.stringify({
          messageId,
          conversationId,
          senderId,
          content,
          replyToMsgId,
          serverTimestamp: serverTimestamp.toISOString(),
        }),
      }],
    });

    return { messageId, serverTimestamp, status: 'sent' };
  }

  // Fetch paginated message history (cursor-based, not offset)
  async getMessages({ conversationId, beforeMessageId, limit = 50 }) {
    const params = [conversationId, limit];
    let query;

    if (beforeMessageId) {
      query = `SELECT * FROM messages
               WHERE conversation_id = ?
               AND message_id < ?
               AND is_deleted = false
               ORDER BY message_id DESC
               LIMIT ?`;
      params.splice(1, 0, beforeMessageId);
    } else {
      query = `SELECT * FROM messages
               WHERE conversation_id = ?
               AND is_deleted = false
               ORDER BY message_id DESC
               LIMIT ?`;
    }

    const result = await cassandra.execute(query, params, { prepare: true });

    return result.rows.map(row => ({
      messageId: row.message_id.toString(),
      conversationId: row.conversation_id,
      senderId: row.sender_id,
      content: JSON.parse(row.content),
      replyToMsgId: row.reply_to_msg_id?.toString() ?? null,
      serverTimestamp: row.server_timestamp,
      isEdited: row.is_edited,
      isDeleted: row.is_deleted,
    }));
  }

  // Soft delete a message
  async deleteMessage({ messageId, conversationId, requesterId }) {
    // Verify ownership
    const [msg] = await this.getMessages({ conversationId, limit: 1 });
    if (!msg || msg.senderId !== requesterId) {
      throw new Error('UNAUTHORIZED');
    }

    await cassandra.execute(
      `UPDATE messages SET is_deleted = true
       WHERE conversation_id = ? AND message_id = ?`,
      [conversationId, messageId],
      { prepare: true }
    );

    // Notify conversation members via Kafka
    await kafka.send({
      topic: 'message-events',
      messages: [{
        key: conversationId,
        value: JSON.stringify({ type: 'message_deleted', messageId, conversationId }),
      }],
    });
  }

  // Edit a message (content update)
  async editMessage({ messageId, conversationId, requesterId, newContent }) {
    await cassandra.execute(
      `UPDATE messages SET content = ?, is_edited = true
       WHERE conversation_id = ? AND message_id = ?`,
      [JSON.stringify(newContent), conversationId, messageId],
      { prepare: true }
    );

    await kafka.send({
      topic: 'message-events',
      messages: [{
        key: conversationId,
        value: JSON.stringify({
          type: 'message_edited',
          messageId,
          conversationId,
          newContent,
        }),
      }],
    });
  }

  // Gap fill: messages since a given ID (for reconnect sync)
  async getMessagesSince({ conversationId, sinceMessageId, limit = 100 }) {
    const result = await cassandra.execute(
      `SELECT * FROM messages
       WHERE conversation_id = ?
       AND message_id > ?
       AND is_deleted = false
       ORDER BY message_id ASC
       LIMIT ?`,
      [conversationId, sinceMessageId, limit],
      { prepare: true }
    );

    return result.rows.map(row => ({
      messageId: row.message_id.toString(),
      senderId: row.sender_id,
      content: JSON.parse(row.content),
      serverTimestamp: row.server_timestamp,
    }));
  }

  async #updateConversationLastMessage(conversationId, messageId, timestamp) {
    const pg = require('./db/postgres');
    await pg.query(
      `UPDATE conversations
       SET last_message_id = $1, last_message_at = $2
       WHERE conversation_id = $3`,
      [messageId, timestamp, conversationId]
    );
  }
}

module.exports = new MessageService();
```

### 8.4 Presence Service

```javascript
// presenceService.js
const redis = require('./db/redis');
const kafka = require('./kafka/producer');

const PRESENCE_TTL = 30; // seconds
const TYPING_TTL = 5;    // seconds

class PresenceService {
  // Set user online (called on WebSocket auth + heartbeat)
  async setOnline(userId) {
    const pipeline = redis.pipeline();
    pipeline.setex(`user:${userId}:presence`, PRESENCE_TTL, 'online');
    pipeline.del(`user:${userId}:lastSeen`); // Clear stale lastSeen
    await pipeline.exec();

    // Notify contacts about status change (via Kafka → fan-out to contacts' servers)
    await kafka.send({
      topic: 'presence',
      messages: [{
        key: userId,
        value: JSON.stringify({ userId, status: 'online', timestamp: Date.now() }),
      }],
    });
  }

  // Set user offline (called on WebSocket disconnect — all devices gone)
  async setOffline(userId) {
    const pipeline = redis.pipeline();
    pipeline.del(`user:${userId}:presence`);
    pipeline.set(`user:${userId}:lastSeen`, Date.now());
    await pipeline.exec();

    await kafka.send({
      topic: 'presence',
      messages: [{
        key: userId,
        value: JSON.stringify({ userId, status: 'offline', lastSeen: Date.now() }),
      }],
    });
  }

  // Get presence for multiple users (batch)
  async getPresence(userIds) {
    if (userIds.length === 0) return {};

    const keys = userIds.map(id => `user:${id}:presence`);
    const lastSeenKeys = userIds.map(id => `user:${id}:lastSeen`);

    const [presenceValues, lastSeenValues] = await Promise.all([
      redis.mget(...keys),
      redis.mget(...lastSeenKeys),
    ]);

    const result = {};
    for (let i = 0; i < userIds.length; i++) {
      const userId = userIds[i];
      result[userId] = {
        status: presenceValues[i] === 'online' ? 'online' : 'offline',
        lastSeen: lastSeenValues[i] ? parseInt(lastSeenValues[i]) : null,
      };
    }
    return result;
  }

  // Typing indicator — call every 3s while user is typing
  async setTyping(userId, conversationId) {
    const key = `typing:${conversationId}:${userId}`;
    const isNew = await redis.set(key, '1', 'EX', TYPING_TTL, 'NX');

    if (isNew) {
      // Only broadcast start event, not every refresh
      await kafka.send({
        topic: 'presence',
        messages: [{
          key: conversationId,
          value: JSON.stringify({
            type: 'typing_start',
            userId,
            conversationId,
            timestamp: Date.now(),
          }),
        }],
      });
    } else {
      // Refresh TTL without broadcasting
      await redis.expire(key, TYPING_TTL);
    }
  }

  async clearTyping(userId, conversationId) {
    const key = `typing:${conversationId}:${userId}`;
    const deleted = await redis.del(key);

    if (deleted) {
      await kafka.send({
        topic: 'presence',
        messages: [{
          key: conversationId,
          value: JSON.stringify({
            type: 'typing_stop',
            userId,
            conversationId,
          }),
        }],
      });
    }
  }

  // Get all users currently typing in a conversation
  async getTypingUsers(conversationId) {
    const pattern = `typing:${conversationId}:*`;
    const keys = await redis.keys(pattern);

    return keys.map(key => key.split(':')[2]); // Extract userIds
  }
}

module.exports = new PresenceService();
```

### 8.5 Fan-out Service (Group Messages)

```javascript
// fanoutService.js
// Kafka consumer that routes messages to recipient servers

const { Kafka } = require('kafkajs');
const redis = require('./db/redis');
const postgres = require('./db/postgres');
const connectionManager = require('./connectionManager');
const notificationService = require('./notificationService');

const kafka = new Kafka({ brokers: process.env.KAFKA_BROKERS.split(',') });
const consumer = kafka.consumer({ groupId: 'fanout-service' });

const GROUP_THRESHOLD = 200; // members — write vs read fan-out

class FanoutService {
  async start() {
    await consumer.connect();
    await consumer.subscribe({ topics: ['messages', 'message-events'], fromBeginning: false });

    await consumer.run({
      eachMessage: async ({ topic, message }) => {
        const event = JSON.parse(message.value.toString());

        if (topic === 'messages') {
          await this.#fanoutMessage(event);
        } else if (topic === 'message-events') {
          await this.#fanoutEvent(event);
        }
      },
    });
  }

  async #fanoutMessage(event) {
    const { messageId, conversationId, senderId, content, serverTimestamp } = event;

    // Fetch conversation members (with cache)
    const members = await this.#getMembers(conversationId);

    const isLargeGroup = members.length >= GROUP_THRESHOLD;

    if (isLargeGroup) {
      // Fan-out on Read: just notify "new message" — clients fetch themselves
      await this.#notifyNewMessage(conversationId, messageId, members, senderId);
    } else {
      // Fan-out on Write: push full message to each member
      const messagePayload = {
        type: 'message',
        messageId,
        conversationId,
        senderId,
        content,
        serverTimestamp,
      };

      await this.#deliverToMembers(members, senderId, messagePayload, conversationId);
    }
  }

  async #deliverToMembers(members, senderId, messagePayload, conversationId) {
    const deliveryPromises = members
      .filter(m => m.userId !== senderId) // Don't echo to sender (they already have it)
      .map(async (member) => {
        // Increment unread counter atomically
        await redis.incr(`unread:${member.userId}:${conversationId}`);

        // Try to route to user's current server
        const targetServer = await redis.get(`user:${member.userId}:server`);

        if (targetServer) {
          // User is online somewhere
          await connectionManager.routeToUser(member.userId, messagePayload);
          // Receipt: delivered
          await this.#markDelivered(messagePayload.messageId, conversationId, member.userId);
        } else {
          // User is offline — push notification
          notificationService.sendPush(member.userId, {
            conversationId,
            senderId,
            messagePreview: this.#getPreview(messagePayload.content),
          }).catch(err => console.error('Push failed:', err));
        }
      });

    await Promise.allSettled(deliveryPromises); // Don't fail entire fan-out on one error
  }

  async #notifyNewMessage(conversationId, messageId, members, senderId) {
    const notification = { type: 'new_message_notify', conversationId, messageId };

    const promises = members
      .filter(m => m.userId !== senderId)
      .map(async (member) => {
        await redis.incr(`unread:${member.userId}:${conversationId}`);
        const targetServer = await redis.get(`user:${member.userId}:server`);

        if (targetServer) {
          await connectionManager.routeToUser(member.userId, notification);
        } else {
          await notificationService.sendPush(member.userId, {
            conversationId,
            senderId,
            messagePreview: 'New message',
          });
        }
      });

    await Promise.allSettled(promises);
  }

  async #getMembers(conversationId) {
    const cacheKey = `group:${conversationId}:members`;
    const cached = await redis.get(cacheKey);

    if (cached) {
      return JSON.parse(cached);
    }

    // Fetch from PostgreSQL
    const result = await postgres.query(
      `SELECT user_id, is_muted FROM conversation_members
       WHERE conversation_id = $1 AND left_at IS NULL`,
      [conversationId]
    );

    const members = result.rows.map(r => ({ userId: r.user_id, isMuted: r.is_muted }));

    // Cache for 5 minutes
    await redis.setex(cacheKey, 300, JSON.stringify(members));

    return members;
  }

  async #markDelivered(messageId, conversationId, userId) {
    const cassandra = require('./db/cassandra');
    await cassandra.execute(
      `INSERT INTO message_receipts (conversation_id, message_id, user_id, status, timestamp)
       VALUES (?, ?, ?, 'delivered', toTimestamp(now()))`,
      [conversationId, messageId, userId],
      { prepare: true }
    );
  }

  async #fanoutEvent(event) {
    // Forward message_deleted / message_edited to conversation members
    const members = await this.#getMembers(event.conversationId);
    const promises = members.map(m =>
      connectionManager.routeToUser(m.userId, event)
    );
    await Promise.allSettled(promises);
  }

  #getPreview(content) {
    switch (content.type) {
      case 'text': return content.body.substring(0, 80);
      case 'image': return '📷 Photo';
      case 'video': return '🎥 Video';
      case 'audio': return '🎵 Voice message';
      case 'file': return `📄 ${content.filename}`;
      default: return 'New message';
    }
  }
}

const fanoutService = new FanoutService();
fanoutService.start().catch(console.error);

module.exports = fanoutService;
```

### 8.6 Rate Limiter (Token Bucket)

```javascript
// rateLimiter.js
// Redis Lua script for atomic token bucket — prevents race conditions

const redis = require('./db/redis');

// Lua script: atomically check and consume tokens
// Returns: [allowed (0|1), remaining_tokens, reset_at_ms]
const TOKEN_BUCKET_SCRIPT = `
  local key = KEYS[1]
  local capacity = tonumber(ARGV[1])
  local refillRate = tonumber(ARGV[2])   -- tokens per second
  local cost = tonumber(ARGV[3])
  local now = tonumber(ARGV[4])          -- current time in ms

  local bucket = redis.call('HMGET', key, 'tokens', 'lastRefill')
  local tokens = tonumber(bucket[1])
  local lastRefill = tonumber(bucket[2])

  if tokens == nil then
    -- First request: initialize full bucket
    tokens = capacity
    lastRefill = now
  end

  -- Refill based on elapsed time
  local elapsed = (now - lastRefill) / 1000   -- seconds
  local newTokens = math.min(capacity, tokens + elapsed * refillRate)

  if newTokens < cost then
    -- Not enough tokens — rate limited
    redis.call('HMSET', key, 'tokens', newTokens, 'lastRefill', now)
    redis.call('EXPIRE', key, 3600)
    return {0, math.floor(newTokens), math.floor(lastRefill + ((cost - newTokens) / refillRate) * 1000)}
  end

  -- Consume tokens
  local remaining = newTokens - cost
  redis.call('HMSET', key, 'tokens', remaining, 'lastRefill', now)
  redis.call('EXPIRE', key, 3600)
  return {1, math.floor(remaining), 0}
`;

class RateLimiter {
  // Preload script SHA for efficiency
  #scriptSha = null;

  async #loadScript() {
    if (!this.#scriptSha) {
      this.#scriptSha = await redis.script('LOAD', TOKEN_BUCKET_SCRIPT);
    }
    return this.#scriptSha;
  }

  async check({ userId, action, cost = 1 }) {
    const config = RATE_LIMIT_CONFIGS[action];
    if (!config) throw new Error(`Unknown action: ${action}`);

    const key = `ratelimit:${userId}:${action}`;
    const sha = await this.#loadScript();

    const [allowed, remaining, retryAfterMs] = await redis.evalsha(
      sha, 1, key,
      config.capacity,
      config.refillRate,
      cost,
      Date.now()
    );

    return {
      allowed: allowed === 1,
      remaining: parseInt(remaining),
      retryAfter: retryAfterMs ? Math.ceil((retryAfterMs - Date.now()) / 1000) : 0,
      limit: config.capacity,
    };
  }

  // Express middleware factory
  middleware(action) {
    return async (req, res, next) => {
      const userId = req.user?.userId;
      if (!userId) return res.status(401).json({ error: 'Unauthorized' });

      const result = await this.check({ userId, action });

      res.set({
        'X-RateLimit-Limit': result.limit,
        'X-RateLimit-Remaining': result.remaining,
        'X-RateLimit-RetryAfter': result.retryAfter,
      });

      if (!result.allowed) {
        return res.status(429).json({
          error: 'RATE_LIMITED',
          retryAfter: result.retryAfter,
          message: `Too many requests. Try again in ${result.retryAfter}s`,
        });
      }

      next();
    };
  }
}

const RATE_LIMIT_CONFIGS = {
  send_message: { capacity: 60,  refillRate: 1  },  // 60/min
  create_group: { capacity: 5,   refillRate: 0.00006 }, // 5/day
  upload_media: { capacity: 10,  refillRate: 0.167 }, // 10/min
  search:       { capacity: 30,  refillRate: 0.5  },  // 30/min
  api_general:  { capacity: 100, refillRate: 1.67 },  // 100/min
};

module.exports = new RateLimiter();
```

### 8.7 Message Queue Consumer

```javascript
// kafkaConsumer.js
// Base class for Kafka consumers with graceful shutdown, DLQ, backpressure

const { Kafka, logLevel } = require('kafkajs');

class MessageQueueConsumer {
  #kafka;
  #consumer;
  #isRunning = false;
  #dlqProducer;

  constructor({ groupId, topics, handler, maxRetries = 3 }) {
    this.groupId = groupId;
    this.topics = topics;
    this.handler = handler;
    this.maxRetries = maxRetries;

    this.#kafka = new Kafka({
      clientId: `${groupId}-${process.env.SERVER_ID}`,
      brokers: process.env.KAFKA_BROKERS.split(','),
      logLevel: logLevel.WARN,
      retry: {
        retries: 5,
        initialRetryTime: 300,
        multiplier: 2,
      },
    });

    this.#consumer = this.#kafka.consumer({
      groupId,
      sessionTimeout: 30_000,
      heartbeatInterval: 3_000,
      maxBytesPerPartition: 1_048_576, // 1MB per partition
    });

    this.#dlqProducer = this.#kafka.producer();
  }

  async start() {
    await this.#consumer.connect();
    await this.#dlqProducer.connect();

    for (const topic of this.topics) {
      await this.#consumer.subscribe({ topic, fromBeginning: false });
    }

    this.#isRunning = true;

    await this.#consumer.run({
      autoCommit: false, // Manual commit for exactly-once semantics
      partitionsConsumedConcurrently: 4,

      eachMessage: async ({ topic, partition, message, heartbeat }) => {
        let payload;
        try {
          payload = JSON.parse(message.value.toString());
        } catch (err) {
          console.error('Failed to parse message:', message.value.toString());
          await this.#sendToDLQ(topic, message, 'PARSE_ERROR');
          await this.#consumer.commitOffsets([{
            topic, partition,
            offset: (BigInt(message.offset) + 1n).toString(),
          }]);
          return;
        }

        let attempt = 0;
        let lastError;

        while (attempt < this.maxRetries) {
          try {
            await heartbeat(); // Prevent session timeout during long processing
            await this.handler({ topic, payload, message });

            // Commit offset only after successful processing
            await this.#consumer.commitOffsets([{
              topic, partition,
              offset: (BigInt(message.offset) + 1n).toString(),
            }]);
            return;

          } catch (err) {
            lastError = err;
            attempt++;

            if (attempt < this.maxRetries) {
              const delay = Math.min(100 * Math.pow(2, attempt), 5000);
              await new Promise(resolve => setTimeout(resolve, delay));
            }
          }
        }

        // Exhausted retries — send to DLQ
        console.error(`DLQ: ${topic} after ${this.maxRetries} retries:`, lastError);
        await this.#sendToDLQ(topic, message, lastError.message);

        await this.#consumer.commitOffsets([{
          topic, partition,
          offset: (BigInt(message.offset) + 1n).toString(),
        }]);
      },
    });
  }

  async #sendToDLQ(originalTopic, message, errorReason) {
    await this.#dlqProducer.send({
      topic: `${originalTopic}.dlq`,
      messages: [{
        key: message.key,
        value: message.value,
        headers: {
          ...message.headers,
          'x-original-topic': originalTopic,
          'x-error-reason': errorReason,
          'x-failed-at': Date.now().toString(),
        },
      }],
    });
  }

  async stop() {
    this.#isRunning = false;
    await this.#consumer.disconnect();
    await this.#dlqProducer.disconnect();
  }
}

module.exports = MessageQueueConsumer;
```

### 8.8 Notification Service

```javascript
// notificationService.js
const admin = require('firebase-admin'); // FCM
const apn = require('@parse/node-apn');  // APNs
const postgres = require('./db/postgres');
const redis = require('./db/redis');

// Initialize Firebase Admin
admin.initializeApp({
  credential: admin.credential.cert(JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT)),
});

// Initialize APNs
const apnProvider = new apn.Provider({
  token: {
    key: process.env.APN_KEY,
    keyId: process.env.APN_KEY_ID,
    teamId: process.env.APN_TEAM_ID,
  },
  production: process.env.NODE_ENV === 'production',
});

class NotificationService {
  // Send push notification to a user (all their devices)
  async sendPush(userId, { conversationId, senderId, messagePreview }) {
    // Check user notification preferences
    const prefs = await this.#getUserPreferences(userId, conversationId);
    if (!prefs.pushEnabled || prefs.isMuted) return;

    // Get all device tokens for user
    const devices = await this.#getDeviceTokens(userId);
    if (devices.length === 0) return;

    // Get unread count for badge
    const unreadCount = await this.#getUnreadCount(userId);

    // Get sender name
    const senderName = await this.#getSenderName(senderId);

    // E2EE check — don't include content in push for E2EE convs
    const isE2EE = await this.#isE2EEConversation(conversationId);
    const title = isE2EE ? 'New Message' : senderName;
    const body = isE2EE ? 'You have a new message' : messagePreview;

    const results = await Promise.allSettled(
      devices.map(device => this.#sendToDevice(device, {
        title, body, unreadCount, conversationId, senderId,
      }))
    );

    // Remove stale tokens (invalid tokens returned by APNs/FCM)
    const staleTokens = results
      .map((r, i) => r.status === 'rejected' && r.reason?.stale ? devices[i].token : null)
      .filter(Boolean);

    if (staleTokens.length > 0) {
      await postgres.query(
        `DELETE FROM device_tokens WHERE token = ANY($1)`,
        [staleTokens]
      );
    }
  }

  async #sendToDevice(device, { title, body, unreadCount, conversationId, senderId }) {
    if (device.platform === 'ios') {
      const notification = new apn.Notification();
      notification.alert = { title, body };
      notification.badge = unreadCount;
      notification.sound = 'default';
      notification.topic = process.env.APN_BUNDLE_ID;
      notification.payload = { conversationId, senderId };

      const result = await apnProvider.send(notification, device.token);
      if (result.failed.length > 0) {
        const failure = result.failed[0];
        if (failure.response?.reason === 'BadDeviceToken') {
          throw Object.assign(new Error('Stale token'), { stale: true });
        }
      }

    } else if (device.platform === 'android') {
      const message = {
        token: device.token,
        notification: { title, body },
        android: {
          priority: 'high',
          notification: {
            channelId: 'messages',
            sound: 'default',
            clickAction: 'OPEN_CONVERSATION',
          },
        },
        data: {
          conversationId,
          senderId,
          badge: unreadCount.toString(),
        },
      };

      try {
        await admin.messaging().send(message);
      } catch (err) {
        if (err.code === 'messaging/registration-token-not-registered') {
          throw Object.assign(new Error('Stale token'), { stale: true });
        }
        throw err;
      }
    }
  }

  async #getDeviceTokens(userId) {
    const result = await postgres.query(
      `SELECT token, platform FROM device_tokens
       WHERE user_id = $1 AND is_active = true`,
      [userId]
    );
    return result.rows;
  }

  async #getUnreadCount(userId) {
    const pattern = `unread:${userId}:*`;
    const keys = await redis.keys(pattern);
    if (keys.length === 0) return 0;

    const values = await redis.mget(...keys);
    return values.reduce((sum, v) => sum + (parseInt(v) || 0), 0);
  }

  async #getSenderName(senderId) {
    const result = await postgres.query(
      `SELECT display_name FROM users WHERE user_id = $1`,
      [senderId]
    );
    return result.rows[0]?.display_name || 'Someone';
  }

  async #getUserPreferences(userId, conversationId) {
    const result = await postgres.query(
      `SELECT cm.is_muted, u.push_notifications_enabled
       FROM conversation_members cm
       JOIN users u ON u.user_id = cm.user_id
       WHERE cm.user_id = $1 AND cm.conversation_id = $2`,
      [userId, conversationId]
    );
    const row = result.rows[0];
    return {
      isMuted: row?.is_muted ?? false,
      pushEnabled: row?.push_notifications_enabled ?? true,
    };
  }

  async #isE2EEConversation(conversationId) {
    const result = await postgres.query(
      `SELECT is_e2ee FROM conversations WHERE conversation_id = $1`,
      [conversationId]
    );
    return result.rows[0]?.is_e2ee ?? false;
  }

  // Register a device token (called on app open / token refresh)
  async registerDevice(userId, { token, platform, deviceModel, osVersion }) {
    await postgres.query(
      `INSERT INTO device_tokens (user_id, token, platform, device_model, os_version, is_active, registered_at)
       VALUES ($1, $2, $3, $4, $5, true, NOW())
       ON CONFLICT (token) DO UPDATE SET
         user_id = $1, is_active = true, registered_at = NOW()`,
      [userId, token, platform, deviceModel, osVersion]
    );
  }
}

module.exports = new NotificationService();
```

### 8.9 Read Receipt Tracker

```javascript
// receiptService.js
const cassandra = require('./db/cassandra');
const redis = require('./db/redis');
const connectionManager = require('./connectionManager');
const kafka = require('./kafka/producer');

class ReceiptService {
  // Called when user opens a conversation (batch mark as read)
  async markConversationRead(userId, conversationId) {
    // Get last message ID in conversation
    const lastMsgResult = await cassandra.execute(
      `SELECT message_id FROM messages
       WHERE conversation_id = ? AND is_deleted = false
       ORDER BY message_id DESC LIMIT 1`,
      [conversationId],
      { prepare: true }
    );

    if (lastMsgResult.rowLength === 0) return;

    const lastMessageId = lastMsgResult.rows[0].message_id.toString();

    // Update user's last read position in PostgreSQL
    await require('./db/postgres').query(
      `UPDATE conversation_members
       SET last_read_msg_id = $1
       WHERE user_id = $2 AND conversation_id = $3`,
      [lastMessageId, userId, conversationId]
    );

    // Clear unread count
    await redis.del(`unread:${userId}:${conversationId}`);

    // Notify message senders about read (they need to update their UI)
    await kafka.send({
      topic: 'receipts',
      messages: [{
        key: conversationId,
        value: JSON.stringify({
          type: 'read',
          conversationId,
          readBy: userId,
          upToMessageId: lastMessageId,
          timestamp: Date.now(),
        }),
      }],
    });
  }

  // Called when a message is delivered to a device
  async markDelivered(messageId, conversationId, userId) {
    // Write to Cassandra
    await cassandra.execute(
      `INSERT INTO message_receipts (conversation_id, message_id, user_id, status, timestamp)
       VALUES (?, ?, ?, 'delivered', toTimestamp(now()))
       IF NOT EXISTS`,
      [conversationId, messageId, userId],
      { prepare: true }
    );

    // Publish to Kafka (receipt fan-out to sender)
    await kafka.send({
      topic: 'receipts',
      messages: [{
        key: conversationId,
        value: JSON.stringify({
          type: 'delivered',
          messageId,
          conversationId,
          deliveredTo: userId,
          timestamp: Date.now(),
        }),
      }],
    });
  }

  // Consume receipt events and forward to message sender
  async processReceiptEvent(event) {
    const { type, conversationId, messageId, readBy, deliveredTo, upToMessageId } = event;

    // Find the sender(s) of the relevant messages
    let senderIds;

    if (type === 'read') {
      // Get all senders of messages up to upToMessageId
      const result = await cassandra.execute(
        `SELECT DISTINCT sender_id FROM messages
         WHERE conversation_id = ?
         AND message_id <= ?
         AND is_deleted = false`,
        [conversationId, upToMessageId],
        { prepare: true }
      );
      senderIds = result.rows.map(r => r.sender_id.toString());
    } else {
      const result = await cassandra.execute(
        `SELECT sender_id FROM messages
         WHERE conversation_id = ? AND message_id = ?`,
        [conversationId, messageId],
        { prepare: true }
      );
      senderIds = result.rows.map(r => r.sender_id.toString());
    }

    // Push receipt update to each sender's client
    const receiptPayload = {
      type: `receipt_${type}`,
      conversationId,
      messageId: messageId || upToMessageId,
      userId: readBy || deliveredTo,
      timestamp: event.timestamp,
    };

    for (const senderId of senderIds) {
      await connectionManager.routeToUser(senderId, receiptPayload);
    }
  }

  // Get read receipts for a specific message (who read it)
  async getReceipts(conversationId, messageId) {
    const result = await cassandra.execute(
      `SELECT user_id, status, timestamp FROM message_receipts
       WHERE conversation_id = ? AND message_id = ?`,
      [conversationId, messageId],
      { prepare: true }
    );

    return result.rows.reduce((acc, row) => {
      acc[row.user_id] = { status: row.status, timestamp: row.timestamp };
      return acc;
    }, {});
  }

  // Get unread count for a user in all conversations
  async getUnreadCounts(userId) {
    const pattern = `unread:${userId}:*`;
    const keys = await redis.keys(pattern);
    if (keys.length === 0) return {};

    const values = await redis.mget(...keys);

    return keys.reduce((acc, key, i) => {
      const conversationId = key.split(':')[2];
      acc[conversationId] = parseInt(values[i]) || 0;
      return acc;
    }, {});
  }
}

module.exports = new ReceiptService();
```

### 8.10 Conversation Service

```javascript
// conversationService.js
const postgres = require('./db/postgres');
const redis = require('./db/redis');

class ConversationService {
  // Create a 1:1 direct conversation (idempotent)
  async getOrCreateDirect(userIdA, userIdB) {
    // Canonical ordering to prevent duplicates
    const [u1, u2] = [userIdA, userIdB].sort();

    const existing = await postgres.query(
      `SELECT c.conversation_id
       FROM conversations c
       JOIN conversation_members cm1 ON cm1.conversation_id = c.conversation_id AND cm1.user_id = $1
       JOIN conversation_members cm2 ON cm2.conversation_id = c.conversation_id AND cm2.user_id = $2
       WHERE c.type = 'direct'
       LIMIT 1`,
      [u1, u2]
    );

    if (existing.rows.length > 0) {
      return existing.rows[0].conversation_id;
    }

    // Create new direct conversation (transaction)
    const client = await postgres.connect();
    try {
      await client.query('BEGIN');

      const convResult = await client.query(
        `INSERT INTO conversations (type, created_by) VALUES ('direct', $1) RETURNING conversation_id`,
        [userIdA]
      );
      const conversationId = convResult.rows[0].conversation_id;

      await client.query(
        `INSERT INTO conversation_members (conversation_id, user_id, role)
         VALUES ($1, $2, 'member'), ($1, $3, 'member')`,
        [conversationId, userIdA, userIdB]
      );

      await client.query('COMMIT');
      return conversationId;
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }
  }

  // Create a group conversation
  async createGroup({ name, creatorId, memberIds, isE2EE = false }) {
    if (memberIds.length > 1000) {
      throw new Error('Group cannot exceed 1000 members');
    }

    const allMembers = [...new Set([creatorId, ...memberIds])];

    const client = await postgres.connect();
    try {
      await client.query('BEGIN');

      const convResult = await client.query(
        `INSERT INTO conversations (type, name, created_by, is_e2ee)
         VALUES ('group', $1, $2, $3)
         RETURNING conversation_id`,
        [name, creatorId, isE2EE]
      );
      const conversationId = convResult.rows[0].conversation_id;

      // Bulk insert members
      const memberValues = allMembers.map((userId, i) => {
        const role = userId === creatorId ? 'owner' : 'member';
        return `($1, $${i + 2}, '${role}')`;
      });

      await client.query(
        `INSERT INTO conversation_members (conversation_id, user_id, role)
         VALUES ${memberValues.join(',')}`,
        [conversationId, ...allMembers]
      );

      await client.query('COMMIT');

      // Invalidate member cache
      await redis.del(`group:${conversationId}:members`);

      return conversationId;
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }
  }

  // Get conversations for a user (inbox view) — paginated
  async getUserConversations(userId, { limit = 20, beforeTimestamp } = {}) {
    const params = [userId, limit];
    let whereClause = 'WHERE cm.user_id = $1 AND cm.left_at IS NULL';

    if (beforeTimestamp) {
      whereClause += ` AND c.last_message_at < $${params.length + 1}`;
      params.push(beforeTimestamp);
    }

    const result = await postgres.query(
      `SELECT
         c.conversation_id,
         c.type,
         c.name,
         c.avatar_url,
         c.last_message_at,
         c.last_message_id,
         cm.is_muted,
         cm.is_archived,
         cm.last_read_msg_id
       FROM conversations c
       JOIN conversation_members cm ON cm.conversation_id = c.conversation_id
       ${whereClause}
       ORDER BY c.last_message_at DESC NULLS LAST
       LIMIT $2`,
      params
    );

    // Enrich with unread counts (batch Redis MGET)
    const convIds = result.rows.map(r => r.conversation_id);
    const unreadKeys = convIds.map(id => `unread:${userId}:${id}`);

    let unreadCounts = {};
    if (unreadKeys.length > 0) {
      const values = await redis.mget(...unreadKeys);
      convIds.forEach((id, i) => {
        unreadCounts[id] = parseInt(values[i]) || 0;
      });
    }

    return result.rows.map(row => ({
      conversationId: row.conversation_id,
      type: row.type,
      name: row.name,
      avatarUrl: row.avatar_url,
      lastMessageAt: row.last_message_at,
      lastMessageId: row.last_message_id?.toString(),
      isMuted: row.is_muted,
      isArchived: row.is_archived,
      unreadCount: unreadCounts[row.conversation_id] || 0,
    }));
  }

  // Add member to group
  async addMember(conversationId, userId, addedByUserId) {
    // Check adder has permission
    const adder = await this.#getMemberRole(conversationId, addedByUserId);
    if (!['owner', 'admin'].includes(adder)) throw new Error('UNAUTHORIZED');

    // Check group size
    const countResult = await postgres.query(
      `SELECT COUNT(*) as cnt FROM conversation_members
       WHERE conversation_id = $1 AND left_at IS NULL`,
      [conversationId]
    );

    if (parseInt(countResult.rows[0].cnt) >= 1000) {
      throw new Error('GROUP_FULL');
    }

    await postgres.query(
      `INSERT INTO conversation_members (conversation_id, user_id, role)
       VALUES ($1, $2, 'member')
       ON CONFLICT (conversation_id, user_id) DO UPDATE SET left_at = NULL`,
      [conversationId, userId]
    );

    await redis.del(`group:${conversationId}:members`);
  }

  async #getMemberRole(conversationId, userId) {
    const result = await postgres.query(
      `SELECT role FROM conversation_members
       WHERE conversation_id = $1 AND user_id = $2 AND left_at IS NULL`,
      [conversationId, userId]
    );
    return result.rows[0]?.role;
  }
}

module.exports = new ConversationService();
```

### 8.11 REST API Layer (Express)

```javascript
// server.js — Express REST API + WebSocket server bootstrap

const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const helmet = require('helmet');
const cors = require('cors');
const compression = require('compression');

const connectionManager = require('./connectionManager');
const messageService = require('./messageService');
const conversationService = require('./conversationService');
const presenceService = require('./presenceService');
const receiptService = require('./receiptService');
const rateLimiter = require('./rateLimiter');
const auth = require('./middleware/auth');

const app = express();
const server = http.createServer(app);

// ── Security & middleware ─────────────────────────────────────────
app.use(helmet());
app.use(cors({ origin: process.env.ALLOWED_ORIGINS?.split(',') }));
app.use(compression());
app.use(express.json({ limit: '1mb' }));

// ── WebSocket Server ──────────────────────────────────────────────
const wss = new WebSocket.Server({ server, path: '/ws' });

wss.on('connection', (ws, req) => {
  connectionManager.handleConnection(ws, req);
});

// Route messages through message service
connectionManager.on('client_message', async ({ type, _senderId, ...payload }) => {
  try {
    switch (type) {
      case 'send_message': {
        const { conversationId, content, clientMsgId, replyToMsgId } = payload;
        const result = await messageService.sendMessage({
          conversationId, senderId: _senderId, content, clientMsgId, replyToMsgId,
        });
        // Send ACK back to sender
        await connectionManager.routeToUser(_senderId, {
          type: 'message_ack',
          clientMsgId,
          messageId: result.messageId,
          serverTimestamp: result.serverTimestamp,
          status: 'sent',
        });
        break;
      }
      case 'typing_start':
        await presenceService.setTyping(_senderId, payload.conversationId);
        break;
      case 'typing_stop':
        await presenceService.clearTyping(_senderId, payload.conversationId);
        break;
      case 'mark_read':
        await receiptService.markConversationRead(_senderId, payload.conversationId);
        break;
    }
  } catch (err) {
    console.error('Error handling client message:', err);
  }
});

// ── REST API Routes ───────────────────────────────────────────────

// Health check
app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    connections: connectionManager.getOnlineCount(),
    uptime: process.uptime(),
  });
});

// --- Conversations ---
app.get('/conversations',
  auth,
  rateLimiter.middleware('api_general'),
  async (req, res) => {
    try {
      const { limit, before } = req.query;
      const conversations = await conversationService.getUserConversations(
        req.user.userId,
        { limit: parseInt(limit) || 20, beforeTimestamp: before }
      );
      res.json({ conversations });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  }
);

app.post('/conversations/direct',
  auth,
  rateLimiter.middleware('api_general'),
  async (req, res) => {
    try {
      const { recipientId } = req.body;
      const conversationId = await conversationService.getOrCreateDirect(
        req.user.userId, recipientId
      );
      res.json({ conversationId });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  }
);

app.post('/conversations/group',
  auth,
  rateLimiter.middleware('create_group'),
  async (req, res) => {
    try {
      const { name, memberIds, isE2EE } = req.body;
      const conversationId = await conversationService.createGroup({
        name, creatorId: req.user.userId, memberIds, isE2EE,
      });
      res.status(201).json({ conversationId });
    } catch (err) {
      if (err.message === 'GROUP_FULL') return res.status(400).json({ error: 'Group is full' });
      res.status(500).json({ error: err.message });
    }
  }
);

// --- Messages ---
app.get('/conversations/:conversationId/messages',
  auth,
  rateLimiter.middleware('api_general'),
  async (req, res) => {
    try {
      const { conversationId } = req.params;
      const { before, limit } = req.query;

      const messages = await messageService.getMessages({
        conversationId,
        beforeMessageId: before,
        limit: Math.min(parseInt(limit) || 50, 100),
      });

      res.json({ messages, hasMore: messages.length === (parseInt(limit) || 50) });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  }
);

app.delete('/messages/:messageId',
  auth,
  async (req, res) => {
    try {
      const { messageId } = req.params;
      const { conversationId } = req.body;
      await messageService.deleteMessage({
        messageId, conversationId, requesterId: req.user.userId,
      });
      res.json({ success: true });
    } catch (err) {
      if (err.message === 'UNAUTHORIZED') return res.status(403).json({ error: 'Unauthorized' });
      res.status(500).json({ error: err.message });
    }
  }
);

// --- Presence ---
app.get('/presence',
  auth,
  rateLimiter.middleware('api_general'),
  async (req, res) => {
    try {
      const { userIds } = req.query; // comma-separated
      const ids = userIds.split(',').slice(0, 100); // max 100 per request
      const presence = await presenceService.getPresence(ids);
      res.json({ presence });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  }
);

// --- Media upload URL ---
app.post('/media/upload-url',
  auth,
  rateLimiter.middleware('upload_media'),
  async (req, res) => {
    try {
      const { filename, mimeType, size } = req.body;

      if (size > 100 * 1024 * 1024) { // 100MB limit
        return res.status(400).json({ error: 'File too large' });
      }

      const mediaService = require('./mediaService');
      const { uploadUrl, mediaId } = await mediaService.getPresignedUrl({
        filename, mimeType, size, userId: req.user.userId,
      });

      res.json({ uploadUrl, mediaId });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  }
);

// --- Graceful shutdown ---
process.on('SIGTERM', async () => {
  console.log('SIGTERM received — draining connections...');
  server.close(() => process.exit(0));
  // Close WebSocket connections gracefully
  wss.clients.forEach(ws => ws.close(1001, 'Server shutting down'));
  setTimeout(() => process.exit(0), 30_000); // Force exit after 30s
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Chat server listening on :${PORT}`));

module.exports = { app, server };
```

### 8.12 Client-Side Message Sync

```javascript
// chatClient.js — Browser/Node client SDK

class ChatClient {
  #ws = null;
  #userId = null;
  #token = null;
  #pendingMessages = new Map(); // clientMsgId → {resolve, reject, timeout}
  #messageHandlers = new Set();
  #reconnectAttempt = 0;
  #maxReconnectDelay = 60_000;
  #isConnected = false;

  // Map<conversationId, lastMessageId> — for gap fill on reconnect
  #lastSeenMessageId = new Map();

  constructor({ serverUrl, token }) {
    this.serverUrl = serverUrl;
    this.#token = token;
  }

  connect() {
    return new Promise((resolve, reject) => {
      this.#ws = new WebSocket(this.serverUrl);

      this.#ws.onopen = () => {
        // Authenticate immediately after connection
        this.#send({ type: 'auth', token: this.#token });
      };

      this.#ws.onmessage = (event) => {
        const message = JSON.parse(event.data);
        this.#handleServerMessage(message, resolve);
      };

      this.#ws.onclose = (event) => {
        this.#isConnected = false;
        if (event.code !== 1000) { // Not a clean close
          this.#scheduleReconnect();
        }
      };

      this.#ws.onerror = (err) => {
        console.error('WebSocket error:', err);
        reject(err);
      };
    });
  }

  #handleServerMessage(message, authResolve) {
    switch (message.type) {
      case 'auth_ack':
        this.#userId = message.userId;
        this.#isConnected = true;
        this.#reconnectAttempt = 0;
        authResolve(this); // Resolve connect() promise

        // Request gap fill for all active conversations
        this.#requestGapFill();
        break;

      case 'message':
        // Notify all registered handlers
        this.#messageHandlers.forEach(handler => handler(message));
        // Send delivery receipt
        this.#send({ type: 'mark_read', conversationId: message.conversationId });
        // Update last seen
        this.#lastSeenMessageId.set(message.conversationId, message.messageId);
        break;

      case 'message_ack':
        // Resolve pending send promise
        const pending = this.#pendingMessages.get(message.clientMsgId);
        if (pending) {
          clearTimeout(pending.timeout);
          pending.resolve({ messageId: message.messageId, status: 'sent' });
          this.#pendingMessages.delete(message.clientMsgId);
        }
        break;

      case 'receipt_delivered':
      case 'receipt_read':
        this.#messageHandlers.forEach(handler => handler(message));
        break;

      case 'typing_start':
      case 'typing_stop':
        this.#messageHandlers.forEach(handler => handler(message));
        break;

      case 'new_message_notify':
        // Large group — fetch messages from REST API
        this.#fetchMissedMessages(message.conversationId, message.messageId);
        break;

      case 'error':
        console.error('Server error:', message);
        break;
    }
  }

  // Send a message — returns promise that resolves when server ACKs
  sendMessage({ conversationId, content, replyToMsgId } = {}) {
    const clientMsgId = crypto.randomUUID();

    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.#pendingMessages.delete(clientMsgId);
        reject(new Error('Message send timeout'));
      }, 10_000);

      this.#pendingMessages.set(clientMsgId, { resolve, reject, timeout });

      this.#send({
        type: 'send_message',
        clientMsgId,
        conversationId,
        content,
        replyToMsgId,
      });
    });
  }

  onMessage(handler) {
    this.#messageHandlers.add(handler);
    return () => this.#messageHandlers.delete(handler); // Returns unsubscribe
  }

  startTyping(conversationId) {
    this.#send({ type: 'typing_start', conversationId });
  }

  stopTyping(conversationId) {
    this.#send({ type: 'typing_stop', conversationId });
  }

  // Request messages since our last known message (gap fill on reconnect)
  async #requestGapFill() {
    for (const [conversationId, lastMessageId] of this.#lastSeenMessageId) {
      await this.#fetchMissedMessages(conversationId, lastMessageId);
    }
  }

  async #fetchMissedMessages(conversationId, sinceMessageId) {
    try {
      const resp = await fetch(
        `/conversations/${conversationId}/messages?since=${sinceMessageId}&limit=100`,
        { headers: { Authorization: `Bearer ${this.#token}` } }
      );
      const { messages } = await resp.json();

      messages.forEach(msg => {
        this.#messageHandlers.forEach(handler => handler({ type: 'message', ...msg }));
      });

      if (messages.length > 0) {
        const latest = messages[messages.length - 1];
        this.#lastSeenMessageId.set(conversationId, latest.messageId);
      }
    } catch (err) {
      console.error('Gap fill failed:', err);
    }
  }

  #scheduleReconnect() {
    this.#reconnectAttempt++;
    const delay = Math.min(
      1000 * Math.pow(2, this.#reconnectAttempt) + Math.random() * 1000,
      this.#maxReconnectDelay
    );

    console.log(`Reconnecting in ${Math.round(delay / 1000)}s (attempt ${this.#reconnectAttempt})`);

    setTimeout(() => {
      this.connect().catch(err => {
        console.error('Reconnect failed:', err);
      });
    }, delay);
  }

  #send(data) {
    if (this.#ws?.readyState === WebSocket.OPEN) {
      this.#ws.send(JSON.stringify(data));
    }
  }

  disconnect() {
    this.#ws?.close(1000, 'Client disconnect');
  }
}

// Usage example:
// const client = new ChatClient({ serverUrl: 'wss://chat.example.com/ws', token });
// await client.connect();
// client.onMessage(msg => console.log('New message:', msg));
// await client.sendMessage({ conversationId: 'conv_abc', content: { type: 'text', body: 'Hello!' } });
```

---

## 9. Failure Scenarios & Resilience

| Failure | Detection | Recovery |
|---|---|---|
| Chat server crash | ELB health checks (30s) | Clients reconnect via exponential backoff; sessions in Redis survive |
| Redis failure | Sentinel / Redis Cluster | Graceful degradation: routing falls back to broadcast |
| Kafka broker down | Kafka leader election (ISR) | Producers retry; messages durable in Kafka log |
| Cassandra node down | Gossip protocol | Quorum reads/writes (RF=3, QUORUM); auto-repair |
| PostgreSQL primary down | Patroni / RDS failover | Failover ~30s; writes fail, reads from replica |
| Network partition | Split-brain | Cassandra: last-write-wins; message ordering preserved via Snowflake |
| Message delivery failure | No ACK in 10s | Client retries with same clientMsgId (idempotent) |
| DLQ overflow | Alert on DLQ depth | Human intervention; replay after root cause fix |

---

## 10. Monitoring & Observability

### Metrics (Prometheus / Datadog)
```
# Golden signals
chat_messages_per_second              (rate)
chat_p99_delivery_latency_ms          (latency < 100ms)
chat_websocket_connections_active     (saturation)
chat_error_rate_percent               (errors)

# Business metrics
chat_messages_delivered_total
chat_push_notifications_sent_total
chat_media_uploads_total_bytes
chat_group_messages_fanout_duration_ms

# Infrastructure
kafka_consumer_lag{group="fanout-service"}    (should be < 10K)
redis_memory_used_bytes
cassandra_write_latency_p99
```

### Alerts
```yaml
- alert: HighDeliveryLatency
  expr: chat_p99_delivery_latency_ms > 500
  severity: critical

- alert: KafkaConsumerLag
  expr: kafka_consumer_lag{group="fanout-service"} > 100000
  severity: warning

- alert: WebSocketConnectionDrop
  expr: rate(chat_websocket_connections_active[5m]) < -10000
  severity: critical

- alert: MessageDeliveryFailureRate
  expr: chat_message_delivery_error_rate > 0.01
  severity: warning
```

### Distributed Tracing
- OpenTelemetry spans from client → WebSocket → Kafka → Cassandra
- Trace ID propagated through all message headers
- Jaeger / Tempo for trace storage

### Logging
- Structured JSON logs (no printf)
- Log levels: ERROR for failures, WARN for retries, INFO for lifecycle, DEBUG for tracing
- Never log message content (privacy)

---

## 11. Trade-offs & Alternatives

| Decision | Choice Made | Alternative | Trade-off |
|---|---|---|---|
| Real-time protocol | WebSocket | Long polling, SSE | WS: lowest latency but stateful; LP: simpler, higher latency |
| Message storage | Cassandra | DynamoDB, MongoDB | Cassandra: best write throughput, tunable consistency; DynamoDB: managed, pricier |
| Message broker | Kafka | RabbitMQ, SQS | Kafka: ordered, replayable, high throughput; RabbitMQ: simpler, lower throughput |
| Fan-out strategy | Hybrid (write < 200, read ≥ 200) | Pure write / pure read | Hybrid: optimal; pure write: simpler but kills large groups; pure read: slower delivery |
| Message ordering | Snowflake IDs | ULIDs, Mongo ObjectId | Snowflake: 64-bit, sortable, no coordination; ULID: similar but 128-bit |
| Presence | Redis TTL + heartbeat | DB-backed | Redis: fast, ephemeral; DB: durable but slow for hot path |
| Search | Elasticsearch | PostgreSQL FTS | ES: distributed, relevance scoring; PGFTS: simpler, sufficient at small scale |
| Sharding | conversation_id partitioning | user_id sharding | Conv-ID: all messages together (good for history); user-ID: user data together |

---

## 12. Security Considerations

```
Authentication:
  - JWT (RS256) issued by Auth Service — short lived (15 min) + refresh token (30 days)
  - JWT verified on every WebSocket auth and REST request
  - Refresh tokens stored in Redis, rotated on use

Transport Security:
  - TLS 1.3 minimum for all connections (WS → WSS, HTTP → HTTPS)
  - Certificate pinning on mobile clients
  - HSTS headers on all REST endpoints

Input Validation:
  - Message content: max 64KB, strip HTML, validate UTF-8
  - File uploads: magic byte validation (not just Content-Type)
  - Rate limiting: per-user, per-IP, per-device

Privacy:
  - Never log message content
  - PII fields encrypted at rest (phone numbers, emails)
  - GDPR: data export API, account deletion purges all messages
  - Conversation membership checked before every message delivery

Authorization:
  - Message send: must be member of conversation
  - Message delete: only sender can delete (or group admin)
  - Group admin actions: role check before execution

Anti-abuse:
  - Spam detection: ML model on message frequency/content patterns
  - Block list: blocked user cannot send to blocker
  - Report system: flagged messages reviewed (cannot read E2EE)
  - NSFW media scanning (non-E2EE only)
```

---

## 13. Interview Cheat Sheet

### Numbers to Remember
```
500M DAU → 150M concurrent WebSocket connections
~700K messages/second peak throughput
~5000 WebSocket servers needed
P99 delivery latency: < 100ms
Message storage: ~4 TB text/day, ~1000 TB media/day
Snowflake ID: 64-bit, 4096 IDs/ms/machine
Cassandra RF=3, QUORUM for consistency
Redis key TTL: presence=30s, session=30d
Fan-out threshold: 200 members (write vs read)
```

### Common Follow-up Questions

**Q: How do you handle message ordering?**
> Snowflake IDs are time-sortable. Client shows messages sorted by server-assigned Snowflake ID. For same-millisecond conflicts, use (senderId + clientSeq) for deterministic ordering.

**Q: What if Kafka is down?**
> Message Service writes directly to Cassandra synchronously (durability). WebSocket server retries Kafka publish with exponential backoff. No message loss — just delayed fan-out.

**Q: How do you scale the WebSocket tier?**
> Horizontal: more servers. Sticky sessions at LB level for initial routing. Connection state in Redis (not local) — server stateless except for active sockets. Auto-scale based on connection count.

**Q: How does a user reconnect after disconnect?**
> Client stores last seen messageId per conversation. On reconnect: auth, then GET /messages?since={lastId} for each active conversation. Server sends queued messages from Cassandra.

**Q: How to handle very large groups (100K members)?**
> Pure fan-out on read. Store message once. Notify: "new message in conversation X". Clients poll (or receive lightweight notification) and fetch full message. Reduces fan-out from O(n) writes to O(1) write + O(n) reads on demand.

**Q: Trade-off between consistency and availability?**
> We choose AP (availability + partition tolerance) per CAP theorem. Messages are eventually consistent. Causal ordering guaranteed (Snowflake IDs). In case of partition: messages written, fan-out delayed. Users see "Sent" but not "Delivered" until partition heals.

**Q: How do you implement message search with E2EE?**
> Server-side search impossible for E2EE (can't read content). Solutions: (1) client-side index stored encrypted on device, (2) client-side search in app memory, (3) for non-E2EE: Elasticsearch server-side index.

---

*This document covers the complete system design for a production-grade chat/messaging system at FAANG scale. Each section maps to a real interview prompt — use the HLD for 20-minute sessions, add LLD code for 45-minute deep dives.*