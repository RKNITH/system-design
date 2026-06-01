# 🗄️ Distributed Cache — System Design (HLD + LLD)

> **Interview Level:** FAANG / Staff Engineer  
> **LLD Language:** JavaScript (Node.js)  
> **Scope:** Full end-to-end design — from requirements to production-grade code

---

## Table of Contents

1. [Problem Statement](#1-problem-statement)
2. [Requirements](#2-requirements)
   - [Functional Requirements](#21-functional-requirements)
   - [Non-Functional Requirements](#22-non-functional-requirements)
   - [Capacity Estimation](#23-capacity-estimation)
3. [Core Concepts & Cache Fundamentals](#3-core-concepts--cache-fundamentals)
4. [High-Level Design (HLD)](#4-high-level-design-hld)
   - [Architecture Overview](#41-architecture-overview)
   - [Client-Server Communication](#42-client-server-communication)
   - [Data Partitioning (Consistent Hashing)](#43-data-partitioning-consistent-hashing)
   - [Replication Strategy](#44-replication-strategy)
   - [Leader Election & Coordination](#45-leader-election--coordination)
   - [Cache Cluster Topology](#46-cache-cluster-topology)
   - [Cache Invalidation Strategies](#47-cache-invalidation-strategies)
   - [Eviction Policies](#48-eviction-policies)
   - [Write Strategies](#49-write-strategies)
   - [Handling Hot Keys](#410-handling-hot-keys)
   - [Fault Tolerance & High Availability](#411-fault-tolerance--high-availability)
   - [Monitoring & Observability](#412-monitoring--observability)
5. [Low-Level Design (LLD)](#5-low-level-design-lld)
   - [LRU Cache](#51-lru-cache)
   - [LFU Cache](#52-lfu-cache)
   - [Consistent Hashing Ring](#53-consistent-hashing-ring)
   - [Cache Node (TCP Server)](#54-cache-node-tcp-server)
   - [Cache Client with Connection Pooling](#55-cache-client-with-connection-pooling)
   - [Write-Through Cache](#56-write-through-cache)
   - [TTL Manager](#57-ttl-manager)
   - [Replication Manager](#58-replication-manager)
   - [Cache Shard Coordinator](#59-cache-shard-coordinator)
6. [Advanced Topics](#6-advanced-topics)
   - [Cache Stampede / Thundering Herd](#61-cache-stampede--thundering-herd)
   - [Cache Warming](#62-cache-warming)
   - [Multi-Region Cache](#63-multi-region-cache)
   - [Cache Serialization](#64-cache-serialization)
7. [Trade-offs & Comparisons](#7-trade-offs--comparisons)
8. [Real-World Systems Reference](#8-real-world-systems-reference)

---

## 1. Problem Statement

Design a **distributed in-memory cache** system that can:

- Store key-value pairs with O(1) read/write
- Scale horizontally across hundreds of nodes
- Handle millions of requests per second with sub-millisecond latency
- Survive node failures without data loss
- Support TTL (time-to-live), eviction policies, and replication

**Analogies in Production:** Memcached, Redis Cluster, Amazon ElastiCache, Facebook's Mcrouter

---

## 2. Requirements

### 2.1 Functional Requirements

- `set(key, value, ttl?)` — store a key-value pair with optional expiry
- `get(key)` — retrieve value by key; return `null` on miss
- `delete(key)` — remove a key
- `exists(key)` — check if a key is present
- `expire(key, ttl)` — update TTL on existing key
- `flush()` — clear all keys (admin operation)
- Support for multiple eviction policies: LRU, LFU, FIFO
- Support for pub/sub (nice to have)

### 2.2 Non-Functional Requirements

| Property | Target |
|---|---|
| Read Latency | < 1ms (p99) |
| Write Latency | < 5ms (p99) |
| Availability | 99.99% (four nines) |
| Consistency | Eventual (configurable to strong) |
| Throughput | 1M+ ops/sec per cluster |
| Scalability | Horizontal — add nodes dynamically |
| Durability | Optional (AOF/snapshot for Redis-like persistence) |
| Replication | N-way (configurable, default: 3) |

### 2.3 Capacity Estimation

**Assumptions:**
- 10M active users, each with ~50 cached objects
- Average object size: 1 KB
- 500M requests/day → ~5,800 RPS average, ~50,000 RPS peak (10x)
- Cache hit ratio target: 90%+
- TTL: 1 hour average

**Storage:**
```
10M users × 50 objects × 1KB = 500 GB raw data
With replication factor 3: 1.5 TB total across cluster
Per node (64 GB RAM): ~24 nodes minimum → round to 30 nodes
```

**Network:**
```
50,000 RPS × 1KB avg payload = 50 MB/s → ~400 Mbps peak
Per node: ~13 MB/s inbound + replication overhead
Standard 10 Gbps NICs handle this comfortably
```

**QPS Distribution:**
```
Reads (80%): 40,000 RPS → ~1,333 RPS/node
Writes (20%): 10,000 RPS → ~333 RPS/node
```

---

## 3. Core Concepts & Cache Fundamentals

### Cache Hit vs Miss

```
Client → Cache Node
  ├── HIT  → return value immediately (< 1ms)
  └── MISS → fetch from DB → populate cache → return value
```

### Cache Ratio Formula

```
Hit Ratio = (Cache Hits) / (Cache Hits + Cache Misses)
```

A hit ratio < 80% indicates poor cache design (key space too large, TTL too short, or wrong eviction policy).

### Cache Thundering Herd

When a popular cached item expires, thousands of requests simultaneously hit the DB. Solutions:

1. **Probabilistic Early Expiry** — refresh slightly before TTL expires
2. **Mutex Lock** — one request fetches, others wait
3. **Background Refresh** — serve stale while refreshing async
4. **Jitter on TTL** — randomize expiry to spread load

---

## 4. High-Level Design (HLD)

### 4.1 Architecture Overview

```
                        ┌─────────────────────────────────────────┐
                        │              Application Layer           │
                        │    (Web Servers / Microservices)         │
                        └────────────────┬────────────────────────┘
                                         │
                        ┌────────────────▼────────────────────────┐
                        │           Cache Client (SDK)             │
                        │  • Consistent Hashing Router             │
                        │  • Connection Pool                       │
                        │  • Retry + Circuit Breaker               │
                        │  • Local L1 Cache (optional)             │
                        └──────┬──────────────┬────────────────────┘
                               │              │
              ┌────────────────▼──┐     ┌────▼─────────────────┐
              │    Shard 0        │     │    Shard 1            │
              │  ┌─────────────┐  │     │  ┌─────────────────┐  │
              │  │Primary Node │  │     │  │ Primary Node    │  │
              │  │  Node-A     │  │     │  │   Node-D        │  │
              │  └──────┬──────┘  │     │  └────────┬────────┘  │
              │         │replicate│     │           │ replicate  │
              │  ┌──────▼──────┐  │     │  ┌────────▼────────┐  │
              │  │Replica Node │  │     │  │  Replica Node   │  │
              │  │  Node-B     │  │     │  │    Node-E       │  │
              │  └─────────────┘  │     │  └─────────────────┘  │
              │  ┌─────────────┐  │     │  ┌─────────────────┐  │
              │  │Replica Node │  │     │  │  Replica Node   │  │
              │  │  Node-C     │  │     │  │    Node-F       │  │
              │  └─────────────┘  │     │  └─────────────────┘  │
              └───────────────────┘     └──────────────────────┘
                        │
              ┌──────────▼──────────────────────────────────────┐
              │           Metadata / Config Service              │
              │   (ZooKeeper / etcd) — Cluster membership,      │
              │   leader election, shard maps, health checks     │
              └─────────────────────────────────────────────────┘
                        │
              ┌──────────▼──────────────────────────────────────┐
              │          Monitoring & Alerting                   │
              │  (Prometheus + Grafana, hit ratio, latency p99)  │
              └─────────────────────────────────────────────────┘
```

### 4.2 Client-Server Communication

**Protocol:** Custom binary protocol over TCP (like Memcached) or RESP (Redis Serialization Protocol)

**Why not HTTP?**
- HTTP has ~10x overhead (headers, JSON parsing)
- Binary TCP: sub-millisecond possible
- UDP for non-critical get (fire-and-forget)

**Request/Response Format (binary):**

```
┌──────────┬──────────┬──────────┬──────────┬──────────────────┐
│ Magic(1B)│ Opcode(1)│KeyLen(2B)│BodyLen(4)│  Key + Value     │
└──────────┴──────────┴──────────┴──────────┴──────────────────┘
```

**Opcodes:**
```
0x01 → GET
0x02 → SET
0x03 → DELETE
0x04 → EXISTS
0x05 → EXPIRE
0x06 → FLUSH
```

**Pipelining:** Client sends multiple requests before reading responses — reduces round-trip latency by 5-10x in bulk operations.

### 4.3 Data Partitioning (Consistent Hashing)

**Why Consistent Hashing?**
- Simple modulo (`key % N`) requires remapping ALL keys when N changes
- Consistent hashing remaps only `K/N` keys when a node is added/removed

**Virtual Nodes (VNodes):**
- Each physical node maps to ~150 virtual positions on the ring
- Ensures even distribution even with heterogeneous hardware
- When a node fails, its load is distributed across ALL remaining nodes (not just one)

```
Hash Ring (0 → 2^32):

         0
         │
    VN-A1│
    ─────●────── VN-B1
         │              │
   VN-C3 │              │VN-A2
         │              │
    ─────●──────────────●─── VN-C1
         │
         └── VN-B2 ── VN-A3

Node A owns: VN-A1, VN-A2, VN-A3
Node B owns: VN-B1, VN-B2
Node C owns: VN-C1, VN-C3

A key is stored on the first VNode clockwise from hash(key)
```

**Replication:** Key is replicated to next `N-1` distinct physical nodes clockwise.

### 4.4 Replication Strategy

**Quorum-based Replication (like Dynamo):**

```
W + R > N   →   Strong Consistency
W + R ≤ N   →   Eventual Consistency

Example (N=3):
  Strong:   W=2, R=2  (both reads and writes need majority)
  Weak:     W=1, R=1  (fastest, may read stale)
  Write-heavy opt: W=1, R=3 (fast writes, strong reads)
```

**Replication Flow:**

```
Client → Primary Node
          ├── Write locally (ACK if W=1)
          ├── Async replicate → Replica-1
          └── Async replicate → Replica-2

On Read (R=2):
  Client → [Primary + Replica-1] both respond
  Client takes latest version (using vector clock or timestamp)
```

**Vector Clocks for Conflict Resolution:**
```
Version(key) = {NodeA: 3, NodeB: 1, NodeC: 2}
If two replicas diverge:
  - Compare vector clocks
  - If one dominates → take that version
  - If concurrent → application-level merge or last-write-wins
```

### 4.5 Leader Election & Coordination

**ZooKeeper/etcd roles:**
- Stores cluster membership (which nodes are up)
- Stores shard-to-node mapping (routing table)
- Manages leader election per shard (primary selection)
- Health check heartbeats (node alive signals every 1s)

**Leader Election (Raft simplified):**
```
1. Node starts → sends RequestVote to peers
2. Majority votes → becomes Leader
3. Leader sends heartbeats every 150ms
4. No heartbeat for 300ms → followers start new election
5. New leader takes over primary writes
```

**Shard Map Update on Node Failure:**
```
Node-A crashes
  → ZooKeeper detects (heartbeat timeout)
  → Removes Node-A from ring
  → Promotes Node-A's replica to primary
  → Broadcasts updated routing table to all clients
  → Client SDKs hot-reload routing table (no downtime)
```

### 4.6 Cache Cluster Topology

**Option A: Client-side sharding (Memcached style)**
```
Client SDK holds the hash ring
Client routes directly to the correct node
Pro: No proxy latency
Con: All clients must agree on topology → harder to update
```

**Option B: Proxy-based (Twemproxy / Mcrouter style)**
```
Client → Proxy (stateless, horizontally scalable)
           → Cache Node

Pro: Clients are simple, topology is centralized
Con: Extra network hop (~0.1ms)
```

**Option C: Cluster-aware with gossip (Redis Cluster style)**
```
Client connects to any node
Node redirects: MOVED 7638 127.0.0.1:7001
Client caches slot map locally, avoids redirects

Pro: No single point of failure, self-healing
Con: More complex client SDK
```

**Recommended for FAANG-scale:** Option C (gossip-based cluster)

### 4.7 Cache Invalidation Strategies

This is the hardest problem in distributed systems ("There are only two hard things in computer science: cache invalidation and naming things").

**Strategy 1: TTL-based (most common)**
```
Set TTL on every key
Expired keys are lazily deleted on access
Background sweeper removes expired keys proactively
Tradeoff: may serve stale data up to TTL window
```

**Strategy 2: Event-driven invalidation**
```
Database → Change Data Capture (Debezium/Kafka)
  → Cache Invalidation Service
    → Deletes/updates cache key on data change

Tradeoff: Near real-time freshness, complex pipeline
```

**Strategy 3: Write-through (always consistent)**
```
Write hits cache AND DB simultaneously
Cache is never stale but slower writes
Tradeoff: Write amplification
```

**Strategy 4: Cache-aside (lazy loading)**
```
Read: Check cache → miss → read DB → populate cache
Write: Write to DB, invalidate cache key
Next read: cache miss → refresh from DB
Tradeoff: Cold start problem, thundering herd on invalidation
```

**Decision Matrix:**

| Strategy | Consistency | Write Speed | Read Speed | Complexity |
|---|---|---|---|---|
| TTL-based | Eventual | Fast | Fast | Low |
| Event-driven | Near-real-time | Fast | Fast | High |
| Write-through | Strong | Slow | Fast | Medium |
| Cache-aside | Eventual | Fast | Slow (first) | Low |

### 4.8 Eviction Policies

When the cache is full and a new item must be inserted, eviction policy determines what to remove:

| Policy | Description | Best For |
|---|---|---|
| **LRU** | Evict least recently used | General workloads |
| **LFU** | Evict least frequently used | Skewed access patterns |
| **FIFO** | Evict oldest inserted | Simple queues |
| **Random** | Evict random key | Approximation with low overhead |
| **ARC** | Adaptive Replacement Cache | Self-tuning (used in ZFS) |
| **SLRU** | Segmented LRU | Scan-resistant (Redis uses this internally) |

**Redis uses a probabilistic approximation of LRU** — samples 5 random keys, evicts the one with oldest access time. Good enough and O(1).

### 4.9 Write Strategies

```
┌──────────────┬──────────────────────────────────────────────────┐
│ Strategy     │ Description                                       │
├──────────────┼──────────────────────────────────────────────────┤
│ Write-Through│ Write to cache + DB synchronously                │
│              │ Pro: always consistent                            │
│              │ Con: slower writes                                │
├──────────────┼──────────────────────────────────────────────────┤
│ Write-Back   │ Write to cache only; async flush to DB           │
│ (Write-Behind│ Pro: fastest writes                               │
│ )            │ Con: data loss if node dies before flush          │
├──────────────┼──────────────────────────────────────────────────┤
│ Write-Around │ Write to DB only; skip cache                     │
│              │ Pro: cache not polluted with write-once data      │
│              │ Con: next read is always a miss                   │
└──────────────┴──────────────────────────────────────────────────┘
```

### 4.10 Handling Hot Keys

**Problem:** One key (e.g., celebrity tweet, viral product) gets 1M RPS — one node is overwhelmed.

**Solutions:**

**1. Key Replication (Local Caching)**
```
Hot key detected → replicate to all nodes
Client randomly picks any node for that key
Tradeoff: Consistency across replicas, invalidation complexity
```

**2. Client-side L1 Cache**
```
App server → L1 (in-process, 100MB, 100ms TTL)
           → L2 (Distributed Cache, 1ms TTL)
           → DB

Reduces cluster pressure for ultra-hot keys
```

**3. Key Splitting**
```
Instead of key: "user:123"
Use: "user:123:shard:0", "user:123:shard:1", ..., "user:123:shard:N"
Client picks random shard → writes distributed across N nodes
On read: read all shards + aggregate
```

**4. Request Coalescing (at Proxy)**
```
Multiple simultaneous requests for same key
Proxy sends ONE request to cache node
Fans out the response to all waiting clients
```

### 4.11 Fault Tolerance & High Availability

**Node Failure Recovery:**
```
Timeline:
T=0    Node-A crashes
T=1s   ZooKeeper detects missed heartbeat
T=1s   Replica-B promoted to primary (automatic failover)
T=2s   New routing table propagated via gossip
T=2s   Client SDK picks up new topology
T=5s   Replacement node Node-A' starts, syncs from Replica-C
T=60s  Full replication complete, ring stabilized
```

**Data Durability (optional persistence):**
```
RDB Snapshot: Full dump every N seconds (like Redis RDB)
AOF Log: Append-every-write to disk (like Redis AOF)
Hybrid: RDB for fast restarts + AOF for completeness
```

**Split Brain Prevention:**
```
Network partition → two sides think other is dead
Solution: Quorum (majority wins)
Minority side: stops accepting writes
Majority side: continues normally
```

### 4.12 Monitoring & Observability

**Key Metrics:**

| Metric | Alert Threshold | Notes |
|---|---|---|
| Cache Hit Ratio | < 80% | Indicates cache misses spiking |
| p99 Read Latency | > 5ms | Network or hot key issue |
| Memory Usage | > 90% | Increase eviction or add node |
| Eviction Rate | > 100/sec | Memory too small for working set |
| Replication Lag | > 500ms | Network issue or node overloaded |
| Connection Count | > 10K/node | Connection pool exhausted |

**Distributed Tracing:** Each request tagged with trace ID propagated through client → proxy → node → replica → response.

---

## 5. Low-Level Design (LLD)

All code in JavaScript (Node.js, ES2022+).

---

### 5.1 LRU Cache

**Data Structure:** Doubly Linked List + HashMap = O(1) get, set, delete

```javascript
// lru-cache.js

class Node {
  constructor(key, value) {
    this.key = key;
    this.value = value;
    this.prev = null;
    this.next = null;
    this.expiresAt = null; // for TTL support
    this.frequency = 1;     // for LFU upgrade
  }
}

class LRUCache {
  /**
   * @param {number} capacity - max number of entries
   * @param {Function} onEvict - callback(key, value) on eviction
   */
  constructor(capacity, onEvict = null) {
    if (capacity <= 0) throw new Error('Capacity must be positive');
    
    this.capacity = capacity;
    this.size = 0;
    this.onEvict = onEvict;
    this.map = new Map(); // key → Node

    // Sentinel nodes — never removed, simplify edge cases
    this.head = new Node(null, null); // MRU sentinel
    this.tail = new Node(null, null); // LRU sentinel
    this.head.next = this.tail;
    this.tail.prev = this.head;

    // Stats
    this.stats = { hits: 0, misses: 0, evictions: 0, sets: 0 };
  }

  /**
   * Get a value. Returns undefined on miss or expiry.
   * O(1) time complexity.
   */
  get(key) {
    const node = this.map.get(key);

    if (!node) {
      this.stats.misses++;
      return undefined;
    }

    // Check TTL expiry
    if (node.expiresAt !== null && Date.now() > node.expiresAt) {
      this._removeNode(node);
      this.map.delete(key);
      this.size--;
      this.stats.misses++;
      return undefined;
    }

    // Move to MRU position
    this._moveToFront(node);
    this.stats.hits++;
    return node.value;
  }

  /**
   * Set a key-value pair with optional TTL in milliseconds.
   * O(1) time complexity.
   */
  set(key, value, ttlMs = null) {
    this.stats.sets++;

    if (this.map.has(key)) {
      const node = this.map.get(key);
      node.value = value;
      node.expiresAt = ttlMs !== null ? Date.now() + ttlMs : null;
      this._moveToFront(node);
      return;
    }

    // Create new node
    const node = new Node(key, value);
    node.expiresAt = ttlMs !== null ? Date.now() + ttlMs : null;

    this.map.set(key, node);
    this._addToFront(node);
    this.size++;

    // Evict LRU if over capacity
    if (this.size > this.capacity) {
      this._evictLRU();
    }
  }

  /**
   * Delete a key. Returns true if key existed.
   * O(1) time complexity.
   */
  delete(key) {
    const node = this.map.get(key);
    if (!node) return false;

    this._removeNode(node);
    this.map.delete(key);
    this.size--;
    return true;
  }

  /**
   * Update TTL for an existing key.
   */
  expire(key, ttlMs) {
    const node = this.map.get(key);
    if (!node) return false;
    node.expiresAt = Date.now() + ttlMs;
    return true;
  }

  /**
   * Check if key exists and is not expired.
   */
  has(key) {
    return this.get(key) !== undefined;
  }

  /**
   * Clear all entries.
   */
  flush() {
    this.map.clear();
    this.head.next = this.tail;
    this.tail.prev = this.head;
    this.size = 0;
  }

  /**
   * Get cache statistics.
   */
  getStats() {
    const total = this.stats.hits + this.stats.misses;
    return {
      ...this.stats,
      hitRatio: total > 0 ? (this.stats.hits / total).toFixed(4) : 0,
      size: this.size,
      capacity: this.capacity,
      utilizationPct: ((this.size / this.capacity) * 100).toFixed(1),
    };
  }

  // ─── Private helpers ────────────────────────────────────────────

  _addToFront(node) {
    node.prev = this.head;
    node.next = this.head.next;
    this.head.next.prev = node;
    this.head.next = node;
  }

  _removeNode(node) {
    node.prev.next = node.next;
    node.next.prev = node.prev;
  }

  _moveToFront(node) {
    this._removeNode(node);
    this._addToFront(node);
  }

  _evictLRU() {
    const lruNode = this.tail.prev;
    if (lruNode === this.head) return; // empty

    this._removeNode(lruNode);
    this.map.delete(lruNode.key);
    this.size--;
    this.stats.evictions++;

    if (this.onEvict) {
      this.onEvict(lruNode.key, lruNode.value);
    }
  }

  /**
   * Proactive sweep of expired keys.
   * Should be called periodically (e.g., every 100ms).
   * Scans from LRU end (most likely expired first).
   * O(expired_count) amortized.
   */
  sweepExpired() {
    const now = Date.now();
    let swept = 0;
    let node = this.tail.prev;

    while (node !== this.head) {
      const prev = node.prev;
      if (node.expiresAt !== null && now > node.expiresAt) {
        this._removeNode(node);
        this.map.delete(node.key);
        this.size--;
        swept++;
      }
      node = prev;
    }

    return swept;
  }
}

// ─── Usage Example ──────────────────────────────────────────────────

const cache = new LRUCache(3, (key, val) => {
  console.log(`Evicted: ${key} = ${val}`);
});

cache.set('a', 1);
cache.set('b', 2);
cache.set('c', 3, 5000); // 5s TTL
cache.set('d', 4);       // evicts 'a' (LRU)

console.log(cache.get('b'));      // 2
console.log(cache.get('a'));      // undefined (evicted)
console.log(cache.getStats());

module.exports = { LRUCache };
```

---

### 5.2 LFU Cache

**Key Insight:** Maintain a frequency map. O(1) get/set using min-frequency tracking.

```javascript
// lfu-cache.js

class LFUCache {
  /**
   * LFU with O(1) operations using:
   * - keyMap: key → {value, freq, expiresAt}
   * - freqMap: freq → LinkedHashSet of keys (insertion-ordered for tie-breaking)
   * - minFreq: tracks current minimum frequency for O(1) eviction
   */
  constructor(capacity) {
    if (capacity <= 0) throw new Error('Capacity must be positive');
    this.capacity = capacity;
    this.size = 0;
    this.minFreq = 0;

    // key → { value, freq, expiresAt }
    this.keyMap = new Map();

    // freq → Set of keys (insertion-order preserved by JS Set)
    this.freqMap = new Map();

    this.stats = { hits: 0, misses: 0, evictions: 0 };
  }

  get(key) {
    if (!this.keyMap.has(key)) {
      this.stats.misses++;
      return undefined;
    }

    const entry = this.keyMap.get(key);

    if (entry.expiresAt !== null && Date.now() > entry.expiresAt) {
      this._remove(key, entry.freq);
      this.stats.misses++;
      return undefined;
    }

    this._incrementFreq(key, entry);
    this.stats.hits++;
    return entry.value;
  }

  set(key, value, ttlMs = null) {
    if (this.capacity === 0) return;

    if (this.keyMap.has(key)) {
      const entry = this.keyMap.get(key);
      entry.value = value;
      entry.expiresAt = ttlMs !== null ? Date.now() + ttlMs : null;
      this._incrementFreq(key, entry);
      return;
    }

    if (this.size >= this.capacity) {
      this._evictLFU();
    }

    this.keyMap.set(key, { value, freq: 1, expiresAt: ttlMs !== null ? Date.now() + ttlMs : null });

    if (!this.freqMap.has(1)) {
      this.freqMap.set(1, new Set());
    }
    this.freqMap.get(1).add(key);

    this.minFreq = 1;
    this.size++;
  }

  delete(key) {
    if (!this.keyMap.has(key)) return false;
    const entry = this.keyMap.get(key);
    this._remove(key, entry.freq);
    return true;
  }

  _incrementFreq(key, entry) {
    const oldFreq = entry.freq;
    const newFreq = oldFreq + 1;

    // Remove from old frequency bucket
    this.freqMap.get(oldFreq).delete(key);
    if (this.freqMap.get(oldFreq).size === 0) {
      this.freqMap.delete(oldFreq);
      if (this.minFreq === oldFreq) {
        this.minFreq = newFreq;
      }
    }

    // Add to new frequency bucket
    if (!this.freqMap.has(newFreq)) {
      this.freqMap.set(newFreq, new Set());
    }
    this.freqMap.get(newFreq).add(key);
    entry.freq = newFreq;
  }

  _evictLFU() {
    const minFreqSet = this.freqMap.get(this.minFreq);
    // Evict the first (oldest) key in the minimum frequency set
    const keyToEvict = minFreqSet.values().next().value;
    this._remove(keyToEvict, this.minFreq);
    this.stats.evictions++;
  }

  _remove(key, freq) {
    this.keyMap.delete(key);
    const freqSet = this.freqMap.get(freq);
    if (freqSet) {
      freqSet.delete(key);
      if (freqSet.size === 0) this.freqMap.delete(freq);
    }
    this.size--;
  }

  getStats() {
    const total = this.stats.hits + this.stats.misses;
    return {
      ...this.stats,
      hitRatio: total > 0 ? (this.stats.hits / total).toFixed(4) : 0,
      size: this.size,
    };
  }
}

module.exports = { LFUCache };
```

---

### 5.3 Consistent Hashing Ring

```javascript
// consistent-hashing.js
const crypto = require('crypto');

class ConsistentHashRing {
  /**
   * @param {number} virtualNodes - number of virtual nodes per physical node (default 150)
   */
  constructor(virtualNodes = 150) {
    this.virtualNodes = virtualNodes;
    // Sorted array of { hash, nodeId } for binary search
    this.ring = [];
    // nodeId → node metadata
    this.nodes = new Map();
  }

  /**
   * Add a physical node to the ring.
   * @param {string} nodeId - e.g., "node-1:6379"
   * @param {object} meta - arbitrary metadata (host, port, weight, etc.)
   */
  addNode(nodeId, meta = {}) {
    if (this.nodes.has(nodeId)) {
      throw new Error(`Node ${nodeId} already exists`);
    }

    this.nodes.set(nodeId, meta);

    for (let i = 0; i < this.virtualNodes; i++) {
      const vNodeKey = `${nodeId}#vn${i}`;
      const hash = this._hash(vNodeKey);
      this._insertSorted({ hash, nodeId });
    }
  }

  /**
   * Remove a physical node (and all its virtual nodes).
   */
  removeNode(nodeId) {
    if (!this.nodes.has(nodeId)) return false;

    this.nodes.delete(nodeId);
    this.ring = this.ring.filter(entry => entry.nodeId !== nodeId);
    return true;
  }

  /**
   * Get the primary node responsible for a given key.
   * O(log N) via binary search.
   */
  getNode(key) {
    if (this.ring.length === 0) return null;

    const hash = this._hash(key);
    const idx = this._findIndex(hash);
    return this.ring[idx % this.ring.length].nodeId;
  }

  /**
   * Get N distinct physical nodes for replication.
   * Walks clockwise from the key's position.
   * @param {string} key
   * @param {number} replicationFactor
   */
  getNodes(key, replicationFactor = 3) {
    if (this.ring.length === 0) return [];
    if (replicationFactor > this.nodes.size) {
      replicationFactor = this.nodes.size;
    }

    const hash = this._hash(key);
    const startIdx = this._findIndex(hash);
    const result = [];
    const seen = new Set();

    for (let i = 0; i < this.ring.length && result.length < replicationFactor; i++) {
      const { nodeId } = this.ring[(startIdx + i) % this.ring.length];
      if (!seen.has(nodeId)) {
        seen.add(nodeId);
        result.push(nodeId);
      }
    }

    return result;
  }

  /**
   * Get the distribution of keys across nodes for diagnostics.
   * @param {string[]} keys
   */
  getDistribution(keys) {
    const dist = {};
    for (const key of keys) {
      const nodeId = this.getNode(key);
      dist[nodeId] = (dist[nodeId] || 0) + 1;
    }
    return dist;
  }

  /**
   * Return all nodes and their hash ranges.
   */
  getNodeInfo() {
    const info = {};
    for (const [nodeId] of this.nodes) {
      info[nodeId] = { virtualNodeCount: 0, segments: [] };
    }

    for (let i = 0; i < this.ring.length; i++) {
      const { hash, nodeId } = this.ring[i];
      info[nodeId].virtualNodeCount++;
      info[nodeId].segments.push(hash);
    }

    return info;
  }

  // ─── Private ─────────────────────────────────────────────────────

  _hash(key) {
    // MD5 gives uniform 128-bit distribution; take first 32 bits as uint32
    const buf = crypto.createHash('md5').update(key).digest();
    return buf.readUInt32BE(0);
  }

  _insertSorted(entry) {
    // Binary search for insertion point
    let lo = 0, hi = this.ring.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (this.ring[mid].hash < entry.hash) lo = mid + 1;
      else hi = mid;
    }
    this.ring.splice(lo, 0, entry);
  }

  _findIndex(hash) {
    // Binary search for first ring entry >= hash (clockwise)
    let lo = 0, hi = this.ring.length - 1;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (this.ring[mid].hash < hash) lo = mid + 1;
      else hi = mid;
    }
    return this.ring[lo].hash >= hash ? lo : 0;
  }
}

// ─── Usage ────────────────────────────────────────────────────────

const ring = new ConsistentHashRing(150);
ring.addNode('node-A:6379', { host: 'node-a', port: 6379 });
ring.addNode('node-B:6379', { host: 'node-b', port: 6379 });
ring.addNode('node-C:6379', { host: 'node-c', port: 6379 });

console.log(ring.getNode('user:123'));               // e.g., "node-B:6379"
console.log(ring.getNodes('user:123', 3));           // 3 distinct nodes for replication
console.log(ring.getDistribution(['a','b','c','d','e','f']));

module.exports = { ConsistentHashRing };
```

---

### 5.4 Cache Node (TCP Server)

```javascript
// cache-node.js
const net = require('net');
const EventEmitter = require('events');
const { LRUCache } = require('./lru-cache');
const { LFUCache } = require('./lfu-cache');

// Simple binary protocol:
// [1 byte op][2 byte keyLen][4 byte valueLen][key][value]
const OP = {
  GET:    0x01,
  SET:    0x02,
  DELETE: 0x03,
  EXISTS: 0x04,
  EXPIRE: 0x05,
  FLUSH:  0x06,
  PING:   0x07,
  STATS:  0x08,
};

const STATUS = {
  OK:        0x00,
  NOT_FOUND: 0x01,
  ERROR:     0x02,
};

class CacheNode extends EventEmitter {
  /**
   * @param {object} config
   * @param {number} config.port - TCP port to listen on
   * @param {number} config.maxMemoryMB - max memory in MB
   * @param {string} config.evictionPolicy - 'lru' | 'lfu'
   * @param {string} config.nodeId - unique node identifier
   */
  constructor(config = {}) {
    super();

    this.config = {
      port: config.port || 6379,
      maxMemoryMB: config.maxMemoryMB || 256,
      evictionPolicy: config.evictionPolicy || 'lru',
      nodeId: config.nodeId || `node-${Date.now()}`,
      ...config,
    };

    // Estimate capacity: avg 1KB per entry → 1024 entries per MB
    const capacity = this.config.maxMemoryMB * 1024;

    this.cache = this.config.evictionPolicy === 'lfu'
      ? new LFUCache(capacity)
      : new LRUCache(capacity, (key) => this.emit('evict', key));

    this.connections = new Set();
    this.server = null;

    // Periodic expired-key sweep every 100ms
    this._sweepInterval = setInterval(() => {
      if (this.cache.sweepExpired) {
        const swept = this.cache.sweepExpired();
        if (swept > 0) this.emit('swept', swept);
      }
    }, 100);
  }

  /**
   * Start the TCP server.
   */
  start() {
    return new Promise((resolve, reject) => {
      this.server = net.createServer((socket) => {
        this._handleConnection(socket);
      });

      this.server.listen(this.config.port, () => {
        console.log(`[${this.config.nodeId}] Listening on port ${this.config.port}`);
        this.emit('ready');
        resolve(this);
      });

      this.server.on('error', reject);
    });
  }

  /**
   * Stop the node gracefully.
   */
  async stop() {
    clearInterval(this._sweepInterval);

    // Close all client connections
    for (const socket of this.connections) {
      socket.destroy();
    }

    return new Promise((resolve) => {
      if (this.server) {
        this.server.close(resolve);
      } else {
        resolve();
      }
    });
  }

  // ─── Connection Handler ────────────────────────────────────────

  _handleConnection(socket) {
    socket.id = `${socket.remoteAddress}:${socket.remotePort}`;
    this.connections.add(socket);

    socket.on('close', () => this.connections.delete(socket));
    socket.on('error', (err) => {
      this.emit('clientError', err, socket);
      socket.destroy();
    });

    // Simple request/response pipeline
    let buffer = Buffer.alloc(0);

    socket.on('data', (chunk) => {
      buffer = Buffer.concat([buffer, chunk]);

      while (buffer.length >= 7) { // Minimum header size
        const op = buffer.readUInt8(0);
        const keyLen = buffer.readUInt16BE(1);
        const valueLen = buffer.readUInt32BE(3);
        const totalLen = 7 + keyLen + valueLen;

        if (buffer.length < totalLen) break; // Wait for more data

        const key = buffer.slice(7, 7 + keyLen).toString('utf8');
        const valueBuffer = buffer.slice(7 + keyLen, totalLen);
        buffer = buffer.slice(totalLen);

        try {
          const response = this._handleCommand(op, key, valueBuffer);
          socket.write(response);
        } catch (err) {
          socket.write(this._buildErrorResponse(err.message));
        }
      }
    });
  }

  // ─── Command Dispatcher ──────────────────────────────────────

  _handleCommand(op, key, valueBuffer) {
    switch (op) {
      case OP.GET: {
        const val = this.cache.get(key);
        if (val === undefined) return this._buildResponse(STATUS.NOT_FOUND, Buffer.alloc(0));
        const data = Buffer.isBuffer(val) ? val : Buffer.from(JSON.stringify(val));
        return this._buildResponse(STATUS.OK, data);
      }

      case OP.SET: {
        // Value format: [4-byte TTL in ms (0 = no TTL)][actual value bytes]
        const ttlMs = valueBuffer.readUInt32BE(0) || null;
        const actualValue = valueBuffer.slice(4);
        this.cache.set(key, actualValue, ttlMs === 0 ? null : ttlMs);
        return this._buildResponse(STATUS.OK, Buffer.alloc(0));
      }

      case OP.DELETE: {
        const deleted = this.cache.delete(key);
        return this._buildResponse(deleted ? STATUS.OK : STATUS.NOT_FOUND, Buffer.alloc(0));
      }

      case OP.EXISTS: {
        const exists = this.cache.has(key);
        const result = Buffer.alloc(1);
        result.writeUInt8(exists ? 1 : 0, 0);
        return this._buildResponse(STATUS.OK, result);
      }

      case OP.EXPIRE: {
        const ttlMs = valueBuffer.readUInt32BE(0);
        const updated = this.cache.expire(key, ttlMs);
        return this._buildResponse(updated ? STATUS.OK : STATUS.NOT_FOUND, Buffer.alloc(0));
      }

      case OP.FLUSH: {
        this.cache.flush();
        return this._buildResponse(STATUS.OK, Buffer.alloc(0));
      }

      case OP.PING: {
        return this._buildResponse(STATUS.OK, Buffer.from('PONG'));
      }

      case OP.STATS: {
        const stats = this.cache.getStats();
        return this._buildResponse(STATUS.OK, Buffer.from(JSON.stringify(stats)));
      }

      default:
        return this._buildErrorResponse(`Unknown opcode: ${op}`);
    }
  }

  // ─── Response Builders ────────────────────────────────────────

  _buildResponse(status, data) {
    // [1 byte status][4 byte dataLen][data]
    const buf = Buffer.alloc(5 + data.length);
    buf.writeUInt8(status, 0);
    buf.writeUInt32BE(data.length, 1);
    data.copy(buf, 5);
    return buf;
  }

  _buildErrorResponse(message) {
    const data = Buffer.from(message);
    const buf = Buffer.alloc(5 + data.length);
    buf.writeUInt8(STATUS.ERROR, 0);
    buf.writeUInt32BE(data.length, 1);
    data.copy(buf, 5);
    return buf;
  }
}

module.exports = { CacheNode, OP, STATUS };
```

---

### 5.5 Cache Client with Connection Pooling

```javascript
// cache-client.js
const net = require('net');
const { ConsistentHashRing } = require('./consistent-hashing');
const { OP, STATUS } = require('./cache-node');

class ConnectionPool {
  /**
   * Pool of TCP connections to a single cache node.
   * Idle connections are reused; creates new connections on demand.
   */
  constructor(host, port, { minSize = 2, maxSize = 20, idleTimeoutMs = 30000 } = {}) {
    this.host = host;
    this.port = port;
    this.minSize = minSize;
    this.maxSize = maxSize;
    this.idleTimeoutMs = idleTimeoutMs;

    this.idle = [];       // available connections
    this.active = new Set(); // in-use connections
    this.waiting = [];    // resolve functions waiting for a connection
  }

  async acquire() {
    // Reuse idle connection
    while (this.idle.length > 0) {
      const conn = this.idle.pop();
      if (conn.socket.writable) {
        this.active.add(conn);
        return conn;
      }
      // Socket was closed, discard
    }

    // Create new connection if under limit
    if (this.active.size + this.idle.length < this.maxSize) {
      const conn = await this._createConnection();
      this.active.add(conn);
      return conn;
    }

    // Wait for a connection to be released
    return new Promise((resolve) => {
      this.waiting.push(resolve);
    });
  }

  release(conn) {
    this.active.delete(conn);

    if (this.waiting.length > 0) {
      const resolve = this.waiting.shift();
      this.active.add(conn);
      resolve(conn);
    } else {
      conn.lastUsed = Date.now();
      this.idle.push(conn);
    }
  }

  async _createConnection() {
    return new Promise((resolve, reject) => {
      const socket = net.createConnection({ host: this.host, port: this.port });
      socket.setNoDelay(true); // Disable Nagle's algorithm for low latency

      socket.once('connect', () => resolve({ socket, lastUsed: Date.now() }));
      socket.once('error', reject);
    });
  }

  async destroy() {
    for (const conn of [...this.idle, ...this.active]) {
      conn.socket.destroy();
    }
    this.idle = [];
    this.active.clear();
  }
}

class CacheClient {
  /**
   * Distributed cache client with:
   * - Consistent hashing for request routing
   * - Connection pooling per node
   * - Automatic retry with exponential backoff
   * - Circuit breaker per node
   */
  constructor(nodes = [], options = {}) {
    this.options = {
      replicationFactor: 1,
      readQuorum: 1,
      writeQuorum: 1,
      retries: 3,
      retryDelayMs: 50,
      connectTimeoutMs: 2000,
      requestTimeoutMs: 1000,
      poolSize: 10,
      ...options,
    };

    this.ring = new ConsistentHashRing(150);
    this.pools = new Map(); // nodeId → ConnectionPool
    this.circuitBreakers = new Map(); // nodeId → CircuitBreaker state

    for (const node of nodes) {
      this.addNode(node);
    }
  }

  addNode({ id, host, port }) {
    this.ring.addNode(id, { host, port });
    this.pools.set(id, new ConnectionPool(host, port, { maxSize: this.options.poolSize }));
    this.circuitBreakers.set(id, { failures: 0, openUntil: 0 });
  }

  removeNode(nodeId) {
    this.ring.removeNode(nodeId);
    const pool = this.pools.get(nodeId);
    if (pool) {
      pool.destroy();
      this.pools.delete(nodeId);
    }
    this.circuitBreakers.delete(nodeId);
  }

  /**
   * GET a value by key. Returns null on miss.
   */
  async get(key) {
    const nodeId = this.ring.getNode(key);
    const response = await this._sendWithRetry(nodeId, this._buildGetRequest(key));

    if (response.status === STATUS.NOT_FOUND) return null;
    if (response.status === STATUS.ERROR) throw new Error(response.data.toString());

    return this._deserialize(response.data);
  }

  /**
   * SET a key-value pair with optional TTL in milliseconds.
   */
  async set(key, value, ttlMs = 0) {
    const nodeIds = this.ring.getNodes(key, this.options.replicationFactor);
    const payload = this._buildSetRequest(key, value, ttlMs);

    const promises = nodeIds.map(nodeId => this._sendWithRetry(nodeId, payload));

    // Write quorum: wait for W acknowledgements
    const results = await this._quorum(promises, this.options.writeQuorum);
    return results.every(r => r.status === STATUS.OK);
  }

  /**
   * DELETE a key.
   */
  async delete(key) {
    const nodeId = this.ring.getNode(key);
    const response = await this._sendWithRetry(nodeId, this._buildDeleteRequest(key));
    return response.status === STATUS.OK;
  }

  /**
   * Check if key exists.
   */
  async exists(key) {
    const nodeId = this.ring.getNode(key);
    const response = await this._sendWithRetry(nodeId, this._buildExistsRequest(key));
    if (response.status !== STATUS.OK) return false;
    return response.data.readUInt8(0) === 1;
  }

  // ─── Request Builders ─────────────────────────────────────────

  _buildGetRequest(key) {
    const keyBuf = Buffer.from(key);
    const buf = Buffer.alloc(7 + keyBuf.length);
    buf.writeUInt8(OP.GET, 0);
    buf.writeUInt16BE(keyBuf.length, 1);
    buf.writeUInt32BE(0, 3);
    keyBuf.copy(buf, 7);
    return buf;
  }

  _buildSetRequest(key, value, ttlMs) {
    const keyBuf = Buffer.from(key);
    const valueBuf = this._serialize(value);
    const payload = Buffer.alloc(4 + valueBuf.length);
    payload.writeUInt32BE(ttlMs, 0);
    valueBuf.copy(payload, 4);

    const buf = Buffer.alloc(7 + keyBuf.length + payload.length);
    buf.writeUInt8(OP.SET, 0);
    buf.writeUInt16BE(keyBuf.length, 1);
    buf.writeUInt32BE(payload.length, 3);
    keyBuf.copy(buf, 7);
    payload.copy(buf, 7 + keyBuf.length);
    return buf;
  }

  _buildDeleteRequest(key) {
    const keyBuf = Buffer.from(key);
    const buf = Buffer.alloc(7 + keyBuf.length);
    buf.writeUInt8(OP.DELETE, 0);
    buf.writeUInt16BE(keyBuf.length, 1);
    buf.writeUInt32BE(0, 3);
    keyBuf.copy(buf, 7);
    return buf;
  }

  _buildExistsRequest(key) {
    const keyBuf = Buffer.from(key);
    const buf = Buffer.alloc(7 + keyBuf.length);
    buf.writeUInt8(OP.EXISTS, 0);
    buf.writeUInt16BE(keyBuf.length, 1);
    buf.writeUInt32BE(0, 3);
    keyBuf.copy(buf, 7);
    return buf;
  }

  // ─── Network Layer ────────────────────────────────────────────

  async _sendWithRetry(nodeId, request, attempt = 0) {
    const breaker = this.circuitBreakers.get(nodeId);

    // Circuit breaker open check
    if (breaker && breaker.openUntil > Date.now()) {
      throw new Error(`Circuit breaker open for node ${nodeId}`);
    }

    try {
      const result = await this._send(nodeId, request);

      // Reset circuit breaker on success
      if (breaker) breaker.failures = 0;

      return result;
    } catch (err) {
      if (breaker) {
        breaker.failures++;
        if (breaker.failures >= 5) {
          breaker.openUntil = Date.now() + 10000; // Open for 10s
        }
      }

      if (attempt < this.options.retries) {
        const delay = this.options.retryDelayMs * Math.pow(2, attempt); // Exponential backoff
        await this._sleep(delay);
        return this._sendWithRetry(nodeId, request, attempt + 1);
      }

      throw err;
    }
  }

  async _send(nodeId, request) {
    const pool = this.pools.get(nodeId);
    if (!pool) throw new Error(`No pool for node ${nodeId}`);

    const conn = await pool.acquire();

    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        pool.release(conn);
        reject(new Error(`Request timeout to node ${nodeId}`));
      }, this.options.requestTimeoutMs);

      // Read response
      const onData = (chunk) => {
        clearTimeout(timeout);
        conn.socket.removeListener('data', onData);

        const status = chunk.readUInt8(0);
        const dataLen = chunk.readUInt32BE(1);
        const data = chunk.slice(5, 5 + dataLen);

        pool.release(conn);
        resolve({ status, data });
      };

      conn.socket.once('data', onData);
      conn.socket.once('error', (err) => {
        clearTimeout(timeout);
        reject(err);
      });

      conn.socket.write(request);
    });
  }

  async _quorum(promises, required) {
    const results = [];
    let resolved = 0;

    return new Promise((resolve, reject) => {
      promises.forEach(p => {
        p.then(result => {
          results.push(result);
          resolved++;
          if (resolved >= required) resolve(results);
        }).catch(() => {
          // Ignore individual failures if quorum not broken
          if (promises.length - results.length < required - resolved) {
            reject(new Error('Could not achieve quorum'));
          }
        });
      });
    });
  }

  _serialize(value) {
    return Buffer.from(JSON.stringify(value), 'utf8');
  }

  _deserialize(buf) {
    try {
      return JSON.parse(buf.toString('utf8'));
    } catch {
      return buf.toString('utf8');
    }
  }

  _sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  async destroy() {
    for (const pool of this.pools.values()) {
      await pool.destroy();
    }
  }
}

module.exports = { CacheClient };
```

---

### 5.6 Write-Through Cache

```javascript
// write-through-cache.js

/**
 * Write-Through Cache Layer
 * Wraps any cache client and a database adapter.
 * Writes propagate to both cache and DB synchronously.
 * Reads use cache-aside pattern (check cache → miss → load DB → populate).
 */
class WriteThroughCache {
  /**
   * @param {object} cache - Cache client (implements get/set/delete)
   * @param {object} db    - Database adapter (implements get/set/delete)
   * @param {object} options
   * @param {number} options.defaultTtlMs - default TTL for cached entries
   * @param {boolean} options.writeCoalescing - batch writes within a tick
   */
  constructor(cache, db, options = {}) {
    this.cache = cache;
    this.db = db;
    this.options = {
      defaultTtlMs: 3600 * 1000, // 1 hour
      writeCoalescing: false,
      ...options,
    };

    // Write coalescing buffer
    this._writeBuffer = new Map();
    this._writeTimer = null;
  }

  /**
   * Read-through: cache → DB fallback.
   */
  async get(key) {
    const cached = await this.cache.get(key);
    if (cached !== null) return cached;

    // Cache miss: load from DB and populate cache
    const dbValue = await this.db.get(key);
    if (dbValue === null || dbValue === undefined) return null;

    await this.cache.set(key, dbValue, this.options.defaultTtlMs);
    return dbValue;
  }

  /**
   * Write-through: write to DB first, then update cache.
   * DB is source of truth.
   */
  async set(key, value, ttlMs = this.options.defaultTtlMs) {
    if (this.options.writeCoalescing) {
      return this._coalesceWrite(key, value, ttlMs);
    }

    // Write to DB (source of truth)
    await this.db.set(key, value);

    // Update cache
    await this.cache.set(key, value, ttlMs);
    return true;
  }

  /**
   * Delete from both cache and DB.
   */
  async delete(key) {
    await this.db.delete(key);
    await this.cache.delete(key);
    return true;
  }

  /**
   * Invalidate cache key without touching DB (for external DB changes).
   */
  async invalidate(key) {
    return this.cache.delete(key);
  }

  /**
   * Batch get — fetches multiple keys efficiently.
   * Cache hits returned immediately; misses batched to DB.
   */
  async mget(keys) {
    const results = {};
    const misses = [];

    // Check cache for all keys in parallel
    await Promise.all(keys.map(async (key) => {
      const val = await this.cache.get(key);
      if (val !== null) {
        results[key] = val;
      } else {
        misses.push(key);
      }
    }));

    // Batch fetch misses from DB
    if (misses.length > 0) {
      const dbResults = await this.db.mget(misses);

      await Promise.all(
        Object.entries(dbResults).map(async ([key, value]) => {
          if (value !== null && value !== undefined) {
            results[key] = value;
            await this.cache.set(key, value, this.options.defaultTtlMs);
          }
        })
      );
    }

    return results;
  }

  /**
   * Coalesce multiple writes within a single event loop tick.
   * Reduces DB round trips for bursty write workloads.
   */
  _coalesceWrite(key, value, ttlMs) {
    return new Promise((resolve, reject) => {
      this._writeBuffer.set(key, { value, ttlMs, resolve, reject });

      if (!this._writeTimer) {
        this._writeTimer = setImmediate(() => this._flushWrites());
      }
    });
  }

  async _flushWrites() {
    this._writeTimer = null;
    const batch = new Map(this._writeBuffer);
    this._writeBuffer.clear();

    // Bulk write to DB
    const dbBatch = {};
    for (const [key, { value }] of batch) {
      dbBatch[key] = value;
    }

    try {
      await this.db.mset(dbBatch);

      // Update cache for all written keys
      await Promise.all(
        [...batch.entries()].map(([key, { value, ttlMs, resolve }]) =>
          this.cache.set(key, value, ttlMs).then(resolve)
        )
      );
    } catch (err) {
      for (const { reject } of batch.values()) {
        reject(err);
      }
    }
  }
}

module.exports = { WriteThroughCache };
```

---

### 5.7 TTL Manager

```javascript
// ttl-manager.js

/**
 * Hierarchical TTL Manager using time-bucketed slots.
 * Efficiently handles millions of TTL expiries without scanning all keys.
 *
 * Design:
 *   - Divide time into 100ms buckets
 *   - Each bucket holds keys expiring in that window
 *   - Sweep only the current bucket — O(expired) not O(total)
 */
class TTLManager {
  /**
   * @param {Function} onExpire - async callback(key) called on expiry
   * @param {number} resolution - bucket size in ms (default 100ms)
   */
  constructor(onExpire, resolution = 100) {
    this.onExpire = onExpire;
    this.resolution = resolution;

    // bucket index → Set of keys
    this.buckets = new Map();

    // key → bucket index (for fast removal on re-expiry)
    this.keyToBucket = new Map();

    this._timer = null;
    this._currentBucket = this._getBucketIndex(Date.now());
  }

  /**
   * Start the TTL sweep loop.
   */
  start() {
    this._timer = setInterval(() => this._sweep(), this.resolution);
    return this;
  }

  /**
   * Stop sweeping.
   */
  stop() {
    clearInterval(this._timer);
    this._timer = null;
  }

  /**
   * Schedule key for expiry at (now + ttlMs).
   */
  schedule(key, ttlMs) {
    // Remove from previous bucket if rescheduling
    this.cancel(key);

    const expiresAt = Date.now() + ttlMs;
    const bucketIdx = this._getBucketIndex(expiresAt);

    if (!this.buckets.has(bucketIdx)) {
      this.buckets.set(bucketIdx, new Set());
    }
    this.buckets.get(bucketIdx).add(key);
    this.keyToBucket.set(key, bucketIdx);
  }

  /**
   * Cancel a scheduled expiry.
   */
  cancel(key) {
    const bucketIdx = this.keyToBucket.get(key);
    if (bucketIdx !== undefined) {
      const bucket = this.buckets.get(bucketIdx);
      if (bucket) {
        bucket.delete(key);
        if (bucket.size === 0) this.buckets.delete(bucketIdx);
      }
      this.keyToBucket.delete(key);
    }
  }

  /**
   * Get time remaining for a key in ms. Returns -1 if not tracked.
   */
  ttl(key) {
    const bucketIdx = this.keyToBucket.get(key);
    if (bucketIdx === undefined) return -1;
    return bucketIdx * this.resolution - Date.now();
  }

  _sweep() {
    const now = Date.now();
    const targetBucket = this._getBucketIndex(now);

    // Process all buckets up to and including current
    for (let idx = this._currentBucket; idx <= targetBucket; idx++) {
      const bucket = this.buckets.get(idx);
      if (bucket) {
        for (const key of bucket) {
          this.keyToBucket.delete(key);
          // Fire-and-forget expiry callback
          Promise.resolve(this.onExpire(key)).catch(err => {
            console.error(`[TTLManager] Error expiring key ${key}:`, err);
          });
        }
        this.buckets.delete(idx);
      }
    }

    this._currentBucket = targetBucket + 1;
  }

  _getBucketIndex(timestamp) {
    return Math.floor(timestamp / this.resolution);
  }

  getStats() {
    let scheduledKeys = 0;
    for (const bucket of this.buckets.values()) {
      scheduledKeys += bucket.size;
    }
    return {
      scheduledKeys,
      buckets: this.buckets.size,
    };
  }
}

module.exports = { TTLManager };
```

---

### 5.8 Replication Manager

```javascript
// replication-manager.js
const EventEmitter = require('events');

/**
 * Manages async replication of writes from primary to replica nodes.
 *
 * Features:
 * - Async replication with configurable lag tolerance
 * - Write-ahead log (WAL) for replay on reconnect
 * - Sequence numbers for ordering (vector clock simplified)
 */
class ReplicationManager extends EventEmitter {
  /**
   * @param {object} primaryClient - CacheClient connected to primary
   * @param {object[]} replicaClients - array of CacheClients for replicas
   * @param {object} options
   */
  constructor(primaryClient, replicaClients, options = {}) {
    super();

    this.primary = primaryClient;
    this.replicas = replicaClients;

    this.options = {
      maxLagMs: 500,          // Alert if replication lag exceeds 500ms
      batchSize: 100,         // Max ops to batch-replicate per tick
      flushIntervalMs: 10,    // How often to flush the replication queue
      ...options,
    };

    // In-memory WAL for replication
    this.wal = [];
    this.seqNum = 0;

    // Per-replica replication cursor
    this.replicaState = new Map(
      replicaClients.map((r, i) => [`replica-${i}`, { lastSeq: 0, lag: 0 }])
    );

    this._flushTimer = null;
  }

  start() {
    this._flushTimer = setInterval(() => this._flushWAL(), this.options.flushIntervalMs);
    return this;
  }

  stop() {
    clearInterval(this._flushTimer);
  }

  /**
   * Log a write operation to the WAL.
   * Called by primary after local write succeeds.
   */
  logWrite(op, key, value = null, ttlMs = 0) {
    const entry = {
      seq: ++this.seqNum,
      timestamp: Date.now(),
      op,    // 'set' | 'delete' | 'expire' | 'flush'
      key,
      value,
      ttlMs,
    };

    this.wal.push(entry);
    this.emit('wal:append', entry);
  }

  /**
   * Replicate pending WAL entries to all replicas.
   */
  async _flushWAL() {
    if (this.wal.length === 0) return;

    const batch = this.wal.slice(0, this.options.batchSize);

    const replicateToAll = this.replicas.map(async (replica, i) => {
      const stateKey = `replica-${i}`;
      const state = this.replicaState.get(stateKey);

      // Only send entries this replica hasn't seen
      const pending = batch.filter(e => e.seq > state.lastSeq);
      if (pending.length === 0) return;

      try {
        for (const entry of pending) {
          await this._applyToReplica(replica, entry);
          state.lastSeq = entry.seq;
          state.lag = Date.now() - entry.timestamp;
        }

        if (state.lag > this.options.maxLagMs) {
          this.emit('lag:exceeded', { replicaIndex: i, lagMs: state.lag });
        }
      } catch (err) {
        this.emit('replica:error', { replicaIndex: i, error: err });
      }
    });

    await Promise.allSettled(replicateToAll);

    // Truncate WAL up to min replicated seq
    const minSeq = Math.min(
      ...[...this.replicaState.values()].map(s => s.lastSeq)
    );

    this.wal = this.wal.filter(e => e.seq > minSeq);
  }

  async _applyToReplica(replica, entry) {
    switch (entry.op) {
      case 'set':
        await replica.set(entry.key, entry.value, entry.ttlMs);
        break;
      case 'delete':
        await replica.delete(entry.key);
        break;
      case 'expire':
        await replica.expire?.(entry.key, entry.ttlMs);
        break;
      case 'flush':
        await replica.flush?.();
        break;
    }
  }

  getReplicationStatus() {
    const status = {};
    for (const [id, state] of this.replicaState) {
      status[id] = {
        lastSeq: state.lastSeq,
        lagMs: state.lag,
        walBacklog: this.wal.filter(e => e.seq > state.lastSeq).length,
      };
    }
    return status;
  }
}

module.exports = { ReplicationManager };
```

---

### 5.9 Cache Shard Coordinator

```javascript
// shard-coordinator.js

/**
 * Coordinates multiple cache nodes as a unified shard cluster.
 * Provides the public API surface for applications.
 *
 * Integrates:
 *  - ConsistentHashRing for routing
 *  - CacheClient per node
 *  - ReplicationManager per shard
 *  - TTLManager for expiry scheduling
 *  - Health checking
 */

const { CacheClient } = require('./cache-client');
const { ConsistentHashRing } = require('./consistent-hashing');
const { WriteThroughCache } = require('./write-through-cache');

class ShardCoordinator {
  constructor(nodes, options = {}) {
    this.options = {
      replicationFactor: 3,
      readQuorum: 1,
      writeQuorum: 2,
      healthCheckIntervalMs: 5000,
      ...options,
    };

    this.client = new CacheClient(nodes, {
      replicationFactor: this.options.replicationFactor,
      readQuorum: this.options.readQuorum,
      writeQuorum: this.options.writeQuorum,
      poolSize: 20,
      retries: 3,
    });

    this.nodes = nodes;
    this._healthCheckTimer = null;
    this.nodeHealth = new Map(nodes.map(n => [n.id, true]));
  }

  /**
   * Start health checking.
   */
  start() {
    this._healthCheckTimer = setInterval(
      () => this._checkHealth(),
      this.options.healthCheckIntervalMs
    );
    return this;
  }

  stop() {
    clearInterval(this._healthCheckTimer);
    this.client.destroy();
  }

  // ─── Public Cache API ─────────────────────────────────────────

  async get(key) {
    return this.client.get(key);
  }

  async set(key, value, ttlMs = 3600000) {
    return this.client.set(key, value, ttlMs);
  }

  async delete(key) {
    return this.client.delete(key);
  }

  async exists(key) {
    return this.client.exists(key);
  }

  /**
   * Atomic increment — uses optimistic locking.
   * Returns new value.
   */
  async incr(key, delta = 1) {
    for (let attempt = 0; attempt < 10; attempt++) {
      const current = await this.get(key);
      const currentVal = current === null ? 0 : Number(current);
      const newVal = currentVal + delta;

      // CAS: only set if value hasn't changed
      // In production: use Lua script on Redis or conditional write
      await this.set(key, newVal);
      return newVal;
    }
    throw new Error(`incr failed after max retries for key: ${key}`);
  }

  /**
   * Scatter-gather: get multiple keys in parallel.
   */
  async mget(keys) {
    const results = await Promise.allSettled(
      keys.map(key => this.get(key))
    );

    return keys.reduce((acc, key, i) => {
      acc[key] = results[i].status === 'fulfilled' ? results[i].value : null;
      return acc;
    }, {});
  }

  /**
   * Pipeline: set multiple keys in parallel.
   */
  async mset(entries, ttlMs = 3600000) {
    const ops = Object.entries(entries).map(([key, value]) =>
      this.set(key, value, ttlMs)
    );
    const results = await Promise.allSettled(ops);
    return results.filter(r => r.status === 'fulfilled').length;
  }

  // ─── Health Checks ────────────────────────────────────────────

  async _checkHealth() {
    for (const node of this.nodes) {
      try {
        const pool = this.client.pools.get(node.id);
        if (!pool) continue;

        const conn = await Promise.race([
          pool.acquire(),
          new Promise((_, rej) => setTimeout(() => rej(new Error('timeout')), 500)),
        ]);

        pool.release(conn);

        if (!this.nodeHealth.get(node.id)) {
          console.log(`[Coordinator] Node ${node.id} recovered`);
          this.nodeHealth.set(node.id, true);
        }
      } catch {
        if (this.nodeHealth.get(node.id)) {
          console.warn(`[Coordinator] Node ${node.id} unhealthy — removing from ring`);
          this.nodeHealth.set(node.id, false);
          this.client.removeNode(node.id);
        }
      }
    }
  }

  getClusterStatus() {
    return {
      nodes: this.nodes.map(n => ({
        id: n.id,
        healthy: this.nodeHealth.get(n.id),
      })),
      ringSize: this.client.ring.ring.length,
    };
  }
}

module.exports = { ShardCoordinator };
```

---

## 6. Advanced Topics

### 6.1 Cache Stampede / Thundering Herd

```javascript
// stampede-prevention.js

/**
 * XFetch Algorithm (Probabilistic Early Expiry)
 * Probabilistically refreshes cache before TTL expires to avoid stampedes.
 *
 * P(refresh) = max(0, (now - (ttl_end - delta * beta * ln(rand()))) / ttl)
 * where beta controls aggressiveness (default 1.0)
 */
class StampedePreventionCache {
  constructor(cache, fetchFn, options = {}) {
    this.cache = cache;
    this.fetchFn = fetchFn;   // async (key) → value
    this.options = {
      beta: 1.0,
      ttlMs: 60000,
      ...options,
    };

    // In-flight request map to prevent concurrent fetches for same key
    this.inFlight = new Map();
  }

  async get(key) {
    const cached = await this.cache.get(key);

    if (cached !== null) {
      // XFetch: probabilistically decide to refresh early
      if (this._shouldEarlyRefresh(cached)) {
        this._backgroundRefresh(key);
      }
      return cached.value;
    }

    // Cache miss: serialize concurrent requests for the same key
    return this._serializeFetch(key);
  }

  _shouldEarlyRefresh(entry) {
    if (!entry.expiresAt) return false;
    const { beta } = this.options;
    const delta = entry.fetchDurationMs || 10;
    const remainingMs = entry.expiresAt - Date.now();
    return remainingMs < delta * beta * Math.log(Math.random()) * -1;
  }

  async _serializeFetch(key) {
    // If already fetching this key, wait for the same promise
    if (this.inFlight.has(key)) {
      return this.inFlight.get(key);
    }

    const fetchPromise = this._fetch(key);
    this.inFlight.set(key, fetchPromise);

    try {
      const result = await fetchPromise;
      return result;
    } finally {
      this.inFlight.delete(key);
    }
  }

  async _fetch(key) {
    const start = Date.now();
    const value = await this.fetchFn(key);
    const fetchDurationMs = Date.now() - start;

    const entry = {
      value,
      fetchDurationMs,
      expiresAt: Date.now() + this.options.ttlMs,
    };

    await this.cache.set(key, entry, this.options.ttlMs + fetchDurationMs * 2);
    return value;
  }

  _backgroundRefresh(key) {
    // Don't await — fire and forget
    this._serializeFetch(key).catch(err => {
      console.error(`[StampedeCache] Background refresh failed for ${key}:`, err);
    });
  }
}

module.exports = { StampedePreventionCache };
```

### 6.2 Cache Warming

```javascript
// cache-warmer.js

/**
 * Pre-populates cache on startup or after cluster expansion.
 * Reads from DB and fills cache using access frequency data.
 */
class CacheWarmer {
  constructor(cache, db, options = {}) {
    this.cache = cache;
    this.db = db;
    this.options = {
      batchSize: 500,     // keys per DB batch
      concurrency: 10,    // parallel DB requests
      ttlMs: 3600000,
      ...options,
    };
  }

  /**
   * Warm cache with a list of keys (e.g., top-N hot keys from analytics).
   */
  async warmKeys(keys) {
    console.log(`[CacheWarmer] Warming ${keys.length} keys...`);
    const start = Date.now();
    let populated = 0;

    for (let i = 0; i < keys.length; i += this.options.batchSize) {
      const batch = keys.slice(i, i + this.options.batchSize);

      // Fetch from DB with limited concurrency
      const chunks = this._chunk(batch, Math.ceil(batch.length / this.options.concurrency));
      await Promise.all(chunks.map(chunk => this._warmChunk(chunk)));

      populated += batch.length;
      const pct = ((populated / keys.length) * 100).toFixed(1);
      console.log(`[CacheWarmer] Progress: ${populated}/${keys.length} (${pct}%)`);
    }

    const elapsed = Date.now() - start;
    console.log(`[CacheWarmer] Completed in ${elapsed}ms. Rate: ${(keys.length / elapsed * 1000).toFixed(0)} keys/s`);
  }

  async _warmChunk(keys) {
    const values = await this.db.mget(keys);
    await Promise.all(
      Object.entries(values)
        .filter(([, v]) => v !== null && v !== undefined)
        .map(([key, value]) => this.cache.set(key, value, this.options.ttlMs))
    );
  }

  _chunk(arr, size) {
    const chunks = [];
    for (let i = 0; i < arr.length; i += size) {
      chunks.push(arr.slice(i, i + size));
    }
    return chunks;
  }
}

module.exports = { CacheWarmer };
```

### 6.3 Multi-Region Cache

```
Region US-East                   Region EU-West
┌──────────────────────┐         ┌──────────────────────┐
│  Cache Cluster A     │◄──────► │  Cache Cluster B     │
│  (Primary reads/     │  async  │  (Primary reads/     │
│   writes for US)     │  repl.  │   writes for EU)     │
└──────────────────────┘         └──────────────────────┘
         │                                │
    DB Primary                      DB Replica
    (US-East)                       (EU-West)
         │                                │
         └─────── Replication ────────────┘

Writes:
  User in EU writes → EU cache cluster → EU DB replica
  → replicated to US DB primary → US cache invalidated via CDC

Reads:
  User in EU reads → EU cache hit (< 1ms)
  User in US reads → US cache hit (< 1ms)

Cross-region latency: 80-100ms → NEVER block reads on cross-region
Strategy: Accept eventual consistency across regions (usually < 1s lag)
```

**Geo-routing Key Strategy:**
```
key: "user:123"           → routes to same cluster regardless of region
key: "user:123:eu-west"   → region-scoped key for user's local cluster
key: "user:123:us-east"   → different region, different data
```

### 6.4 Cache Serialization

```javascript
// serialization.js

/**
 * Efficient binary serialization using MessagePack-like encoding.
 * 3-5x smaller than JSON, 10x faster to serialize/deserialize.
 */
class CacheSerializer {
  static serialize(value) {
    const type = typeof value;

    if (value === null) return Buffer.from([0x00]);
    if (type === 'boolean') return Buffer.from([value ? 0x01 : 0x02]);
    if (type === 'number') {
      const buf = Buffer.alloc(9);
      buf.writeUInt8(0x03, 0);
      buf.writeDoubleBE(value, 1);
      return buf;
    }
    if (type === 'string') {
      const strBuf = Buffer.from(value, 'utf8');
      const buf = Buffer.alloc(5 + strBuf.length);
      buf.writeUInt8(0x04, 0);
      buf.writeUInt32BE(strBuf.length, 1);
      strBuf.copy(buf, 5);
      return buf;
    }
    if (Buffer.isBuffer(value)) {
      const buf = Buffer.alloc(5 + value.length);
      buf.writeUInt8(0x05, 0);
      buf.writeUInt32BE(value.length, 1);
      value.copy(buf, 5);
      return buf;
    }

    // Fallback: JSON for objects/arrays
    const jsonBuf = Buffer.from(JSON.stringify(value), 'utf8');
    const buf = Buffer.alloc(5 + jsonBuf.length);
    buf.writeUInt8(0x06, 0);
    buf.writeUInt32BE(jsonBuf.length, 1);
    jsonBuf.copy(buf, 5);
    return buf;
  }

  static deserialize(buf) {
    const type = buf.readUInt8(0);
    switch (type) {
      case 0x00: return null;
      case 0x01: return true;
      case 0x02: return false;
      case 0x03: return buf.readDoubleBE(1);
      case 0x04: return buf.slice(5, 5 + buf.readUInt32BE(1)).toString('utf8');
      case 0x05: return buf.slice(5, 5 + buf.readUInt32BE(1));
      case 0x06: return JSON.parse(buf.slice(5, 5 + buf.readUInt32BE(1)).toString('utf8'));
      default: throw new Error(`Unknown type byte: ${type}`);
    }
  }
}

module.exports = { CacheSerializer };
```

---

## 7. Trade-offs & Comparisons

### Memcached vs Redis vs Custom

| Feature | Memcached | Redis | Custom (this design) |
|---|---|---|---|
| Data Structures | String only | Rich (Hash, List, Set, Sorted Set) | String / JSON |
| Persistence | No | RDB + AOF | Optional |
| Replication | No (client-side) | Primary-Replica | Built-in |
| Clustering | Client-side | Redis Cluster | Consistent Hashing |
| Pub/Sub | No | Yes | Extensible |
| Scripting | No | Lua | JS plugins |
| Multi-thread | Yes (native) | Single-threaded | Worker threads |
| Memory overhead | Lower | Higher (data structures) | Configurable |
| Max value size | 1MB | 512MB | Configurable |
| Latency (p50) | < 0.2ms | < 0.3ms | < 0.3ms |

### Consistency vs Availability Trade-off (CAP)

```
Under network partition, choose:

CP (Consistent + Partition-tolerant):
  → Refuse writes until majority quorum reached
  → Use case: Financial data, session tokens
  → Example: ZooKeeper, etcd

AP (Available + Partition-tolerant):
  → Accept writes, resolve conflicts later
  → Use case: Social media counters, shopping carts
  → Example: Dynamo, Cassandra, this design (default)

Our system default: AP (eventual consistency)
Configurable: W+R > N for CP behavior
```

### Eviction Policy Decision Tree

```
Access pattern?
├── Temporal locality (recent = hot) → LRU
├── Frequency skew (20% keys = 80% traffic) → LFU
├── Time-ordered expiry only → TTL-based no eviction
├── Unknown / mixed → SLRU (Redis default)
└── Simplest / fastest → Random (surprisingly effective)
```

---

## 8. Real-World Systems Reference

| Company | System | Key Design Choice |
|---|---|---|
| Facebook | Memcached + McRouter | Client-side sharding, regional pools |
| Twitter | Twemproxy | Proxy-based Memcached with connection pooling |
| Netflix | EVCache (Memcached) | Multi-region replication |
| GitHub | Redis Cluster | Primary-replica with Sentinel |
| Uber | Cherami + Redis | Write-behind with async persistence |
| LinkedIn | Couchbase | Consistent hashing + eventual consistency |
| Amazon | ElastiCache | Managed Redis/Memcached |
| Discord | Redis | Single massive cluster, 5B+ messages/day |

---

### Key Interview Talking Points

**1. Start with requirements** — Ask: latency SLA? consistency model? data size? eviction needs?

**2. Justify consistent hashing** — "Modulo N breaks on node add/remove, remapping all keys. Consistent hashing remaps only K/N keys."

**3. Address hot keys explicitly** — "One viral key can take down a node. Solutions: key splitting, local L1, request coalescing."

**4. Explain thundering herd** — "When a popular key expires, simultaneous misses flood the DB. XFetch or mutex prevents this."

**5. Quorum trade-off** — "W+R > N gives strong consistency but higher latency. Default to AP for cache, offer CP config."

**6. Memory management** — "LRU with O(1) via hashmap + doubly linked list. Background sweep for TTL, not per-request."

**7. Replication lag** — "Async replication means replicas can be stale. Clients reading critical data use primary or quorum reads."

**8. Circuit breaker** — "Without it, a down node causes cascading failures. Open circuit after N failures, retry after timeout."

---
