# 🔍 Search Engine — System Design

> **FAANG-Level System Design | HLD + LLD (JavaScript)**
> Covers Google-scale web search: crawling, indexing, ranking, serving, and everything in between.

---

## Table of Contents

1. [Problem Statement & Requirements](#1-problem-statement--requirements)
2. [Capacity Estimation & Back-of-Envelope Math](#2-capacity-estimation--back-of-envelope-math)
3. [High-Level Design (HLD)](#3-high-level-design-hld)
   - 3.1 [Architecture Overview](#31-architecture-overview)
   - 3.2 [Web Crawler](#32-web-crawler)
   - 3.3 [Document Processor & Parser](#33-document-processor--parser)
   - 3.4 [Indexing Pipeline](#34-indexing-pipeline)
   - 3.5 [Inverted Index](#35-inverted-index)
   - 3.6 [Ranking Engine](#36-ranking-engine)
   - 3.7 [Query Processor](#37-query-processor)
   - 3.8 [Search API Layer](#38-search-api-layer)
   - 3.9 [Caching Layer](#39-caching-layer)
   - 3.10 [Storage Architecture](#310-storage-architecture)
   - 3.11 [Data Flow Diagram](#311-data-flow-diagram)
4. [Low-Level Design (LLD) — JavaScript](#4-low-level-design-lld--javascript)
   - 4.1 [Web Crawler — LLD](#41-web-crawler--lld)
   - 4.2 [URL Frontier & Deduplication](#42-url-frontier--deduplication)
   - 4.3 [HTML Parser & Content Extractor](#43-html-parser--content-extractor)
   - 4.4 [Tokenizer & Text Processor](#44-tokenizer--text-processor)
   - 4.5 [Inverted Index Builder](#45-inverted-index-builder)
   - 4.6 [PageRank Algorithm](#46-pagerank-algorithm)
   - 4.7 [TF-IDF Scorer](#47-tf-idf-scorer)
   - 4.8 [BM25 Ranker](#48-bm25-ranker)
   - 4.9 [Query Parser & Executor](#49-query-parser--executor)
   - 4.10 [Query Suggestion (Autocomplete)](#410-query-suggestion-autocomplete)
   - 4.11 [Search Result Aggregator](#411-search-result-aggregator)
   - 4.12 [Rate Limiter](#412-rate-limiter)
   - 4.13 [Distributed Cache (LRU)](#413-distributed-cache-lru)
   - 4.14 [Bloom Filter for URL Dedup](#414-bloom-filter-for-url-dedup)
   - 4.15 [Consistent Hashing for Sharding](#415-consistent-hashing-for-sharding)
5. [Database Design](#5-database-design)
6. [API Design](#6-api-design)
7. [Scalability & Fault Tolerance](#7-scalability--fault-tolerance)
8. [Security Considerations](#8-security-considerations)
9. [Monitoring & Observability](#9-monitoring--observability)
10. [Trade-offs & Design Decisions](#10-trade-offs--design-decisions)
11. [Interview Tips & Common Questions](#11-interview-tips--common-questions)

---

## 1. Problem Statement & Requirements

### 1.1 Functional Requirements

- **Crawl** the web continuously and discover new/updated pages
- **Index** web pages — extract, parse, tokenize, and store content
- **Rank** documents by relevance using signals like TF-IDF, BM25, PageRank, freshness
- **Serve queries** — accept a user query and return top-N ranked results in < 200ms
- **Autocomplete / Query Suggestions** — suggest queries as the user types
- **Advanced search operators** — `site:`, `filetype:`, `"exact phrase"`, `-exclude`
- **Spell correction** — handle typos and suggest alternatives
- **Safe search filtering** — filter adult/spam content

### 1.2 Non-Functional Requirements

| Property | Target |
|---|---|
| Availability | 99.99% (< 1hr downtime/year) |
| Search Latency (P99) | < 200ms |
| Crawl Coverage | ~10B pages indexed |
| Throughput | ~100K queries/sec globally |
| Freshness | Trending pages re-crawled within minutes |
| Consistency | Eventual (index lag acceptable) |
| Durability | Zero data loss for index |
| Fault tolerance | No single point of failure |

### 1.3 Out of Scope

- Image/video search (different indexing strategy)
- Personalized results (separate ML pipeline)
- Shopping/ads integration
- Voice search (ASR layer on top)

---

## 2. Capacity Estimation & Back-of-Envelope Math

### 2.1 Crawling

```
Web pages to index:    ~10 billion (10^10)
Average page size:     ~100 KB
Recrawl frequency:     ~30 days for most pages
Pages crawled/day:     10B / 30 = ~333M pages/day
                                  = ~4,000 pages/second
Bandwidth (inbound):   4,000 * 100KB = ~400 MB/s crawl bandwidth
Storage (raw HTML):    10B * 100KB = 1 PB raw HTML storage
```

### 2.2 Indexing

```
Tokens per page (avg):      1,000
Total tokens (10B pages):   10^13 tokens
Inverted index entry size:  ~30 bytes (termID + docID + position + score)
Total inverted index size:  10^13 * 30 bytes = ~300 TB
Compressed (60% ratio):     ~120 TB per index shard
```

### 2.3 Query Serving

```
Daily active users:         1 billion
Queries/user/day:           3
Total queries/day:          3 billion
Peak QPS (10x avg):         ~100,000 QPS
Result size per query:      ~10 KB (10 results with snippets)
Outbound bandwidth:         100K * 10KB = ~1 GB/s
```

### 2.4 Infrastructure Summary

| Component | Count |
|---|---|
| Crawler workers | ~1,000 machines |
| Index shards | ~500 shards |
| Replicas per shard | 3 |
| Total index machines | 1,500 |
| Query serving machines | ~500 |
| Cache servers | ~100 |

---

## 3. High-Level Design (HLD)

### 3.1 Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                         USER / CLIENT                               │
└────────────────────────────┬────────────────────────────────────────┘
                             │ HTTPS Query
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    GLOBAL LOAD BALANCER (Anycast)                   │
│              GeoDNS → Nearest Region (US/EU/APAC)                   │
└────────────────────────────┬────────────────────────────────────────┘
                             │
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
        ┌──────────┐  ┌──────────┐  ┌──────────┐
        │  API GW  │  │  API GW  │  │  API GW  │    ← Rate Limiting
        │  (US)    │  │  (EU)    │  │ (APAC)   │      Auth, Logging
        └────┬─────┘  └────┬─────┘  └────┬─────┘
             └─────────────┼─────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        QUERY SERVICE                                │
│   ┌──────────────┐  ┌───────────────┐  ┌──────────────────────┐   │
│   │ Query Parser │→ │ Spell Correct │→ │  Query Expansion     │   │
│   │ (tokenize,   │  │ (edit dist.)  │  │  (synonyms, stems)   │   │
│   │  normalize)  │  └───────────────┘  └──────────────────────┘   │
│   └──────┬───────┘                                                  │
└──────────┼──────────────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     DISTRIBUTED CACHE (Redis)                       │
│              Cache-Aside: hit → return, miss → index                │
└────────────────────────────┬────────────────────────────────────────┘
                             │ (cache miss)
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        INDEX SERVING LAYER                          │
│                                                                     │
│   ┌─────────────┐   ┌─────────────┐   ┌─────────────┐             │
│   │ Index Shard │   │ Index Shard │   │ Index Shard │  (500+)     │
│   │   [A-D]     │   │   [E-L]     │   │   [M-Z]     │  shards     │
│   └──────┬──────┘   └──────┬──────┘   └──────┬──────┘             │
│          └─────────────────┼─────────────────┘                     │
│                            │ Partial results                        │
│                            ▼                                        │
│                   ┌─────────────────┐                               │
│                   │  RESULT MERGER  │  ← Sort, Deduplicate          │
│                   │  + RANKER       │  ← PageRank, BM25, Freshness  │
│                   └─────────────────┘                               │
└─────────────────────────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        DOCUMENT STORE                               │
│   Fetch page title, URL, snippet for top-N results                  │
│   (Key-Value store: docID → {title, url, snippet, meta})            │
└─────────────────────────────────────────────────────────────────────┘

─────────────────────── OFFLINE PIPELINE ────────────────────────────

┌──────────────┐   ┌─────────────────┐   ┌──────────────────────┐
│  URL SEED    │→  │   URL FRONTIER  │→  │   WEB CRAWLER        │
│  (seed list) │   │  (Priority Queue│   │  (Distributed)       │
│              │   │   + Politeness) │   │  1000+ workers       │
└──────────────┘   └─────────────────┘   └──────────┬───────────┘
                                                      │ Raw HTML
                                                      ▼
                                         ┌──────────────────────┐
                                         │  HTML PARSER         │
                                         │  + Content Extractor │
                                         │  + Link Extractor    │
                                         └──────────┬───────────┘
                                                    │
                          ┌─────────────────────────┼──────────────┐
                          ▼                         ▼              ▼
               ┌──────────────────┐   ┌──────────────────┐  ┌──────────────┐
               │  TEXT PROCESSOR  │   │  LINK GRAPH DB   │  │  DOC STORE   │
               │  (tokenize,      │   │  (PageRank input)│  │  (raw pages) │
               │  stem, index)    │   └──────────────────┘  └──────────────┘
               └────────┬─────────┘
                        ▼
               ┌──────────────────┐
               │ INVERTED INDEX   │
               │  BUILDER         │
               │  (MapReduce /    │
               │   Spark)         │
               └────────┬─────────┘
                        ▼
               ┌──────────────────┐
               │  INDEX SHARDS    │
               │  (distributed,   │
               │   replicated)    │
               └──────────────────┘
```

---

### 3.2 Web Crawler

The crawler is a distributed system responsible for fetching billions of web pages.

**Key Responsibilities:**
- Fetch URLs from the frontier queue
- Respect `robots.txt` and crawl-delay
- Follow redirects (max depth limit)
- Detect and skip duplicate content
- Extract new URLs and feed back to frontier

**Key Design Decisions:**

| Decision | Choice | Reason |
|---|---|---|
| Crawl politeness | 1 req/domain/sec | Avoid overloading servers |
| Duplicate detection | SimHash + Bloom Filter | Scalable near-duplicate detection |
| Priority | Freshness + PageRank | High-value pages crawled more often |
| Protocol | HTTP/1.1, HTTP/2, DNS pre-fetch | Throughput optimization |
| Distributed coordination | Apache Kafka + ZooKeeper | Decoupled, fault-tolerant |

**Crawler Architecture:**

```
URL Frontier (Kafka)
        │
   ┌────▼────┐
   │ Fetcher │ × 1000 workers (Node.js async I/O)
   └────┬────┘
        │
   ┌────▼────────────┐
   │ DNS Cache       │ ← Reduce DNS latency (in-memory, TTL-aware)
   └────┬────────────┘
        │
   ┌────▼──────────────┐
   │ robots.txt Cache  │ ← Per-domain, cached 24hrs
   └────┬──────────────┘
        │ fetch page
   ┌────▼──────────────┐
   │ Content Processor │ ← Parse HTML, extract links, fingerprint
   └────┬──────────────┘
        │
   ┌────▼──────────────┐    ┌─────────────────┐
   │ URL Extractor     │───►│ Dedup Filter    │── Bloom Filter
   └───────────────────┘    │ (Bloom + DB)    │── SHA256 hash store
                            └────────┬────────┘
                                     │ new URLs only
                                     ▼
                            URL Frontier (Kafka)
```

---

### 3.3 Document Processor & Parser

After fetching raw HTML, the Document Processor prepares content for indexing.

**Pipeline Steps:**
1. **HTML Parsing** — Extract `<title>`, `<meta>`, `<h1-h6>`, `<body>`, `<a href>`
2. **Boilerplate Removal** — Strip nav bars, footers, ads (using DOM heuristics)
3. **Language Detection** — Detect language (CLD3); index per-language
4. **Content Normalization** — Unicode normalization, lowercase, charset detection
5. **Metadata Extraction** — OpenGraph, Schema.org structured data
6. **Duplicate/Near-Duplicate Detection** — SimHash fingerprinting
7. **Spam Detection** — Keyword stuffing, link farm signals → discard or penalize

---

### 3.4 Indexing Pipeline

Uses a **MapReduce / Apache Spark** batch pipeline for large-scale index builds, supplemented by a **real-time stream** (Kafka + Flink) for freshness.

```
Batch Path (full index rebuild, weekly):
  HDFS Raw Pages → Spark Map (tokenize) → Spark Reduce (merge posting lists)
                                        → Sorted Inverted Index Files
                                        → Push to Index Shards

Incremental Path (delta index, per-minute):
  Kafka (new pages) → Flink (real-time tokenizer) → Delta index segment
                                                   → Merge into serving index
```

**Why two paths?**
- Batch gives a complete, optimized index
- Incremental ensures fresh content appears within minutes
- Index serving merges both: query both delta + base index, merge results

---

### 3.5 Inverted Index

The inverted index is the core data structure of any search engine.

**Structure:**

```
term → [ (docID, term_frequency, positions[], field_boost, doc_score) ]

Example:
"python" → [
  (doc_42, tf=5, pos=[12,45,67,89,200], field=title, score=0.92),
  (doc_87, tf=2, pos=[5,300],           field=body,  score=0.41),
  ...
]
```

**Posting List Optimizations:**
- **Sorted by docID** — enables fast set operations (AND/OR/NOT) using merge
- **Delta encoding** — store gaps between docIDs, not absolute values → 60% compression
- **Variable-length encoding (VByte)** — compact integer representation
- **Skip pointers** — every √n entries, add skip pointer for fast AND queries
- **Block-max WAND** — prune non-competitive documents during retrieval

**Sharding Strategy:**

```
Option 1: Term-based sharding
  - All postings for term T go to shard hash(T) % N
  - PRO: One shard per query term
  - CON: Hot terms create hot shards (imbalanced load)

Option 2: Document-based sharding (CHOSEN)
  - All terms for doc D go to shard hash(D) % N
  - PRO: Even load distribution, resilient to popular terms
  - CON: Query must fan-out to ALL shards, merge results
  - Used by: Google, Bing, Elasticsearch
```

---

### 3.6 Ranking Engine

Ranking combines multiple signals to order results by relevance.

**Ranking Signals:**

| Signal | Type | Description |
|---|---|---|
| BM25 | Text relevance | Probabilistic term-frequency model (best baseline) |
| TF-IDF | Text relevance | Classic term weighting |
| PageRank | Authority | Link graph authority score |
| Freshness | Temporal | Recency of page content/last-crawl |
| URL depth | Structure | Root pages usually more important |
| Anchor text | Context | Text others use to link to this page |
| Click-through rate | Behavioral | Historical user engagement |
| Domain authority | Trust | Age, inbound link count of domain |
| Core Web Vitals | UX | Page speed, layout stability |
| Semantic similarity | NLP | BERT/dense vectors for query-doc match |

**Score Formula (simplified):**

```
final_score(q, d) =
    α * BM25(q, d)
  + β * PageRank(d)
  + γ * FreshnessScore(d)
  + δ * AnchorTextScore(q, d)
  + ε * SemanticSimilarity(q, d)
  + ζ * ClickScore(q, d)
```

Weights (α, β, γ...) are learned via Learning-to-Rank (LambdaMART).

---

### 3.7 Query Processor

Handles raw query text → ranked document IDs.

**Processing Steps:**

```
Raw Query: "best python web frameworks 2024"
    │
    ▼
1. Tokenize:       ["best", "python", "web", "frameworks", "2024"]
    │
    ▼
2. Normalize:      lowercase, unicode NFC, remove punctuation
    │
    ▼
3. Stopword removal: (optional — "best" kept for intent)
    │
    ▼
4. Stemming/Lemma:  "frameworks" → "framework"
    │
    ▼
5. Spell correction: "pythn" → "python"
    │
    ▼
6. Synonym expansion: "web" → ["web", "http", "internet"] (optional)
    │
    ▼
7. Query classification: informational / navigational / transactional
    │
    ▼
8. Index lookup: fetch posting lists for each term
    │
    ▼
9. Candidate retrieval: AND/OR merge posting lists → candidate set
    │
    ▼
10. Scoring: BM25 + PageRank + other signals → ranked list
    │
    ▼
11. Top-K selection: WAND/MaxScore pruning → return top 100 candidates
    │
    ▼
12. Snippet generation: extract relevant excerpt per result
```

---

### 3.8 Search API Layer

RESTful + gRPC API surface.

```
Public REST API:
  GET /search?q=<query>&page=1&num=10&lang=en&safe=off

Internal gRPC API (frontend → index shards):
  SearchService.Query(QueryRequest) → SearchResponse

Query fanout:
  Query server broadcasts to ALL index shards in parallel
  Each shard returns top-K local results
  Query server merges (sort + dedup) → global top-K
  Fetch full doc metadata for top-10 from Doc Store
  Return to user
```

---

### 3.9 Caching Layer

```
Cache Hierarchy:
  L1: In-process LRU cache (per query server, ~100k entries, ~1GB RAM)
      ↓ miss
  L2: Distributed Redis cache (cross-region, ~10M entries, ~100GB)
      ↓ miss
  L3: Index shards (cold path)

Cache key:   SHA256(normalized_query + lang + safe_search)
Cache TTL:   Popular queries: 1 hour
             Trending/news:   1 minute
             Long-tail:       24 hours

Cache invalidation:
  - TTL-based expiry (primary)
  - Manual purge on index updates for specific URLs
  - Write-through for autocomplete trie
```

---

### 3.10 Storage Architecture

| Data | Storage System | Reason |
|---|---|---|
| Raw HTML pages | HDFS / S3 | Cheap, sequential-write optimized |
| Inverted index | Custom binary files on SSDs | Low latency random reads |
| Document metadata | Cassandra / Bigtable | Wide-column, high write throughput |
| Link graph | Neo4j / custom graph store | PageRank computation |
| Crawl state / URL frontier | Kafka + Redis | High-throughput queue |
| Query logs | ClickHouse / BigQuery | Analytical queries at scale |
| User signals (CTR) | Kafka → Data Warehouse | Streaming ingestion |
| Autocomplete trie | Redis / in-memory | Sub-millisecond lookup |

---

### 3.11 Data Flow Diagram

```
[Web]
  │  crawl
  ▼
[Crawler Cluster] ──raw HTML──► [Message Queue (Kafka)]
                                       │
                              ┌────────┴─────────┐
                              ▼                  ▼
                     [Doc Processor]     [Link Extractor]
                              │                  │
                    (text, tokens)         (new URLs)
                              │                  │
                              ▼                  ▼
                     [Index Builder]     [URL Frontier]
                        (Spark)
                              │
                    (inverted index)
                              │
                              ▼
                     [Index Shards] ◄────────────────────┐
                              │                          │
                              │                          │
[User Query] ──► [Query Service] ──fanout──► [Shard 1..N]
                     │    │                             │
                   [Cache] │◄──── partial results ──────┘
                           │
                    [Result Merger]
                           │
                    [Doc Metadata Store]
                           │
                    [Search Results] ──► [User]
```

---

## 4. Low-Level Design (LLD) — JavaScript

---

### 4.1 Web Crawler — LLD

```javascript
// ============================================================
// WebCrawler — Distributed Async Crawler (Node.js)
// ============================================================

const https = require('https');
const http  = require('http');
const { URL } = require('url');

class WebCrawler {
  /**
   * @param {Object} config
   * @param {URLFrontier}   config.frontier       - Priority queue of URLs to crawl
   * @param {BloomFilter}   config.seenUrls       - Deduplication filter
   * @param {RobotsCache}   config.robotsCache    - Cached robots.txt rules
   * @param {DocumentStore} config.docStore       - Raw page storage
   * @param {number}        config.concurrency    - Max parallel requests
   * @param {number}        config.crawlDelayMs   - Min ms between requests to same domain
   */
  constructor(config) {
    this.frontier       = config.frontier;
    this.seenUrls       = config.seenUrls;
    this.robotsCache    = config.robotsCache;
    this.docStore       = config.docStore;
    this.concurrency    = config.concurrency || 100;
    this.crawlDelayMs   = config.crawlDelayMs || 1000;
    this.domainLastFetch = new Map(); // domain → timestamp (politeness)
    this.activeRequests  = 0;
  }

  async start() {
    console.log('[Crawler] Starting...');
    while (true) {
      // Throttle to max concurrency
      while (this.activeRequests >= this.concurrency) {
        await this._sleep(50);
      }

      const urlEntry = await this.frontier.dequeue();
      if (!urlEntry) {
        await this._sleep(500);
        continue;
      }

      this.activeRequests++;
      this._fetchAndProcess(urlEntry).finally(() => {
        this.activeRequests--;
      });
    }
  }

  async _fetchAndProcess({ url, priority, depth }) {
    try {
      const parsedUrl = new URL(url);
      const domain = parsedUrl.hostname;

      // ── Politeness: enforce crawl delay per domain ──────────
      await this._enforceCrawlDelay(domain);

      // ── robots.txt check ────────────────────────────────────
      const allowed = await this.robotsCache.isAllowed(url, 'MySearchBot/1.0');
      if (!allowed) {
        console.debug(`[Crawler] robots.txt disallows: ${url}`);
        return;
      }

      // ── Fetch page ──────────────────────────────────────────
      const response = await this._fetch(url);
      if (!response) return;

      const { statusCode, headers, body } = response;

      // Handle redirects (already followed by _fetch, but log)
      if (statusCode >= 400) {
        console.warn(`[Crawler] HTTP ${statusCode} for ${url}`);
        return;
      }

      const contentType = headers['content-type'] || '';
      if (!contentType.includes('text/html')) return; // only crawl HTML

      // ── Fingerprint for near-dup detection ──────────────────
      const fingerprint = this._simHash(body);
      if (await this.seenUrls.hasSeen(fingerprint)) {
        console.debug(`[Crawler] Near-duplicate, skipping: ${url}`);
        return;
      }
      await this.seenUrls.markSeen(fingerprint);

      // ── Store raw HTML ───────────────────────────────────────
      const docId = this._generateDocId(url);
      await this.docStore.save(docId, {
        url,
        crawledAt: Date.now(),
        statusCode,
        headers,
        body,
        depth,
      });

      // ── Extract and enqueue new URLs ─────────────────────────
      const links = this._extractLinks(body, url);
      for (const link of links) {
        if (!await this.seenUrls.hasUrl(link)) {
          await this.seenUrls.markUrl(link);
          const linkPriority = this._computePriority(link, depth + 1);
          await this.frontier.enqueue({ url: link, priority: linkPriority, depth: depth + 1 });
        }
      }

      console.info(`[Crawler] Fetched: ${url} (depth=${depth}, links=${links.length})`);

    } catch (err) {
      console.error(`[Crawler] Error fetching ${url}: ${err.message}`);
    }
  }

  async _fetch(url, redirectCount = 0) {
    if (redirectCount > 5) return null;
    return new Promise((resolve) => {
      const parsedUrl = new URL(url);
      const lib = parsedUrl.protocol === 'https:' ? https : http;

      const options = {
        hostname: parsedUrl.hostname,
        path: parsedUrl.pathname + parsedUrl.search,
        method: 'GET',
        headers: {
          'User-Agent': 'MySearchBot/1.0 (+https://mysearch.com/bot)',
          'Accept': 'text/html,application/xhtml+xml',
          'Accept-Encoding': 'gzip, deflate',
        },
        timeout: 10000,
      };

      const req = lib.request(options, (res) => {
        // Handle redirects
        if ([301, 302, 303, 307, 308].includes(res.statusCode) && res.headers.location) {
          const redirectUrl = new URL(res.headers.location, url).href;
          resolve(this._fetch(redirectUrl, redirectCount + 1));
          return;
        }

        const chunks = [];
        res.on('data', chunk => chunks.push(chunk));
        res.on('end', () => {
          resolve({
            statusCode: res.statusCode,
            headers: res.headers,
            body: Buffer.concat(chunks).toString('utf-8'),
          });
        });
      });

      req.on('error', () => resolve(null));
      req.on('timeout', () => { req.destroy(); resolve(null); });
      req.end();
    });
  }

  _extractLinks(htmlBody, baseUrl) {
    const links = [];
    const linkRegex = /href=["']([^"']+)["']/gi;
    let match;
    while ((match = linkRegex.exec(htmlBody)) !== null) {
      try {
        const resolvedUrl = new URL(match[1], baseUrl).href;
        if (resolvedUrl.startsWith('http')) {
          links.push(resolvedUrl);
        }
      } catch (_) {
        // Ignore malformed URLs
      }
    }
    return [...new Set(links)]; // deduplicate within page
  }

  _computePriority(url, depth) {
    // Lower number = higher priority
    // Prioritize shallow URLs, short URLs, known high-value domains
    let score = depth * 10;
    if (url.split('/').length <= 4) score -= 5; // root/top-level pages
    return score;
  }

  _simHash(content) {
    // Simplified SimHash — real implementation uses 64-bit hash of n-gram shingles
    let hash = 0;
    const tokens = content.split(/\s+/).slice(0, 200); // sample first 200 tokens
    for (const token of tokens) {
      let h = this._fnv1a(token);
      for (let i = 0; i < 32; i++) {
        hash ^= (h & (1 << i)) ? (1 << i) : 0;
      }
    }
    return hash.toString(16);
  }

  _fnv1a(str) {
    let hash = 2166136261;
    for (let i = 0; i < str.length; i++) {
      hash ^= str.charCodeAt(i);
      hash = (hash * 16777619) >>> 0;
    }
    return hash;
  }

  _generateDocId(url) {
    return Buffer.from(url).toString('base64').replace(/[/+=]/g, '_');
  }

  async _enforceCrawlDelay(domain) {
    const lastFetch = this.domainLastFetch.get(domain) || 0;
    const wait = this.crawlDelayMs - (Date.now() - lastFetch);
    if (wait > 0) await this._sleep(wait);
    this.domainLastFetch.set(domain, Date.now());
  }

  _sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

module.exports = { WebCrawler };
```

---

### 4.2 URL Frontier & Deduplication

```javascript
// ============================================================
// URLFrontier — Priority Queue with domain bucketing
// ============================================================

class URLFrontier {
  /**
   * In production: backed by Kafka with priority topics.
   * Here: in-memory min-heap per priority tier.
   * 
   * Design:
   *   - 3 priority tiers: HIGH (0-9), MED (10-19), LOW (20+)
   *   - Domain buckets: one sub-queue per domain (politeness)
   *   - Scheduler picks one URL per domain in round-robin
   */
  constructor() {
    this.queues = {
      high: [],  // priority 0-9
      med:  [],  // priority 10-19
      low:  [],  // priority 20+
    };
    this.domainBuckets = new Map(); // domain → URL[]
    this.size = 0;
  }

  async enqueue({ url, priority, depth }) {
    const tier = priority < 10 ? 'high' : priority < 20 ? 'med' : 'low';
    const domain = new URL(url).hostname;

    if (!this.domainBuckets.has(domain)) {
      this.domainBuckets.set(domain, []);
    }
    this.domainBuckets.get(domain).push({ url, priority, depth });
    this.queues[tier].push({ url, priority, depth, domain });
    this.queues[tier].sort((a, b) => a.priority - b.priority); // heap in prod
    this.size++;
  }

  async dequeue() {
    // Try high priority first, then med, then low
    for (const tier of ['high', 'med', 'low']) {
      if (this.queues[tier].length > 0) {
        const entry = this.queues[tier].shift();
        this.size--;
        // Remove from domain bucket
        const bucket = this.domainBuckets.get(entry.domain);
        if (bucket) {
          const idx = bucket.findIndex(e => e.url === entry.url);
          if (idx !== -1) bucket.splice(idx, 1);
        }
        return entry;
      }
    }
    return null;
  }

  getSize() { return this.size; }
}

module.exports = { URLFrontier };
```

---

### 4.3 HTML Parser & Content Extractor

```javascript
// ============================================================
// ContentExtractor — Parse HTML, extract meaningful text
// ============================================================

class ContentExtractor {
  /**
   * In production: use htmlparser2 + cheerio (Node.js)
   * Here: lightweight regex-based for illustration
   */

  extract(html, url) {
    return {
      url,
      title:       this._extractTitle(html),
      metaDesc:    this._extractMeta(html, 'description'),
      h1:          this._extractHeadings(html, 'h1'),
      h2:          this._extractHeadings(html, 'h2'),
      bodyText:    this._extractBodyText(html),
      outboundLinks: this._extractLinks(html, url),
      language:    this._detectLanguage(html),
      canonicalUrl: this._extractCanonical(html, url),
      structuredData: this._extractJsonLd(html),
    };
  }

  _extractTitle(html) {
    const match = html.match(/<title[^>]*>([^<]+)<\/title>/i);
    return match ? this._decodeEntities(match[1].trim()) : '';
  }

  _extractMeta(html, name) {
    const match = html.match(
      new RegExp(`<meta[^>]+name=["']${name}["'][^>]+content=["']([^"']+)["']`, 'i')
    ) || html.match(
      new RegExp(`<meta[^>]+content=["']([^"']+)["'][^>]+name=["']${name}["']`, 'i')
    );
    return match ? this._decodeEntities(match[1]) : '';
  }

  _extractHeadings(html, tag) {
    const results = [];
    const regex = new RegExp(`<${tag}[^>]*>([^<]+)<\/${tag}>`, 'gi');
    let match;
    while ((match = regex.exec(html)) !== null) {
      results.push(this._decodeEntities(match[1].trim()));
    }
    return results;
  }

  _extractBodyText(html) {
    // 1. Remove script and style blocks
    let text = html
      .replace(/<script[\s\S]*?<\/script>/gi, ' ')
      .replace(/<style[\s\S]*?<\/style>/gi, ' ')
      .replace(/<nav[\s\S]*?<\/nav>/gi, ' ')      // remove nav
      .replace(/<footer[\s\S]*?<\/footer>/gi, ' ') // remove footer
      .replace(/<header[\s\S]*?<\/header>/gi, ' '); // remove header

    // 2. Strip all remaining HTML tags
    text = text.replace(/<[^>]+>/g, ' ');

    // 3. Decode HTML entities and normalize whitespace
    text = this._decodeEntities(text);
    text = text.replace(/\s+/g, ' ').trim();

    return text.substring(0, 50000); // cap at 50KB of text
  }

  _extractLinks(html, baseUrl) {
    const links = [];
    const regex = /<a[^>]+href=["']([^"'#?][^"']*)["'][^>]*>([^<]*)<\/a>/gi;
    let match;
    while ((match = regex.exec(html)) !== null) {
      try {
        const href = new URL(match[1], baseUrl).href;
        const anchorText = match[2].trim();
        if (href.startsWith('http')) {
          links.push({ url: href, anchorText });
        }
      } catch (_) {}
    }
    return links;
  }

  _extractCanonical(html, defaultUrl) {
    const match = html.match(/<link[^>]+rel=["']canonical["'][^>]+href=["']([^"']+)["']/i);
    return match ? match[1] : defaultUrl;
  }

  _extractJsonLd(html) {
    const match = html.match(/<script[^>]+type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/i);
    if (!match) return null;
    try {
      return JSON.parse(match[1]);
    } catch (_) {
      return null;
    }
  }

  _detectLanguage(html) {
    const match = html.match(/<html[^>]+lang=["']([a-z-]+)["']/i);
    return match ? match[1].toLowerCase() : 'en';
  }

  _decodeEntities(str) {
    return str
      .replace(/&amp;/g, '&')
      .replace(/&lt;/g, '<')
      .replace(/&gt;/g, '>')
      .replace(/&quot;/g, '"')
      .replace(/&#39;/g, "'")
      .replace(/&nbsp;/g, ' ')
      .replace(/&#(\d+);/g, (_, code) => String.fromCharCode(code));
  }
}

module.exports = { ContentExtractor };
```

---

### 4.4 Tokenizer & Text Processor

```javascript
// ============================================================
// TextProcessor — Tokenize, normalize, stem
// ============================================================

class TextProcessor {
  constructor() {
    this.stopwords = new Set([
      'a', 'an', 'the', 'is', 'are', 'was', 'were', 'be', 'been',
      'being', 'have', 'has', 'had', 'do', 'does', 'did', 'will',
      'would', 'shall', 'should', 'may', 'might', 'can', 'could',
      'of', 'in', 'on', 'at', 'to', 'for', 'with', 'by', 'from',
      'and', 'or', 'but', 'not', 'this', 'that', 'it', 'its',
    ]);
  }

  /**
   * Full processing pipeline for indexing
   * @param {string} text
   * @returns {{ tokens: string[], termFrequency: Map<string, number>, positions: Map<string, number[]> }}
   */
  processForIndex(text) {
    const tokens = this.tokenize(text);
    const normalized = tokens.map(t => this.normalize(t));
    const filtered = normalized.filter(t => t.length > 1 && !this.stopwords.has(t));
    const stemmed = filtered.map(t => this.stem(t));

    const termFrequency = new Map();
    const positions = new Map();

    stemmed.forEach((term, position) => {
      if (!term) return;
      termFrequency.set(term, (termFrequency.get(term) || 0) + 1);
      if (!positions.has(term)) positions.set(term, []);
      positions.get(term).push(position);
    });

    return { tokens: stemmed.filter(Boolean), termFrequency, positions };
  }

  /**
   * Lightweight processing for query (no aggressive stemming, keep intent)
   */
  processQuery(query) {
    const tokens = this.tokenize(query);
    const normalized = tokens.map(t => this.normalize(t));
    const filtered = normalized.filter(t => t.length > 0);
    return filtered.map(t => this.stem(t)).filter(Boolean);
  }

  tokenize(text) {
    // Split on whitespace and punctuation, keep alphanumeric + hyphens
    return text
      .toLowerCase()
      .replace(/[^\w\s-]/g, ' ')    // replace punct with space
      .replace(/-+/g, ' ')          // split hyphenated words
      .split(/\s+/)
      .filter(t => t.length > 0);
  }

  normalize(token) {
    return token
      .toLowerCase()
      .replace(/[^a-z0-9]/g, '')  // keep only alphanumeric
      .trim();
  }

  /**
   * Porter Stemmer (simplified — production uses natural library)
   * Reduces words to their root form: "running" → "run"
   */
  stem(word) {
    if (word.length <= 3) return word;

    const rules = [
      [/sses$/, 'ss'],
      [/ies$/, 'i'],
      [/ss$/, 'ss'],
      [/s$/, ''],
      [/eed$/, 'ee'],
      [/ed$/, ''],
      [/ing$/, ''],
      [/ational$/, 'ate'],
      [/tional$/, 'tion'],
      [/enci$/, 'ence'],
      [/anci$/, 'ance'],
      [/izer$/, 'ize'],
      [/iser$/, 'ise'],
      [/alism$/, 'al'],
      [/ation$/, 'ate'],
      [/ator$/, 'ate'],
      [/ness$/, ''],
      [/ment$/, ''],
      [/ful$/, ''],
      [/ous$/, ''],
      [/ive$/, ''],
      [/ize$/, ''],
      [/ise$/, ''],
      [/ly$/, ''],
      [/er$/, ''],
    ];

    let stem = word;
    for (const [pattern, replacement] of rules) {
      if (pattern.test(stem) && stem.length > 4) {
        stem = stem.replace(pattern, replacement);
        break;
      }
    }
    return stem || word;
  }

  /**
   * N-gram generator for autocomplete / phrase matching
   */
  ngrams(tokens, n = 2) {
    const result = [];
    for (let i = 0; i <= tokens.length - n; i++) {
      result.push(tokens.slice(i, i + n).join(' '));
    }
    return result;
  }
}

module.exports = { TextProcessor };
```

---

### 4.5 Inverted Index Builder

```javascript
// ============================================================
// InvertedIndex — Core index data structure
// ============================================================

/**
 * PostingEntry: one entry in a posting list
 * {
 *   docId:     string,   — document identifier
 *   tf:        number,   — term frequency in this doc
 *   positions: number[], — positions of term in doc
 *   fieldBoost:number,   — 3x if in title, 2x if in h1, 1x if in body
 * }
 */

class InvertedIndex {
  constructor() {
    // term → PostingEntry[]  (sorted by docId for merge operations)
    this.index = new Map();

    // docId → { url, title, docLength, pageRank }
    this.docMetadata = new Map();

    // Total number of documents
    this.totalDocs = 0;
  }

  /**
   * Add a document to the index
   * @param {string} docId
   * @param {Object} doc - { url, title, h1, bodyText }
   */
  addDocument(docId, doc) {
    const processor = new TextProcessor(); // reuse module from 4.4
    const fields = [
      { text: doc.title,    boost: 3.0, field: 'title' },
      { text: doc.h1.join(' '), boost: 2.5, field: 'h1' },
      { text: doc.metaDesc, boost: 1.5, field: 'meta' },
      { text: doc.bodyText, boost: 1.0, field: 'body' },
    ];

    let totalTerms = 0;

    for (const { text, boost, field } of fields) {
      if (!text) continue;
      const { termFrequency, positions } = processor.processForIndex(text);
      totalTerms += [...termFrequency.values()].reduce((a, b) => a + b, 0);

      for (const [term, tf] of termFrequency) {
        if (!this.index.has(term)) {
          this.index.set(term, []);
        }

        const postings = this.index.get(term);
        const existing = postings.find(p => p.docId === docId);

        if (existing) {
          // Merge: add tf from different fields
          existing.tf += tf * boost;
          existing.positions.push(...(positions.get(term) || []));
        } else {
          postings.push({
            docId,
            tf: tf * boost,
            positions: positions.get(term) || [],
            field,
            fieldBoost: boost,
          });
        }
      }
    }

    this.docMetadata.set(docId, {
      url: doc.url,
      title: doc.title,
      docLength: totalTerms,
      pageRank: doc.pageRank || 0.15, // default PageRank
      crawledAt: Date.now(),
    });

    this.totalDocs++;
  }

  /**
   * Retrieve posting list for a term
   * @param {string} term
   * @returns {Array} posting entries, sorted by score descending
   */
  getPostings(term) {
    return this.index.get(term) || [];
  }

  /**
   * Boolean AND: docs containing ALL terms
   * Uses merge of sorted posting lists (O(n))
   */
  andQuery(terms) {
    if (terms.length === 0) return [];

    const postingLists = terms
      .map(t => this.getPostings(t).map(p => p.docId).sort())
      .sort((a, b) => a.length - b.length); // process shortest list first (optimization)

    let result = new Set(postingLists[0]);

    for (let i = 1; i < postingLists.length; i++) {
      const nextSet = new Set(postingLists[i]);
      for (const docId of result) {
        if (!nextSet.has(docId)) result.delete(docId);
      }
    }

    return [...result];
  }

  /**
   * Boolean OR: docs containing ANY term
   */
  orQuery(terms) {
    const result = new Set();
    for (const term of terms) {
      for (const posting of this.getPostings(term)) {
        result.add(posting.docId);
      }
    }
    return [...result];
  }

  /**
   * Phrase query: docs where terms appear consecutively
   * Uses position information
   */
  phraseQuery(terms) {
    if (terms.length === 0) return [];
    if (terms.length === 1) return this.getPostings(terms[0]).map(p => p.docId);

    const results = [];
    const firstPostings = this.getPostings(terms[0]);

    for (const posting of firstPostings) {
      const docId = posting.docId;
      let isPhrase = true;

      for (let i = 1; i < terms.length; i++) {
        const nextPostings = this.getPostings(terms[i]);
        const nextPosting = nextPostings.find(p => p.docId === docId);
        if (!nextPosting) { isPhrase = false; break; }

        // Check if any position in next term = any position in prev term + 1
        const prevPositions = new Set(
          i === 1
            ? posting.positions
            : (this.getPostings(terms[i-1]).find(p => p.docId === docId) || {}).positions || []
        );
        const adjacent = nextPosting.positions.some(pos => prevPositions.has(pos - 1));
        if (!adjacent) { isPhrase = false; break; }
      }

      if (isPhrase) results.push(docId);
    }

    return results;
  }

  getDocMetadata(docId) {
    return this.docMetadata.get(docId);
  }

  getTotalDocs() {
    return this.totalDocs;
  }

  getDocumentFrequency(term) {
    return (this.index.get(term) || []).length;
  }

  /**
   * Serialize index to binary format for storage
   * Production: use Protocol Buffers or custom binary format
   */
  serialize() {
    const obj = {};
    for (const [term, postings] of this.index) {
      obj[term] = postings;
    }
    return JSON.stringify({
      index: obj,
      metadata: Object.fromEntries(this.docMetadata),
      totalDocs: this.totalDocs,
    });
  }

  static deserialize(data) {
    const parsed = JSON.parse(data);
    const idx = new InvertedIndex();
    idx.index = new Map(Object.entries(parsed.index));
    idx.docMetadata = new Map(Object.entries(parsed.metadata));
    idx.totalDocs = parsed.totalDocs;
    return idx;
  }
}

module.exports = { InvertedIndex };
```

---

### 4.6 PageRank Algorithm

```javascript
// ============================================================
// PageRank — Link authority scoring
// ============================================================

/**
 * Classic iterative PageRank (Google's original algorithm)
 * 
 * Formula:
 *   PR(A) = (1 - d) + d * Σ [ PR(B) / OutLinks(B) ]
 * 
 * Where:
 *   d = damping factor (~0.85) — probability of following a link
 *   PR(B) = PageRank of page B linking to A
 *   OutLinks(B) = number of outbound links from B
 */

class PageRankCalculator {
  /**
   * @param {Map<string, string[]>} linkGraph - Map<pageUrl, outboundUrls[]>
   * @param {number} dampingFactor - typically 0.85
   * @param {number} maxIterations - typically 50-100
   * @param {number} convergenceThreshold - stop when max delta < this
   */
  constructor(linkGraph, dampingFactor = 0.85, maxIterations = 100, convergenceThreshold = 1e-6) {
    this.linkGraph          = linkGraph;
    this.dampingFactor      = dampingFactor;
    this.maxIterations      = maxIterations;
    this.convergenceThreshold = convergenceThreshold;

    // Build reverse graph: page → pages that link TO it
    this.inboundLinks = new Map();
    this.outboundCounts = new Map();

    for (const [page, outlinks] of linkGraph) {
      this.outboundCounts.set(page, outlinks.length);
      for (const target of outlinks) {
        if (!this.inboundLinks.has(target)) this.inboundLinks.set(target, []);
        this.inboundLinks.get(target).push(page);
      }
    }
  }

  calculate() {
    const pages = [...this.linkGraph.keys()];
    const N = pages.length;

    if (N === 0) return new Map();

    // Initialize all pages with equal rank
    let ranks = new Map();
    for (const page of pages) {
      ranks.set(page, 1 / N);
    }

    for (let iter = 0; iter < this.maxIterations; iter++) {
      const newRanks = new Map();
      let maxDelta = 0;

      // Handle dangling nodes (pages with no outbound links)
      // Their rank is redistributed uniformly to all pages
      let danglingSum = 0;
      for (const page of pages) {
        if ((this.outboundCounts.get(page) || 0) === 0) {
          danglingSum += ranks.get(page);
        }
      }

      for (const page of pages) {
        // Contribution from dangling nodes
        let rank = (1 - this.dampingFactor) / N + this.dampingFactor * (danglingSum / N);

        // Contribution from inbound links
        const inbound = this.inboundLinks.get(page) || [];
        for (const source of inbound) {
          const sourceRank = ranks.get(source) || 0;
          const outCount = this.outboundCounts.get(source) || 1;
          rank += this.dampingFactor * (sourceRank / outCount);
        }

        newRanks.set(page, rank);
        maxDelta = Math.max(maxDelta, Math.abs(rank - ranks.get(page)));
      }

      ranks = newRanks;

      if (maxDelta < this.convergenceThreshold) {
        console.log(`[PageRank] Converged after ${iter + 1} iterations (maxDelta=${maxDelta.toExponential(2)})`);
        break;
      }
    }

    // Normalize to [0, 1]
    const maxRank = Math.max(...ranks.values());
    const normalized = new Map();
    for (const [page, rank] of ranks) {
      normalized.set(page, rank / maxRank);
    }

    return normalized;
  }
}

// ── Example Usage ────────────────────────────────────────────
const exampleGraph = new Map([
  ['A', ['B', 'C']],
  ['B', ['C']],
  ['C', ['A']],
  ['D', ['C']],
]);

const pr = new PageRankCalculator(exampleGraph);
const scores = pr.calculate();
// C gets highest score (most inbound links), A next, etc.

module.exports = { PageRankCalculator };
```

---

### 4.7 TF-IDF Scorer

```javascript
// ============================================================
// TF-IDF Scorer
// ============================================================

/**
 * TF-IDF (Term Frequency — Inverse Document Frequency)
 * 
 * TF(t, d)  = (count of t in d) / (total terms in d)
 * IDF(t)    = log(N / df(t)) + 1          (smoothed)
 * TF-IDF    = TF(t, d) * IDF(t)
 */

class TFIDFScorer {
  constructor(invertedIndex) {
    this.index = invertedIndex;
  }

  /**
   * Score a document against a query
   * @param {string[]} queryTerms
   * @param {string} docId
   * @returns {number} TF-IDF score
   */
  score(queryTerms, docId) {
    const meta = this.index.getDocMetadata(docId);
    if (!meta) return 0;

    const N = this.index.getTotalDocs();
    let totalScore = 0;

    for (const term of queryTerms) {
      const postings = this.index.getPostings(term);
      const posting = postings.find(p => p.docId === docId);
      if (!posting) continue;

      const tf = posting.tf / (meta.docLength || 1);
      const df = postings.length;
      const idf = Math.log(N / (df + 1)) + 1; // smooth IDF

      totalScore += tf * idf * posting.fieldBoost;
    }

    return totalScore;
  }

  /**
   * Score all documents matching at least one query term
   * Returns sorted array of { docId, score }
   */
  rankDocuments(queryTerms) {
    const candidates = this.index.orQuery(queryTerms);
    return candidates
      .map(docId => ({ docId, score: this.score(queryTerms, docId) }))
      .sort((a, b) => b.score - a.score);
  }
}

module.exports = { TFIDFScorer };
```

---

### 4.8 BM25 Ranker

```javascript
// ============================================================
// BM25 Ranker — State-of-the-art probabilistic relevance model
// ============================================================

/**
 * BM25 (Best Match 25)
 * 
 * Score(d, q) = Σ IDF(qi) * [ tf(qi,d) * (k1+1) ] / [ tf(qi,d) + k1*(1 - b + b*|d|/avgdl) ]
 * 
 * Parameters (empirically tuned):
 *   k1 = 1.5    — term frequency saturation (higher = more TF effect)
 *   b  = 0.75   — length normalization (0 = no normalization, 1 = full)
 * 
 * BM25 outperforms TF-IDF because:
 *   - Saturates TF (doubling count doesn't double score)
 *   - Length normalization prevents favoring long docs
 */

class BM25Ranker {
  constructor(invertedIndex, k1 = 1.5, b = 0.75) {
    this.index = invertedIndex;
    this.k1 = k1;
    this.b = b;
    this._avgDocLength = null;
  }

  _getAvgDocLength() {
    if (this._avgDocLength !== null) return this._avgDocLength;

    let total = 0;
    let count = 0;
    // In production: pre-computed and cached
    // Here: compute from metadata
    for (const [_, meta] of this.index.docMetadata) {
      total += meta.docLength || 0;
      count++;
    }
    this._avgDocLength = count > 0 ? total / count : 1;
    return this._avgDocLength;
  }

  /**
   * Compute IDF for a term
   * IDF(t) = log( (N - df + 0.5) / (df + 0.5) + 1 )
   */
  _idf(term) {
    const N = this.index.getTotalDocs();
    const df = this.index.getDocumentFrequency(term);
    return Math.log((N - df + 0.5) / (df + 0.5) + 1);
  }

  /**
   * Score a single document against query terms
   */
  score(queryTerms, docId) {
    const meta = this.index.getDocMetadata(docId);
    if (!meta) return 0;

    const avgDL = this._getAvgDocLength();
    const dl = meta.docLength || 1;
    const k1 = this.k1;
    const b = this.b;
    let totalScore = 0;

    for (const term of queryTerms) {
      const postings = this.index.getPostings(term);
      const posting = postings.find(p => p.docId === docId);
      if (!posting) continue;

      const tf = posting.tf;
      const idf = this._idf(term);

      // BM25 term score
      const numerator   = tf * (k1 + 1);
      const denominator = tf + k1 * (1 - b + b * (dl / avgDL));
      const termScore   = idf * (numerator / denominator);

      // Field boost already baked into tf during indexing
      totalScore += termScore;
    }

    // Combine with PageRank (log to compress range)
    const pageRankBoost = 1 + Math.log(1 + (meta.pageRank || 0));
    return totalScore * pageRankBoost;
  }

  /**
   * Full BM25 ranking over candidate documents
   * @param {string[]} queryTerms
   * @param {string[]} [candidateDocIds] - pre-filtered candidates, or null for all
   * @returns {{ docId: string, score: number }[]}
   */
  rankDocuments(queryTerms, candidateDocIds = null) {
    const candidates = candidateDocIds || this.index.orQuery(queryTerms);
    return candidates
      .map(docId => ({ docId, score: this.score(queryTerms, docId) }))
      .filter(r => r.score > 0)
      .sort((a, b) => b.score - a.score);
  }
}

module.exports = { BM25Ranker };
```

---

### 4.9 Query Parser & Executor

```javascript
// ============================================================
// QueryParser — Parse advanced query syntax
// ============================================================

/**
 * Supports:
 *   "exact phrase"    → phrase query
 *   -exclude          → NOT operator
 *   site:domain.com   → filter by domain
 *   filetype:pdf      → filter by file type
 *   term1 OR term2    → OR operator
 *   term1 term2       → AND (default)
 */

class QueryParser {
  parse(rawQuery) {
    const result = {
      mustTerms:    [],    // All these terms must match (AND)
      shouldTerms:  [],    // At least one should match (OR)
      mustNotTerms: [],    // None of these should match (NOT)
      phrases:      [],    // Exact phrases ["word1 word2"]
      filters:      {},    // site:, filetype:, lang:, etc.
    };

    let query = rawQuery.trim();

    // 1. Extract quoted phrases
    const phraseRegex = /"([^"]+)"/g;
    query = query.replace(phraseRegex, (_, phrase) => {
      result.phrases.push(phrase.toLowerCase());
      return '';
    });

    // 2. Extract field operators (site:, filetype:, lang:)
    const fieldRegex = /(\w+):(\S+)/g;
    query = query.replace(fieldRegex, (_, field, value) => {
      result.filters[field.toLowerCase()] = value.toLowerCase();
      return '';
    });

    // 3. Extract OR groups
    const orRegex = /(\S+)\s+OR\s+(\S+)/g;
    query = query.replace(orRegex, (_, t1, t2) => {
      result.shouldTerms.push(t1.toLowerCase(), t2.toLowerCase());
      return '';
    });

    // 4. Extract NOT terms
    const notRegex = /-(\S+)/g;
    query = query.replace(notRegex, (_, term) => {
      result.mustNotTerms.push(term.toLowerCase());
      return '';
    });

    // 5. Remaining terms are AND
    const remaining = query.trim().split(/\s+/).filter(Boolean);
    result.mustTerms.push(...remaining.map(t => t.toLowerCase()));

    return result;
  }
}

// ============================================================
// QueryExecutor — Execute parsed query against index
// ============================================================

class QueryExecutor {
  /**
   * @param {InvertedIndex} index
   * @param {BM25Ranker} ranker
   * @param {TextProcessor} processor
   */
  constructor(index, ranker, processor) {
    this.index = index;
    this.ranker = ranker;
    this.processor = processor;
    this.parser = new QueryParser();
  }

  /**
   * Execute a raw query and return ranked results
   * @param {string} rawQuery
   * @param {{ page: number, pageSize: number, safe: boolean }} options
   * @returns {{ results: SearchResult[], totalCount: number, queryTime: number }}
   */
  execute(rawQuery, options = {}) {
    const startTime = Date.now();
    const { page = 1, pageSize = 10 } = options;

    // 1. Parse query
    const parsed = this.parser.parse(rawQuery);

    // 2. Normalize terms
    const mustTerms = parsed.mustTerms.flatMap(t => this.processor.processQuery(t));
    const shouldTerms = parsed.shouldTerms.flatMap(t => this.processor.processQuery(t));
    const mustNotTerms = parsed.mustNotTerms.flatMap(t => this.processor.processQuery(t));

    if (mustTerms.length === 0 && shouldTerms.length === 0 && parsed.phrases.length === 0) {
      return { results: [], totalCount: 0, queryTime: Date.now() - startTime };
    }

    // 3. Find candidate documents
    let candidates;

    if (mustTerms.length > 0) {
      // All must-terms must match
      candidates = new Set(this.index.andQuery(mustTerms));
    } else {
      candidates = new Set();
    }

    // Add phrase matches
    for (const phrase of parsed.phrases) {
      const phraseTokens = this.processor.processQuery(phrase);
      const phraseMatches = this.index.phraseQuery(phraseTokens);
      phraseMatches.forEach(id => candidates.add(id));
    }

    // Add OR results (if no required terms, OR terms define the set)
    if (shouldTerms.length > 0 && mustTerms.length === 0) {
      const orResults = this.index.orQuery(shouldTerms);
      orResults.forEach(id => candidates.add(id));
    }

    // 4. Remove NOT matches
    if (mustNotTerms.length > 0) {
      const notDocIds = new Set(this.index.orQuery(mustNotTerms));
      for (const id of notDocIds) candidates.delete(id);
    }

    // 5. Apply filters (site:, filetype:, etc.)
    let filtered = [...candidates].filter(docId => {
      const meta = this.index.getDocMetadata(docId);
      if (!meta) return false;
      if (parsed.filters.site && !meta.url.includes(parsed.filters.site)) return false;
      if (parsed.filters.filetype && !meta.url.endsWith(`.${parsed.filters.filetype}`)) return false;
      return true;
    });

    // 6. Rank with BM25
    const allQueryTerms = [...mustTerms, ...shouldTerms];
    const ranked = this.ranker.rankDocuments(allQueryTerms, filtered);

    // 7. Paginate
    const totalCount = ranked.length;
    const paged = ranked.slice((page - 1) * pageSize, page * pageSize);

    // 8. Build result objects
    const results = paged.map(({ docId, score }) => {
      const meta = this.index.getDocMetadata(docId);
      return {
        docId,
        url: meta?.url || '',
        title: meta?.title || '',
        snippet: this._generateSnippet(docId, allQueryTerms),
        score: parseFloat(score.toFixed(4)),
        pageRank: meta?.pageRank || 0,
        crawledAt: meta?.crawledAt,
      };
    });

    return {
      results,
      totalCount,
      queryTime: Date.now() - startTime,
      parsedQuery: parsed,
    };
  }

  _generateSnippet(docId, queryTerms) {
    // In production: fetch raw text from doc store, find best passage
    // Here: return placeholder
    return `...relevant excerpt containing ${queryTerms.slice(0, 3).join(', ')}...`;
  }
}

module.exports = { QueryParser, QueryExecutor };
```

---

### 4.10 Query Suggestion (Autocomplete)

```javascript
// ============================================================
// AutocompleteEngine — Trie-based query suggestions
// ============================================================

/**
 * Architecture:
 *   - Trie for prefix matching
 *   - Each trie node stores top-K suggestions with frequency
 *   - Updated from query logs (batch + streaming)
 *   - In production: stored in Redis sorted sets (ZRANGEBYLEX)
 */

class TrieNode {
  constructor() {
    this.children   = new Map();  // char → TrieNode
    this.isEnd      = false;
    this.frequency  = 0;          // how often this query was searched
    this.topK       = [];         // top-K completions cached at this node
  }
}

class AutocompleteEngine {
  constructor(topK = 5) {
    this.root = new TrieNode();
    this.topK = topK;
  }

  /**
   * Record a user query (called after every search)
   */
  recordQuery(query, frequency = 1) {
    const normalized = query.toLowerCase().trim();
    if (!normalized) return;

    let node = this.root;
    for (const char of normalized) {
      if (!node.children.has(char)) {
        node.children.set(char, new TrieNode());
      }
      node = node.children.get(char);
    }
    node.isEnd = true;
    node.frequency += frequency;

    // Propagate: update topK cache up the trie path
    this._updateTopK(normalized, frequency);
  }

  _updateTopK(query, frequency) {
    let node = this.root;
    for (const char of query) {
      node = node.children.get(char);
      if (!node) break;

      // Insert into topK (sorted by frequency desc)
      const existing = node.topK.find(s => s.query === query);
      if (existing) {
        existing.frequency += frequency;
      } else {
        node.topK.push({ query, frequency });
      }
      // Keep only top-K
      node.topK.sort((a, b) => b.frequency - a.frequency);
      if (node.topK.length > this.topK) {
        node.topK = node.topK.slice(0, this.topK);
      }
    }
  }

  /**
   * Get autocomplete suggestions for a prefix
   * @param {string} prefix
   * @returns {{ query: string, frequency: number }[]}
   */
  getSuggestions(prefix) {
    const normalized = prefix.toLowerCase().trim();
    let node = this.root;

    for (const char of normalized) {
      if (!node.children.has(char)) return []; // no matches
      node = node.children.get(char);
    }

    // Return pre-cached topK at this node (O(prefix_length))
    return node.topK.length > 0
      ? node.topK
      : this._collectSuggestions(node, normalized);
  }

  /**
   * DFS to collect all completions (fallback when topK cache empty)
   */
  _collectSuggestions(node, prefix) {
    const results = [];

    const dfs = (current, word) => {
      if (current.isEnd) {
        results.push({ query: word, frequency: current.frequency });
      }
      if (results.length >= this.topK) return;

      for (const [char, child] of current.children) {
        dfs(child, word + char);
      }
    };

    dfs(node, prefix);
    return results.sort((a, b) => b.frequency - a.frequency).slice(0, this.topK);
  }

  /**
   * Bulk load from query log (used for initial population)
   * @param {{ query: string, count: number }[]} queryLog
   */
  bulkLoad(queryLog) {
    for (const { query, count } of queryLog) {
      this.recordQuery(query, count);
    }
    console.log(`[Autocomplete] Loaded ${queryLog.length} queries`);
  }
}

module.exports = { AutocompleteEngine };
```

---

### 4.11 Search Result Aggregator

```javascript
// ============================================================
// ResultAggregator — Merge partial results from multiple shards
// ============================================================

/**
 * In distributed setup:
 *   - Query fan-out to N index shards in parallel
 *   - Each shard returns top-K local results
 *   - Aggregator merges → global top-K
 *   - Deduplicates by URL (canonical URL normalization)
 */

class ResultAggregator {
  /**
   * @param {number} globalTopK - final number of results to return
   */
  constructor(globalTopK = 100) {
    this.globalTopK = globalTopK;
  }

  /**
   * Merge results from multiple shards
   * @param {Array<{ docId: string, score: number, url: string, title: string }[]>} shardResults
   * @returns {{ docId: string, score: number }[]} sorted, deduplicated
   */
  merge(shardResults) {
    const seen = new Map(); // normalizedUrl → best result

    // Flatten and deduplicate
    for (const shardResult of shardResults) {
      for (const result of shardResult) {
        const normalizedUrl = this._normalizeUrl(result.url);

        if (!seen.has(normalizedUrl) || seen.get(normalizedUrl).score < result.score) {
          seen.set(normalizedUrl, result);
        }
      }
    }

    // Sort by score and return top-K
    return [...seen.values()]
      .sort((a, b) => b.score - a.score)
      .slice(0, this.globalTopK);
  }

  /**
   * Normalize URL to detect duplicates:
   *   - Strip trailing slash
   *   - Remove www prefix
   *   - Lowercase
   *   - Remove tracking params (utm_*, fbclid, etc.)
   */
  _normalizeUrl(url) {
    try {
      const parsed = new URL(url);
      // Remove common tracking params
      const trackingParams = ['utm_source', 'utm_medium', 'utm_campaign', 'utm_content',
                              'utm_term', 'fbclid', 'gclid', 'ref', 'source'];
      trackingParams.forEach(p => parsed.searchParams.delete(p));

      let normalized = parsed.hostname.replace(/^www\./, '') + parsed.pathname;
      normalized = normalized.replace(/\/+$/, '').toLowerCase(); // remove trailing slash
      const qs = parsed.searchParams.toString();
      return qs ? normalized + '?' + qs : normalized;
    } catch (_) {
      return url.toLowerCase();
    }
  }

  /**
   * Score fusion for multi-signal ranking
   * Combines BM25 score + PageRank + Freshness
   */
  fuseScores(results, pageRanks, freshness) {
    const BM25_WEIGHT     = 0.55;
    const PAGERANK_WEIGHT = 0.30;
    const FRESHNESS_WEIGHT= 0.15;

    const maxBM25 = Math.max(...results.map(r => r.score), 1);

    return results.map(result => {
      const bm25Normalized = result.score / maxBM25;
      const pr = pageRanks.get(result.docId) || 0;
      const fresh = this._freshnessScore(freshness.get(result.docId) || 0);

      const fusedScore = BM25_WEIGHT * bm25Normalized
                       + PAGERANK_WEIGHT * pr
                       + FRESHNESS_WEIGHT * fresh;

      return { ...result, fusedScore };
    }).sort((a, b) => b.fusedScore - a.fusedScore);
  }

  _freshnessScore(crawledAtMs) {
    if (!crawledAtMs) return 0;
    const ageHours = (Date.now() - crawledAtMs) / (1000 * 3600);
    if (ageHours < 1)    return 1.0;   // fresh within last hour
    if (ageHours < 24)   return 0.8;
    if (ageHours < 168)  return 0.5;   // last week
    if (ageHours < 720)  return 0.2;   // last month
    return 0.05;
  }
}

module.exports = { ResultAggregator };
```

---

### 4.12 Rate Limiter

```javascript
// ============================================================
// RateLimiter — Token Bucket Algorithm
// ============================================================

/**
 * Token Bucket:
 *   - Bucket holds max `capacity` tokens
 *   - Tokens refill at `refillRate` tokens/second
 *   - Each request consumes 1 token
 *   - Request allowed if tokens > 0, else reject with 429
 * 
 * Alternative: Sliding Window Log (more accurate, more memory)
 * Choice: Token Bucket because it allows short bursts gracefully
 */

class TokenBucketRateLimiter {
  constructor(capacity, refillRatePerSecond) {
    this.capacity = capacity;
    this.refillRate = refillRatePerSecond;
    this.buckets = new Map(); // clientId → { tokens, lastRefill }
    this.cleanupIntervalMs = 60000;

    // Periodically clean up stale buckets
    setInterval(() => this._cleanup(), this.cleanupIntervalMs);
  }

  /**
   * Check if request is allowed
   * @param {string} clientId - IP address or API key
   * @returns {{ allowed: boolean, remainingTokens: number, retryAfterMs?: number }}
   */
  allowRequest(clientId) {
    const now = Date.now();

    if (!this.buckets.has(clientId)) {
      this.buckets.set(clientId, { tokens: this.capacity, lastRefill: now });
    }

    const bucket = this.buckets.get(clientId);

    // Refill based on elapsed time
    const elapsed = (now - bucket.lastRefill) / 1000; // seconds
    const refilled = elapsed * this.refillRate;
    bucket.tokens = Math.min(this.capacity, bucket.tokens + refilled);
    bucket.lastRefill = now;

    if (bucket.tokens >= 1) {
      bucket.tokens -= 1;
      return { allowed: true, remainingTokens: Math.floor(bucket.tokens) };
    } else {
      const retryAfterMs = Math.ceil((1 - bucket.tokens) / this.refillRate * 1000);
      return { allowed: false, remainingTokens: 0, retryAfterMs };
    }
  }

  _cleanup() {
    const cutoff = Date.now() - 300000; // 5 minutes
    for (const [clientId, bucket] of this.buckets) {
      if (bucket.lastRefill < cutoff) {
        this.buckets.delete(clientId);
      }
    }
  }
}

// ── Tiered rate limiting for different client types ──────────

class TieredRateLimiter {
  constructor() {
    this.tiers = {
      anonymous: new TokenBucketRateLimiter(10,   1),    // 10 req burst, 1/sec
      basic:     new TokenBucketRateLimiter(100,  10),   // 100 burst, 10/sec
      premium:   new TokenBucketRateLimiter(1000, 100),  // 1000 burst, 100/sec
      internal:  new TokenBucketRateLimiter(1e6,  1e4),  // effectively unlimited
    };
  }

  allowRequest(clientId, tier = 'anonymous') {
    const limiter = this.tiers[tier] || this.tiers.anonymous;
    return limiter.allowRequest(clientId);
  }
}

module.exports = { TokenBucketRateLimiter, TieredRateLimiter };
```

---

### 4.13 Distributed Cache (LRU)

```javascript
// ============================================================
// LRU Cache — O(1) get/put using HashMap + Doubly Linked List
// ============================================================

class LRUNode {
  constructor(key, value) {
    this.key   = key;
    this.value = value;
    this.prev  = null;
    this.next  = null;
    this.expiresAt = null;
  }
}

class LRUCache {
  /**
   * @param {number} capacity - max number of entries
   * @param {number} defaultTTLMs - default TTL in milliseconds
   */
  constructor(capacity, defaultTTLMs = 3600000) {
    this.capacity    = capacity;
    this.defaultTTL  = defaultTTLMs;
    this.map         = new Map(); // key → LRUNode
    this.hits        = 0;
    this.misses      = 0;

    // Sentinel nodes (dummy head and tail)
    this.head = new LRUNode(null, null); // MRU side
    this.tail = new LRUNode(null, null); // LRU side
    this.head.next = this.tail;
    this.tail.prev = this.head;
  }

  get(key) {
    const node = this.map.get(key);

    if (!node) {
      this.misses++;
      return null;
    }

    if (node.expiresAt && Date.now() > node.expiresAt) {
      this._remove(node);
      this.map.delete(key);
      this.misses++;
      return null;
    }

    // Move to front (most recently used)
    this._moveToFront(node);
    this.hits++;
    return node.value;
  }

  put(key, value, ttlMs = this.defaultTTL) {
    if (this.map.has(key)) {
      const node = this.map.get(key);
      node.value = value;
      node.expiresAt = ttlMs ? Date.now() + ttlMs : null;
      this._moveToFront(node);
      return;
    }

    if (this.map.size >= this.capacity) {
      // Evict LRU (tail.prev)
      const lru = this.tail.prev;
      if (lru !== this.head) {
        this._remove(lru);
        this.map.delete(lru.key);
      }
    }

    const node = new LRUNode(key, value);
    node.expiresAt = ttlMs ? Date.now() + ttlMs : null;
    this._insertAtFront(node);
    this.map.set(key, node);
  }

  delete(key) {
    const node = this.map.get(key);
    if (!node) return false;
    this._remove(node);
    this.map.delete(key);
    return true;
  }

  getStats() {
    const total = this.hits + this.misses;
    return {
      size: this.map.size,
      capacity: this.capacity,
      hits: this.hits,
      misses: this.misses,
      hitRate: total > 0 ? (this.hits / total).toFixed(3) : '0.000',
    };
  }

  _insertAtFront(node) {
    node.next = this.head.next;
    node.prev = this.head;
    this.head.next.prev = node;
    this.head.next = node;
  }

  _remove(node) {
    node.prev.next = node.next;
    node.next.prev = node.prev;
  }

  _moveToFront(node) {
    this._remove(node);
    this._insertAtFront(node);
  }
}

module.exports = { LRUCache };
```

---

### 4.14 Bloom Filter for URL Dedup

```javascript
// ============================================================
// BloomFilter — Probabilistic membership testing
// ============================================================

/**
 * Bloom Filter:
 *   - Uses k hash functions, m-bit array
 *   - add(x):  set bits at h1(x), h2(x), ..., hk(x)
 *   - has(x):  all k bits set? → "probably yes" : "definitely no"
 * 
 * False positive rate: (1 - e^(-kn/m))^k
 *   where n = number of elements
 * 
 * For 1B URLs, 1% FP rate:
 *   m = 9.6 bits/element = ~1.2 GB
 *   k = 7 hash functions
 */

class BloomFilter {
  /**
   * @param {number} expectedElements - expected number of insertions
   * @param {number} falsePositiveRate - e.g., 0.01 = 1%
   */
  constructor(expectedElements = 1_000_000, falsePositiveRate = 0.01) {
    // Optimal bit array size and hash count
    this.m = Math.ceil(-expectedElements * Math.log(falsePositiveRate) / (Math.LN2 ** 2));
    this.k = Math.ceil(this.m / expectedElements * Math.LN2);
    this.bits = new Uint8Array(Math.ceil(this.m / 8));
    this.count = 0;

    console.log(`[BloomFilter] m=${this.m} bits (~${(this.m/8/1024/1024).toFixed(1)} MB), k=${this.k} hashes`);
  }

  /**
   * Add an element to the filter
   */
  add(element) {
    const hashes = this._getHashes(element);
    for (const h of hashes) {
      this._setBit(h);
    }
    this.count++;
  }

  /**
   * Test membership — may return false positive, never false negative
   */
  has(element) {
    const hashes = this._getHashes(element);
    return hashes.every(h => this._getBit(h));
  }

  _getHashes(element) {
    // Double hashing: hi(x) = h1(x) + i * h2(x)
    // Only 2 hash functions needed to simulate k independent functions
    const h1 = this._fnv1a(element);
    const h2 = this._djb2(element);

    const hashes = [];
    for (let i = 0; i < this.k; i++) {
      hashes.push(Math.abs((h1 + i * h2) % this.m));
    }
    return hashes;
  }

  _setBit(index) {
    this.bits[Math.floor(index / 8)] |= (1 << (index % 8));
  }

  _getBit(index) {
    return (this.bits[Math.floor(index / 8)] & (1 << (index % 8))) !== 0;
  }

  _fnv1a(str) {
    let hash = 2166136261;
    for (let i = 0; i < str.length; i++) {
      hash ^= str.charCodeAt(i);
      hash = Math.imul(hash, 16777619);
    }
    return hash >>> 0;
  }

  _djb2(str) {
    let hash = 5381;
    for (let i = 0; i < str.length; i++) {
      hash = Math.imul(hash, 33) ^ str.charCodeAt(i);
    }
    return hash >>> 0;
  }

  getFalsePositiveRate() {
    const exponent = -this.k * this.count / this.m;
    return Math.pow(1 - Math.exp(exponent), this.k);
  }

  getStats() {
    return {
      expectedElements: Math.ceil(this.m * Math.LN2 / this.k),
      insertedElements: this.count,
      bitArraySizeMB: (this.bits.length / 1024 / 1024).toFixed(2),
      hashFunctions: this.k,
      estimatedFPRate: this.getFalsePositiveRate().toFixed(6),
    };
  }
}

module.exports = { BloomFilter };
```

---

### 4.15 Consistent Hashing for Sharding

```javascript
// ============================================================
// ConsistentHashRing — Minimal reshuffling when nodes change
// ============================================================

/**
 * Consistent Hashing:
 *   - Arrange all nodes on a virtual ring [0, 2^32)
 *   - Each node occupies multiple virtual nodes (replicas)
 *     for even load distribution
 *   - To find shard for key K: walk clockwise from hash(K)
 *   - Adding/removing a node only moves ~K/N keys (not all)
 *
 * Used for: distributing index shards, cache servers
 */

const crypto = require('crypto');

class ConsistentHashRing {
  /**
   * @param {number} virtualNodes - replicas per physical node
   */
  constructor(virtualNodes = 150) {
    this.virtualNodes = virtualNodes;
    this.ring = new Map();      // hash → nodeName
    this.sortedHashes = [];     // sorted array of hashes on ring
    this.nodes = new Set();
  }

  addNode(nodeName) {
    this.nodes.add(nodeName);
    for (let i = 0; i < this.virtualNodes; i++) {
      const virtualKey = `${nodeName}:vnode:${i}`;
      const hash = this._hash(virtualKey);
      this.ring.set(hash, nodeName);
      this.sortedHashes.push(hash);
    }
    this.sortedHashes.sort((a, b) => (a > b ? 1 : -1));
  }

  removeNode(nodeName) {
    this.nodes.delete(nodeName);
    for (let i = 0; i < this.virtualNodes; i++) {
      const virtualKey = `${nodeName}:vnode:${i}`;
      const hash = this._hash(virtualKey);
      this.ring.delete(hash);
      const idx = this.sortedHashes.indexOf(hash);
      if (idx !== -1) this.sortedHashes.splice(idx, 1);
    }
  }

  /**
   * Get the node responsible for a given key
   */
  getNode(key) {
    if (this.sortedHashes.length === 0) return null;

    const hash = this._hash(key);

    // Binary search for first hash >= key hash (clockwise)
    let lo = 0, hi = this.sortedHashes.length;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (this.sortedHashes[mid] < hash) lo = mid + 1;
      else hi = mid;
    }

    // Wrap around ring
    const idx = lo < this.sortedHashes.length ? lo : 0;
    const ringHash = this.sortedHashes[idx];
    return this.ring.get(ringHash);
  }

  /**
   * Get N replica nodes for a key (for replication)
   */
  getReplicaNodes(key, replicaCount = 3) {
    if (this.sortedHashes.length === 0) return [];

    const hash = this._hash(key);
    const seenNodes = new Set();
    const replicas = [];

    let lo = 0, hi = this.sortedHashes.length;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (this.sortedHashes[mid] < hash) lo = mid + 1;
      else hi = mid;
    }

    let idx = lo < this.sortedHashes.length ? lo : 0;

    while (replicas.length < replicaCount && replicas.length < this.nodes.size) {
      const node = this.ring.get(this.sortedHashes[idx % this.sortedHashes.length]);
      if (!seenNodes.has(node)) {
        seenNodes.add(node);
        replicas.push(node);
      }
      idx++;
    }

    return replicas;
  }

  getDistribution() {
    const dist = new Map();
    for (const node of this.nodes) dist.set(node, 0);
    for (const node of this.ring.values()) {
      dist.set(node, (dist.get(node) || 0) + 1);
    }
    return dist;
  }

  _hash(key) {
    return crypto.createHash('md5').update(key).digest('hex').substring(0, 8);
  }
}

module.exports = { ConsistentHashRing };
```

---

## 5. Database Design

### 5.1 Document Metadata Store (Cassandra)

```
Table: documents
  PRIMARY KEY: (doc_id)
  Columns:
    doc_id       UUID
    url          TEXT
    canonical_url TEXT
    title        TEXT
    meta_desc    TEXT
    body_snippet TEXT    -- first 500 chars of body
    language     TEXT
    content_hash TEXT    -- SHA256 for change detection
    page_rank    FLOAT
    crawled_at   TIMESTAMP
    indexed_at   TIMESTAMP
    http_status  INT
    content_type TEXT

Partition strategy: doc_id UUID (random) → even distribution
Replication: 3 replicas across 3 datacenters
Consistency: LOCAL_QUORUM for reads, LOCAL_QUORUM for writes
```

### 5.2 Link Graph Store (Custom / Neo4j)

```
Nodes: Page { url, domain, pageRank }
Edges: LinksTo { anchorText, rel }

Queries:
  - Inbound links for URL X
  - PageRank computation (graph traversal)
  - Anchor text extraction for URL X

Scale: 10B pages × 30 avg links = 300B edges
Storage: ~6 TB compressed adjacency list (custom binary format)
```

### 5.3 URL Frontier State (Redis + Kafka)

```
Redis:
  SET  seen_urls:<hash>  "1"   (Bloom Filter backed by Redis Bitfield)
  ZSET url_queue         score=priority, member=url

Kafka:
  Topic: crawl-high-priority    (partitions=1000)
  Topic: crawl-normal-priority  (partitions=1000)
  Topic: crawl-low-priority     (partitions=1000)
  Topic: parsed-documents       (partitions=2000)
  Topic: new-urls               (partitions=2000)
```

### 5.4 Query Logs (ClickHouse)

```sql
CREATE TABLE query_logs (
  query_id     UUID,
  query_text   String,
  user_id      Nullable(String),
  session_id   String,
  ip_hash      String,          -- hashed for privacy
  results_count UInt32,
  clicked_docid Nullable(String),
  clicked_rank  Nullable(UInt8),
  query_time_ms UInt32,
  created_at    DateTime
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (created_at, query_text);
```

---

## 6. API Design

### 6.1 Search API

```
GET /v1/search

Query Parameters:
  q        string  REQUIRED  Search query
  page     int     default=1  Page number
  num      int     default=10 Results per page (max 100)
  lang     string  default=en Language filter
  safe     string  default=on Safe search: on|moderate|off
  freshness string optional  d (day) | w (week) | m (month)

Response 200:
{
  "query": "python web frameworks",
  "total_results": 4820000,
  "query_time_ms": 87,
  "results": [
    {
      "position": 1,
      "url": "https://www.djangoproject.com/",
      "title": "The Web framework for perfectionists with deadlines | Django",
      "snippet": "Django makes it easier to build better Web apps more quickly...",
      "domain": "djangoproject.com",
      "page_rank": 0.87,
      "cached_url": "https://cache.mysearch.com/?q=...",
      "indexed_at": "2024-01-15T10:30:00Z"
    }
  ],
  "suggestions": [
    "python web frameworks comparison",
    "python web frameworks 2024"
  ],
  "spell_correction": null
}

Response 429 (Rate limited):
{
  "error": "rate_limit_exceeded",
  "retry_after_ms": 2000
}
```

### 6.2 Autocomplete API

```
GET /v1/suggest?q=pyt&lang=en&limit=8

Response 200:
{
  "prefix": "pyt",
  "suggestions": [
    { "query": "python tutorial", "type": "query" },
    { "query": "python download",  "type": "query" },
    { "query": "python list",      "type": "query" }
  ]
}
```

### 6.3 Indexing API (Internal)

```
POST /internal/v1/index
Body: { docId, url, title, body, pageRank, crawledAt }
→ 202 Accepted (async indexing)

DELETE /internal/v1/index/:docId
→ 200 OK (marks for removal; takes effect on next index merge)

POST /internal/v1/recrawl
Body: { urls: ["https://..."] }
→ 202 Accepted (queues for immediate recrawl)
```

---

## 7. Scalability & Fault Tolerance

### 7.1 Horizontal Scaling

```
Query Serving:
  - Stateless query servers → scale behind load balancer
  - Index shards: add more shards; rehash using consistent hashing
  - Cache: add Redis nodes; consistent hashing maintains locality

Crawling:
  - Add crawler workers; URLs partitioned by domain hash
  - Kafka partitions scale with crawler count

Indexing:
  - Spark jobs scale with cluster size
  - Incremental delta index merges in background (Lucene-style merging)
```

### 7.2 Replication & Durability

```
Index Shards:     3 replicas (1 primary + 2 read replicas)
Document Store:   Cassandra RF=3, LOCAL_QUORUM
Raw HTML:         S3 with cross-region replication (99.999999999% durability)
Query Logs:       Kafka with replication factor 3
```

### 7.3 Failure Modes & Mitigations

| Failure | Detection | Mitigation |
|---|---|---|
| Index shard down | Health checks (every 5s) | Route to replica; background re-election |
| Crawler worker crash | Heartbeat timeout | Kafka consumer group rebalance; uncommitted offsets retried |
| Cache server down | Redis Sentinel | Failover to replica in < 1s; cold start from index |
| Query server down | LB health check | Remove from rotation; traffic redistributed |
| Partial shard results | Timeout + threshold | Return results from available shards (graceful degradation) |
| Data center failure | Multi-region active-active | GeoDNS routes to healthy region |

### 7.4 Query Timeout & Fallback

```javascript
// Scatter-gather with timeout and partial results
async function fanoutQuery(queryTerms, shards, timeoutMs = 150) {
  const promises = shards.map(shard =>
    Promise.race([
      shard.query(queryTerms),
      new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), timeoutMs))
    ]).catch(() => []) // failed shard returns empty; other shards continue
  );

  const shardResults = await Promise.allSettled(promises);
  return shardResults
    .filter(r => r.status === 'fulfilled')
    .map(r => r.value);
}
```

---

## 8. Security Considerations

### 8.1 Input Validation & Sanitization

```
- Max query length: 2048 characters
- Sanitize HTML/script injection in query string
- Normalize Unicode (prevent homograph attacks in URLs)
- Strip null bytes and control characters
```

### 8.2 Crawler Security

```
- Only crawl HTTP/HTTPS (no file://, ftp://)
- Max redirect hops: 5
- Max page size: 10 MB (skip larger)
- DNS rebinding protection: resolve DNS once; reject RFC 1918 addresses
- SSRF protection: blocklist internal IP ranges (10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16)
- Sandbox HTML parsing (separate process with seccomp)
```

### 8.3 Anti-Spam & Search Quality

```
- Detect and penalize: link farms, keyword stuffing, hidden text, cloaking
- Spam signals: excessive exact-match keyword density > 15%,
                many outbound links with unrelated anchor text,
                copied content from other domains (SimHash distance)
- Real-time classifier: lightweight ML model on fetched content
```

### 8.4 Privacy

```
- Query logs: anonymize IP (hash + truncate to /24), no PII stored
- Log retention: 90 days for raw logs, 2 years for aggregated
- GDPR: right-to-erasure endpoint removes URLs from index within 24hrs
- No user tracking without explicit consent
```

---

## 9. Monitoring & Observability

### 9.1 Key Metrics

**Crawler:**
```
- crawler.pages_fetched_per_second      (target: > 4,000)
- crawler.fetch_error_rate              (target: < 2%)
- crawler.avg_fetch_latency_ms          (target: < 2,000ms)
- crawler.frontier_size                 (healthy: 100M-10B URLs)
- crawler.duplicate_rate                (typically 15-25%)
```

**Indexing:**
```
- indexer.docs_indexed_per_second
- indexer.index_lag_seconds             (time from crawl to searchable)
- indexer.index_size_gb
- indexer.merge_duration_ms
```

**Serving:**
```
- search.query_latency_p50/p95/p99     (target: p99 < 200ms)
- search.queries_per_second            (target: 100K QPS globally)
- search.cache_hit_rate                (target: > 70%)
- search.zero_results_rate             (target: < 1%)
- search.error_rate_5xx                (target: < 0.01%)
- search.shard_timeout_rate            (target: < 0.1%)
```

### 9.2 Distributed Tracing

```
- Every query gets a trace_id propagated through all components
- Spans: API Gateway → Query Parser → Cache Lookup → Shard Fan-out → Result Merge
- Tools: Jaeger / Zipkin / AWS X-Ray
- Sampling: 1% of all requests; 100% of slow requests (> 500ms)
```

### 9.3 Alerting (PagerDuty / OpsGenie)

```
CRITICAL:  P99 latency > 500ms for > 2 minutes
CRITICAL:  Error rate > 1% for > 1 minute
CRITICAL:  Cache hit rate < 40% for > 5 minutes
WARNING:   Crawler throughput drops > 50% from baseline
WARNING:   Index lag > 30 minutes for news content
WARNING:   Any index shard replica count < 2
```

---

## 10. Trade-offs & Design Decisions

### 10.1 Term-based vs Document-based Index Sharding

| | Term-based | Document-based |
|---|---|---|
| Query fan-out | One shard per term | All shards |
| Load balance | Imbalanced (hot terms) | Balanced |
| Scalability | Hard (can't split one term) | Easy (add shards) |
| Used by | Early Google | Modern Google, Elasticsearch |
| **Chosen** | | ✅ Document-based |

### 10.2 BM25 vs TF-IDF vs Dense Vectors

| | TF-IDF | BM25 | Dense (BERT) |
|---|---|---|---|
| Quality | Good | Better | Best |
| Latency | Fast | Fast | Slow (GPU) |
| Infra cost | Low | Low | High |
| Interpretability | High | High | Low |
| **Approach** | Fallback | **Primary** | Reranking top-20 |

### 10.3 Batch vs Streaming Indexing

```
Chosen: Hybrid (Lambda Architecture)
  - Batch layer: complete Spark index rebuild weekly
  - Speed layer: Kafka + Flink real-time delta index
  - Serving: merge delta (recent) + batch (comprehensive) at query time
  
Why: Batch gives best index quality; stream ensures freshness for news
```

### 10.4 Push vs Pull for Index Updates

```
Chosen: Pull (re-crawl schedule)
  - Crawlers pull pages on schedule (based on freshness heuristics)
  - Sitemaps (sitemap.xml) hint crawler at updated pages
  - Push supplement: Indexing API for high-priority/verified publishers
  
Why: Web has no universal push mechanism; schedule-based pull is robust
```

---

## 11. Interview Tips & Common Questions

### Q: How do you handle 100K QPS?

> Fan-out to parallel index shards (each handling ~200 QPS), with a large Redis cache absorbing 70%+ of traffic. Cache hit ratio is maintained by serving popular queries from memory. Index shards are stateless replicas (3 each) behind load balancers.

### Q: How do you keep the index fresh for breaking news?

> Two-tier approach: (1) sitemaps + crawl ping notifications from major publishers trigger immediate high-priority re-crawl; (2) news crawler pool runs continuously, recrawling tier-1 news sites every 5-15 minutes. Delta index is updated in real-time via Kafka/Flink and merged with base index at query time.

### Q: How do you handle spell correction?

> Pre-built dictionary from query logs (queries that led to click-throughs) + edit distance (Levenshtein, max 2). Also use n-gram language model to score candidates. For queries with zero results, proactively suggest correction. "Did you mean: X?" uses beam search over correction candidates.

### Q: How do you prevent spam/SEO manipulation?

> Multi-layered: (1) TrustRank — propagate trust from known-good seed sites through link graph; (2) SpamRank — penalize link-farm patterns; (3) content classifiers for keyword stuffing and hidden text; (4) manual review queue for flagged domains; (5) regular index audits.

### Q: How does PageRank scale to 10B pages?

> Distributed iterative computation on graph cluster (Apache Spark GraphX / Pregel). Key optimizations: (1) personalized PageRank per topic; (2) topic-sensitive PageRank for improved quality; (3) incremental update only for pages with changed inbound links (rather than full recompute); (4) run weekly, cache results in document store.

### Q: What's your single biggest bottleneck?

> Index shard fan-out latency. Every query must wait for the slowest shard (tail latency problem). Mitigations: hedged requests (send to 2 replicas; use first response), aggressive timeout with graceful degradation, MaxScore/WAND pruning to reduce per-shard work, and keeping index shards in RAM (mmap).

### Q: How do you handle billion-scale URL deduplication?

> Two-level: (1) Bloom filter (1.2 GB RAM, 1% FP rate) for O(1) quick check; (2) SHA256 hash store in Redis for exact deduplication; (3) SimHash for near-duplicate content detection after fetching. Bloom filter prevents the vast majority of URL re-fetches without DB access.

---

## Quick Reference Card

```
┌─────────────────────────────────────────────────────────┐
│              SEARCH ENGINE — SYSTEM SNAPSHOT            │
├─────────────────┬───────────────────────────────────────┤
│ Scale           │ 10B pages, 100K QPS, 1B users         │
│ Crawl speed     │ 4,000 pages/sec, 400 MB/s bandwidth   │
│ Index size      │ ~120 TB (compressed)                   │
│ Shards          │ 500 doc-sharded, RF=3                  │
│ Cache hit rate  │ ~70% (Redis, LRU + TTL)                │
│ Query latency   │ P99 < 200ms                            │
│ Availability    │ 99.99% (multi-region active-active)    │
├─────────────────┼───────────────────────────────────────┤
│ Key Algorithms  │ BM25 (ranking primary)                 │
│                 │ PageRank (authority)                   │
│                 │ SimHash (near-dup detection)           │
│                 │ Bloom Filter (URL dedup)               │
│                 │ Token Bucket (rate limiting)           │
│                 │ Consistent Hashing (sharding)          │
│                 │ LRU Cache (result caching)             │
│                 │ Porter Stemmer (text normalization)    │
├─────────────────┼───────────────────────────────────────┤
│ Storage         │ HDFS/S3 (raw HTML)                     │
│                 │ Cassandra (doc metadata)               │
│                 │ Redis (cache, frontier)                │
│                 │ Kafka (event streaming)                │
│                 │ ClickHouse (query analytics)           │
└─────────────────┴───────────────────────────────────────┘
```

---

*This document covers a FAANG-level search engine design. In a real interview, focus on clarifying requirements first, drive the HLD before LLD, and be ready to deep-dive on any single component. Always discuss trade-offs explicitly — interviewers value reasoning over memorized answers.*