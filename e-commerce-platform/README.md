# 🛒 E-Commerce Platform — System Design
> **FAANG-Level System Design Document**  
> Covers: High-Level Design (HLD) + Low-Level Design (LLD in JavaScript)  
> Scale Target: 10M DAU | 100K TPS peak | 99.99% Availability

---

## 📑 Table of Contents

1. [Requirements & Estimations](#1-requirements--estimations)
2. [High-Level Design (HLD)](#2-high-level-design-hld)
   - [Core Architecture](#21-core-architecture)
   - [Microservices Overview](#22-microservices-overview)
   - [Data Flow Diagrams](#23-data-flow-diagrams)
   - [CDN & Edge Strategy](#24-cdn--edge-strategy)
   - [Database Strategy](#25-database-strategy)
   - [Caching Strategy](#26-caching-strategy)
   - [Message Queue Architecture](#27-message-queue-architecture)
   - [Search Architecture](#28-search-architecture)
   - [Payment Architecture](#29-payment-architecture)
3. [Low-Level Design (LLD)](#3-low-level-design-lld)
   - [API Gateway](#31-api-gateway)
   - [User Service](#32-user-service)
   - [Product Service](#33-product-service)
   - [Inventory Service](#34-inventory-service)
   - [Cart Service](#35-cart-service)
   - [Order Service](#36-order-service)
   - [Payment Service](#37-payment-service)
   - [Notification Service](#38-notification-service)
   - [Search Service](#39-search-service)
   - [Recommendation Engine](#310-recommendation-engine)
   - [Review & Rating Service](#311-review--rating-service)
   - [Delivery Tracking Service](#312-delivery-tracking-service)
4. [Non-Functional Requirements (NFR) Deep Dive](#4-non-functional-requirements-deep-dive)
5. [Security Architecture](#5-security-architecture)
6. [Observability & Monitoring](#6-observability--monitoring)
7. [Disaster Recovery & Resiliency](#7-disaster-recovery--resiliency)
8. [Deployment & DevOps](#8-deployment--devops)

---

## 1. Requirements & Estimations

### 1.1 Functional Requirements

| Domain | Feature |
|--------|---------|
| **User** | Registration, Login (OAuth2 / OTP / Password), Profile Management, Address Book |
| **Product** | CRUD, Categories, Variants (size/color), Images, Tags |
| **Search** | Full-text search, Filters, Faceted search, Autocomplete, Spell-check |
| **Cart** | Add/Remove/Update, Guest Cart, Cart Merge on login, Price Locking |
| **Order** | Place, Track, Cancel, Return, Refund |
| **Payment** | Multi-gateway (Razorpay/Stripe), COD, Wallet, UPI, EMI |
| **Inventory** | Stock tracking, Low-stock alerts, Warehouse routing |
| **Notifications** | Email, SMS, Push (Web + Mobile), In-App |
| **Recommendations** | Collaborative filtering, Content-based, Trending |
| **Reviews** | Ratings, Reviews, Upvote, Seller Reply |
| **Delivery** | Real-time tracking, ETA, Multi-vendor logistics |
| **Admin** | Seller dashboard, Analytics, Promotions, Coupons |

### 1.2 Non-Functional Requirements

| NFR | Target |
|-----|--------|
| **Availability** | 99.99% (< 52 min/year downtime) |
| **Latency (p99)** | < 200ms for reads, < 500ms for writes |
| **Throughput** | 100K TPS peak (sale events), 10K TPS normal |
| **Scalability** | Horizontal auto-scaling per service |
| **Durability** | Zero data loss for orders/payments (RPO = 0) |
| **Consistency** | Strong consistency for inventory/payment; eventual for catalog |
| **Security** | PCI-DSS Level 1, OWASP Top 10 |

### 1.3 Capacity Estimation

```
Users:          100M registered, 10M DAU
Products:       50M SKUs
Orders/day:     2M orders
Peak QPS:       100K (flash sales)
Storage:
  - Product images:   50M × 5 images × 500KB = 125 TB → CDN
  - User data:        100M × 2KB = 200 GB → PostgreSQL
  - Orders:           2M/day × 365 × 5yr × 2KB = ~7 TB → PostgreSQL + archive
  - Product catalog:  50M × 10KB = 500 GB → MongoDB
  - Cache (Redis):    ~50 GB active working set
  - Search Index:     ~300 GB → Elasticsearch

Bandwidth:
  - Ingress: ~10 Gbps peak
  - Egress:  ~50 Gbps peak (image-heavy)
```

---

## 2. High-Level Design (HLD)

### 2.1 Core Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                           CLIENT LAYER                              │
│    Web (React)    Mobile (iOS/Android)    PWA    Third-Party APIs   │
└──────────────────────────────┬──────────────────────────────────────┘
                               │ HTTPS
┌──────────────────────────────▼──────────────────────────────────────┐
│                    CDN (CloudFront / Akamai)                        │
│              Static Assets, Images, Edge Caching                   │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────────┐
│                  WAF + DDoS Protection (AWS Shield)                 │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────────┐
│               API GATEWAY (Kong / AWS API Gateway)                  │
│     Rate Limiting │ Auth │ SSL Termination │ Request Routing        │
│     Load Balancing │ Circuit Breaker │ Request Aggregation          │
└──────────┬──────────┬──────────┬──────────┬──────────┬─────────────┘
           │          │          │          │          │
    ┌──────▼──┐ ┌─────▼──┐ ┌────▼───┐ ┌───▼────┐ ┌──▼──────┐
    │  User   │ │Product │ │ Order  │ │ Cart   │ │ Search  │
    │Service  │ │Service │ │Service │ │Service │ │Service  │
    └──────┬──┘ └─────┬──┘ └────┬───┘ └───┬────┘ └──┬──────┘
           │          │         │          │         │
    ┌──────▼──┐ ┌─────▼──┐ ┌────▼───┐ ┌───▼────┐ ┌──▼──────┐
    │Inventory│ │Payment │ │Notify  │ │Review  │ │Delivery │
    │Service  │ │Service │ │Service │ │Service │ │Service  │
    └──────┬──┘ └─────┬──┘ └────┬───┘ └───┬────┘ └──┬──────┘
           │          │         │          │         │
┌──────────▼──────────▼─────────▼──────────▼─────────▼────────────────┐
│                     MESSAGE BUS (Apache Kafka)                       │
│  Topics: orders, payments, inventory, notifications, analytics      │
└──────────────────────────────────────────────────────────────────────┘
           │          │         │          │         │
    ┌──────▼──┐ ┌─────▼──┐ ┌────▼───┐ ┌───▼────┐ ┌──▼──────┐
    │PostgreSQL│ │MongoDB │ │ Redis  │ │Elastic-│ │  S3 /   │
    │(Orders) │ │(Product│ │(Cache) │ │ search │ │  MinIO  │
    │         │ │Catalog)│ │        │ │        │ │(Images) │
    └─────────┘ └────────┘ └────────┘ └────────┘ └─────────┘
```

### 2.2 Microservices Overview

| Service | Responsibility | Tech Stack | DB |
|---------|---------------|------------|-----|
| **User Service** | Auth, profile, address | Node.js + Express | PostgreSQL |
| **Product Service** | Catalog, variants, images | Node.js | MongoDB |
| **Inventory Service** | Stock levels, reservations | Node.js | PostgreSQL (ACID) |
| **Cart Service** | Session cart, persistent cart | Node.js | Redis + PostgreSQL |
| **Order Service** | Order lifecycle, state machine | Node.js | PostgreSQL |
| **Payment Service** | Payments, refunds, wallet | Node.js | PostgreSQL |
| **Search Service** | Full-text, filters, autocomplete | Node.js | Elasticsearch |
| **Notification Service** | Email/SMS/Push | Node.js | MongoDB |
| **Recommendation Service** | ML ranking, personalization | Python + Node.js | Redis + MongoDB |
| **Review Service** | Ratings, reviews | Node.js | MongoDB |
| **Delivery Service** | Tracking, ETA | Node.js | MongoDB + Redis |
| **Analytics Service** | Events, funnel, reporting | Node.js | ClickHouse |

### 2.3 Data Flow Diagrams

#### Order Placement Flow (Critical Path)

```
User
 │
 ├─1─► API Gateway (auth token validation)
 │
 ├─2─► Cart Service (fetch cart items + prices)
 │
 ├─3─► Inventory Service (reserve stock — distributed lock via Redis)
 │         └── If reservation fails → return "Out of Stock"
 │
 ├─4─► Order Service (create Order in PENDING state)
 │
 ├─5─► Payment Service (initiate payment)
 │         ├── Payment SUCCESS
 │         │     └─► Order Service (update state → CONFIRMED)
 │         │     └─► Inventory Service (confirm deduction)
 │         │     └─► Kafka: publish "order.placed" event
 │         └── Payment FAILED
 │               └─► Inventory Service (release reservation)
 │               └─► Order Service (update state → FAILED)
 │
 └─6─► Notification Service (async via Kafka)
           ├── Email confirmation
           ├── SMS
           └── Push notification
```

#### Flash Sale / High-Concurrency Flow

```
100K concurrent users
        │
        ▼
  Rate Limiter (Token Bucket per user, Leaky Bucket per IP)
        │
        ▼
  Queue Entry (Redis Queue / SQS FIFO)
        │
        ▼
  Inventory Pre-check (Redis atomic DECR)
        │
        ▼
  Order Worker Pool (process from queue)
        │
        ▼
  DB Write (PostgreSQL, with connection pooling via PgBouncer)
```

### 2.4 CDN & Edge Strategy

```
Strategy:
  - Static Assets (JS, CSS, fonts): Cache-Control: max-age=31536000 (1yr)
  - Product Images: Stored in S3, served via CloudFront
    - Image variants (thumbnail/medium/large) generated on-the-fly via Lambda@Edge
    - WebP conversion at edge for supported browsers
  - API Responses: Edge caching for public catalog (TTL = 60s)
    - Cache key includes: Accept-Language, Currency
    - Cache invalidation via cache tags on product updates
  - Geo-routing: Route users to nearest datacenter (multi-region: us-east, ap-south, eu-west)

Image URL pattern:
  https://cdn.store.com/products/{productId}/{size}/{filename}.webp
  Sizes: thumbnail(100x100), small(300x300), medium(600x600), large(1200x1200)
```

### 2.5 Database Strategy

#### Database Selection Rationale

| Data Type | Database | Reason |
|-----------|----------|--------|
| Users, Orders, Payments | **PostgreSQL** | ACID, relational joins, financial integrity |
| Product Catalog | **MongoDB** | Flexible schema (varying attributes per category), nested documents |
| Sessions, Cart, Cache | **Redis** | Sub-millisecond reads, TTL support, atomic ops |
| Search Index | **Elasticsearch** | Full-text, faceted search, relevance scoring |
| Analytics Events | **ClickHouse** | Columnar, fast aggregations, petabyte scale |
| Images/Files | **S3 / MinIO** | Object storage, CDN integration |

#### Sharding Strategy

```
PostgreSQL (Orders):
  - Shard by: user_id % N (range-based for time queries also supported)
  - Each shard: primary + 2 replicas (sync replication for primary writes)
  - Read replicas serve GET requests (eventual consistency acceptable)
  - PgBouncer for connection pooling (max 10K connections)

MongoDB (Products):
  - Shard key: category_id + product_id (compound)
  - Chunk size: 64MB
  - Mongos routers in each AZ

Redis (Cache/Sessions):
  - Redis Cluster: 6 nodes (3 primary + 3 replicas)
  - Hash slots: 16384, distributed evenly
  - Eviction policy: allkeys-lru
```

#### Database Schema (Key Tables)

```sql
-- Users
CREATE TABLE users (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  email VARCHAR(255) UNIQUE NOT NULL,
  phone VARCHAR(20) UNIQUE,
  password_hash VARCHAR(255),
  is_verified BOOLEAN DEFAULT FALSE,
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW()
);

-- Products (MongoDB document structure)
{
  "_id": "ObjectId",
  "sku": "PROD-001",
  "title": "Product Title",
  "description": "...",
  "category": { "id": "cat_001", "name": "Electronics", "path": ["root","electronics"] },
  "brand": "BrandName",
  "variants": [
    { "variantId": "v1", "color": "Red", "size": "M", "price": 999, "comparePrice": 1299 }
  ],
  "attributes": { "material": "Cotton", "weight": "500g" },
  "images": ["url1", "url2"],
  "tags": ["sale", "trending"],
  "rating": { "average": 4.3, "count": 1250 },
  "status": "ACTIVE",
  "createdAt": "ISODate",
  "updatedAt": "ISODate"
}

-- Orders
CREATE TABLE orders (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID NOT NULL REFERENCES users(id),
  status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
  -- PENDING → CONFIRMED → PROCESSING → SHIPPED → DELIVERED | CANCELLED | RETURNED
  total_amount DECIMAL(12,2) NOT NULL,
  discount_amount DECIMAL(12,2) DEFAULT 0,
  tax_amount DECIMAL(12,2) DEFAULT 0,
  shipping_amount DECIMAL(12,2) DEFAULT 0,
  final_amount DECIMAL(12,2) NOT NULL,
  shipping_address_id UUID,
  coupon_code VARCHAR(50),
  payment_id UUID,
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE order_items (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  order_id UUID NOT NULL REFERENCES orders(id),
  product_id VARCHAR(50) NOT NULL,
  variant_id VARCHAR(50),
  seller_id UUID,
  quantity INT NOT NULL,
  unit_price DECIMAL(12,2) NOT NULL,
  total_price DECIMAL(12,2) NOT NULL,
  status VARCHAR(50) DEFAULT 'PENDING'
);

-- Inventory
CREATE TABLE inventory (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  product_id VARCHAR(50) NOT NULL,
  variant_id VARCHAR(50),
  warehouse_id UUID NOT NULL,
  available_qty INT NOT NULL DEFAULT 0,
  reserved_qty INT NOT NULL DEFAULT 0,
  total_qty INT GENERATED ALWAYS AS (available_qty + reserved_qty) STORED,
  low_stock_threshold INT DEFAULT 10,
  updated_at TIMESTAMP DEFAULT NOW(),
  UNIQUE(product_id, variant_id, warehouse_id)
);

-- Payments
CREATE TABLE payments (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  order_id UUID NOT NULL REFERENCES orders(id),
  user_id UUID NOT NULL REFERENCES users(id),
  gateway VARCHAR(50) NOT NULL, -- STRIPE, RAZORPAY, COD, WALLET
  gateway_payment_id VARCHAR(255) UNIQUE,
  amount DECIMAL(12,2) NOT NULL,
  currency VARCHAR(10) DEFAULT 'INR',
  status VARCHAR(50) NOT NULL, -- INITIATED, SUCCESS, FAILED, REFUNDED
  method VARCHAR(50), -- UPI, CARD, NETBANKING, WALLET, COD
  metadata JSONB,
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW()
);
```

### 2.6 Caching Strategy

```
Multi-Level Cache Architecture:
─────────────────────────────

L1: Browser Cache
  - Static assets (JS/CSS): 1 year via Cache-Control immutable
  - API responses: 30s for product catalog

L2: CDN Cache (CloudFront)
  - Product pages: TTL = 60s
  - Category pages: TTL = 300s
  - User-specific: no CDN cache (pass-through)

L3: Application Cache (Redis)
  - Product details:     Key=product:{id}        TTL=300s
  - Category listing:    Key=category:{id}:page:{n} TTL=120s
  - User session:        Key=session:{token}      TTL=3600s
  - Cart:                Key=cart:{userId}        TTL=86400s
  - Inventory count:     Key=inventory:{productId}:{variantId} TTL=30s
  - Flash sale price:    Key=sale:{saleId}:{productId} TTL=sale duration
  - Rate limit counter:  Key=ratelimit:{userId}:{action} TTL=60s

Cache Invalidation:
  - Write-through for inventory (update cache on every stock change)
  - Cache-aside for products (invalidate on product update event via Kafka)
  - TTL-based expiry for everything else
  - Tag-based invalidation: when a product updates, invalidate all keys
    tagged with that product_id (via Redis Sets tracking tags → keys)

Cache Stampede Prevention:
  - Mutex lock: Only one thread fetches from DB when cache misses
    (Redis SET NX with TTL as distributed lock)
  - Probabilistic early expiration (XFetch algorithm)
```

### 2.7 Message Queue Architecture

```
Kafka Cluster: 3 brokers, RF=3, min.insync.replicas=2

Topics & Partitions:
─────────────────────────────────────────────────────────
Topic                    Partitions  Retention   Consumers
─────────────────────────────────────────────────────────
order.placed             24          7 days      Notification, Inventory, Analytics
order.status.updated     24          7 days      Notification, Analytics
payment.success          24          30 days     Order, Notification, Analytics
payment.failed           24          30 days     Order, Inventory, Notification
inventory.low_stock      6           3 days      Notification (to seller)
inventory.updated        12          3 days      Search (catalog sync), Cache Invalidation
product.updated          12          7 days      Search, CDN Invalidation
user.registered          6           7 days      Email (welcome), Analytics
review.created           12          7 days      Analytics, Recommendation Engine
search.query             24          3 days      Recommendation Engine (behavior)
delivery.status.updated  12          7 days      Notification, Analytics
─────────────────────────────────────────────────────────

Dead Letter Queue (DLQ):
  - All topics have .dlq counterpart
  - Retry policy: 3 retries with exponential backoff (1s, 4s, 16s)
  - After DLQ, alert PagerDuty + store in DB for manual resolution

Consumer Groups:
  - Each service has its own consumer group (independent offsets)
  - Exactly-once semantics for payment events (Kafka Transactions + idempotent consumer)
```

### 2.8 Search Architecture

```
Elasticsearch Cluster: 5 data nodes, 2 master nodes, 2 coordinating nodes

Index Design:
─────────────────────────
Index: products_v{version}  (versioned for zero-downtime reindex)
  Shards: 10 primary, 1 replica each
  
Mapping:
{
  "title":       { type: "text", analyzer: "standard", copy_to: "search_all" }
  "title.raw":   { type: "keyword" }  // for exact sort/filter
  "description": { type: "text", analyzer: "standard" }
  "brand":       { type: "keyword" }
  "category":    { type: "keyword" }
  "tags":        { type: "keyword" }
  "price":       { type: "double" }
  "rating":      { type: "double" }
  "in_stock":    { type: "boolean" }
  "created_at":  { type: "date" }
  "search_all":  { type: "text" }  // catch-all field
  "suggest":     { type: "completion" }  // autocomplete
}

Search Features:
  1. Full-text:    multi_match on title, description, brand, tags
  2. Filters:      term/range queries on category, price, brand, rating
  3. Facets:       aggregations for price ranges, brands, categories
  4. Autocomplete: completion suggester on product titles + brands
  5. Spell-check:  did_you_mean via phrase suggester
  6. Ranking:      BM25 + function score (boost by rating, sales count, freshness)
  7. Personalized: user behavior signals injected into function score

Sync Strategy:
  - Product updates → Kafka topic "product.updated" → ES consumer
  - Bulk reindex: scheduled nightly for full consistency check
  - Index alias swap for zero-downtime reindex:
      products (alias) → products_v3 (current)
      reindex to products_v4 → swap alias atomically
```

### 2.9 Payment Architecture

```
Payment Flow (Saga Pattern):

1. INITIATE: Client calls /payments/initiate
   → Creates payment record (status=INITIATED)
   → Returns payment_session_token

2. PROCESS (on Payment Gateway):
   → User completes payment on Razorpay/Stripe hosted page
   → Gateway webhook fires to /payments/webhook

3. VERIFY:
   → Verify webhook signature (HMAC-SHA256)
   → Idempotency check (payment_id already processed? skip)
   → Update payment status
   → Publish to Kafka

4. RECONCILIATION (async, every 5 min):
   → Poll gateway API for pending payments older than 10 min
   → Reconcile with local DB
   → Handle edge cases: network failure, double-charge

5. REFUND:
   → Validate refund eligibility (order status, time window)
   → Call gateway refund API
   → Update payment record (status=REFUNDED, refund_amount)
   → Notify user

Idempotency:
  - Every payment request has client-generated idempotency_key
  - Server stores (idempotency_key → response) in Redis for 24hr
  - Duplicate requests return cached response without reprocessing

PCI-DSS Compliance:
  - Never store raw card data (tokenized by gateway)
  - All payment data in dedicated subnet with no internet access
  - TLS 1.3 only for payment endpoints
  - Card tokens stored encrypted (AES-256)
```

---

## 3. Low-Level Design (LLD)

> All code in JavaScript (Node.js) following production patterns.

### 3.1 API Gateway

```javascript
// api-gateway/src/index.js
const express = require('express');
const { createProxyMiddleware } = require('http-proxy-middleware');
const rateLimit = require('express-rate-limit');
const { verifyJWT } = require('./auth');
const { circuitBreaker } = require('./circuitBreaker');
const logger = require('./logger');

const app = express();

// ─── Service Registry ───────────────────────────────────────────────
const SERVICE_REGISTRY = {
  user:         process.env.USER_SERVICE_URL,
  product:      process.env.PRODUCT_SERVICE_URL,
  order:        process.env.ORDER_SERVICE_URL,
  cart:         process.env.CART_SERVICE_URL,
  payment:      process.env.PAYMENT_SERVICE_URL,
  search:       process.env.SEARCH_SERVICE_URL,
  inventory:    process.env.INVENTORY_SERVICE_URL,
  notification: process.env.NOTIFICATION_SERVICE_URL,
  review:       process.env.REVIEW_SERVICE_URL,
  delivery:     process.env.DELIVERY_SERVICE_URL,
};

// ─── Rate Limiting ───────────────────────────────────────────────────
const globalLimiter = rateLimit({
  windowMs: 60 * 1000,   // 1 minute
  max: 1000,             // 1000 req/min per IP
  standardHeaders: true,
  legacyHeaders: false,
  keyGenerator: (req) => req.ip,
  handler: (req, res) => {
    logger.warn({ ip: req.ip, path: req.path }, 'Rate limit exceeded');
    res.status(429).json({ error: 'Too many requests', retryAfter: 60 });
  }
});

const authLimiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 min
  max: 20,
  message: { error: 'Too many login attempts' }
});

const paymentLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 10, // Only 10 payment requests per minute per IP
});

app.use(globalLimiter);

// ─── Auth Middleware ─────────────────────────────────────────────────
const PUBLIC_ROUTES = [
  { method: 'GET', path: /^\/api\/products/ },
  { method: 'GET', path: /^\/api\/categories/ },
  { method: 'GET', path: /^\/api\/search/ },
  { method: 'POST', path: /^\/api\/users\/login/ },
  { method: 'POST', path: /^\/api\/users\/register/ },
  { method: 'POST', path: /^\/api\/payments\/webhook/ }, // webhook (has own HMAC auth)
];

const authMiddleware = async (req, res, next) => {
  const isPublic = PUBLIC_ROUTES.some(
    r => r.method === req.method && r.path.test(req.path)
  );
  if (isPublic) return next();

  const token = req.headers.authorization?.split(' ')[1];
  if (!token) return res.status(401).json({ error: 'Unauthorized' });

  try {
    const decoded = await verifyJWT(token);
    req.user = decoded;  // { userId, email, role, sessionId }
    req.headers['x-user-id'] = decoded.userId;
    req.headers['x-user-role'] = decoded.role;
    next();
  } catch (err) {
    return res.status(401).json({ error: 'Invalid or expired token' });
  }
};

app.use(authMiddleware);

// ─── Circuit Breaker + Proxy ─────────────────────────────────────────
const createServiceProxy = (serviceName, target) => {
  const cb = circuitBreaker(serviceName, {
    threshold: 5,         // open after 5 consecutive failures
    timeout: 30000,       // 30s before trying half-open
    successThreshold: 2,  // close after 2 successes in half-open
  });

  return [
    cb.middleware(),
    createProxyMiddleware({
      target,
      changeOrigin: true,
      pathRewrite: { [`^/api/${serviceName}`]: '/api' },
      on: {
        error: (err, req, res) => {
          logger.error({ serviceName, err }, 'Proxy error');
          res.status(502).json({ error: 'Service temporarily unavailable' });
        },
        proxyReq: (proxyReq, req) => {
          // Forward correlation ID for distributed tracing
          proxyReq.setHeader('x-correlation-id', req.headers['x-correlation-id'] || generateId());
          proxyReq.setHeader('x-request-time', Date.now().toString());
        }
      }
    })
  ];
};

// ─── Route Registration ──────────────────────────────────────────────
app.use('/api/users/login',   authLimiter);
app.use('/api/payments',      paymentLimiter);

Object.entries(SERVICE_REGISTRY).forEach(([name, url]) => {
  app.use(`/api/${name}`, ...createServiceProxy(name, url));
});

// ─── Health Check ────────────────────────────────────────────────────
app.get('/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

app.listen(3000, () => console.log('API Gateway running on port 3000'));
```

---

### 3.2 User Service

```javascript
// user-service/src/services/UserService.js

const bcrypt = require('bcrypt');
const jwt = require('jsonwebtoken');
const { v4: uuidv4 } = require('uuid');
const db = require('../db/postgres');
const redis = require('../db/redis');
const kafka = require('../messaging/kafka');

const SALT_ROUNDS = 12;
const ACCESS_TOKEN_TTL = '15m';
const REFRESH_TOKEN_TTL = '30d';
const SESSION_TTL_SECONDS = 30 * 24 * 60 * 60; // 30 days

class UserService {

  // ─── Registration ─────────────────────────────────────────────────
  async register({ email, phone, password, name }) {
    // 1. Check duplicate
    const existing = await db.query(
      'SELECT id FROM users WHERE email = $1 OR phone = $2',
      [email, phone]
    );
    if (existing.rows.length > 0) {
      throw new ConflictError('Email or phone already registered');
    }

    // 2. Hash password
    const passwordHash = await bcrypt.hash(password, SALT_ROUNDS);

    // 3. Create user
    const userId = uuidv4();
    await db.query(
      `INSERT INTO users (id, email, phone, password_hash, name, is_verified)
       VALUES ($1, $2, $3, $4, $5, false)`,
      [userId, email, phone, passwordHash, name]
    );

    // 4. Send verification OTP (async via Kafka)
    await kafka.publish('user.registered', {
      userId, email, phone, name,
      event: 'REGISTRATION',
      timestamp: Date.now()
    });

    return { userId, message: 'OTP sent to email/phone' };
  }

  // ─── Login ─────────────────────────────────────────────────────────
  async login({ email, password, deviceId, ip }) {
    // 1. Fetch user
    const { rows } = await db.query(
      'SELECT * FROM users WHERE email = $1',
      [email]
    );
    if (!rows.length) throw new AuthError('Invalid credentials');
    const user = rows[0];

    // 2. Check account lock (brute force protection)
    const failKey = `auth:fails:${user.id}`;
    const fails = await redis.get(failKey);
    if (parseInt(fails) >= 5) {
      throw new AuthError('Account temporarily locked. Try again in 15 minutes.');
    }

    // 3. Verify password
    const valid = await bcrypt.compare(password, user.password_hash);
    if (!valid) {
      await redis.multi()
        .incr(failKey)
        .expire(failKey, 15 * 60) // 15 min lockout window
        .exec();
      throw new AuthError('Invalid credentials');
    }

    // 4. Clear fail counter on success
    await redis.del(failKey);

    // 5. Generate tokens
    const sessionId = uuidv4();
    const accessToken = jwt.sign(
      { userId: user.id, email: user.email, role: user.role, sessionId },
      process.env.JWT_SECRET,
      { expiresIn: ACCESS_TOKEN_TTL }
    );
    const refreshToken = jwt.sign(
      { userId: user.id, sessionId },
      process.env.JWT_REFRESH_SECRET,
      { expiresIn: REFRESH_TOKEN_TTL }
    );

    // 6. Store session in Redis
    const sessionData = {
      userId: user.id, email: user.email, role: user.role,
      deviceId, ip, createdAt: Date.now()
    };
    await redis.setex(
      `session:${sessionId}`,
      SESSION_TTL_SECONDS,
      JSON.stringify(sessionData)
    );

    // 7. Store refresh token (hashed) in DB for rotation
    const refreshHash = await bcrypt.hash(refreshToken, 4); // fast hash for tokens
    await db.query(
      `INSERT INTO refresh_tokens (user_id, token_hash, session_id, expires_at, device_id)
       VALUES ($1, $2, $3, NOW() + INTERVAL '30 days', $4)`,
      [user.id, refreshHash, sessionId, deviceId]
    );

    return {
      accessToken,
      refreshToken,
      user: { id: user.id, email: user.email, name: user.name, role: user.role }
    };
  }

  // ─── Refresh Token ─────────────────────────────────────────────────
  async refreshToken(oldRefreshToken) {
    let decoded;
    try {
      decoded = jwt.verify(oldRefreshToken, process.env.JWT_REFRESH_SECRET);
    } catch {
      throw new AuthError('Invalid refresh token');
    }

    // Verify token exists in DB (rotation check)
    const { rows } = await db.query(
      `SELECT * FROM refresh_tokens
       WHERE session_id = $1 AND user_id = $2 AND revoked = false AND expires_at > NOW()`,
      [decoded.sessionId, decoded.userId]
    );
    if (!rows.length) throw new AuthError('Refresh token revoked or expired');

    const tokenRecord = rows[0];
    const valid = await bcrypt.compare(oldRefreshToken, tokenRecord.token_hash);
    if (!valid) {
      // Possible token theft — revoke ALL tokens for this user
      await db.query('UPDATE refresh_tokens SET revoked = true WHERE user_id = $1', [decoded.userId]);
      throw new AuthError('Token reuse detected. All sessions terminated.');
    }

    // Rotate: revoke old, issue new
    await db.query('UPDATE refresh_tokens SET revoked = true WHERE id = $1', [tokenRecord.id]);

    const user = await this.getUserById(decoded.userId);
    return this.generateTokenPair(user, decoded.sessionId);
  }

  // ─── Logout ────────────────────────────────────────────────────────
  async logout(sessionId) {
    await redis.del(`session:${sessionId}`);
    await db.query(
      'UPDATE refresh_tokens SET revoked = true WHERE session_id = $1',
      [sessionId]
    );
  }

  // ─── Address Management ────────────────────────────────────────────
  async addAddress(userId, addressData) {
    const { name, line1, line2, city, state, pincode, country, phone, isDefault } = addressData;

    if (isDefault) {
      // Unset existing default
      await db.query(
        'UPDATE user_addresses SET is_default = false WHERE user_id = $1',
        [userId]
      );
    }

    const { rows } = await db.query(
      `INSERT INTO user_addresses
        (id, user_id, name, line1, line2, city, state, pincode, country, phone, is_default)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
       RETURNING *`,
      [uuidv4(), userId, name, line1, line2, city, state, pincode, country, phone, isDefault]
    );

    return rows[0];
  }
}

module.exports = new UserService();
```

---

### 3.3 Product Service

```javascript
// product-service/src/services/ProductService.js

const { ObjectId } = require('mongodb');
const db = require('../db/mongo');
const redis = require('../db/redis');
const kafka = require('../messaging/kafka');
const s3 = require('../storage/s3');
const slugify = require('slugify');

const PRODUCT_CACHE_TTL = 300; // 5 minutes
const CATEGORY_CACHE_TTL = 600; // 10 minutes

class ProductService {

  // ─── Create Product ────────────────────────────────────────────────
  async createProduct(sellerId, productData) {
    const {
      title, description, category, brand,
      variants, attributes, tags, status = 'DRAFT'
    } = productData;

    const slug = slugify(title, { lower: true, strict: true });
    const sku = await this.generateSKU(category.id, brand);

    const product = {
      sku,
      slug,
      sellerId,
      title,
      description,
      category,
      brand,
      variants: variants.map(v => ({
        variantId: new ObjectId().toString(),
        ...v,
        price: parseFloat(v.price),
        comparePrice: v.comparePrice ? parseFloat(v.comparePrice) : null,
      })),
      attributes,
      images: [],
      tags: tags || [],
      rating: { average: 0, count: 0 },
      salesCount: 0,
      status,
      createdAt: new Date(),
      updatedAt: new Date(),
    };

    const result = await db.collection('products').insertOne(product);
    const productId = result.insertedId.toString();

    // Sync to search index async
    await kafka.publish('product.updated', {
      action: 'CREATE',
      productId,
      product: { ...product, _id: productId }
    });

    return { productId, ...product };
  }

  // ─── Get Product (Cache-Aside) ─────────────────────────────────────
  async getProduct(productId) {
    const cacheKey = `product:${productId}`;

    // L1: Redis cache
    const cached = await redis.get(cacheKey);
    if (cached) return JSON.parse(cached);

    // L2: MongoDB
    const product = await db.collection('products').findOne(
      { _id: new ObjectId(productId), status: { $ne: 'DELETED' } }
    );
    if (!product) throw new NotFoundError('Product not found');

    // Store in cache
    await redis.setex(cacheKey, PRODUCT_CACHE_TTL, JSON.stringify(product));

    return product;
  }

  // ─── List Products (Category Page) ────────────────────────────────
  async listProducts({ categoryId, filters = {}, sort = {}, page = 1, limit = 24 }) {
    const cacheKey = `category:${categoryId}:${JSON.stringify({ filters, sort, page })}`;

    const cached = await redis.get(cacheKey);
    if (cached) return JSON.parse(cached);

    const query = {
      'category.id': categoryId,
      status: 'ACTIVE',
      ...(filters.brand    && { brand: { $in: filters.brand } }),
      ...(filters.minPrice && { 'variants.price': { $gte: filters.minPrice } }),
      ...(filters.maxPrice && { 'variants.price': { $lte: filters.maxPrice } }),
      ...(filters.rating   && { 'rating.average': { $gte: filters.rating } }),
      ...(filters.inStock  && { /* join with inventory service */ }),
    };

    const sortMap = {
      'price_asc':   { 'variants.price': 1 },
      'price_desc':  { 'variants.price': -1 },
      'rating':      { 'rating.average': -1 },
      'newest':      { 'createdAt': -1 },
      'popularity':  { 'salesCount': -1 },
    };

    const sortQuery = sortMap[sort.field] || { 'salesCount': -1 };

    const [products, total] = await Promise.all([
      db.collection('products')
        .find(query)
        .sort(sortQuery)
        .skip((page - 1) * limit)
        .limit(limit)
        .toArray(),
      db.collection('products').countDocuments(query),
    ]);

    // Build facets for filters
    const facets = await this.buildFacets(categoryId, query);

    const result = {
      products,
      pagination: { page, limit, total, pages: Math.ceil(total / limit) },
      facets,
    };

    await redis.setex(cacheKey, CATEGORY_CACHE_TTL, JSON.stringify(result));
    return result;
  }

  // ─── Build Search Facets ───────────────────────────────────────────
  async buildFacets(categoryId, baseQuery) {
    const pipeline = [
      { $match: { ...baseQuery, 'category.id': categoryId } },
      {
        $facet: {
          brands: [
            { $group: { _id: '$brand', count: { $sum: 1 } } },
            { $sort: { count: -1 } },
            { $limit: 20 }
          ],
          priceRanges: [
            {
              $bucket: {
                groupBy: '$variants.price',
                boundaries: [0, 500, 1000, 2500, 5000, 10000, 50000],
                default: '50000+',
                output: { count: { $sum: 1 } }
              }
            }
          ],
          ratings: [
            { $group: { _id: { $floor: '$rating.average' }, count: { $sum: 1 } } }
          ]
        }
      }
    ];

    const [result] = await db.collection('products').aggregate(pipeline).toArray();
    return result;
  }

  // ─── Image Upload ──────────────────────────────────────────────────
  async uploadProductImage(productId, fileBuffer, mimeType) {
    const filename = `${productId}/${Date.now()}-original.jpg`;

    // Upload original
    await s3.upload({
      Bucket: process.env.S3_BUCKET,
      Key: `products/${filename}`,
      Body: fileBuffer,
      ContentType: mimeType,
      CacheControl: 'max-age=31536000',
    }).promise();

    const imageUrl = `${process.env.CDN_URL}/products/${filename}`;

    // Update product
    await db.collection('products').updateOne(
      { _id: new ObjectId(productId) },
      {
        $push: { images: imageUrl },
        $set: { updatedAt: new Date() }
      }
    );

    // Invalidate cache
    await redis.del(`product:${productId}`);
    await kafka.publish('product.updated', { action: 'IMAGE_ADDED', productId });

    return imageUrl;
  }

  // ─── Update Product (with cache invalidation) ─────────────────────
  async updateProduct(productId, sellerId, updates) {
    const allowed = ['title', 'description', 'variants', 'attributes', 'tags', 'status'];
    const filtered = Object.fromEntries(
      Object.entries(updates).filter(([k]) => allowed.includes(k))
    );

    const result = await db.collection('products').findOneAndUpdate(
      { _id: new ObjectId(productId), sellerId },
      { $set: { ...filtered, updatedAt: new Date() } },
      { returnDocument: 'after' }
    );

    if (!result.value) throw new NotFoundError('Product not found or unauthorized');

    // Invalidate multi-level cache
    await redis.del(`product:${productId}`);

    // Sync search index
    await kafka.publish('product.updated', {
      action: 'UPDATE',
      productId,
      product: result.value
    });

    return result.value;
  }
}

module.exports = new ProductService();
```

---

### 3.4 Inventory Service

```javascript
// inventory-service/src/services/InventoryService.js
// CRITICAL: Must be strongly consistent — uses DB transactions + Redis atomic ops

const db = require('../db/postgres');
const redis = require('../db/redis');
const kafka = require('../messaging/kafka');

const RESERVATION_TTL = 15 * 60;  // 15 min reservation lock
const LOW_STOCK_THRESHOLD = 10;

class InventoryService {

  // ─── Check Stock ───────────────────────────────────────────────────
  async getStock(productId, variantId) {
    // Redis first (fast path)
    const cacheKey = `inventory:${productId}:${variantId || 'default'}`;
    const cached = await redis.get(cacheKey);
    if (cached !== null) return { available: parseInt(cached), cached: true };

    // DB fallback
    const { rows } = await db.query(
      `SELECT available_qty FROM inventory
       WHERE product_id = $1 AND (variant_id = $2 OR variant_id IS NULL)
       AND available_qty > 0
       ORDER BY available_qty DESC LIMIT 1`,
      [productId, variantId]
    );

    const qty = rows[0]?.available_qty || 0;
    await redis.setex(cacheKey, 30, qty.toString()); // TTL = 30s
    return { available: qty, cached: false };
  }

  // ─── Reserve Stock (Checkout Step) ────────────────────────────────
  // Uses Redis DECRBY for atomic operation + DB for durability
  async reserveStock(orderId, items) {
    const reservations = [];
    const rollbackQueue = [];

    for (const item of items) {
      const { productId, variantId, quantity } = item;
      const lockKey = `inventory:lock:${productId}:${variantId || 'default'}`;
      const cacheKey = `inventory:${productId}:${variantId || 'default'}`;

      // Distributed lock (prevent race condition)
      const lock = await redis.set(lockKey, orderId, 'NX', 'EX', 5); // 5s lock
      if (!lock) throw new ConflictError(`Cannot reserve ${productId}: locked`);

      try {
        // Atomic check-and-decrement in Redis
        const remaining = await redis.decrby(cacheKey, quantity);
        if (remaining < 0) {
          await redis.incrby(cacheKey, quantity); // rollback Redis
          throw new OutOfStockError(`Insufficient stock for ${productId}`);
        }

        // Persist reservation to DB
        const { rows } = await db.query(
          `UPDATE inventory
           SET available_qty = available_qty - $1,
               reserved_qty = reserved_qty + $1,
               updated_at = NOW()
           WHERE product_id = $2
             AND (variant_id = $3 OR variant_id IS NULL)
             AND available_qty >= $1
           RETURNING *`,
          [quantity, productId, variantId]
        );

        if (!rows.length) {
          await redis.incrby(cacheKey, quantity); // rollback Redis
          throw new OutOfStockError(`Insufficient stock for ${productId}`);
        }

        reservations.push({ productId, variantId, quantity, reservationId: `${orderId}:${productId}` });
        rollbackQueue.push({ productId, variantId, quantity });

        // Set reservation expiry key
        await redis.setex(
          `reservation:${orderId}:${productId}`,
          RESERVATION_TTL,
          quantity.toString()
        );

        // Check low stock alert
        if (rows[0].available_qty <= LOW_STOCK_THRESHOLD) {
          await kafka.publish('inventory.low_stock', {
            productId, variantId,
            remaining: rows[0].available_qty,
            threshold: LOW_STOCK_THRESHOLD
          });
        }
      } finally {
        await redis.del(lockKey); // release lock
      }
    }

    return { success: true, reservations };
  }

  // ─── Confirm Reservation (after payment success) ───────────────────
  async confirmReservation(orderId, items) {
    const client = await db.pool.connect();
    try {
      await client.query('BEGIN');

      for (const { productId, variantId, quantity } of items) {
        await client.query(
          `UPDATE inventory
           SET reserved_qty = reserved_qty - $1,
               total_sold = total_sold + $1,
               updated_at = NOW()
           WHERE product_id = $2
             AND (variant_id = $3 OR variant_id IS NULL)`,
          [quantity, productId, variantId]
        );

        // Clear reservation key
        await redis.del(`reservation:${orderId}:${productId}`);
      }

      await client.query('COMMIT');
      await kafka.publish('inventory.updated', { orderId, items, action: 'CONFIRMED' });
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }
  }

  // ─── Release Reservation (on payment failure / expiry) ────────────
  async releaseReservation(orderId, items) {
    const client = await db.pool.connect();
    try {
      await client.query('BEGIN');

      for (const { productId, variantId, quantity } of items) {
        await client.query(
          `UPDATE inventory
           SET available_qty = available_qty + $1,
               reserved_qty = reserved_qty - $1,
               updated_at = NOW()
           WHERE product_id = $2
             AND (variant_id = $3 OR variant_id IS NULL)`,
          [quantity, productId, variantId]
        );

        // Restore Redis cache
        const cacheKey = `inventory:${productId}:${variantId || 'default'}`;
        await redis.incrby(cacheKey, quantity);
        await redis.del(`reservation:${orderId}:${productId}`);
      }

      await client.query('COMMIT');
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }
  }
}

module.exports = new InventoryService();
```

---

### 3.5 Cart Service

```javascript
// cart-service/src/services/CartService.js

const redis = require('../db/redis');
const db = require('../db/postgres');
const ProductService = require('../../product-service/client');
const InventoryService = require('../../inventory-service/client');

const GUEST_CART_TTL = 7 * 24 * 60 * 60;    // 7 days
const USER_CART_TTL  = 30 * 24 * 60 * 60;   // 30 days

class CartService {

  cartKey(userId)      { return `cart:user:${userId}`; }
  guestCartKey(token)  { return `cart:guest:${token}`; }

  // ─── Get Cart ──────────────────────────────────────────────────────
  async getCart(userId) {
    const raw = await redis.hgetall(this.cartKey(userId));
    if (!raw || !Object.keys(raw).length) return { items: [], total: 0, itemCount: 0 };

    const items = Object.values(raw).map(v => JSON.parse(v));

    // Enrich with fresh prices & stock status
    const enriched = await this.enrichCartItems(items);

    const total = enriched.reduce((sum, item) => {
      return sum + (item.currentPrice * item.quantity);
    }, 0);

    return {
      items: enriched,
      total: parseFloat(total.toFixed(2)),
      itemCount: enriched.reduce((s, i) => s + i.quantity, 0),
      savings: this.calculateSavings(enriched)
    };
  }

  // ─── Add to Cart ───────────────────────────────────────────────────
  async addToCart(userId, { productId, variantId, quantity = 1 }) {
    // 1. Validate product exists and is active
    const product = await ProductService.getProduct(productId);
    if (product.status !== 'ACTIVE') throw new BadRequestError('Product not available');

    const variant = variantId
      ? product.variants.find(v => v.variantId === variantId)
      : product.variants[0];
    if (!variant) throw new NotFoundError('Variant not found');

    // 2. Check stock
    const stock = await InventoryService.getStock(productId, variantId);
    if (stock.available < quantity) throw new BadRequestError('Insufficient stock');

    // 3. Check max quantity per item (fraud/abuse prevention)
    const MAX_QTY = 10;
    const cartKey = this.cartKey(userId);
    const itemKey = `${productId}:${variantId || 'default'}`;
    const existing = await redis.hget(cartKey, itemKey);
    const existingQty = existing ? JSON.parse(existing).quantity : 0;

    if (existingQty + quantity > MAX_QTY) {
      throw new BadRequestError(`Maximum quantity per item is ${MAX_QTY}`);
    }

    // 4. Store cart item
    const cartItem = {
      productId,
      variantId: variantId || null,
      quantity: existingQty + quantity,
      addedPrice: variant.price,      // locked at add time
      title: product.title,
      image: product.images[0] || null,
      brand: product.brand,
      variant: { color: variant.color, size: variant.size },
      addedAt: Date.now(),
    };

    await redis.hset(cartKey, itemKey, JSON.stringify(cartItem));
    await redis.expire(cartKey, USER_CART_TTL);

    return { success: true, item: cartItem };
  }

  // ─── Update Quantity ───────────────────────────────────────────────
  async updateCartItem(userId, productId, variantId, quantity) {
    const cartKey = this.cartKey(userId);
    const itemKey = `${productId}:${variantId || 'default'}`;

    if (quantity <= 0) {
      await redis.hdel(cartKey, itemKey);
      return { success: true, removed: true };
    }

    const existing = await redis.hget(cartKey, itemKey);
    if (!existing) throw new NotFoundError('Item not in cart');

    const item = JSON.parse(existing);
    item.quantity = quantity;

    await redis.hset(cartKey, itemKey, JSON.stringify(item));
    return { success: true, item };
  }

  // ─── Merge Guest Cart on Login ─────────────────────────────────────
  async mergeGuestCart(userId, guestToken) {
    const guestKey = this.guestCartKey(guestToken);
    const userKey = this.cartKey(userId);

    const guestItems = await redis.hgetall(guestKey);
    if (!guestItems || !Object.keys(guestItems).length) return;

    for (const [itemKey, rawItem] of Object.entries(guestItems)) {
      const guestItem = JSON.parse(rawItem);
      const existingRaw = await redis.hget(userKey, itemKey);

      if (existingRaw) {
        // Merge: take max quantity (guest or user)
        const existing = JSON.parse(existingRaw);
        guestItem.quantity = Math.max(existing.quantity, guestItem.quantity);
      }

      await redis.hset(userKey, itemKey, JSON.stringify(guestItem));
    }

    await redis.expire(userKey, USER_CART_TTL);
    await redis.del(guestKey); // delete guest cart
  }

  // ─── Enrich Cart with Current Prices ──────────────────────────────
  async enrichCartItems(items) {
    const productIds = [...new Set(items.map(i => i.productId))];

    // Batch fetch products
    const products = await ProductService.getProductsBatch(productIds);
    const productMap = new Map(products.map(p => [p._id.toString(), p]));

    return items.map(item => {
      const product = productMap.get(item.productId);
      if (!product) return { ...item, isAvailable: false };

      const variant = item.variantId
        ? product.variants.find(v => v.variantId === item.variantId)
        : product.variants[0];

      const currentPrice = variant?.price || item.addedPrice;
      const priceChanged = currentPrice !== item.addedPrice;

      return {
        ...item,
        currentPrice,
        comparePrice: variant?.comparePrice || null,
        priceChanged,
        priceChangeAmount: priceChanged ? currentPrice - item.addedPrice : 0,
        isAvailable: product.status === 'ACTIVE',
      };
    });
  }

  calculateSavings(items) {
    return items.reduce((sum, item) => {
      if (item.comparePrice && item.comparePrice > item.currentPrice) {
        return sum + (item.comparePrice - item.currentPrice) * item.quantity;
      }
      return sum;
    }, 0);
  }
}

module.exports = new CartService();
```

---

### 3.6 Order Service

```javascript
// order-service/src/services/OrderService.js
// Implements Order State Machine + Saga Pattern

const { v4: uuidv4 } = require('uuid');
const db = require('../db/postgres');
const kafka = require('../messaging/kafka');
const CartService = require('../../cart-service/client');
const InventoryService = require('../../inventory-service/client');
const PaymentService = require('../../payment-service/client');
const CouponService = require('./CouponService');

// Order State Machine
const ORDER_STATES = {
  PENDING:     { next: ['CONFIRMED', 'FAILED', 'CANCELLED'] },
  CONFIRMED:   { next: ['PROCESSING', 'CANCELLED'] },
  PROCESSING:  { next: ['SHIPPED', 'CANCELLED'] },
  SHIPPED:     { next: ['DELIVERED', 'RETURNED'] },
  DELIVERED:   { next: ['RETURNED', 'COMPLETED'] },
  COMPLETED:   { next: [] },
  CANCELLED:   { next: [] },
  FAILED:      { next: [] },
  RETURNED:    { next: ['REFUNDED'] },
  REFUNDED:    { next: [] },
};

class OrderService {

  // ─── Place Order (Saga Orchestrator) ──────────────────────────────
  async placeOrder(userId, { cartItems, addressId, couponCode, paymentMethod }) {
    const orderId = uuidv4();
    let inventoryReserved = false;

    try {
      // 1. Validate cart
      if (!cartItems || !cartItems.length) throw new BadRequestError('Cart is empty');

      // 2. Get fresh prices (prevent price manipulation)
      const validatedItems = await this.validateAndPriceItems(cartItems);

      // 3. Calculate totals
      const pricing = await this.calculatePricing(validatedItems, couponCode, userId);

      // 4. Reserve inventory
      await InventoryService.reserveStock(orderId, validatedItems);
      inventoryReserved = true;

      // 5. Create order in DB (PENDING state)
      const order = await this.createOrderRecord(orderId, userId, {
        items: validatedItems,
        ...pricing,
        addressId,
        couponCode,
        paymentMethod,
      });

      // 6. Initiate payment
      const payment = await PaymentService.initiatePayment({
        orderId,
        userId,
        amount: pricing.finalAmount,
        method: paymentMethod,
        metadata: { couponCode }
      });

      // 7. Clear cart (optimistically, can restore on failure)
      await CartService.clearCart(userId);

      return {
        orderId,
        paymentSessionToken: payment.sessionToken,
        paymentUrl: payment.redirectUrl,
        amount: pricing.finalAmount,
        estimatedDelivery: this.calculateDeliveryDate(),
      };

    } catch (err) {
      // Saga Compensation: rollback inventory if reserved
      if (inventoryReserved) {
        await InventoryService.releaseReservation(orderId, cartItems).catch(
          e => console.error('Inventory rollback failed', e)
        );
      }
      throw err;
    }
  }

  // ─── State Transition ──────────────────────────────────────────────
  async transitionOrder(orderId, newStatus, metadata = {}) {
    const { rows } = await db.query(
      'SELECT status FROM orders WHERE id = $1 FOR UPDATE', // pessimistic lock
      [orderId]
    );
    if (!rows.length) throw new NotFoundError('Order not found');

    const currentStatus = rows[0].status;
    const allowed = ORDER_STATES[currentStatus]?.next || [];

    if (!allowed.includes(newStatus)) {
      throw new BadRequestError(
        `Invalid transition: ${currentStatus} → ${newStatus}`
      );
    }

    const { rows: updated } = await db.query(
      `UPDATE orders
       SET status = $1, updated_at = NOW(),
           metadata = metadata || $2::jsonb
       WHERE id = $3 RETURNING *`,
      [newStatus, JSON.stringify(metadata), orderId]
    );

    // Publish event for each transition
    await kafka.publish('order.status.updated', {
      orderId,
      userId: updated[0].user_id,
      previousStatus: currentStatus,
      newStatus,
      metadata,
      timestamp: Date.now()
    });

    return updated[0];
  }

  // ─── Calculate Pricing ─────────────────────────────────────────────
  async calculatePricing(items, couponCode, userId) {
    const subtotal = items.reduce((s, i) => s + i.price * i.quantity, 0);

    let discountAmount = 0;
    let couponData = null;

    if (couponCode) {
      couponData = await CouponService.validate(couponCode, userId, items, subtotal);
      discountAmount = couponData.discountAmount;
    }

    const taxRate = 0.18; // 18% GST (simplified)
    const taxableAmount = subtotal - discountAmount;
    const taxAmount = parseFloat((taxableAmount * taxRate).toFixed(2));

    const shippingAmount = subtotal >= 499 ? 0 : 49; // Free shipping above ₹499

    const finalAmount = parseFloat((taxableAmount + taxAmount + shippingAmount).toFixed(2));

    return { subtotal, discountAmount, taxAmount, shippingAmount, finalAmount, couponData };
  }

  // ─── Validate & Price Items (prevents price manipulation) ──────────
  async validateAndPriceItems(cartItems) {
    // Fetch current prices from product service (source of truth)
    const ProductService = require('../../product-service/client');

    return Promise.all(cartItems.map(async item => {
      const product = await ProductService.getProduct(item.productId);
      const variant = item.variantId
        ? product.variants.find(v => v.variantId === item.variantId)
        : product.variants[0];

      return {
        ...item,
        price: variant.price,  // ALWAYS use server-side price
        title: product.title,
        image: product.images[0],
        sellerId: product.sellerId,
      };
    }));
  }

  // ─── Cancel Order ──────────────────────────────────────────────────
  async cancelOrder(orderId, userId, reason) {
    const { rows } = await db.query(
      'SELECT * FROM orders WHERE id = $1 AND user_id = $2',
      [orderId, userId]
    );
    if (!rows.length) throw new NotFoundError('Order not found');

    const order = rows[0];
    const cancellable = ['PENDING', 'CONFIRMED', 'PROCESSING'];
    if (!cancellable.includes(order.status)) {
      throw new BadRequestError('Order cannot be cancelled at this stage');
    }

    await this.transitionOrder(orderId, 'CANCELLED', { reason, cancelledBy: 'USER' });

    // Trigger refund if already paid
    if (order.payment_id) {
      await PaymentService.initiateRefund({
        orderId,
        paymentId: order.payment_id,
        amount: order.final_amount,
        reason: 'ORDER_CANCELLED'
      });
    }

    // Release inventory
    const items = await db.query('SELECT * FROM order_items WHERE order_id = $1', [orderId]);
    await InventoryService.releaseReservation(orderId, items.rows);

    return { success: true, message: 'Order cancelled successfully' };
  }

  calculateDeliveryDate() {
    const date = new Date();
    date.setDate(date.getDate() + 5); // 3-5 business days
    return date.toISOString().split('T')[0];
  }
}

module.exports = new OrderService();
```

---

### 3.7 Payment Service

```javascript
// payment-service/src/services/PaymentService.js

const crypto = require('crypto');
const { v4: uuidv4 } = require('uuid');
const db = require('../db/postgres');
const redis = require('../db/redis');
const kafka = require('../messaging/kafka');
const Razorpay = require('razorpay');
const Stripe = require('stripe');

const razorpay = new Razorpay({
  key_id: process.env.RAZORPAY_KEY_ID,
  key_secret: process.env.RAZORPAY_SECRET
});
const stripe = new Stripe(process.env.STRIPE_SECRET_KEY);

class PaymentService {

  // ─── Initiate Payment ──────────────────────────────────────────────
  async initiatePayment({ orderId, userId, amount, method, metadata }) {
    // Idempotency check
    const idempotencyKey = `payment:initiate:${orderId}`;
    const existing = await redis.get(idempotencyKey);
    if (existing) return JSON.parse(existing);

    const paymentId = uuidv4();

    // Create payment record
    await db.query(
      `INSERT INTO payments (id, order_id, user_id, gateway, amount, currency, status, method, metadata)
       VALUES ($1, $2, $3, $4, $5, $6, 'INITIATED', $7, $8)`,
      [paymentId, orderId, userId, this.resolveGateway(method), amount, 'INR', method, JSON.stringify(metadata)]
    );

    let gatewayResponse;

    if (method === 'COD') {
      // No gateway needed
      await this.handleCOD(paymentId, orderId);
      return { paymentId, method: 'COD', status: 'CONFIRMED' };
    }

    if (this.resolveGateway(method) === 'RAZORPAY') {
      const order = await razorpay.orders.create({
        amount: Math.round(amount * 100), // paise
        currency: 'INR',
        receipt: orderId,
        notes: { paymentId, userId }
      });
      gatewayResponse = {
        gatewayOrderId: order.id,
        sessionToken: order.id,
        keyId: process.env.RAZORPAY_KEY_ID,
        amount: order.amount,
        currency: order.currency,
      };
    }

    if (this.resolveGateway(method) === 'STRIPE') {
      const intent = await stripe.paymentIntents.create({
        amount: Math.round(amount * 100), // cents
        currency: 'inr',
        metadata: { orderId, paymentId, userId }
      });
      gatewayResponse = {
        clientSecret: intent.client_secret,
        sessionToken: intent.id,
      };
    }

    // Update payment with gateway ID
    await db.query(
      'UPDATE payments SET gateway_payment_id = $1 WHERE id = $2',
      [gatewayResponse.sessionToken, paymentId]
    );

    const result = { paymentId, ...gatewayResponse };

    // Cache for idempotency (24hr)
    await redis.setex(idempotencyKey, 86400, JSON.stringify(result));

    return result;
  }

  // ─── Handle Webhook (from payment gateway) ────────────────────────
  async handleWebhook(gateway, signature, rawBody, payload) {
    // 1. Verify webhook signature
    this.verifyWebhookSignature(gateway, signature, rawBody);

    // 2. Extract payment info
    const { gatewayPaymentId, status, orderId } = this.parseWebhookPayload(gateway, payload);

    // 3. Idempotency: skip if already processed
    const processedKey = `webhook:processed:${gateway}:${gatewayPaymentId}`;
    const alreadyProcessed = await redis.set(processedKey, '1', 'NX', 'EX', 86400);
    if (!alreadyProcessed) {
      console.log(`Duplicate webhook: ${gatewayPaymentId}`);
      return { status: 'DUPLICATE_SKIPPED' };
    }

    // 4. Update payment status
    const newStatus = status === 'captured' || status === 'succeeded' ? 'SUCCESS' : 'FAILED';

    const { rows } = await db.query(
      `UPDATE payments
       SET status = $1, updated_at = NOW()
       WHERE gateway_payment_id = $2
       RETURNING *`,
      [newStatus, gatewayPaymentId]
    );

    if (!rows.length) throw new NotFoundError('Payment record not found');
    const payment = rows[0];

    // 5. Publish to Kafka for downstream
    const topic = newStatus === 'SUCCESS' ? 'payment.success' : 'payment.failed';
    await kafka.publish(topic, {
      paymentId: payment.id,
      orderId: payment.order_id,
      userId: payment.user_id,
      amount: payment.amount,
      method: payment.method,
      gateway,
      gatewayPaymentId,
      timestamp: Date.now()
    });

    return { status: 'PROCESSED', paymentId: payment.id, newStatus };
  }

  // ─── Initiate Refund ───────────────────────────────────────────────
  async initiateRefund({ orderId, paymentId, amount, reason }) {
    const { rows } = await db.query(
      'SELECT * FROM payments WHERE id = $1 AND order_id = $2 AND status = $3',
      [paymentId, orderId, 'SUCCESS']
    );
    if (!rows.length) throw new BadRequestError('No eligible payment found for refund');

    const payment = rows[0];

    if (payment.method === 'COD') {
      // COD refund: bank transfer or wallet credit
      await this.issueCODRefund(payment, amount, reason);
    } else if (payment.gateway === 'RAZORPAY') {
      await razorpay.payments.refund(payment.gateway_payment_id, {
        amount: Math.round(amount * 100),
        notes: { reason, orderId }
      });
    } else if (payment.gateway === 'STRIPE') {
      await stripe.refunds.create({
        payment_intent: payment.gateway_payment_id,
        amount: Math.round(amount * 100),
        reason: 'requested_by_customer'
      });
    }

    await db.query(
      `UPDATE payments SET status = 'REFUNDED', refund_amount = $1, refund_reason = $2, updated_at = NOW()
       WHERE id = $3`,
      [amount, reason, paymentId]
    );

    await kafka.publish('payment.refunded', {
      paymentId, orderId, amount, reason, timestamp: Date.now()
    });

    return { success: true };
  }

  // ─── Webhook Signature Verification ────────────────────────────────
  verifyWebhookSignature(gateway, signature, rawBody) {
    if (gateway === 'RAZORPAY') {
      const expected = crypto
        .createHmac('sha256', process.env.RAZORPAY_WEBHOOK_SECRET)
        .update(rawBody)
        .digest('hex');
      if (expected !== signature) throw new AuthError('Invalid webhook signature');
    }
    if (gateway === 'STRIPE') {
      stripe.webhooks.constructEvent(rawBody, signature, process.env.STRIPE_WEBHOOK_SECRET);
    }
  }

  resolveGateway(method) {
    const map = {
      UPI: 'RAZORPAY', CARD: 'RAZORPAY', NETBANKING: 'RAZORPAY',
      WALLET: 'RAZORPAY', COD: 'COD', STRIPE: 'STRIPE'
    };
    return map[method] || 'RAZORPAY';
  }
}

module.exports = new PaymentService();
```

---

### 3.8 Notification Service

```javascript
// notification-service/src/services/NotificationService.js
// Fan-out service consuming Kafka events

const nodemailer = require('nodemailer');
const twilio = require('twilio');
const admin = require('firebase-admin');
const kafka = require('../messaging/kafka');
const db = require('../db/mongo');
const redis = require('../db/redis');

const twilioClient = twilio(process.env.TWILIO_ACCOUNT_SID, process.env.TWILIO_AUTH_TOKEN);

class NotificationService {

  constructor() {
    this.setupKafkaConsumers();
  }

  // ─── Kafka Event Consumers ─────────────────────────────────────────
  async setupKafkaConsumers() {
    const eventHandlers = {
      'order.placed':           this.onOrderPlaced.bind(this),
      'order.status.updated':   this.onOrderStatusUpdated.bind(this),
      'payment.success':        this.onPaymentSuccess.bind(this),
      'payment.failed':         this.onPaymentFailed.bind(this),
      'inventory.low_stock':    this.onLowStock.bind(this),
      'user.registered':        this.onUserRegistered.bind(this),
    };

    for (const [topic, handler] of Object.entries(eventHandlers)) {
      await kafka.consume(topic, 'notification-service', async (message) => {
        const event = JSON.parse(message.value);
        await handler(event);
      });
    }
  }

  // ─── Event Handlers ────────────────────────────────────────────────
  async onOrderPlaced({ orderId, userId, amount }) {
    const user = await this.getUser(userId);
    await Promise.all([
      this.sendEmail(user.email, 'order_confirmed', { orderId, amount, name: user.name }),
      this.sendSMS(user.phone, `Order #${orderId.slice(-8)} confirmed! Amount: ₹${amount}`),
      this.sendPushNotification(userId, {
        title: 'Order Confirmed! 🎉',
        body: `Your order #${orderId.slice(-8)} has been placed successfully.`,
        data: { type: 'ORDER', orderId }
      }),
    ]);
  }

  async onOrderStatusUpdated({ orderId, userId, newStatus, metadata }) {
    const messages = {
      SHIPPED:   { title: 'Your order is on its way! 🚚', sms: `Order #${orderId.slice(-8)} shipped. Track: ${metadata.trackingUrl}` },
      DELIVERED: { title: 'Order Delivered! ✅', sms: `Order #${orderId.slice(-8)} delivered. Rate your experience!` },
      CANCELLED: { title: 'Order Cancelled', sms: `Order #${orderId.slice(-8)} has been cancelled.` },
    };

    const msg = messages[newStatus];
    if (!msg) return;

    const user = await this.getUser(userId);
    await Promise.all([
      this.sendPushNotification(userId, { title: msg.title, body: msg.sms, data: { orderId } }),
      this.sendSMS(user.phone, msg.sms),
    ]);
  }

  // ─── Email ─────────────────────────────────────────────────────────
  async sendEmail(to, templateId, variables) {
    const template = await this.getTemplate(templateId);
    const html = this.renderTemplate(template.html, variables);

    // Log notification
    await this.logNotification({ type: 'EMAIL', to, templateId, variables });

    await this.emailTransport.sendMail({
      from: `"ShopX" <noreply@shopx.com>`,
      to,
      subject: this.renderTemplate(template.subject, variables),
      html,
    });
  }

  // ─── SMS ───────────────────────────────────────────────────────────
  async sendSMS(to, message) {
    if (!to) return;

    // Deduplication: don't send same SMS twice in 5 min
    const dedupKey = `sms:dedup:${to}:${crypto.createHash('md5').update(message).digest('hex')}`;
    const sent = await redis.set(dedupKey, '1', 'NX', 'EX', 300);
    if (!sent) return;

    await twilioClient.messages.create({
      body: message,
      from: process.env.TWILIO_PHONE,
      to: `+91${to}`, // India prefix
    });
  }

  // ─── Push Notification ─────────────────────────────────────────────
  async sendPushNotification(userId, { title, body, data }) {
    // Get device tokens for user
    const tokens = await db.collection('device_tokens').find({ userId }).toArray();
    if (!tokens.length) return;

    const fcmTokens = tokens.map(t => t.fcmToken);

    const message = {
      notification: { title, body },
      data: { ...data, timestamp: Date.now().toString() },
      tokens: fcmTokens,
      android: { priority: 'high' },
      apns: { headers: { 'apns-priority': '10' } },
    };

    const response = await admin.messaging().sendEachForMulticast(message);

    // Remove invalid tokens
    const invalidTokens = fcmTokens.filter((_, i) => !response.responses[i].success);
    if (invalidTokens.length) {
      await db.collection('device_tokens').deleteMany({ fcmToken: { $in: invalidTokens } });
    }
  }

  // ─── Log Notification ──────────────────────────────────────────────
  async logNotification({ type, to, templateId, variables, status = 'SENT' }) {
    await db.collection('notification_logs').insertOne({
      type, to, templateId, variables, status,
      createdAt: new Date()
    });
  }

  async getUser(userId) {
    const cacheKey = `user:basic:${userId}`;
    const cached = await redis.get(cacheKey);
    if (cached) return JSON.parse(cached);
    // Fetch from user service...
  }
}

module.exports = new NotificationService();
```

---

### 3.9 Search Service

```javascript
// search-service/src/services/SearchService.js

const { Client: ESClient } = require('@elastic/elasticsearch');
const es = new ESClient({ node: process.env.ELASTICSEARCH_URL });
const redis = require('../db/redis');

const INDEX = 'products';

class SearchService {

  // ─── Search ────────────────────────────────────────────────────────
  async search({
    query,
    filters = {},
    sort = 'relevance',
    page = 1,
    limit = 24,
    userId = null
  }) {
    const cacheKey = `search:${JSON.stringify({ query, filters, sort, page })}`;

    // Cache only non-personalized results
    if (!userId) {
      const cached = await redis.get(cacheKey);
      if (cached) return JSON.parse(cached);
    }

    const from = (page - 1) * limit;

    // Build must clauses
    const must = query
      ? [{
          multi_match: {
            query,
            fields: ['title^3', 'brand^2', 'tags^2', 'description'],
            type: 'best_fields',
            fuzziness: 'AUTO',
            minimum_should_match: '75%',
          }
        }]
      : [{ match_all: {} }];

    // Build filter clauses
    const filterClauses = [{ term: { status: 'ACTIVE' } }];
    if (filters.category)  filterClauses.push({ term: { 'category.id': filters.category } });
    if (filters.brand)     filterClauses.push({ terms: { brand: filters.brand } });
    if (filters.minPrice || filters.maxPrice) {
      filterClauses.push({ range: { price: {
        ...(filters.minPrice && { gte: filters.minPrice }),
        ...(filters.maxPrice && { lte: filters.maxPrice }),
      }}});
    }
    if (filters.rating)    filterClauses.push({ range: { 'rating.average': { gte: filters.rating } } });
    if (filters.inStock)   filterClauses.push({ term: { in_stock: true } });

    // Build sort
    const sortMap = {
      relevance:   '_score',
      price_asc:   { price: 'asc' },
      price_desc:  { price: 'desc' },
      rating:      { 'rating.average': 'desc' },
      newest:      { created_at: 'desc' },
      popularity:  { sales_count: 'desc' },
    };

    // Function score for personalization
    const functionScore = userId
      ? await this.buildPersonalizedScore(userId, { must, filterClauses })
      : null;

    const esQuery = {
      index: INDEX,
      body: {
        from,
        size: limit,
        query: functionScore || {
          bool: { must, filter: filterClauses }
        },
        sort: sort === 'relevance' ? ['_score'] : [sortMap[sort] || '_score'],
        aggs: {
          categories: { terms: { field: 'category.id', size: 20 } },
          brands:     { terms: { field: 'brand', size: 30 } },
          price_ranges: {
            range: {
              field: 'price',
              ranges: [
                { to: 500 }, { from: 500, to: 1000 },
                { from: 1000, to: 5000 }, { from: 5000 }
              ]
            }
          },
          ratings: { range: { field: 'rating.average', ranges: [
            { from: 4 }, { from: 3, to: 4 }, { from: 2, to: 3 }
          ]}},
        },
        highlight: {
          fields: { title: {}, description: { fragment_size: 150 } }
        }
      }
    };

    const response = await es.search(esQuery);
    const result = this.formatSearchResponse(response, page, limit);

    // Cache for 60s (public results only)
    if (!userId) {
      await redis.setex(cacheKey, 60, JSON.stringify(result));
    }

    return result;
  }

  // ─── Autocomplete ──────────────────────────────────────────────────
  async autocomplete(prefix, limit = 8) {
    const cacheKey = `autocomplete:${prefix}`;
    const cached = await redis.get(cacheKey);
    if (cached) return JSON.parse(cached);

    const response = await es.search({
      index: INDEX,
      body: {
        suggest: {
          product_suggest: {
            prefix,
            completion: {
              field: 'suggest',
              size: limit,
              skip_duplicates: true,
              fuzzy: { fuzziness: 1 }
            }
          }
        }
      }
    });

    const suggestions = response.body.suggest.product_suggest[0].options.map(o => ({
      text: o.text,
      productId: o._source._id,
      score: o._score,
    }));

    await redis.setex(cacheKey, 30, JSON.stringify(suggestions));
    return suggestions;
  }

  // ─── Personalized Scoring ──────────────────────────────────────────
  async buildPersonalizedScore(userId, { must, filterClauses }) {
    // Get user preference signals from Redis
    const behaviorKey = `user:behavior:${userId}`;
    const behavior = await redis.hgetall(behaviorKey);

    const functions = [];

    // Boost categories user has browsed
    if (behavior?.categories) {
      const categories = JSON.parse(behavior.categories);
      categories.forEach(({ categoryId, score }) => {
        functions.push({
          filter: { term: { 'category.id': categoryId } },
          weight: Math.min(score * 0.5, 3), // cap boost at 3x
        });
      });
    }

    // Boost brands user has purchased
    if (behavior?.purchasedBrands) {
      const brands = JSON.parse(behavior.purchasedBrands);
      brands.forEach(brand => {
        functions.push({ filter: { term: { brand } }, weight: 1.5 });
      });
    }

    // Freshness boost
    functions.push({
      gauss: { created_at: { origin: 'now', scale: '30d', decay: 0.5 } }
    });

    return {
      function_score: {
        query: { bool: { must, filter: filterClauses } },
        functions,
        score_mode: 'sum',
        boost_mode: 'multiply',
      }
    };
  }

  // ─── Sync Product to Index ─────────────────────────────────────────
  async indexProduct(product) {
    const doc = {
      title: product.title,
      description: product.description,
      brand: product.brand,
      category: product.category,
      tags: product.tags,
      price: Math.min(...product.variants.map(v => v.price)),
      compare_price: Math.min(...product.variants.map(v => v.comparePrice || Infinity)),
      rating: product.rating,
      sales_count: product.salesCount || 0,
      in_stock: true, // updated by inventory consumer
      status: product.status,
      created_at: product.createdAt,
      suggest: {
        input: [product.title, product.brand, ...product.tags],
        weight: product.salesCount || 1,
      }
    };

    await es.index({ index: INDEX, id: product._id.toString(), body: doc });
  }

  formatSearchResponse(response, page, limit) {
    const { hits, aggregations } = response.body;
    return {
      products: hits.hits.map(h => ({ ...h._source, _id: h._id, score: h._score, highlight: h.highlight })),
      pagination: { page, limit, total: hits.total.value },
      facets: {
        categories: aggregations?.categories?.buckets,
        brands:     aggregations?.brands?.buckets,
        priceRanges: aggregations?.price_ranges?.buckets,
        ratings:    aggregations?.ratings?.buckets,
      }
    };
  }
}

module.exports = new SearchService();
```

---

### 3.10 Recommendation Engine

```javascript
// recommendation-service/src/services/RecommendationService.js

const redis = require('../db/redis');
const db = require('../db/mongo');
const kafka = require('../messaging/kafka');

class RecommendationService {

  // ─── Track User Behavior ───────────────────────────────────────────
  async trackEvent(userId, eventType, productId, metadata = {}) {
    // Store event for ML processing
    await db.collection('user_events').insertOne({
      userId, eventType, productId, metadata,
      timestamp: new Date()
    });

    // Update real-time behavior signals in Redis
    const behaviorKey = `user:behavior:${userId}`;
    const ttl = 30 * 24 * 60 * 60; // 30 days

    if (eventType === 'VIEW') {
      await redis.zincrby(`user:viewed:${userId}`, 1, productId);
      await redis.expire(`user:viewed:${userId}`, ttl);
    }
    if (eventType === 'PURCHASE') {
      await redis.zincrby(`user:purchased:${userId}`, 1, productId);
      // Also update collaborative filtering data
      await this.updateCollaborativeSignal(userId, productId);
    }
    if (eventType === 'CART_ADD') {
      await redis.zincrby(`user:carted:${userId}`, 1, productId);
    }

    // Publish to analytics pipeline
    await kafka.publish('search.query', { userId, eventType, productId, metadata });
  }

  // ─── Get Recommendations for Homepage ─────────────────────────────
  async getPersonalizedFeed(userId, limit = 20) {
    const cacheKey = `recommendations:user:${userId}`;
    const cached = await redis.get(cacheKey);
    if (cached) return JSON.parse(cached);

    // Multi-strategy recommendations
    const [collaborative, contentBased, trending] = await Promise.all([
      this.getCollaborativeRecs(userId, 10),
      this.getContentBasedRecs(userId, 10),
      this.getTrending(10),
    ]);

    // Merge and deduplicate with weights
    const merged = this.mergeRecommendations([
      { items: collaborative,  weight: 0.5 },
      { items: contentBased,   weight: 0.3 },
      { items: trending,       weight: 0.2 },
    ], limit);

    await redis.setex(cacheKey, 3600, JSON.stringify(merged)); // 1hr cache
    return merged;
  }

  // ─── Collaborative Filtering (User-Based) ─────────────────────────
  async getCollaborativeRecs(userId, limit) {
    // Find similar users based on purchase/view overlap
    const userPurchases = await redis.zrange(`user:purchased:${userId}`, 0, -1);
    if (!userPurchases.length) return [];

    // Find users who bought same items
    const similarUsers = new Map();
    for (const productId of userPurchases.slice(0, 20)) {
      const buyers = await db.collection('user_events').distinct('userId', {
        productId, eventType: 'PURCHASE', userId: { $ne: userId }
      });
      buyers.forEach(u => similarUsers.set(u, (similarUsers.get(u) || 0) + 1));
    }

    // Sort by similarity score
    const topUsers = [...similarUsers.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, 10)
      .map(([u]) => u);

    if (!topUsers.length) return [];

    // Get products these similar users bought that current user hasn't
    const boughtByOthers = await db.collection('user_events')
      .distinct('productId', { userId: { $in: topUsers }, eventType: 'PURCHASE' });

    const notSeenByUser = boughtByOthers
      .filter(p => !userPurchases.includes(p))
      .slice(0, limit);

    return notSeenByUser.map(productId => ({ productId, strategy: 'collaborative' }));
  }

  // ─── Content-Based Filtering ───────────────────────────────────────
  async getContentBasedRecs(userId, limit) {
    // Get user's browsed categories and brands
    const behavior = await redis.hgetall(`user:behavior:${userId}`);
    const viewedProducts = await redis.zrevrange(`user:viewed:${userId}`, 0, 9);

    if (!viewedProducts.length) return [];

    // Fetch viewed product details
    const products = await db.collection('products')
      .find({ _id: { $in: viewedProducts } })
      .project({ category: 1, brand: 1, tags: 1 })
      .toArray();

    // Extract affinity signals
    const categoryScores = {};
    const brandScores = {};

    products.forEach(p => {
      categoryScores[p.category.id] = (categoryScores[p.category.id] || 0) + 1;
      brandScores[p.brand] = (brandScores[p.brand] || 0) + 1;
    });

    const topCategory = Object.entries(categoryScores).sort((a, b) => b[1] - a[1])[0]?.[0];
    const topBrand    = Object.entries(brandScores).sort((a, b) => b[1] - a[1])[0]?.[0];

    if (!topCategory) return [];

    // Find similar products user hasn't seen
    const seen = new Set(viewedProducts);
    const recs = await db.collection('products')
      .find({
        'category.id': topCategory,
        brand: topBrand,
        status: 'ACTIVE',
        _id: { $nin: [...seen] }
      })
      .sort({ 'rating.average': -1, salesCount: -1 })
      .limit(limit)
      .toArray();

    return recs.map(p => ({ productId: p._id.toString(), strategy: 'content_based' }));
  }

  // ─── Trending Products ─────────────────────────────────────────────
  async getTrending(limit) {
    const trendingKey = 'trending:global';
    const cached = await redis.zrevrange(trendingKey, 0, limit - 1, 'WITHSCORES');

    const results = [];
    for (let i = 0; i < cached.length; i += 2) {
      results.push({ productId: cached[i], score: parseFloat(cached[i + 1]), strategy: 'trending' });
    }
    return results;
  }

  // ─── Update Trending (run periodically via cron) ───────────────────
  async updateTrendingProducts() {
    const oneDayAgo = new Date(Date.now() - 24 * 60 * 60 * 1000);

    // Aggregate recent purchases + views
    const pipeline = [
      { $match: { timestamp: { $gte: oneDayAgo }, eventType: { $in: ['PURCHASE', 'VIEW', 'CART_ADD'] } } },
      { $group: {
          _id: '$productId',
          score: {
            $sum: {
              $switch: {
                branches: [
                  { case: { $eq: ['$eventType', 'PURCHASE'] }, then: 10 },
                  { case: { $eq: ['$eventType', 'CART_ADD'] }, then: 3 },
                  { case: { $eq: ['$eventType', 'VIEW'] }, then: 1 },
                ],
                default: 0
              }
            }
          }
        }
      },
      { $sort: { score: -1 } },
      { $limit: 100 }
    ];

    const trending = await db.collection('user_events').aggregate(pipeline).toArray();

    // Update Redis sorted set
    const multi = redis.multi();
    multi.del('trending:global');
    trending.forEach(({ _id, score }) => {
      multi.zadd('trending:global', score, _id);
    });
    multi.expire('trending:global', 3600); // refresh every hour
    await multi.exec();
  }

  // ─── "Frequently Bought Together" ─────────────────────────────────
  async getFrequentlyBoughtTogether(productId, limit = 5) {
    const cacheKey = `fbt:${productId}`;
    const cached = await redis.get(cacheKey);
    if (cached) return JSON.parse(cached);

    // Find orders containing this product
    const orders = await db.collection('orders')
      .distinct('orderId', { 'items.productId': productId });

    // Find other products in those orders
    const coProducts = await db.collection('orders').aggregate([
      { $match: { orderId: { $in: orders } } },
      { $unwind: '$items' },
      { $match: { 'items.productId': { $ne: productId } } },
      { $group: { _id: '$items.productId', count: { $sum: 1 } } },
      { $sort: { count: -1 } },
      { $limit: limit }
    ]).toArray();

    const result = coProducts.map(p => p._id);
    await redis.setex(cacheKey, 3600, JSON.stringify(result));
    return result;
  }

  mergeRecommendations(strategies, limit) {
    const seen = new Set();
    const result = [];

    // Round-robin by weight
    const buckets = strategies.map(s => ({
      items: s.items,
      slots: Math.round(s.weight * limit),
      idx: 0
    }));

    let added = 0;
    while (added < limit) {
      let anyAdded = false;
      for (const bucket of buckets) {
        while (bucket.idx < bucket.items.length && bucket.slots > 0) {
          const item = bucket.items[bucket.idx++];
          if (!seen.has(item.productId)) {
            seen.add(item.productId);
            result.push(item);
            bucket.slots--;
            added++;
            anyAdded = true;
            if (added >= limit) break;
          }
        }
        if (added >= limit) break;
      }
      if (!anyAdded) break;
    }

    return result;
  }
}

module.exports = new RecommendationService();
```

---

### 3.11 Review & Rating Service

```javascript
// review-service/src/services/ReviewService.js

const { v4: uuidv4 } = require('uuid');
const db = require('../db/mongo');
const redis = require('../db/redis');
const kafka = require('../messaging/kafka');
const OrderService = require('../../order-service/client');

class ReviewService {

  // ─── Create Review (verified purchase only) ────────────────────────
  async createReview(userId, productId, { rating, title, body, images = [] }) {
    // 1. Verify purchase
    const hasPurchased = await OrderService.verifyPurchase(userId, productId);
    if (!hasPurchased) throw new ForbiddenError('Can only review purchased products');

    // 2. Check if already reviewed
    const existing = await db.collection('reviews').findOne({ userId, productId });
    if (existing) throw new ConflictError('You have already reviewed this product');

    // 3. Basic content moderation
    await this.moderateContent(body);

    const review = {
      _id: uuidv4(),
      userId,
      productId,
      rating: Math.min(5, Math.max(1, parseInt(rating))),
      title,
      body,
      images,
      helpfulCount: 0,
      helpfulBy: [],
      isVerifiedPurchase: true,
      status: 'PUBLISHED', // Can add 'PENDING_MODERATION' for flagged content
      sellerReply: null,
      createdAt: new Date(),
      updatedAt: new Date(),
    };

    await db.collection('reviews').insertOne(review);

    // 4. Update product rating (atomic)
    await this.updateProductRating(productId, rating, 'ADD');

    // 5. Publish event
    await kafka.publish('review.created', {
      reviewId: review._id,
      productId,
      userId,
      rating,
      timestamp: Date.now()
    });

    return review;
  }

  // ─── Update Product Rating (Incremental) ──────────────────────────
  async updateProductRating(productId, newRating, action) {
    const ProductDB = require('../../../product-service/src/db/mongo');

    // Atomic update using findOneAndUpdate + $inc
    const product = await ProductDB.collection('products').findOneAndUpdate(
      { _id: productId },
      {
        $inc: {
          'rating.count': action === 'ADD' ? 1 : -1,
          'rating.sum': action === 'ADD' ? newRating : -newRating,
        }
      },
      { returnDocument: 'after', projection: { rating: 1 } }
    );

    // Recalculate average
    const { count, sum } = product.value.rating;
    const average = count > 0 ? parseFloat((sum / count).toFixed(2)) : 0;

    await ProductDB.collection('products').updateOne(
      { _id: productId },
      { $set: { 'rating.average': average } }
    );

    // Invalidate product cache
    await redis.del(`product:${productId}`);
  }

  // ─── Get Reviews (paginated) ───────────────────────────────────────
  async getProductReviews(productId, { page = 1, limit = 10, sort = 'helpful', rating: filterRating }) {
    const query = { productId, status: 'PUBLISHED' };
    if (filterRating) query.rating = parseInt(filterRating);

    const sortMap = {
      helpful: { helpfulCount: -1, createdAt: -1 },
      recent:  { createdAt: -1 },
      rating_high: { rating: -1 },
      rating_low:  { rating: 1 },
    };

    const [reviews, total, distribution] = await Promise.all([
      db.collection('reviews')
        .find(query)
        .sort(sortMap[sort] || sortMap.helpful)
        .skip((page - 1) * limit)
        .limit(limit)
        .toArray(),
      db.collection('reviews').countDocuments(query),
      this.getRatingDistribution(productId),
    ]);

    return {
      reviews,
      pagination: { page, limit, total, pages: Math.ceil(total / limit) },
      distribution,
    };
  }

  // ─── Mark Helpful ──────────────────────────────────────────────────
  async markHelpful(reviewId, userId) {
    const review = await db.collection('reviews').findOne({ _id: reviewId });
    if (!review) throw new NotFoundError('Review not found');
    if (review.helpfulBy.includes(userId)) {
      throw new ConflictError('Already marked as helpful');
    }

    await db.collection('reviews').updateOne(
      { _id: reviewId },
      {
        $inc: { helpfulCount: 1 },
        $push: { helpfulBy: userId }
      }
    );

    return { success: true };
  }

  // ─── Rating Distribution ───────────────────────────────────────────
  async getRatingDistribution(productId) {
    const pipeline = [
      { $match: { productId, status: 'PUBLISHED' } },
      { $group: { _id: '$rating', count: { $sum: 1 } } }
    ];

    const result = await db.collection('reviews').aggregate(pipeline).toArray();
    const dist = { 1: 0, 2: 0, 3: 0, 4: 0, 5: 0 };
    result.forEach(({ _id, count }) => { dist[_id] = count; });
    return dist;
  }

  async moderateContent(text) {
    const bannedPatterns = [/\b(spam|scam|fake|phishing)\b/i];
    if (bannedPatterns.some(p => p.test(text))) {
      throw new BadRequestError('Review contains prohibited content');
    }
  }
}

module.exports = new ReviewService();
```

---

### 3.12 Delivery Tracking Service

```javascript
// delivery-service/src/services/DeliveryService.js

const { v4: uuidv4 } = require('uuid');
const db = require('../db/mongo');
const redis = require('../db/redis');
const kafka = require('../messaging/kafka');

// Delivery status state machine
const DELIVERY_STATES = {
  PICKUP_PENDING:    ['PICKED_UP'],
  PICKED_UP:         ['IN_TRANSIT'],
  IN_TRANSIT:        ['OUT_FOR_DELIVERY', 'DELAYED'],
  OUT_FOR_DELIVERY:  ['DELIVERED', 'DELIVERY_ATTEMPTED'],
  DELIVERY_ATTEMPTED:['OUT_FOR_DELIVERY', 'RETURNED_TO_SENDER'],
  DELIVERED:         [],
  DELAYED:           ['IN_TRANSIT'],
  RETURNED_TO_SENDER:[],
};

class DeliveryService {

  // ─── Create Shipment ───────────────────────────────────────────────
  async createShipment(orderId, { warehouseId, addressId, items, carrierId }) {
    const shipmentId = uuidv4();
    const trackingId = this.generateTrackingId();

    const shipment = {
      _id: shipmentId,
      orderId,
      trackingId,
      carrierId, // BLUEDART, DELHIVERY, DTDC, etc.
      warehouseId,
      addressId,
      items,
      status: 'PICKUP_PENDING',
      timeline: [{
        status: 'SHIPMENT_CREATED',
        location: null,
        timestamp: new Date(),
        description: 'Shipment created, awaiting pickup',
      }],
      estimatedDelivery: this.calculateETA(warehouseId, addressId),
      createdAt: new Date(),
      updatedAt: new Date(),
    };

    await db.collection('shipments').insertOne(shipment);

    // Cache tracking info
    await redis.setex(
      `tracking:${trackingId}`,
      7 * 24 * 60 * 60,
      JSON.stringify({ shipmentId, orderId, status: 'PICKUP_PENDING' })
    );

    return { shipmentId, trackingId, estimatedDelivery: shipment.estimatedDelivery };
  }

  // ─── Update Delivery Status ────────────────────────────────────────
  async updateStatus(trackingId, { status, location, description, timestamp }) {
    const shipment = await db.collection('shipments').findOne({ trackingId });
    if (!shipment) throw new NotFoundError('Shipment not found');

    const allowed = DELIVERY_STATES[shipment.status] || [];
    if (!allowed.includes(status)) {
      throw new BadRequestError(`Invalid transition: ${shipment.status} → ${status}`);
    }

    const timelineEntry = {
      status,
      location,
      description,
      timestamp: new Date(timestamp),
    };

    const updated = await db.collection('shipments').findOneAndUpdate(
      { trackingId },
      {
        $set: { status, updatedAt: new Date() },
        $push: { timeline: timelineEntry }
      },
      { returnDocument: 'after' }
    );

    // Update cache
    await redis.setex(
      `tracking:${trackingId}`,
      7 * 24 * 60 * 60,
      JSON.stringify({ shipmentId: shipment._id, orderId: shipment.orderId, status })
    );

    // Publish event
    await kafka.publish('delivery.status.updated', {
      orderId: shipment.orderId,
      trackingId,
      status,
      location,
      description,
      timestamp: Date.now(),
    });

    return updated.value;
  }

  // ─── Get Tracking Info ─────────────────────────────────────────────
  async getTracking(trackingId) {
    const shipment = await db.collection('shipments').findOne(
      { trackingId },
      { projection: { trackingId: 1, status: 1, timeline: 1, estimatedDelivery: 1, carrierId: 1 } }
    );
    if (!shipment) throw new NotFoundError('Shipment not found');

    return {
      ...shipment,
      trackingUrl: `https://track.shopx.com/${trackingId}`,
      carrierTrackingUrl: this.getCarrierTrackingUrl(shipment.carrierId, trackingId),
    };
  }

  // ─── ETA Calculation ───────────────────────────────────────────────
  calculateETA(warehouseId, addressId) {
    // Simplified; real impl uses zone mapping + courier SLAs
    const baseDays = 3;
    const date = new Date();
    date.setDate(date.getDate() + baseDays);
    // Skip weekends
    while (date.getDay() === 0 || date.getDay() === 6) {
      date.setDate(date.getDate() + 1);
    }
    return date;
  }

  generateTrackingId() {
    return `SX${Date.now().toString(36).toUpperCase()}${Math.random().toString(36).slice(2, 6).toUpperCase()}`;
  }

  getCarrierTrackingUrl(carrierId, trackingId) {
    const urls = {
      DELHIVERY: `https://www.delhivery.com/track/package/${trackingId}`,
      BLUEDART:  `https://www.bluedart.com/tracking?trackfor=${trackingId}`,
      DTDC:      `https://www.dtdc.in/trace.asp?txnNo=${trackingId}`,
    };
    return urls[carrierId] || null;
  }
}

module.exports = new DeliveryService();
```

---

## 4. Non-Functional Requirements Deep Dive

### 4.1 Scalability Patterns

```
Horizontal Scaling:
  - All services are stateless (state in Redis/DB)
  - Kubernetes HPA: scale on CPU > 70% OR RPS > threshold
  - Scale-out pre-emptively before expected traffic spikes (flash sales)

Database Read Scaling:
  - PostgreSQL: 1 primary + 3 read replicas (async replication)
  - Read replicas serve GET /orders, GET /products
  - Primary handles all writes + critical reads (checkout inventory)

Connection Pooling:
  - PgBouncer in transaction mode: max 10K client connections → 200 DB connections
  - Redis: connection pool per service instance

Async Processing:
  - Heavy operations via Kafka (notifications, analytics, search sync)
  - Order confirmation: synchronous (needs immediate feedback)
  - Email/SMS: async (Kafka consumer)
  - Reporting: async (ClickHouse consumer)
```

### 4.2 Handling Flash Sales

```javascript
// Flash sale architecture — key patterns

// 1. Pre-warm cache before sale (10 min before)
async function preWarmFlashSale(saleId, products) {
  for (const product of products) {
    // Pre-load inventory into Redis
    const stock = await db.query('SELECT available_qty FROM inventory WHERE product_id = $1', [product.id]);
    await redis.set(`flash:inventory:${saleId}:${product.id}`, stock.rows[0].available_qty);

    // Pre-load price
    await redis.set(`flash:price:${saleId}:${product.id}`, product.salePrice);
  }
}

// 2. Rate limit per user per product during sale
async function flashSaleRateLimit(userId, productId, saleId) {
  const key = `flash:purchase:${saleId}:${userId}:${productId}`;
  const count = await redis.incr(key);
  await redis.expire(key, 3600);
  if (count > 1) throw new Error('Already purchased this flash deal');
}

// 3. Virtual queue for overflow traffic
async function enterFlashSaleQueue(userId, saleId) {
  const position = await redis.lpush(`flash:queue:${saleId}`, userId);
  return { position, estimatedWait: position * 2 }; // 2s per user
}

// 4. Atomic inventory deduction
async function deductFlashInventory(saleId, productId, qty) {
  const inventoryKey = `flash:inventory:${saleId}:${productId}`;
  const remaining = await redis.decrby(inventoryKey, qty);
  if (remaining < 0) {
    await redis.incrby(inventoryKey, qty); // rollback
    throw new Error('SOLD_OUT');
  }
  return remaining;
}
```

---

## 5. Security Architecture

### 5.1 Authentication & Authorization

```
Auth Flow:
  1. Login → JWT Access Token (15 min) + Refresh Token (30 days)
  2. Access Token: stored in memory (not localStorage — XSS protection)
  3. Refresh Token: HttpOnly cookie (CSRF protection via SameSite=Strict)
  4. Token rotation on every refresh (detect theft)
  5. All sessions tracked in Redis (instant revocation)

RBAC:
  Roles: CUSTOMER, SELLER, ADMIN, SUPER_ADMIN
  Resource-based permissions:
    - CUSTOMER: own orders, own profile, public products
    - SELLER: own products, own inventory, own orders
    - ADMIN: all resources, no financial transfers
    - SUPER_ADMIN: everything + financial operations

API Security:
  - All endpoints require HTTPS (TLS 1.3 minimum)
  - CORS: whitelist only (shopx.com, app.shopx.com)
  - CSP headers: script-src 'self'
  - HSTS: max-age=31536000; includeSubDomains; preload
  - X-Frame-Options: DENY (clickjacking)
  - X-Content-Type-Options: nosniff
```

### 5.2 Data Security

```
Sensitive Data:
  - Passwords: bcrypt (cost factor 12)
  - Payment tokens: AES-256-GCM encryption at rest
  - PII (phone, email): field-level encryption in DB
  - Card data: never stored (gateway tokenization only)

SQL Injection:
  - Parameterized queries always ($1, $2 placeholders)
  - ORM with whitelist column selection

Input Validation:
  - zod / joi schema validation on all inputs
  - File uploads: type check (magic bytes, not extension), size limit, scan
  - Rate limit on all write endpoints

Audit Logging:
  - Every admin action logged with userId, action, resource, timestamp, IP
  - Payment actions: immutable audit trail (append-only table)
  - GDPR: user data export/delete endpoints
```

---

## 6. Observability & Monitoring

### 6.1 Three Pillars

```
METRICS (Prometheus + Grafana):
  - Service: request_rate, error_rate, p50/p95/p99 latency
  - Business: orders_per_minute, gmv_per_minute, cart_abandonment_rate
  - Infrastructure: CPU, memory, DB connections, Redis hit rate
  - Custom: checkout_funnel_drop_off, payment_success_rate

Dashboards:
  - Operations: live traffic, error rates, service health
  - Business: GMV, orders, conversions
  - Infrastructure: DB performance, cache rates, queue depths

LOGS (ELK Stack — Elasticsearch, Logstash, Kibana):
  - Structured JSON logs from all services
  - Log levels: ERROR, WARN, INFO, DEBUG
  - Correlation IDs across service calls (x-correlation-id header)
  - PII scrubbing before indexing

TRACES (Jaeger / AWS X-Ray):
  - Distributed tracing across all microservices
  - Trace IDs propagated via headers
  - Identify slow DB queries, cache misses, N+1 queries
```

### 6.2 Alerting

```
PagerDuty Alerts (P0 — 5 min response):
  - Payment service down
  - Error rate > 5% for any service
  - p99 latency > 2s for order/payment APIs
  - DB primary failover triggered

Slack Alerts (P1 — 30 min response):
  - Inventory sync lag > 5 min
  - Queue depth > 10K messages
  - Cache hit rate < 80%
  - Disk usage > 80%

Email Reports (P2 — next business day):
  - Daily summary: orders, GMV, errors
  - Weekly: performance trends, cost analysis
```

---

## 7. Disaster Recovery & Resiliency

### 7.1 Resilience Patterns

```javascript
// Circuit Breaker Implementation
class CircuitBreaker {
  constructor(name, { threshold = 5, timeout = 30000, successThreshold = 2 }) {
    this.name = name;
    this.state = 'CLOSED'; // CLOSED | OPEN | HALF_OPEN
    this.failures = 0;
    this.successes = 0;
    this.lastFailureTime = null;
    this.threshold = threshold;
    this.timeout = timeout;
    this.successThreshold = successThreshold;
  }

  async call(fn) {
    if (this.state === 'OPEN') {
      if (Date.now() - this.lastFailureTime > this.timeout) {
        this.state = 'HALF_OPEN';
      } else {
        throw new Error(`Circuit OPEN for ${this.name}`);
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
      if (this.successes >= this.successThreshold) {
        this.state = 'CLOSED';
        this.successes = 0;
      }
    }
  }

  onFailure() {
    this.failures++;
    this.lastFailureTime = Date.now();
    if (this.failures >= this.threshold) {
      this.state = 'OPEN';
    }
  }
}

// Retry with exponential backoff
async function withRetry(fn, { maxAttempts = 3, baseDelay = 1000, maxDelay = 10000 } = {}) {
  let lastError;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      return await fn();
    } catch (err) {
      lastError = err;
      if (attempt === maxAttempts) break;
      const delay = Math.min(baseDelay * 2 ** (attempt - 1) + Math.random() * 100, maxDelay);
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
  throw lastError;
}
```

### 7.2 Data Backup & Recovery

```
RTO (Recovery Time Objective):  < 30 minutes
RPO (Recovery Point Objective): 0 for payments, 5 min for catalog

PostgreSQL:
  - Continuous WAL archiving to S3 (point-in-time recovery)
  - Daily full backup + hourly incremental
  - Cross-region replica (async) in secondary region
  - Automated failover via Patroni (< 30s)

MongoDB:
  - Replica set: 3 nodes (1 primary + 2 secondary)
  - Daily mongodump to S3
  - Cross-region secondary for DR

Redis:
  - AOF persistence + RDB snapshots
  - Backup to S3 every 5 min
  - Redis Sentinel for auto-failover

Multi-Region Strategy:
  - Primary: ap-south-1 (Mumbai)
  - Secondary: ap-southeast-1 (Singapore)
  - Failover: DNS-based routing via Route53 health checks
  - Data sync: Kafka MirrorMaker for cross-region event replication
```

---

## 8. Deployment & DevOps

### 8.1 Kubernetes Architecture

```yaml
# Each microservice deployment pattern:
apiVersion: apps/v1
kind: Deployment
metadata:
  name: order-service
spec:
  replicas: 3
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 1
      maxUnavailable: 0  # Zero-downtime deployments
  template:
    spec:
      containers:
        - name: order-service
          image: shopx/order-service:v2.3.1
          resources:
            requests: { cpu: "100m", memory: "256Mi" }
            limits:   { cpu: "500m", memory: "512Mi" }
          readinessProbe:
            httpGet: { path: /health, port: 3000 }
            initialDelaySeconds: 5
            periodSeconds: 10
          livenessProbe:
            httpGet: { path: /health, port: 3000 }
            failureThreshold: 3
          env:
            - name: DB_PASSWORD
              valueFrom:
                secretKeyRef: { name: db-secret, key: password }
---
# HPA config
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: order-service
  minReplicas: 3
  maxReplicas: 50
  metrics:
    - type: Resource
      resource: { name: cpu, target: { averageUtilization: 70 } }
    - type: Pods
      pods:
        metric: { name: http_requests_per_second }
        target: { averageValue: "1000" }
```

### 8.2 CI/CD Pipeline

```
Pipeline (GitHub Actions → ArgoCD):

PR →  1. Lint + Unit Tests
      2. Integration Tests (Docker Compose)
      3. Security Scan (Snyk, OWASP)
      4. Build Docker image
      5. Push to ECR with git SHA tag
      6. Deploy to staging (auto)
      7. Smoke tests on staging
      8. Performance test (k6) — p95 < 200ms gate

Merge → 1. Tag image as release candidate
         2. ArgoCD syncs to production (GitOps)
         3. Canary: 5% → 25% → 100% (Flagger)
         4. Automated rollback if error rate spikes

Feature Flags:
  - LaunchDarkly for gradual feature rollouts
  - Kill switches for risky features
  - A/B testing infrastructure
```

---

## Summary: Key Architecture Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Architecture | Microservices | Independent scaling, deployment, and fault isolation |
| Communication | Sync (REST) + Async (Kafka) | REST for user-facing, Kafka for eventual consistency |
| Order consistency | Saga pattern | Distributed transactions without 2PC |
| Inventory locking | Redis atomic DECRBY | Sub-millisecond, prevents overselling |
| Cart storage | Redis Hash | Fast, TTL-based, no schema needed |
| Product catalog | MongoDB | Flexible schema across categories |
| Search | Elasticsearch | Full-text + facets + personalization |
| Session | Redis + JWT | Stateless JWT + Redis for instant revocation |
| Payments | Gateway tokenization | PCI-DSS compliance, no raw card storage |
| Caching | Multi-level (Browser → CDN → Redis) | Minimize DB hits at every layer |
| Scaling | K8s HPA + pre-warming | Handle both gradual and sudden traffic |

---

*Document maintained by Platform Engineering. Last updated: 2026.*