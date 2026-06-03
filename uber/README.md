# 🚗 Uber System Design — FAANG-Level Deep Dive

> Complete High-Level Design (HLD) + Low-Level Design (LLD) in JavaScript  
> Covers: Architecture, APIs, Data Models, Algorithms, Scalability, Fault Tolerance

---

## Table of Contents

1. [Problem Statement & Scope](#1-problem-statement--scope)
2. [Functional Requirements](#2-functional-requirements)
3. [Non-Functional Requirements](#3-non-functional-requirements)
4. [Capacity Estimation & Back-of-Envelope](#4-capacity-estimation--back-of-envelope)
5. [High-Level Design (HLD)](#5-high-level-design-hld)
   - [System Architecture Overview](#51-system-architecture-overview)
   - [Core Microservices](#52-core-microservices)
   - [Communication Patterns](#53-communication-patterns)
   - [Data Flow: Ride Booking Lifecycle](#54-data-flow-ride-booking-lifecycle)
6. [Database Design](#6-database-design)
   - [SQL Schemas](#61-sql-schemas)
   - [NoSQL / Redis Structures](#62-nosql--redis-structures)
   - [Database Selection Rationale](#63-database-selection-rationale)
7. [API Design](#7-api-design)
   - [Rider APIs](#71-rider-apis)
   - [Driver APIs](#72-driver-apis)
   - [Internal Service APIs](#73-internal-service-apis)
8. [Low-Level Design (LLD) in JavaScript](#8-low-level-design-lld-in-javascript)
   - [Location Service (Real-time Tracking)](#81-location-service-real-time-tracking)
   - [Geospatial Indexing with S2 / Geohash](#82-geospatial-indexing-with-s2--geohash)
   - [Driver Matching Engine](#83-driver-matching-engine)
   - [Trip Service](#84-trip-service)
   - [Surge Pricing Engine](#85-surge-pricing-engine)
   - [Fare Calculation Service](#86-fare-calculation-service)
   - [Notification Service](#87-notification-service)
   - [WebSocket / Real-time Communication](#88-websocket--real-time-communication)
   - [Rate Limiter](#89-rate-limiter)
   - [Payment Service](#810-payment-service)
9. [Algorithms & Data Structures](#9-algorithms--data-structures)
   - [Nearest Driver Search (KD-Tree)](#91-nearest-driver-search-kd-tree)
   - [ETA Calculation (Dijkstra)](#92-eta-calculation-dijkstra)
   - [Geohash Encoding](#93-geohash-encoding)
10. [Caching Strategy](#10-caching-strategy)
11. [Message Queue & Event Streaming](#11-message-queue--event-streaming)
12. [Scalability Patterns](#12-scalability-patterns)
13. [Fault Tolerance & Reliability](#13-fault-tolerance--reliability)
14. [Security Design](#14-security-design)
15. [Monitoring & Observability](#15-monitoring--observability)
16. [Trade-offs & Bottlenecks](#16-trade-offs--bottlenecks)

---

## 1. Problem Statement & Scope

Design a ride-hailing platform like Uber that:
- Connects **riders** with nearby **drivers** in real-time
- Handles **millions of concurrent users** globally
- Provides **accurate ETAs**, **dynamic pricing**, and **live location tracking**
- Ensures **payment processing**, **ratings**, and **trip history**

### Out of Scope
- UberEats / Freight
- Driver onboarding / background checks (admin side)
- Regulatory compliance per city (conceptually noted only)

---

## 2. Functional Requirements

| # | Requirement |
|---|-------------|
| FR1 | Rider can request a ride from pickup to destination |
| FR2 | System matches rider to nearest available driver |
| FR3 | Driver can accept or reject a ride request |
| FR4 | Real-time GPS tracking for both rider and driver |
| FR5 | ETA estimation before and during the trip |
| FR6 | Dynamic (surge) pricing based on demand/supply |
| FR7 | Fare calculation at trip end |
| FR8 | Payment processing (card, wallet, cash) |
| FR9 | Ratings and reviews post-trip |
| FR10 | Trip history for both rider and driver |
| FR11 | Push notifications (ride accepted, driver arriving, etc.) |
| FR12 | Cancellation with penalty logic |

---

## 3. Non-Functional Requirements

| # | Requirement | Target |
|---|-------------|--------|
| NFR1 | Availability | 99.99% uptime |
| NFR2 | Latency — Driver matching | < 2 seconds |
| NFR3 | Latency — Location update | < 500ms |
| NFR4 | Consistency | Eventual (location), Strong (payments) |
| NFR5 | Scalability | 10M+ concurrent users |
| NFR6 | Durability | Trip data never lost |
| NFR7 | Fault tolerance | No single point of failure |
| NFR8 | GPS update frequency | Every 4–5 seconds per driver |

---

## 4. Capacity Estimation & Back-of-Envelope

```
Active Riders:       ~10 million
Active Drivers:      ~1 million
Daily Trips:         ~20 million

GPS Updates:
  Drivers send location every 4s
  1M drivers × (1 update / 4s) = 250,000 writes/sec to location store

Ride Requests:
  Peak: ~100,000 requests/min = ~1,667 requests/sec

Storage:
  Trip record ≈ 1 KB
  20M trips/day × 1KB = 20 GB/day
  Yearly: ~7 TB (trips alone)

Location Cache (Redis):
  1M driver entries × 100 bytes = ~100 MB (trivial for Redis)

Bandwidth:
  Location payload = 50 bytes
  250,000 writes/sec × 50 bytes = ~12.5 MB/s inbound
```

---

## 5. High-Level Design (HLD)

### 5.1 System Architecture Overview

```
                          ┌─────────────────────────────────────────────────────┐
                          │                   CLIENT LAYER                      │
                          │       Rider App (iOS/Android)  Driver App           │
                          └───────────────────┬─────────────────────────────────┘
                                              │  HTTPS / WSS
                          ┌───────────────────▼─────────────────────────────────┐
                          │                  API GATEWAY                        │
                          │   Rate Limiting · Auth (JWT) · SSL Termination      │
                          │   Load Balancing · Request Routing                  │
                          └───┬──────────┬────────┬──────────┬──────────────────┘
                              │          │        │          │
              ┌───────────────▼┐  ┌──────▼──┐  ┌─▼──────┐  ┌▼──────────────┐
              │  Auth Service  │  │Location │  │ Trip   │  │ Notification  │
              │  (JWT/OAuth2)  │  │Service  │  │Service │  │   Service     │
              └───────────────┘  └──────┬──┘  └─┬──────┘  └───────────────┘
                                        │        │
              ┌─────────────────────────▼────────▼──────────────────────────┐
              │                    MESSAGE BUS (Kafka)                       │
              │  Topics: location-updates, ride-events, payment-events       │
              └──────┬─────────────┬──────────────┬──────────────┬──────────┘
                     │             │              │              │
              ┌──────▼──┐  ┌──────▼──┐  ┌────────▼──┐  ┌───────▼─────┐
              │Matching │  │Pricing  │  │ Payment   │  │  Analytics  │
              │ Engine  │  │ Engine  │  │ Service   │  │  Service    │
              └──────┬──┘  └─────────┘  └───────────┘  └─────────────┘
                     │
              ┌──────▼───────────────────────────────────────────────────┐
              │                    DATA LAYER                            │
              │  PostgreSQL · Redis Cluster · Cassandra · ElasticSearch  │
              └──────────────────────────────────────────────────────────┘
```

### 5.2 Core Microservices

| Service | Responsibility | Tech Stack |
|---------|---------------|------------|
| **API Gateway** | Routing, Auth, Rate Limiting | Kong / NGINX |
| **Auth Service** | Login, JWT, OAuth2, session | Node.js + Redis |
| **Location Service** | Ingest & serve driver GPS | Node.js + Redis + Kafka |
| **Matching Engine** | Find nearest driver, dispatch | Node.js + Redis (Geo) |
| **Trip Service** | Lifecycle: request → complete | Node.js + PostgreSQL |
| **Pricing Engine** | Base fare + surge multiplier | Node.js + Redis |
| **Payment Service** | Charge, refund, wallet | Node.js + Stripe API + PostgreSQL |
| **Notification Service** | Push, SMS, email | Node.js + FCM/APNs + Twilio |
| **ETA Service** | Route calculation, time estimate | Python + OSRM |
| **Rating Service** | Post-trip feedback | Node.js + PostgreSQL |
| **Analytics Service** | Surge zones, heatmaps | Spark + Cassandra |

### 5.3 Communication Patterns

```
Synchronous (REST/gRPC):
  Client → API Gateway → Trip Service       (ride request)
  Client → API Gateway → Auth Service       (login)
  Trip Service → Payment Service            (charge at end)

Asynchronous (Kafka):
  Location Service → [location-updates topic] → Matching Engine
  Trip Service → [ride-events topic] → Notification Service
  Trip Service → [ride-events topic] → Analytics Service
  Payment Service → [payment-events topic] → Trip Service

Real-time (WebSocket):
  Driver App ↔ Location Service  (GPS stream)
  Rider App  ↔ Trip Service      (driver location push)
```

### 5.4 Data Flow: Ride Booking Lifecycle

```
Step 1: Rider opens app
  → Location Service receives rider GPS
  → Pricing Engine pre-computes surge for that zone

Step 2: Rider requests ride
  POST /rides {pickup, destination, rideType}
  → Trip Service creates Trip (status: SEARCHING)
  → Emits event to Kafka [ride.requested]

Step 3: Matching Engine consumes event
  → Queries Redis GEO for drivers within 5km radius
  → Filters: available, correct vehicle type, rating > threshold
  → Ranks by: distance + ETA + acceptance rate
  → Sends offer to top driver via WebSocket

Step 4: Driver accepts
  → Trip Service updates Trip (status: ACCEPTED, driverId)
  → Notification Service pushes to Rider: "Driver on the way"
  → Real-time location of driver starts streaming to Rider

Step 5: Driver arrives → Trip starts
  → Trip Service: status = IN_PROGRESS, startTime recorded

Step 6: Trip ends
  → Trip Service: status = COMPLETED, endTime, route polyline
  → Fare Calculation Service: computes final fare
  → Payment Service: charges rider, credits driver
  → Notification Service: receipt pushed to rider
  → Rating prompt shown to both parties

Step 7: Rating submitted
  → Rating Service: stores, updates rolling average
```

---

## 6. Database Design

### 6.1 SQL Schemas

```sql
-- PostgreSQL

-- USERS (shared base table)
CREATE TABLE users (
  id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  phone        VARCHAR(20) UNIQUE NOT NULL,
  email        VARCHAR(255) UNIQUE,
  name         VARCHAR(255) NOT NULL,
  role         ENUM('rider', 'driver') NOT NULL,
  created_at   TIMESTAMP DEFAULT NOW(),
  updated_at   TIMESTAMP DEFAULT NOW()
);

-- RIDERS
CREATE TABLE riders (
  user_id          UUID PRIMARY KEY REFERENCES users(id),
  default_payment  UUID REFERENCES payment_methods(id),
  rating           DECIMAL(3,2) DEFAULT 5.00,
  total_trips      INT DEFAULT 0
);

-- DRIVERS
CREATE TABLE drivers (
  user_id          UUID PRIMARY KEY REFERENCES users(id),
  license_number   VARCHAR(50) UNIQUE NOT NULL,
  vehicle_id       UUID REFERENCES vehicles(id),
  rating           DECIMAL(3,2) DEFAULT 5.00,
  total_trips      INT DEFAULT 0,
  is_active        BOOLEAN DEFAULT FALSE,
  is_available     BOOLEAN DEFAULT FALSE
);

-- VEHICLES
CREATE TABLE vehicles (
  id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  driver_id    UUID REFERENCES drivers(user_id),
  make         VARCHAR(100),
  model        VARCHAR(100),
  year         INT,
  plate        VARCHAR(20) UNIQUE NOT NULL,
  color        VARCHAR(50),
  type         ENUM('economy', 'comfort', 'xl', 'black') NOT NULL,
  capacity     INT DEFAULT 4
);

-- TRIPS
CREATE TABLE trips (
  id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  rider_id            UUID REFERENCES riders(user_id),
  driver_id           UUID REFERENCES drivers(user_id),
  vehicle_id          UUID REFERENCES vehicles(id),
  status              ENUM('searching','accepted','arriving','in_progress',
                           'completed','cancelled') NOT NULL,
  pickup_lat          DECIMAL(9,6) NOT NULL,
  pickup_lng          DECIMAL(9,6) NOT NULL,
  pickup_address      TEXT,
  dest_lat            DECIMAL(9,6) NOT NULL,
  dest_lng            DECIMAL(9,6) NOT NULL,
  dest_address        TEXT,
  route_polyline      TEXT,          -- Encoded polyline
  distance_km         DECIMAL(8,3),
  duration_sec        INT,
  base_fare           DECIMAL(10,2),
  surge_multiplier    DECIMAL(4,2) DEFAULT 1.00,
  final_fare          DECIMAL(10,2),
  currency            CHAR(3) DEFAULT 'USD',
  payment_method_id   UUID REFERENCES payment_methods(id),
  payment_status      ENUM('pending','charged','failed','refunded'),
  cancelled_by        ENUM('rider','driver','system'),
  cancel_reason       TEXT,
  requested_at        TIMESTAMP NOT NULL,
  accepted_at         TIMESTAMP,
  pickup_at           TIMESTAMP,
  started_at          TIMESTAMP,
  completed_at        TIMESTAMP,
  created_at          TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_trips_rider    ON trips(rider_id, created_at DESC);
CREATE INDEX idx_trips_driver   ON trips(driver_id, created_at DESC);
CREATE INDEX idx_trips_status   ON trips(status);

-- RATINGS
CREATE TABLE ratings (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  trip_id       UUID UNIQUE REFERENCES trips(id),
  rated_by      UUID REFERENCES users(id),
  rated_user    UUID REFERENCES users(id),
  score         SMALLINT CHECK (score BETWEEN 1 AND 5),
  comment       TEXT,
  created_at    TIMESTAMP DEFAULT NOW()
);

-- PAYMENT METHODS
CREATE TABLE payment_methods (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id         UUID REFERENCES users(id),
  type            ENUM('card','wallet','cash'),
  provider_token  TEXT,           -- Stripe token (encrypted)
  last4           CHAR(4),
  brand           VARCHAR(20),
  is_default      BOOLEAN DEFAULT FALSE,
  created_at      TIMESTAMP DEFAULT NOW()
);

-- SURGE ZONES (snapshotted periodically)
CREATE TABLE surge_snapshots (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  geohash         VARCHAR(12) NOT NULL,
  multiplier      DECIMAL(4,2) NOT NULL,
  active_riders   INT,
  active_drivers  INT,
  recorded_at     TIMESTAMP NOT NULL
);
CREATE INDEX idx_surge_geohash ON surge_snapshots(geohash, recorded_at DESC);
```

### 6.2 NoSQL / Redis Structures

```
# Driver real-time location (Redis GEO)
GEOADD drivers:locations <lng> <lat> <driverId>
GEOPOS drivers:locations <driverId>
GEORADIUS drivers:locations <lng> <lat> 5 km ASC COUNT 20

# Driver availability status (Redis Hash)
HSET driver:<driverId> status "available" vehicleType "economy" rating "4.8"

# Active trip state (Redis Hash — fast lookup)
HSET trip:<tripId> status "in_progress" driverId "xxx" riderId "yyy"
EXPIRE trip:<tripId> 86400

# Surge multiplier per geohash (Redis String)
SET surge:geohash:<hash> 1.8 EX 60    # TTL 60s, recomputed by Pricing Engine

# Rate limiting (Redis sorted set / counter)
INCR ratelimit:<userId>:<minute>
EXPIRE ratelimit:<userId>:<minute> 60

# Session tokens (Redis String)
SET session:<userId> <jwtToken> EX 86400

# Driver ETA cache
SET eta:<driverId>:<tripId> 420 EX 30  # 420 seconds, refresh every 30s

# Cassandra — Trip location history (time-series)
CREATE TABLE trip_locations (
  trip_id    UUID,
  ts         TIMESTAMP,
  lat        DOUBLE,
  lng        DOUBLE,
  speed_kmh  FLOAT,
  PRIMARY KEY ((trip_id), ts)
) WITH CLUSTERING ORDER BY (ts ASC);
```

### 6.3 Database Selection Rationale

| Data | Store | Why |
|------|-------|-----|
| Users, Trips, Payments | PostgreSQL | ACID, relational integrity, complex queries |
| Driver live location | Redis GEO | Sub-millisecond geospatial queries, in-memory |
| Driver availability | Redis Hash | O(1) lookup, TTL auto-expiry |
| Trip location history | Cassandra | Time-series, high write throughput, partition by trip_id |
| Search / Autocomplete | ElasticSearch | Full-text, geo queries for place search |
| Analytics / Heatmaps | Apache Spark + S3 | Batch aggregation over massive datasets |

---

## 7. API Design

### 7.1 Rider APIs

```http
# Auth
POST   /api/v1/auth/otp/send          { phone }
POST   /api/v1/auth/otp/verify        { phone, otp }   → { accessToken, refreshToken }
POST   /api/v1/auth/refresh           { refreshToken }  → { accessToken }

# Ride
POST   /api/v1/rides                  { pickup, destination, vehicleType, paymentMethodId }
GET    /api/v1/rides/:rideId          → { trip, driver, eta, fare }
PATCH  /api/v1/rides/:rideId/cancel   { reason }
GET    /api/v1/rides/history          ?page=1&limit=20

# Fare Estimate (before booking)
POST   /api/v1/fare/estimate          { pickup, destination, vehicleType }
                                      → { estimatedFare, surgeMultiplier, eta }

# Driver Tracking (polling or WebSocket upgrade)
GET    /api/v1/rides/:rideId/driver-location → { lat, lng, heading, eta }

# Ratings
POST   /api/v1/ratings                { tripId, score, comment }

# Payment Methods
GET    /api/v1/payment-methods
POST   /api/v1/payment-methods        { token, type }
DELETE /api/v1/payment-methods/:id
```

### 7.2 Driver APIs

```http
# Status
PATCH  /api/v1/driver/status          { status: "online" | "offline" | "busy" }

# Location (called every 4s by driver app)
POST   /api/v1/driver/location        { lat, lng, heading, speed }

# Ride Offers
GET    /api/v1/driver/offers          → pending offer with SSE/WS
POST   /api/v1/driver/offers/:offerId/accept
POST   /api/v1/driver/offers/:offerId/reject  { reason }

# Trip Control
PATCH  /api/v1/driver/trips/:tripId/pickup    (arrived at pickup)
PATCH  /api/v1/driver/trips/:tripId/start     (trip started)
PATCH  /api/v1/driver/trips/:tripId/complete  { finalLat, finalLng }

# Earnings
GET    /api/v1/driver/earnings        ?period=week|month
GET    /api/v1/driver/trips/history   ?page=1&limit=20
```

### 7.3 Internal Service APIs

```http
# Matching Engine (internal only)
POST   /internal/match                { tripId, pickup, vehicleType }
                                      → { driverId, eta, distance }

# ETA Service
POST   /internal/eta                  { fromLat, fromLng, toLat, toLng }
                                      → { etaSeconds, distanceKm, polyline }

# Pricing Engine
POST   /internal/pricing/surge        { geohash }
                                      → { multiplier, activeRiders, activeDrivers }

POST   /internal/pricing/fare         { distanceKm, durationSec, vehicleType,
                                        surgeMultiplier, cityConfig }
                                      → { fare, breakdown }
```

---

## 8. Low-Level Design (LLD) in JavaScript

### 8.1 Location Service (Real-time Tracking)

```javascript
// services/location/LocationService.js

const Redis = require('ioredis');
const { Kafka } = require('kafkajs');

class LocationService {
  constructor() {
    this.redis = new Redis.Cluster([
      { host: 'redis-node-1', port: 6379 },
      { host: 'redis-node-2', port: 6379 },
    ]);

    this.kafka = new Kafka({ brokers: ['kafka-broker-1:9092', 'kafka-broker-2:9092'] });
    this.producer = this.kafka.producer();
  }

  async init() {
    await this.producer.connect();
  }

  /**
   * Called every ~4 seconds by driver app
   * @param {string} driverId
   * @param {number} lat
   * @param {number} lng
   * @param {number} heading  - degrees 0-359
   * @param {number} speed    - km/h
   * @param {string} tripId   - if on trip
   */
  async updateDriverLocation(driverId, { lat, lng, heading, speed, tripId }) {
    const pipeline = this.redis.pipeline();

    // 1. Update GEO index (for nearby search)
    pipeline.geoadd('drivers:geo', lng, lat, driverId);

    // 2. Store detailed driver state
    pipeline.hset(`driver:location:${driverId}`, {
      lat: lat.toFixed(6),
      lng: lng.toFixed(6),
      heading,
      speed,
      updatedAt: Date.now(),
      tripId: tripId || '',
    });

    // 3. Set TTL — if driver doesn't update for 30s, consider offline
    pipeline.expire(`driver:location:${driverId}`, 30);

    await pipeline.exec();

    // 4. Publish to Kafka for real-time streaming to rider
    if (tripId) {
      await this.producer.send({
        topic: 'driver-location-updates',
        messages: [{
          key: tripId,
          value: JSON.stringify({ driverId, tripId, lat, lng, heading, speed, ts: Date.now() }),
        }],
      });
    }

    // 5. Also store in Cassandra (trip path history) if on trip
    if (tripId) {
      await this.saveTripLocationHistory(tripId, { lat, lng, speed });
    }
  }

  /**
   * Get nearby available drivers
   * @returns {Array<{driverId, distance, lat, lng}>}
   */
  async getNearbyDrivers(lat, lng, radiusKm = 5, vehicleType = 'economy', limit = 20) {
    // Redis GEORADIUS — returns driverIds within radius, sorted by distance
    const results = await this.redis.georadius(
      'drivers:geo',
      lng,
      lat,
      radiusKm,
      'km',
      'ASC',
      'COUNT',
      limit * 3,  // over-fetch, then filter by availability
      'WITHCOORD',
      'WITHDIST'
    );

    // results = [ [driverId, distKm, [lng, lat]], ... ]
    const driverIds = results.map(r => r[0]);

    // Batch fetch driver status
    const pipeline = this.redis.pipeline();
    driverIds.forEach(id => pipeline.hgetall(`driver:${id}`));
    const statuses = await pipeline.exec();

    const available = [];
    for (let i = 0; i < results.length; i++) {
      const [driverId, distStr, [dLng, dLat]] = results[i];
      const status = statuses[i][1];  // [err, value]

      if (
        status &&
        status.isAvailable === '1' &&
        status.vehicleType === vehicleType &&
        parseFloat(status.rating) >= 4.0
      ) {
        available.push({
          driverId,
          distance: parseFloat(distStr),
          lat: parseFloat(dLat),
          lng: parseFloat(dLng),
          rating: parseFloat(status.rating),
          eta: null,  // filled by Matching Engine
        });
      }

      if (available.length >= limit) break;
    }

    return available;
  }

  async getDriverLocation(driverId) {
    const data = await this.redis.hgetall(`driver:location:${driverId}`);
    if (!data || !data.lat) return null;
    return {
      lat: parseFloat(data.lat),
      lng: parseFloat(data.lng),
      heading: parseInt(data.heading),
      speed: parseFloat(data.speed),
      updatedAt: parseInt(data.updatedAt),
    };
  }

  async saveTripLocationHistory(tripId, { lat, lng, speed }) {
    // Cassandra write via driver (cassandra-driver npm package)
    await this.cassandraClient.execute(
      `INSERT INTO trip_locations (trip_id, ts, lat, lng, speed_kmh)
       VALUES (?, toTimestamp(now()), ?, ?, ?)`,
      [tripId, lat, lng, speed],
      { prepare: true }
    );
  }
}

module.exports = new LocationService();
```

---

### 8.2 Geospatial Indexing with S2 / Geohash

```javascript
// utils/geohash.js

const BASE32 = '0123456789bcdefghjkmnpqrstuvwxyz';

class Geohash {
  /**
   * Encode lat/lng to geohash string of given precision
   * precision 6 = ~1.2km cell, precision 7 = ~150m cell
   */
  static encode(lat, lng, precision = 7) {
    let minLat = -90, maxLat = 90;
    let minLng = -180, maxLng = 180;
    let hash = '';
    let bits = 0, bitsTotal = 0, hashValue = 0;
    let isLng = true;

    while (hash.length < precision) {
      if (isLng) {
        const midLng = (minLng + maxLng) / 2;
        if (lng > midLng) { hashValue = (hashValue << 1) + 1; minLng = midLng; }
        else               { hashValue = (hashValue << 1) + 0; maxLng = midLng; }
      } else {
        const midLat = (minLat + maxLat) / 2;
        if (lat > midLat) { hashValue = (hashValue << 1) + 1; minLat = midLat; }
        else              { hashValue = (hashValue << 1) + 0; maxLat = midLat; }
      }

      isLng = !isLng;
      bits++;

      if (bits === 5) {
        hash += BASE32[hashValue];
        bits = 0;
        hashValue = 0;
      }
    }

    return hash;
  }

  /**
   * Decode geohash to bounding box
   */
  static decode(hash) {
    let isLng = true;
    let minLat = -90, maxLat = 90;
    let minLng = -180, maxLng = 180;

    for (const char of hash) {
      const value = BASE32.indexOf(char);
      for (let bits = 4; bits >= 0; bits--) {
        const bitValue = (value >> bits) & 1;
        if (isLng) {
          const mid = (minLng + maxLng) / 2;
          bitValue ? (minLng = mid) : (maxLng = mid);
        } else {
          const mid = (minLat + maxLat) / 2;
          bitValue ? (minLat = mid) : (maxLat = mid);
        }
        isLng = !isLng;
      }
    }

    return {
      lat: (minLat + maxLat) / 2,
      lng: (minLng + maxLng) / 2,
      bbox: { minLat, maxLat, minLng, maxLng },
    };
  }

  /**
   * Get neighboring geohashes (for surge zone overlap)
   */
  static neighbors(hash) {
    const NEIGHBORS = {
      right:  { even: 'bc01fg45telehi89teleij23mn67teleop', odd: 'p0r21436x8zb9dcf5h7kjnmqesgutwvy' },
      left:   { even: '238967debc01teleijhi45teleop89teleqrstuvwxyz', odd: '14365h7k9dcfesgujnmqp0r2twvyx8zb' },
      top:    { even: 'p0r21436x8zb9dcf5h7kjnmqesgutwvy', odd: 'bc01fg45telehi89teleij23mn67teleop' },
      bottom: { even: '14365h7k9dcfesgujnmqp0r2twvyx8zb', odd: '238967debc01teleijhi45teleop89teleqrstuvwxyz' },
    };
    // simplified: return adjacent cells
    const { lat, lng } = this.decode(hash);
    const { bbox } = this.decode(hash);
    const latStep = bbox.maxLat - bbox.minLat;
    const lngStep = bbox.maxLng - bbox.minLng;
    const precision = hash.length;

    return {
      N:  this.encode(lat + latStep, lng, precision),
      S:  this.encode(lat - latStep, lng, precision),
      E:  this.encode(lat, lng + lngStep, precision),
      W:  this.encode(lat, lng - lngStep, precision),
      NE: this.encode(lat + latStep, lng + lngStep, precision),
      NW: this.encode(lat + latStep, lng - lngStep, precision),
      SE: this.encode(lat - latStep, lng + lngStep, precision),
      SW: this.encode(lat - latStep, lng - lngStep, precision),
    };
  }

  static haversineDistance(lat1, lng1, lat2, lng2) {
    const R = 6371; // Earth radius km
    const dLat = (lat2 - lat1) * Math.PI / 180;
    const dLng = (lng2 - lng1) * Math.PI / 180;
    const a = Math.sin(dLat/2) ** 2 +
              Math.cos(lat1 * Math.PI/180) *
              Math.cos(lat2 * Math.PI/180) *
              Math.sin(dLng/2) ** 2;
    return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  }
}

module.exports = Geohash;
```

---

### 8.3 Driver Matching Engine

```javascript
// services/matching/MatchingEngine.js

const LocationService = require('../location/LocationService');
const ETAService = require('../eta/ETAService');
const Redis = require('ioredis');

const OFFER_TIMEOUT_MS = 15000;   // 15 seconds to respond
const MAX_DISPATCH_RADIUS_KM = 8;
const MAX_CANDIDATES = 10;

class MatchingEngine {
  constructor() {
    this.redis = new Redis();
    this.pendingOffers = new Map();  // offerId → { tripId, driverId, timer }
  }

  /**
   * Main entry: find best driver for a trip
   * Uses: Nearest + ETA + Rating + Acceptance Rate
   */
  async matchDriverForTrip(trip) {
    const { id: tripId, pickupLat, pickupLng, vehicleType, riderId } = trip;

    const candidates = await LocationService.getNearbyDrivers(
      pickupLat, pickupLng,
      MAX_DISPATCH_RADIUS_KM,
      vehicleType,
      MAX_CANDIDATES
    );

    if (candidates.length === 0) {
      return { success: false, reason: 'NO_DRIVERS_AVAILABLE' };
    }

    // Fetch ETA for all candidates in parallel
    const candidatesWithETA = await Promise.all(
      candidates.map(async c => {
        const eta = await ETAService.getETA(c.lat, c.lng, pickupLat, pickupLng);
        return { ...c, etaSeconds: eta.seconds };
      })
    );

    // Score and rank drivers
    const ranked = this.rankDrivers(candidatesWithETA);

    // Sequential dispatch: offer to best driver, if rejected try next
    for (const driver of ranked) {
      const result = await this.dispatchOffer(tripId, driver);
      if (result.accepted) {
        return { success: true, driverId: driver.driverId, eta: driver.etaSeconds };
      }
    }

    return { success: false, reason: 'ALL_DRIVERS_REJECTED' };
  }

  /**
   * Score each driver: lower score = better
   * Formula: w1*normalizedETA + w2*(1-rating/5) + w3*(1-acceptanceRate)
   */
  rankDrivers(candidates) {
    const maxETA = Math.max(...candidates.map(c => c.etaSeconds));

    return candidates
      .map(c => ({
        ...c,
        score: (
          0.5 * (c.etaSeconds / maxETA) +
          0.3 * (1 - (c.rating / 5)) +
          0.2 * (1 - (c.acceptanceRate || 0.9))
        ),
      }))
      .sort((a, b) => a.score - b.score);
  }

  /**
   * Send offer to driver and wait for response via Redis pub/sub
   */
  async dispatchOffer(tripId, driver) {
    const offerId = `offer:${tripId}:${driver.driverId}`;

    // Mark driver as "offered" so other trips don't send simultaneously
    await this.redis.set(`driver:offered:${driver.driverId}`, offerId, 'EX', 20);

    // Publish offer to driver's channel (driver app subscribes via WebSocket gateway)
    await this.redis.publish(`driver:${driver.driverId}:offers`, JSON.stringify({
      offerId,
      tripId,
      pickupLat: driver.pickupLat,
      pickupLng: driver.pickupLng,
      etaSeconds: driver.etaSeconds,
      estimatedFare: driver.estimatedFare,
      expiresIn: OFFER_TIMEOUT_MS,
    }));

    // Wait for driver response
    return new Promise(resolve => {
      const subscriber = this.redis.duplicate();
      subscriber.subscribe(`offer:${offerId}:response`);

      const timer = setTimeout(() => {
        subscriber.disconnect();
        this.redis.del(`driver:offered:${driver.driverId}`);
        resolve({ accepted: false, reason: 'TIMEOUT' });
      }, OFFER_TIMEOUT_MS);

      subscriber.on('message', (channel, message) => {
        clearTimeout(timer);
        subscriber.disconnect();
        this.redis.del(`driver:offered:${driver.driverId}`);
        const response = JSON.parse(message);
        resolve({ accepted: response.action === 'ACCEPT' });
      });
    });
  }

  /**
   * Called when driver responds to offer
   */
  async respondToOffer(offerId, driverId, action) {
    await this.redis.publish(
      `offer:${offerId}:response`,
      JSON.stringify({ driverId, action, ts: Date.now() })
    );

    // Update acceptance rate in rolling window
    await this.updateAcceptanceRate(driverId, action === 'ACCEPT');
  }

  async updateAcceptanceRate(driverId, accepted) {
    const key = `driver:acceptance:${driverId}`;
    const pipeline = this.redis.pipeline();
    pipeline.lpush(key, accepted ? 1 : 0);
    pipeline.ltrim(key, 0, 99);  // keep last 100 offers
    pipeline.lrange(key, 0, -1);
    const results = await pipeline.exec();
    const history = results[2][1].map(Number);
    const rate = history.reduce((a, b) => a + b, 0) / history.length;
    await this.redis.hset(`driver:${driverId}`, 'acceptanceRate', rate.toFixed(2));
  }
}

module.exports = new MatchingEngine();
```

---

### 8.4 Trip Service

```javascript
// services/trip/TripService.js

const { v4: uuidv4 } = require('uuid');
const db = require('../../db/postgres');
const redis = require('../../db/redis');
const kafka = require('../../messaging/kafka');
const MatchingEngine = require('../matching/MatchingEngine');
const PricingEngine = require('../pricing/PricingEngine');
const ETAService = require('../eta/ETAService');

const TRIP_STATUS = {
  SEARCHING:   'searching',
  ACCEPTED:    'accepted',
  ARRIVING:    'arriving',
  IN_PROGRESS: 'in_progress',
  COMPLETED:   'completed',
  CANCELLED:   'cancelled',
};

class TripService {
  async requestRide({ riderId, pickupLat, pickupLng, pickupAddress,
                      destLat, destLng, destAddress, vehicleType, paymentMethodId }) {
    // 1. Check if rider has any active trips
    const activeTrip = await this.getActiveTripForRider(riderId);
    if (activeTrip) throw new Error('RIDER_HAS_ACTIVE_TRIP');

    // 2. Get fare estimate
    const etaInfo = await ETAService.getETA(pickupLat, pickupLng, destLat, destLng);
    const surge = await PricingEngine.getSurgeMultiplier(pickupLat, pickupLng);
    const fareEstimate = PricingEngine.estimateFare({
      distanceKm: etaInfo.distanceKm,
      durationSec: etaInfo.seconds,
      vehicleType,
      surgeMultiplier: surge.multiplier,
    });

    // 3. Create trip record
    const tripId = uuidv4();
    await db.query(`
      INSERT INTO trips
        (id, rider_id, status, pickup_lat, pickup_lng, pickup_address,
         dest_lat, dest_lng, dest_address, base_fare, surge_multiplier,
         payment_method_id, requested_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,NOW())
    `, [tripId, riderId, TRIP_STATUS.SEARCHING,
        pickupLat, pickupLng, pickupAddress,
        destLat, destLng, destAddress,
        fareEstimate.baseFare, surge.multiplier, paymentMethodId]);

    // 4. Cache active trip in Redis
    await redis.hset(`trip:${tripId}`, {
      status: TRIP_STATUS.SEARCHING,
      riderId,
      pickupLat, pickupLng, destLat, destLng,
      vehicleType,
    });
    await redis.set(`rider:activeTrip:${riderId}`, tripId, 'EX', 7200);

    // 5. Publish ride.requested event
    await kafka.producer.send({
      topic: 'ride-events',
      messages: [{
        key: tripId,
        value: JSON.stringify({
          event: 'ride.requested',
          tripId, riderId, vehicleType,
          pickupLat, pickupLng,
          estimatedFare: fareEstimate,
          ts: Date.now(),
        }),
      }],
    });

    // 6. Trigger matching asynchronously
    this.triggerMatching(tripId, { riderId, pickupLat, pickupLng, vehicleType });

    return {
      tripId,
      status: TRIP_STATUS.SEARCHING,
      estimatedFare: fareEstimate,
      surgeMultiplier: surge.multiplier,
      estimatedPickupETA: null,  // filled once driver matched
    };
  }

  async triggerMatching(tripId, tripData) {
    const result = await MatchingEngine.matchDriverForTrip({ id: tripId, ...tripData });

    if (!result.success) {
      await this.updateTripStatus(tripId, TRIP_STATUS.CANCELLED, {
        cancelledBy: 'system',
        cancelReason: result.reason,
      });
      await kafka.producer.send({
        topic: 'ride-events',
        messages: [{ key: tripId, value: JSON.stringify({ event: 'ride.no_driver', tripId }) }],
      });
      return;
    }

    await this.acceptTrip(tripId, result.driverId, result.eta);
  }

  async acceptTrip(tripId, driverId, etaSeconds) {
    await db.query(`
      UPDATE trips SET status=$1, driver_id=$2, accepted_at=NOW() WHERE id=$3
    `, [TRIP_STATUS.ACCEPTED, driverId, tripId]);

    await redis.hset(`trip:${tripId}`, {
      status: TRIP_STATUS.ACCEPTED,
      driverId,
      etaSeconds,
    });
    await redis.set(`driver:activeTrip:${driverId}`, tripId, 'EX', 7200);
    await redis.hset(`driver:${driverId}`, 'isAvailable', 0);

    await kafka.producer.send({
      topic: 'ride-events',
      messages: [{
        key: tripId,
        value: JSON.stringify({ event: 'ride.accepted', tripId, driverId, etaSeconds }),
      }],
    });
  }

  async startTrip(tripId, driverId) {
    const trip = await this.getTripFromCache(tripId);
    if (trip.driverId !== driverId) throw new Error('UNAUTHORIZED');

    await db.query(`
      UPDATE trips SET status=$1, started_at=NOW() WHERE id=$2
    `, [TRIP_STATUS.IN_PROGRESS, tripId]);

    await redis.hset(`trip:${tripId}`, 'status', TRIP_STATUS.IN_PROGRESS);

    await kafka.producer.send({
      topic: 'ride-events',
      messages: [{ key: tripId, value: JSON.stringify({ event: 'ride.started', tripId }) }],
    });
  }

  async completeTrip(tripId, driverId, { finalLat, finalLng }) {
    const trip = await this.getTripFromCache(tripId);
    if (trip.driverId !== driverId) throw new Error('UNAUTHORIZED');

    // Calculate final fare
    const result = await db.query(
      'SELECT * FROM trips WHERE id=$1', [tripId]
    );
    const dbTrip = result.rows[0];

    const etaInfo = await ETAService.getETA(
      dbTrip.pickup_lat, dbTrip.pickup_lng, finalLat, finalLng
    );

    const finalFare = PricingEngine.calculateFare({
      distanceKm: etaInfo.distanceKm,
      durationSec: Math.round((Date.now() - new Date(dbTrip.started_at).getTime()) / 1000),
      vehicleType: trip.vehicleType,
      surgeMultiplier: parseFloat(dbTrip.surge_multiplier),
    });

    await db.query(`
      UPDATE trips
      SET status=$1, completed_at=NOW(), final_fare=$2,
          distance_km=$3, duration_sec=$4, route_polyline=$5
      WHERE id=$6
    `, [TRIP_STATUS.COMPLETED, finalFare.total,
        etaInfo.distanceKm, finalFare.durationSec,
        etaInfo.polyline, tripId]);

    // Cleanup Redis
    await redis.del(`trip:${tripId}`);
    await redis.del(`rider:activeTrip:${dbTrip.rider_id}`);
    await redis.del(`driver:activeTrip:${driverId}`);
    await redis.hset(`driver:${driverId}`, 'isAvailable', 1);

    // Trigger payment
    await kafka.producer.send({
      topic: 'payment-events',
      messages: [{
        key: tripId,
        value: JSON.stringify({
          event: 'payment.initiate',
          tripId,
          riderId: dbTrip.rider_id,
          driverId,
          amount: finalFare.total,
          paymentMethodId: dbTrip.payment_method_id,
        }),
      }],
    });

    return { finalFare, status: TRIP_STATUS.COMPLETED };
  }

  async cancelTrip(tripId, userId, { reason, role }) {
    const trip = await this.getTripFromCache(tripId);
    const cancellable = ['searching', 'accepted', 'arriving'];
    if (!cancellable.includes(trip.status)) {
      throw new Error('TRIP_CANNOT_BE_CANCELLED');
    }

    // Cancellation penalty: rider cancels after driver accepted = small fee
    let cancellationFee = 0;
    if (role === 'rider' && trip.status !== TRIP_STATUS.SEARCHING) {
      cancellationFee = 2.00;
    }

    await db.query(`
      UPDATE trips
      SET status='cancelled', cancelled_by=$1, cancel_reason=$2
      WHERE id=$3
    `, [role, reason, tripId]);

    if (trip.driverId) {
      await redis.hset(`driver:${trip.driverId}`, 'isAvailable', 1);
      await redis.del(`driver:activeTrip:${trip.driverId}`);
    }

    await redis.del(`trip:${tripId}`);
    await redis.del(`rider:activeTrip:${trip.riderId}`);

    await kafka.producer.send({
      topic: 'ride-events',
      messages: [{
        key: tripId,
        value: JSON.stringify({
          event: 'ride.cancelled',
          tripId, role, reason, cancellationFee,
        }),
      }],
    });

    return { cancellationFee };
  }

  async getTripFromCache(tripId) {
    const data = await redis.hgetall(`trip:${tripId}`);
    if (!data || !data.status) {
      // Fallback to DB
      const result = await db.query('SELECT * FROM trips WHERE id=$1', [tripId]);
      if (!result.rows[0]) throw new Error('TRIP_NOT_FOUND');
      return result.rows[0];
    }
    return data;
  }

  async getActiveTripForRider(riderId) {
    const tripId = await redis.get(`rider:activeTrip:${riderId}`);
    if (!tripId) return null;
    return this.getTripFromCache(tripId);
  }
}

module.exports = new TripService();
```

---

### 8.5 Surge Pricing Engine

```javascript
// services/pricing/SurgePricingEngine.js

const Redis = require('ioredis');
const Geohash = require('../../utils/geohash');

const SURGE_CONFIG = {
  // demand/supply ratio → multiplier
  thresholds: [
    { ratio: 2.0, multiplier: 1.0 },
    { ratio: 1.5, multiplier: 1.2 },
    { ratio: 1.2, multiplier: 1.5 },
    { ratio: 0.9, multiplier: 1.8 },
    { ratio: 0.7, multiplier: 2.0 },
    { ratio: 0.5, multiplier: 2.5 },
    { ratio: 0,   multiplier: 3.0 },
  ],
  maxMultiplier: 3.0,
  minMultiplier: 1.0,
  cacheExpirySec: 60,
  geohashPrecision: 5,  // ~5km cell
};

class SurgePricingEngine {
  constructor() {
    this.redis = new Redis();
  }

  /**
   * Recomputed every 60 seconds by a background worker per geohash cell
   */
  async computeSurge(lat, lng) {
    const geohash = Geohash.encode(lat, lng, SURGE_CONFIG.geohashPrecision);

    // Count active riders requesting in this zone
    const activeRiders = await this.countActiveRiders(geohash);

    // Count available drivers in this zone
    const availableDrivers = await this.countAvailableDrivers(lat, lng);

    const ratio = availableDrivers === 0
      ? 0
      : activeRiders / availableDrivers;

    const multiplier = this.getRatioMultiplier(ratio);

    // Cache result
    await this.redis.setex(
      `surge:${geohash}`,
      SURGE_CONFIG.cacheExpirySec,
      JSON.stringify({ multiplier, activeRiders, availableDrivers, ratio, computedAt: Date.now() })
    );

    return { geohash, multiplier, activeRiders, availableDrivers, ratio };
  }

  async getSurgeMultiplier(lat, lng) {
    const geohash = Geohash.encode(lat, lng, SURGE_CONFIG.geohashPrecision);
    const cached = await this.redis.get(`surge:${geohash}`);

    if (cached) {
      return JSON.parse(cached);
    }

    // Cache miss — compute on demand
    return this.computeSurge(lat, lng);
  }

  getRatioMultiplier(ratio) {
    for (const threshold of SURGE_CONFIG.thresholds) {
      if (ratio >= threshold.ratio) {
        return threshold.multiplier;
      }
    }
    return SURGE_CONFIG.maxMultiplier;
  }

  async countActiveRiders(geohash) {
    // Track rider searches in Redis sorted set by geohash
    const key = `zone:riders:${geohash}`;
    // Clean up entries older than 3 minutes
    await this.redis.zremrangebyscore(key, '-inf', Date.now() - 180000);
    return this.redis.zcard(key);
  }

  async countAvailableDrivers(lat, lng) {
    // Use GEO command to count available drivers in the zone
    const radiusKm = 3; // ~5km geohash cell radius
    const drivers = await this.redis.georadius(
      'drivers:geo', lng, lat, radiusKm, 'km', 'COUNT', 100
    );
    // Further filter by availability via pipeline
    if (!drivers.length) return 0;
    const pipeline = this.redis.pipeline();
    drivers.forEach(id => pipeline.hget(`driver:${id}`, 'isAvailable'));
    const results = await pipeline.exec();
    return results.filter(([, v]) => v === '1').length;
  }

  /**
   * Track a rider searching in a zone
   */
  async trackRiderSearch(riderId, lat, lng) {
    const geohash = Geohash.encode(lat, lng, SURGE_CONFIG.geohashPrecision);
    const key = `zone:riders:${geohash}`;
    await this.redis.zadd(key, Date.now(), riderId);
    await this.redis.expire(key, 300);
  }
}

module.exports = new SurgePricingEngine();
```

---

### 8.6 Fare Calculation Service

```javascript
// services/pricing/FareCalculationService.js

const PRICING_CONFIG = {
  economy: {
    baseFare:       2.50,
    perKm:          1.20,
    perMinute:      0.25,
    minimumFare:    5.00,
    cancellationFee: 2.00,
    bookingFee:     1.75,
  },
  comfort: {
    baseFare:       4.00,
    perKm:          1.80,
    perMinute:      0.35,
    minimumFare:    8.00,
    cancellationFee: 3.00,
    bookingFee:     2.50,
  },
  xl: {
    baseFare:       5.00,
    perKm:          2.20,
    perMinute:      0.45,
    minimumFare:   10.00,
    cancellationFee: 4.00,
    bookingFee:     3.00,
  },
  black: {
    baseFare:       8.00,
    perKm:          3.50,
    perMinute:      0.75,
    minimumFare:   15.00,
    cancellationFee: 5.00,
    bookingFee:     4.00,
  },
};

class FareCalculationService {
  /**
   * Pre-trip estimate (shown to rider before booking)
   */
  estimateFare({ distanceKm, durationSec, vehicleType, surgeMultiplier = 1.0 }) {
    return this._compute({ distanceKm, durationSec, vehicleType, surgeMultiplier });
  }

  /**
   * Post-trip final fare (uses actual distance/duration from GPS trace)
   */
  calculateFare({ distanceKm, durationSec, vehicleType, surgeMultiplier = 1.0 }) {
    return this._compute({ distanceKm, durationSec, vehicleType, surgeMultiplier });
  }

  _compute({ distanceKm, durationSec, vehicleType, surgeMultiplier }) {
    const config = PRICING_CONFIG[vehicleType] || PRICING_CONFIG.economy;
    const durationMin = durationSec / 60;

    const distanceCharge  = distanceKm * config.perKm;
    const durationCharge  = durationMin * config.perMinute;
    const subtotal        = config.baseFare + distanceCharge + durationCharge;
    const surgedSubtotal  = subtotal * surgeMultiplier;
    const total           = Math.max(surgedSubtotal + config.bookingFee, config.minimumFare);

    return {
      breakdown: {
        baseFare:       config.baseFare.toFixed(2),
        distanceCharge: distanceCharge.toFixed(2),
        durationCharge: durationCharge.toFixed(2),
        bookingFee:     config.bookingFee.toFixed(2),
        surgeMultiplier,
        surgeAmount:    (surgedSubtotal - subtotal).toFixed(2),
      },
      subtotal:     subtotal.toFixed(2),
      total:        parseFloat(total.toFixed(2)),
      currency:     'USD',
      distanceKm:   parseFloat(distanceKm.toFixed(2)),
      durationSec,
      vehicleType,
    };
  }

  /**
   * Driver payout: 75% of fare (after Uber's 25% cut)
   */
  calculateDriverPayout(finalFare) {
    return parseFloat((finalFare * 0.75).toFixed(2));
  }
}

module.exports = new FareCalculationService();
```

---

### 8.7 Notification Service

```javascript
// services/notification/NotificationService.js

const { Kafka } = require('kafkajs');
const admin = require('firebase-admin');  // FCM

const NOTIFICATION_TEMPLATES = {
  'ride.accepted': (data) => ({
    title: 'Driver Found!',
    body: `${data.driverName} is on the way. ETA: ${Math.round(data.etaSeconds / 60)} mins`,
    data: { type: 'RIDE_ACCEPTED', tripId: data.tripId },
  }),
  'ride.arriving': (data) => ({
    title: 'Driver Arriving',
    body: `${data.driverName} is almost there!`,
    data: { type: 'DRIVER_ARRIVING', tripId: data.tripId },
  }),
  'ride.started': (data) => ({
    title: 'Ride Started',
    body: `Enjoy your trip! Destination: ${data.destAddress}`,
    data: { type: 'RIDE_STARTED', tripId: data.tripId },
  }),
  'ride.completed': (data) => ({
    title: 'Trip Complete',
    body: `Total: $${data.finalFare}. Tap to rate your driver.`,
    data: { type: 'RIDE_COMPLETED', tripId: data.tripId },
  }),
  'ride.cancelled': (data) => ({
    title: 'Ride Cancelled',
    body: data.role === 'driver'
      ? 'Your driver cancelled. Finding another driver...'
      : 'Your ride has been cancelled.',
    data: { type: 'RIDE_CANCELLED', tripId: data.tripId },
  }),
  'payment.charged': (data) => ({
    title: 'Payment Processed',
    body: `$${data.amount} charged to ${data.last4 ? `•••• ${data.last4}` : 'your payment method'}`,
    data: { type: 'PAYMENT_CHARGED', tripId: data.tripId },
  }),
};

class NotificationService {
  constructor() {
    admin.initializeApp({ credential: admin.credential.applicationDefault() });
    this.messaging = admin.messaging();
    this.kafka = new Kafka({ brokers: ['kafka:9092'] });
    this.consumer = this.kafka.consumer({ groupId: 'notification-service' });
  }

  async start() {
    await this.consumer.connect();
    await this.consumer.subscribe({ topics: ['ride-events', 'payment-events'] });

    await this.consumer.run({
      eachMessage: async ({ topic, message }) => {
        const event = JSON.parse(message.value.toString());
        await this.handleEvent(event);
      },
    });
  }

  async handleEvent(event) {
    const template = NOTIFICATION_TEMPLATES[event.event];
    if (!template) return;

    const notification = template(event);

    // Determine recipients based on event type
    const recipients = await this.getRecipients(event);

    await Promise.allSettled(
      recipients.map(({ fcmToken, platform }) =>
        this.sendPush(fcmToken, notification, platform)
      )
    );
  }

  async sendPush(fcmToken, { title, body, data }, platform = 'android') {
    const message = {
      token: fcmToken,
      notification: { title, body },
      data: Object.fromEntries(
        Object.entries(data).map(([k, v]) => [k, String(v)])
      ),
      android: { priority: 'high' },
      apns: {
        headers: { 'apns-priority': '10' },
        payload: { aps: { sound: 'default', badge: 1 } },
      },
    };

    try {
      const response = await this.messaging.send(message);
      return { success: true, messageId: response };
    } catch (err) {
      if (err.code === 'messaging/registration-token-not-registered') {
        await this.removeStaleToken(fcmToken);
      }
      throw err;
    }
  }

  async getRecipients(event) {
    // Fetch FCM tokens from user profile DB (simplified)
    const db = require('../../db/postgres');
    const ids = [event.riderId, event.driverId].filter(Boolean);
    if (!ids.length) return [];

    const result = await db.query(
      'SELECT id, fcm_token, platform FROM users WHERE id = ANY($1)',
      [ids]
    );
    return result.rows.map(r => ({ userId: r.id, fcmToken: r.fcm_token, platform: r.platform }));
  }

  async removeStaleToken(fcmToken) {
    const db = require('../../db/postgres');
    await db.query('UPDATE users SET fcm_token=NULL WHERE fcm_token=$1', [fcmToken]);
  }
}

module.exports = new NotificationService();
```

---

### 8.8 WebSocket / Real-time Communication

```javascript
// services/websocket/WebSocketGateway.js

const WebSocket = require('ws');
const Redis = require('ioredis');
const jwt = require('jsonwebtoken');

/**
 * WebSocket Gateway
 * - Driver sends GPS updates
 * - Rider receives live driver location
 * - Driver receives ride offers
 *
 * Scaled via: each server instance manages a shard of connections.
 * Redis pub/sub bridges messages across instances.
 */
class WebSocketGateway {
  constructor(httpServer) {
    this.wss = new WebSocket.Server({ server: httpServer });
    this.connections = new Map();  // userId → WebSocket
    this.redis = new Redis();
    this.subscriber = new Redis();

    this.wss.on('connection', this.handleConnection.bind(this));
  }

  handleConnection(ws, req) {
    // Authenticate via ?token= query param
    const url = new URL(req.url, 'http://localhost');
    const token = url.searchParams.get('token');

    let userId;
    try {
      const decoded = jwt.verify(token, process.env.JWT_SECRET);
      userId = decoded.sub;
    } catch (e) {
      ws.close(4001, 'Unauthorized');
      return;
    }

    this.connections.set(userId, ws);

    // Subscribe to user's personal Redis channel
    this.subscriber.subscribe(`ws:${userId}`);
    this.subscriber.on('message', (channel, message) => {
      if (channel === `ws:${userId}` && ws.readyState === WebSocket.OPEN) {
        ws.send(message);
      }
    });

    ws.on('message', (raw) => this.handleMessage(userId, JSON.parse(raw)));
    ws.on('close', () => {
      this.connections.delete(userId);
      // Driver went offline
      this.redis.hset(`driver:${userId}`, 'isAvailable', 0);
    });

    ws.on('error', console.error);
  }

  async handleMessage(userId, message) {
    const { type, payload } = message;

    switch (type) {
      case 'LOCATION_UPDATE':
        // Driver sending GPS
        await require('../location/LocationService')
          .updateDriverLocation(userId, payload);
        break;

      case 'OFFER_RESPONSE':
        // Driver accepting/rejecting offer
        await require('../matching/MatchingEngine')
          .respondToOffer(payload.offerId, userId, payload.action);
        break;

      case 'PING':
        this.sendToUser(userId, { type: 'PONG', ts: Date.now() });
        break;
    }
  }

  /**
   * Send message to a specific user (may be on different server — use Redis pub/sub)
   */
  async sendToUser(userId, message) {
    const local = this.connections.get(userId);
    if (local && local.readyState === WebSocket.OPEN) {
      local.send(JSON.stringify(message));
    } else {
      // User is on another server — publish to Redis
      await this.redis.publish(`ws:${userId}`, JSON.stringify(message));
    }
  }

  /**
   * Kafka consumer: driver location updates → push to rider
   */
  async consumeLocationUpdates() {
    const { Kafka } = require('kafkajs');
    const kafka = new Kafka({ brokers: ['kafka:9092'] });
    const consumer = kafka.consumer({ groupId: 'ws-gateway' });

    await consumer.connect();
    await consumer.subscribe({ topic: 'driver-location-updates' });

    await consumer.run({
      eachMessage: async ({ message }) => {
        const update = JSON.parse(message.value.toString());
        const { tripId, lat, lng, heading, speed, driverId } = update;

        // Find rider for this trip and push location
        const tripData = await this.redis.hgetall(`trip:${tripId}`);
        if (tripData && tripData.riderId) {
          await this.sendToUser(tripData.riderId, {
            type: 'DRIVER_LOCATION',
            payload: { lat, lng, heading, speed, driverId, tripId },
          });
        }
      },
    });
  }
}

module.exports = WebSocketGateway;
```

---

### 8.9 Rate Limiter

```javascript
// middleware/rateLimiter.js

const Redis = require('ioredis');
const redis = new Redis();

/**
 * Sliding Window Rate Limiter using Redis Sorted Sets
 * More accurate than fixed-window counters
 */
class SlidingWindowRateLimiter {
  constructor({ maxRequests, windowMs }) {
    this.maxRequests = maxRequests;
    this.windowMs = windowMs;
  }

  async isAllowed(identifier) {
    const key = `ratelimit:${identifier}`;
    const now = Date.now();
    const windowStart = now - this.windowMs;

    const pipeline = redis.pipeline();
    // Remove old entries outside window
    pipeline.zremrangebyscore(key, '-inf', windowStart);
    // Add current request
    pipeline.zadd(key, now, `${now}-${Math.random()}`);
    // Count requests in window
    pipeline.zcard(key);
    // Set expiry
    pipeline.expire(key, Math.ceil(this.windowMs / 1000));

    const results = await pipeline.exec();
    const count = results[2][1];

    return {
      allowed: count <= this.maxRequests,
      count,
      remaining: Math.max(0, this.maxRequests - count),
      resetMs: this.windowMs,
    };
  }
}

// Different limiters for different endpoints
const limiters = {
  rideRequest:      new SlidingWindowRateLimiter({ maxRequests: 5,    windowMs: 60_000  }),
  locationUpdate:   new SlidingWindowRateLimiter({ maxRequests: 30,   windowMs: 60_000  }),
  authOTP:          new SlidingWindowRateLimiter({ maxRequests: 3,    windowMs: 300_000 }),
  fareEstimate:     new SlidingWindowRateLimiter({ maxRequests: 20,   windowMs: 60_000  }),
  generalAPI:       new SlidingWindowRateLimiter({ maxRequests: 100,  windowMs: 60_000  }),
};

// Express middleware
function rateLimitMiddleware(limiterName) {
  return async (req, res, next) => {
    const limiter = limiters[limiterName] || limiters.generalAPI;
    const identifier = `${req.user?.id || req.ip}:${limiterName}`;

    const result = await limiter.isAllowed(identifier);

    res.setHeader('X-RateLimit-Limit',     limiter.maxRequests);
    res.setHeader('X-RateLimit-Remaining', result.remaining);
    res.setHeader('X-RateLimit-Reset',     Date.now() + limiter.windowMs);

    if (!result.allowed) {
      return res.status(429).json({
        error: 'TOO_MANY_REQUESTS',
        message: 'Rate limit exceeded. Please try again later.',
        retryAfter: Math.ceil(limiter.windowMs / 1000),
      });
    }

    next();
  };
}

module.exports = { rateLimitMiddleware, limiters };
```

---

### 8.10 Payment Service

```javascript
// services/payment/PaymentService.js

const Stripe = require('stripe');
const { Kafka } = require('kafkajs');
const db = require('../../db/postgres');

const stripe = new Stripe(process.env.STRIPE_SECRET_KEY);

class PaymentService {
  constructor() {
    this.kafka = new Kafka({ brokers: ['kafka:9092'] });
    this.consumer = this.kafka.consumer({ groupId: 'payment-service' });
    this.producer = this.kafka.producer();
  }

  async start() {
    await this.producer.connect();
    await this.consumer.connect();
    await this.consumer.subscribe({ topic: 'payment-events' });
    await this.consumer.run({
      eachMessage: async ({ message }) => {
        const event = JSON.parse(message.value.toString());
        if (event.event === 'payment.initiate') {
          await this.processPayment(event);
        }
      },
    });
  }

  async processPayment({ tripId, riderId, driverId, amount, paymentMethodId }) {
    const client = await db.pool.connect();
    try {
      await client.query('BEGIN');

      // 1. Fetch Stripe token
      const pm = await client.query(
        'SELECT * FROM payment_methods WHERE id=$1', [paymentMethodId]
      );
      const paymentMethod = pm.rows[0];

      if (paymentMethod.type === 'cash') {
        // Cash payment — no Stripe, just record
        await this.recordPayment(client, {
          tripId, riderId, driverId, amount, method: 'cash', status: 'completed',
        });
        await client.query('COMMIT');
        return;
      }

      // 2. Charge via Stripe
      const paymentIntent = await stripe.paymentIntents.create({
        amount: Math.round(amount * 100),  // cents
        currency: 'usd',
        payment_method: paymentMethod.provider_token,
        confirm: true,
        metadata: { tripId, riderId, driverId },
      });

      if (paymentIntent.status !== 'succeeded') {
        throw new Error(`Payment failed: ${paymentIntent.status}`);
      }

      // 3. Record in DB
      await this.recordPayment(client, {
        tripId, riderId, driverId, amount, method: 'card',
        status: 'completed', stripePaymentIntentId: paymentIntent.id,
      });

      // 4. Update trip payment status
      await client.query(
        `UPDATE trips SET payment_status='charged' WHERE id=$1`, [tripId]
      );

      // 5. Schedule driver payout (weekly batch via Stripe Connect)
      const driverPayout = parseFloat((amount * 0.75).toFixed(2));
      await this.scheduleDriverPayout(client, driverId, driverPayout, tripId);

      await client.query('COMMIT');

      // 6. Publish success event
      await this.producer.send({
        topic: 'ride-events',
        messages: [{
          key: tripId,
          value: JSON.stringify({
            event: 'payment.charged',
            tripId, riderId, amount,
            last4: paymentMethod.last4,
          }),
        }],
      });

    } catch (err) {
      await client.query('ROLLBACK');
      await this.producer.send({
        topic: 'payment-events',
        messages: [{
          key: tripId,
          value: JSON.stringify({ event: 'payment.failed', tripId, error: err.message }),
        }],
      });
    } finally {
      client.release();
    }
  }

  async recordPayment(client, data) {
    await client.query(`
      INSERT INTO payments (trip_id, rider_id, driver_id, amount, method, status, stripe_id, created_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,NOW())
    `, [data.tripId, data.riderId, data.driverId, data.amount,
        data.method, data.status, data.stripePaymentIntentId || null]);
  }

  async scheduleDriverPayout(client, driverId, amount, tripId) {
    await client.query(`
      INSERT INTO driver_earnings (driver_id, trip_id, amount, status, created_at)
      VALUES ($1,$2,$3,'pending',NOW())
    `, [driverId, tripId, amount]);
  }

  async refund(tripId, reason) {
    const result = await db.query(
      'SELECT stripe_id FROM payments WHERE trip_id=$1', [tripId]
    );
    const payment = result.rows[0];
    if (!payment?.stripe_id) throw new Error('NO_CHARGEABLE_PAYMENT');

    const refund = await stripe.refunds.create({
      payment_intent: payment.stripe_id,
      reason: 'requested_by_customer',
      metadata: { tripId, reason },
    });

    await db.query(
      `UPDATE trips SET payment_status='refunded' WHERE id=$1`, [tripId]
    );

    return refund;
  }
}

module.exports = new PaymentService();
```

---

## 9. Algorithms & Data Structures

### 9.1 Nearest Driver Search (KD-Tree)

```javascript
// utils/KDTree.js
// Used for in-memory nearest neighbor search (backup to Redis GEO)

class KDNode {
  constructor(point, depth = 0) {
    this.point = point;       // { lat, lng, driverId, ...metadata }
    this.left = null;
    this.right = null;
    this.depth = depth;
  }
}

class KDTree {
  constructor(points) {
    this.root = this.build(points, 0);
  }

  build(points, depth) {
    if (!points.length) return null;
    const axis = depth % 2;  // 0 = lat, 1 = lng
    const key = axis === 0 ? 'lat' : 'lng';

    points.sort((a, b) => a[key] - b[key]);
    const mid = Math.floor(points.length / 2);

    const node = new KDNode(points[mid], depth);
    node.left  = this.build(points.slice(0, mid), depth + 1);
    node.right = this.build(points.slice(mid + 1), depth + 1);
    return node;
  }

  /**
   * Find k nearest neighbors to query point
   */
  kNearest(queryLat, queryLng, k = 5) {
    const heap = [];  // max-heap by distance

    const search = (node) => {
      if (!node) return;

      const dist = this.euclideanDist(queryLat, queryLng,
                                      node.point.lat, node.point.lng);

      if (heap.length < k || dist < heap[0].dist) {
        if (heap.length >= k) heap.shift();
        heap.push({ ...node.point, dist });
        heap.sort((a, b) => b.dist - a.dist);  // max at front
      }

      const axis = node.depth % 2;
      const axisKey = axis === 0 ? 'lat' : 'lng';
      const axisQuery = axis === 0 ? queryLat : queryLng;
      const axisDiff = axisQuery - node.point[axisKey];

      const near  = axisDiff <= 0 ? node.left  : node.right;
      const far   = axisDiff <= 0 ? node.right : node.left;

      search(near);
      if (heap.length < k || Math.abs(axisDiff) < heap[0].dist) {
        search(far);
      }
    };

    search(this.root);
    return heap.sort((a, b) => a.dist - b.dist);
  }

  euclideanDist(lat1, lng1, lat2, lng2) {
    return Math.sqrt((lat1 - lat2) ** 2 + (lng1 - lng2) ** 2);
  }
}

module.exports = KDTree;
```

---

### 9.2 ETA Calculation (Dijkstra)

```javascript
// utils/Dijkstra.js
// Simplified road network shortest path (production uses OSRM)

class PriorityQueue {
  constructor() { this.heap = []; }

  push(item) {
    this.heap.push(item);
    this._bubbleUp(this.heap.length - 1);
  }

  pop() {
    const top = this.heap[0];
    const last = this.heap.pop();
    if (this.heap.length) {
      this.heap[0] = last;
      this._sinkDown(0);
    }
    return top;
  }

  get size() { return this.heap.length; }

  _bubbleUp(i) {
    while (i > 0) {
      const parent = Math.floor((i - 1) / 2);
      if (this.heap[parent].cost <= this.heap[i].cost) break;
      [this.heap[parent], this.heap[i]] = [this.heap[i], this.heap[parent]];
      i = parent;
    }
  }

  _sinkDown(i) {
    const n = this.heap.length;
    while (true) {
      let min = i;
      const l = 2*i+1, r = 2*i+2;
      if (l < n && this.heap[l].cost < this.heap[min].cost) min = l;
      if (r < n && this.heap[r].cost < this.heap[min].cost) min = r;
      if (min === i) break;
      [this.heap[i], this.heap[min]] = [this.heap[min], this.heap[i]];
      i = min;
    }
  }
}

function dijkstra(graph, source, target) {
  /**
   * graph: Map<nodeId, Array<{to, weight}>>
   * Returns: { cost, path }
   */
  const dist = new Map();
  const prev = new Map();
  const pq = new PriorityQueue();

  dist.set(source, 0);
  pq.push({ node: source, cost: 0 });

  while (pq.size > 0) {
    const { node, cost } = pq.pop();

    if (node === target) {
      // Reconstruct path
      const path = [];
      let cur = target;
      while (cur !== undefined) {
        path.unshift(cur);
        cur = prev.get(cur);
      }
      return { cost, path };
    }

    if (cost > (dist.get(node) ?? Infinity)) continue;

    for (const { to, weight } of (graph.get(node) || [])) {
      const newDist = cost + weight;
      if (newDist < (dist.get(to) ?? Infinity)) {
        dist.set(to, newDist);
        prev.set(to, node);
        pq.push({ node: to, cost: newDist });
      }
    }
  }

  return { cost: Infinity, path: [] };
}

module.exports = { dijkstra, PriorityQueue };
```

---

### 9.3 Geohash Encoding

See [Section 8.2](#82-geospatial-indexing-with-s2--geohash) for full implementation.

**Usage in surge pricing:**
```javascript
// Zone precision 5 = ~5km cells
const zone = Geohash.encode(37.7749, -122.4194, 5);  // → "9q8yy"
const neighbors = Geohash.neighbors(zone);
// Check surge for all 9 cells (center + 8 neighbors) for edge coverage
```

---

## 10. Caching Strategy

| Data | Cache | TTL | Invalidation |
|------|-------|-----|--------------|
| Driver live location | Redis GEO | 30s | Auto-expire if no heartbeat |
| Driver availability hash | Redis Hash | 30s | On status change |
| Surge multiplier per zone | Redis String | 60s | Recomputed by Pricing worker |
| Active trip state | Redis Hash | 2h | On trip complete/cancel |
| Rider active trip | Redis String | 2h | On trip end |
| Fare estimate | Redis String | 5min | Request-scoped |
| User profile | Redis Hash | 15min | On profile update |
| JWT session | Redis String | 24h | On logout |
| ETA per route | Redis String | 30s | Request-scoped |

**Cache-aside pattern for user profiles:**
```javascript
async function getUserProfile(userId) {
  const cacheKey = `user:profile:${userId}`;
  const cached = await redis.hgetall(cacheKey);
  if (cached && cached.id) return cached;

  const result = await db.query('SELECT * FROM users WHERE id=$1', [userId]);
  const user = result.rows[0];
  if (user) {
    await redis.hset(cacheKey, user);
    await redis.expire(cacheKey, 900);
  }
  return user;
}
```

---

## 11. Message Queue & Event Streaming

### Kafka Topics

| Topic | Producers | Consumers | Retention |
|-------|-----------|-----------|-----------|
| `ride-events` | Trip Service | Notification, Analytics, Payment | 7 days |
| `driver-location-updates` | Location Service | WS Gateway, Analytics | 1 day |
| `payment-events` | Trip Service, Payment Service | Payment Service, Notification | 14 days |
| `driver-status-changes` | Driver Service | Matching Engine, Analytics | 3 days |
| `rating-events` | Rating Service | Driver Service (rolling avg) | 30 days |

### Event Schema Example

```json
{
  "event": "ride.accepted",
  "version": "1.0",
  "tripId": "550e8400-e29b-41d4-a716-446655440000",
  "riderId": "rider-uuid",
  "driverId": "driver-uuid",
  "etaSeconds": 240,
  "ts": 1718000000000,
  "metadata": {
    "region": "us-west-2",
    "serviceVersion": "2.4.1"
  }
}
```

### Kafka Consumer Groups

```
ride-events topic:
  ├── notification-service   (pushes to rider/driver)
  ├── analytics-service      (heatmaps, reports)
  └── audit-service          (immutable log)

driver-location-updates topic:
  ├── ws-gateway-group       (pushes to rider app)
  └── analytics-service      (path recording)
```

---

## 12. Scalability Patterns

### Horizontal Scaling

```
API Gateway:         Stateless, add nodes behind AWS ALB
Location Service:    Stateless; driver sharded by (driverId % N) to servers
Matching Engine:     Stateless; can run N instances consuming from Kafka
Trip Service:        Stateless; DB connection pooling via PgBouncer
WebSocket Gateway:   Stateful; consistent hashing by userId to server;
                     Redis pub/sub bridges cross-server messages

PostgreSQL:          Primary + Read Replicas (reads go to replicas)
                     Partitioning: trips partitioned by created_at (monthly)
Redis:               Cluster mode (16384 hash slots across nodes)
Kafka:               Topic partitioned by tripId for ordered per-trip processing
```

### Database Sharding

```
Trips table: horizontally sharded by rider_id (hash)
  Shard 0: rider_ids where hash(id) % 4 == 0
  Shard 1: ...

Driver locations: no sharding needed (Redis handles it in-memory)

Cassandra (trip_locations): naturally partitioned by trip_id
```

### Geographic Partitioning

```
Deploy separate stacks per region: US-WEST, US-EAST, EU, APAC
  - Regional Kafka clusters
  - Regional PostgreSQL (with cross-region replication for analytics)
  - Regional Redis clusters
  - Global DNS routing via Route53 latency-based routing
```

---

## 13. Fault Tolerance & Reliability

### Circuit Breaker Pattern

```javascript
// utils/CircuitBreaker.js

class CircuitBreaker {
  constructor(fn, { failureThreshold = 5, timeout = 60000, successThreshold = 2 } = {}) {
    this.fn = fn;
    this.state = 'CLOSED';  // CLOSED | OPEN | HALF_OPEN
    this.failures = 0;
    this.successes = 0;
    this.failureThreshold = failureThreshold;
    this.timeout = timeout;
    this.successThreshold = successThreshold;
    this.nextAttempt = Date.now();
  }

  async execute(...args) {
    if (this.state === 'OPEN') {
      if (Date.now() < this.nextAttempt) {
        throw new Error('CIRCUIT_OPEN: Service unavailable');
      }
      this.state = 'HALF_OPEN';
    }

    try {
      const result = await this.fn(...args);
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
    this.successes = 0;
    if (this.failures >= this.failureThreshold) {
      this.state = 'OPEN';
      this.nextAttempt = Date.now() + this.timeout;
    }
  }
}

// Usage
const etaCircuitBreaker = new CircuitBreaker(ETAService.getETA.bind(ETAService));
```

### Retry with Exponential Backoff

```javascript
async function withRetry(fn, { maxRetries = 3, baseDelayMs = 100 } = {}) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (err) {
      if (attempt === maxRetries) throw err;
      const delay = baseDelayMs * Math.pow(2, attempt - 1) + Math.random() * 100;
      await new Promise(r => setTimeout(r, delay));
    }
  }
}
```

### Idempotency Keys

```javascript
// Prevent duplicate payments on network retry
async function processPaymentIdempotent(tripId, amount, paymentMethodId) {
  const idempotencyKey = `payment:${tripId}`;  // deterministic per trip

  // Check if already processed
  const existing = await redis.get(idempotencyKey);
  if (existing) return JSON.parse(existing);

  const result = await stripe.paymentIntents.create(
    { amount, currency: 'usd', payment_method: paymentMethodId, confirm: true },
    { idempotencyKey }                        // Stripe supports this natively
  );

  await redis.setex(idempotencyKey, 86400, JSON.stringify(result));
  return result;
}
```

### Health Checks

```javascript
// healthcheck.js
app.get('/health', async (req, res) => {
  const checks = await Promise.allSettled([
    db.query('SELECT 1'),
    redis.ping(),
    kafka.admin().listTopics(),
  ]);

  const status = checks.every(c => c.status === 'fulfilled') ? 'healthy' : 'degraded';
  const details = {
    postgres:  checks[0].status === 'fulfilled' ? 'up' : 'down',
    redis:     checks[1].status === 'fulfilled' ? 'up' : 'down',
    kafka:     checks[2].status === 'fulfilled' ? 'up' : 'down',
  };

  res.status(status === 'healthy' ? 200 : 503).json({ status, details });
});
```

---

## 14. Security Design

| Layer | Mechanism |
|-------|-----------|
| Authentication | Phone OTP → JWT (15min) + Refresh Token (30d) |
| Authorization | Role-based (rider/driver/admin) middleware |
| API Gateway | JWT validation before routing |
| Transport | TLS 1.3 everywhere (HTTPS + WSS) |
| Sensitive data | PII encrypted at rest (AES-256) |
| Payment tokens | Stripe tokens (never store raw card numbers) |
| Rate limiting | Per-user + per-IP sliding window |
| SQL injection | Parameterized queries only (no string interpolation) |
| Location privacy | Driver location only shared after trip accepted |
| Audit log | Immutable Kafka topic `audit-events` for all mutations |

### JWT Middleware

```javascript
// middleware/auth.js
const jwt = require('jsonwebtoken');

function authenticate(req, res, next) {
  const token = req.headers.authorization?.split(' ')[1];
  if (!token) return res.status(401).json({ error: 'NO_TOKEN' });

  try {
    const decoded = jwt.verify(token, process.env.JWT_SECRET);
    req.user = { id: decoded.sub, role: decoded.role };
    next();
  } catch (err) {
    if (err.name === 'TokenExpiredError') {
      return res.status(401).json({ error: 'TOKEN_EXPIRED' });
    }
    return res.status(401).json({ error: 'INVALID_TOKEN' });
  }
}

function authorize(...roles) {
  return (req, res, next) => {
    if (!roles.includes(req.user.role)) {
      return res.status(403).json({ error: 'FORBIDDEN' });
    }
    next();
  };
}

module.exports = { authenticate, authorize };
```

---

## 15. Monitoring & Observability

### Key Metrics (Prometheus + Grafana)

```
Business Metrics:
  - trips_requested_total (counter)
  - trips_completed_total (counter)
  - trips_cancelled_total{by: rider|driver|system} (counter)
  - match_success_rate (gauge)
  - average_match_time_seconds (histogram)
  - surge_multiplier_by_zone (gauge)
  - active_drivers_by_city (gauge)

Infrastructure Metrics:
  - location_update_latency_ms (p50, p95, p99)
  - api_response_time_ms (histogram, by endpoint)
  - kafka_consumer_lag (gauge, by topic + group)
  - redis_memory_usage_bytes (gauge)
  - db_connection_pool_active (gauge)
  - websocket_connections_active (gauge)

SLOs:
  - P99 ride request → match < 2 seconds
  - P99 location update ingestion < 500ms
  - Payment success rate > 99.5%
  - API availability > 99.99%
```

### Distributed Tracing

```javascript
// Each request gets a traceId propagated through all services
const { v4: uuidv4 } = require('uuid');

app.use((req, res, next) => {
  req.traceId = req.headers['x-trace-id'] || uuidv4();
  res.setHeader('x-trace-id', req.traceId);
  next();
});

// All service calls pass X-Trace-Id header
// Jaeger / Zipkin collects spans and assembles full trace
```

### Alerting Rules

```yaml
# Prometheus alerting rules (pseudo-YAML)
alerts:
  - name: MatchTimeTooHigh
    condition: avg(match_time_seconds) > 2
    severity: critical
    notify: pagerduty

  - name: KafkaConsumerLagHigh
    condition: kafka_consumer_lag{group="notification-service"} > 10000
    severity: warning
    notify: slack

  - name: NoAvailableDriversInZone
    condition: active_drivers_by_city{city="sf"} < 10
    severity: warning
    notify: slack
```

---

## 16. Trade-offs & Bottlenecks

### Key Design Trade-offs

| Decision | Choice Made | Alternative | Why |
|----------|-------------|-------------|-----|
| Location store | Redis GEO | PostGIS | Redis: sub-ms, in-memory; PostGIS better for complex spatial queries |
| Driver matching | Sequential dispatch | Broadcast to all | Sequential reduces driver notification spam; broadcast is faster but noisy |
| Ride events | Kafka async | Synchronous REST | Kafka: decoupled, retry-able, high throughput; REST simpler but tight coupling |
| Trip state | Redis + PostgreSQL | PostgreSQL only | Redis for hot-path speed; Postgres for durability |
| Geohash precision | 5 (≈5km) for surge | 7 (≈150m) | Too fine = sparse data; too coarse = inaccurate surge |
| WebSocket scaling | Redis pub/sub bridge | Sticky sessions | Pub/sub allows stateless scale-out; sticky sessions limit horizontal scaling |

### Potential Bottlenecks

```
1. Location write throughput (250K writes/sec)
   → Solution: Redis Cluster + pipeline batching per driver

2. Driver matching under surge (10K requests/sec)
   → Solution: Kafka-based async matching; horizontal Matching Engine scaling

3. PostgreSQL write contention (trip updates)
   → Solution: PgBouncer connection pool; CQRS (writes to primary, reads to replica)

4. WebSocket connections (1M+ concurrent)
   → Solution: Multiple WS gateway nodes; load balancing with consistent hashing

5. Kafka lag during peaks
   → Solution: Increase partition count; auto-scaling consumer groups

6. Cold start: city launches with no drivers
   → Solution: Incentive campaigns; driver marketplace; graceful degradation
```

---

## Summary Architecture Checklist

- [x] Microservices with clear bounded contexts
- [x] Async event-driven with Kafka
- [x] Real-time WebSocket with Redis pub/sub for cross-server messaging
- [x] Geospatial indexing with Redis GEO + Geohash
- [x] Dynamic surge pricing with demand/supply ratio
- [x] Sequential driver dispatch with offer timeout
- [x] ACID payments with idempotency keys
- [x] Sliding window rate limiting
- [x] Circuit breaker + retry with backoff
- [x] JWT + OTP authentication
- [x] Multi-layer caching (Redis, CDN)
- [x] Horizontal scaling for all stateless services
- [x] PostgreSQL partitioning for trip history
- [x] Cassandra for time-series GPS history
- [x] Distributed tracing across services
- [x] SLO-based alerting

---

*Prepared for FAANG-level System Design Interview*  
*Stack: Node.js · PostgreSQL · Redis · Kafka · Cassandra · Stripe · Firebase FCM*