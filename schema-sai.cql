-- AstraDB/HCD Schema for Transaction Data Ingest POC
-- This file contains all CQL statements needed to set up the database schema

-- =====================================================================
-- KEYSPACE CREATION
-- =====================================================================

-- For local HCD/Cassandra installations
CREATE KEYSPACE IF NOT EXISTS transactions
WITH replication = {
    'class': 'SimpleStrategy', -- places replicas in a simple, round-robin fashion around the ring 
    'replication_factor': 1
};

-- For multi-datacenter production deployments, use:
-- CREATE KEYSPACE IF NOT EXISTS transactions
-- WITH replication = {
--     'class': 'NetworkTopologyStrategy',
--     'datacenter1': 3,
--     'datacenter2': 3
-- };

-- Note: For AstraDB, keyspaces are typically created through the dashboard
-- If you need to create programmatically, use DataStax DevOps API

USE transactions;

-- =====================================================================
-- MAIN TRANSACTIONS TABLE
-- =====================================================================

CREATE TABLE IF NOT EXISTS transactions (
    -- Primary Key
    -- The UUID determines which node stores the data. 
    -- Random UUIDs = perfect data distribution across your cluster, so you can efficiently query by that UUID.​
    transaction_id UUID PRIMARY KEY, 
    
    -- Card and Transaction Details
    card_number TEXT,
    merchant_id TEXT,
    merchant_name TEXT,
    amount DECIMAL,
    currency TEXT,
    transaction_type TEXT,  -- purchase, refund, authorization, reversal
    
    -- Gateway and Terminal Information
    pos_terminal_id TEXT,
    gateway_id TEXT,
    auth_code TEXT,
    
    -- Temporal Information
    timestamp TIMESTAMP,
    created_at TIMESTAMP,
    
    -- Location Information
    location_country TEXT,
    location_city TEXT,
    location_lat FLOAT,
    location_lng FLOAT,
    
    -- Transaction Status and Risk
    status TEXT,  -- pending, approved, declined, failed
    risk_score FLOAT,
    
    -- Merchant Information
    mcc_code TEXT,  -- Merchant Category Code
    
    -- Financial Information
    processing_fee DECIMAL
) WITH 
    -- Compaction in Cassandra is the process of merging SSTables
    -- SizeTieredCompactionStrategy (STCS)
    -- Default compaction strategy for many workloads (especially write-heavy ones).
    -- Groups SSTables of similar sizes ("tiers") and merges them into a larger SSTable.
    -- The goal: fewer, bigger SSTables → faster reads and better space utilization.
    --  Works best with write-heavy, append-like workloads where updates are less frequent.
    compaction = {  
        'class': 'SizeTieredCompactionStrategy',
        'max_threshold': 32, -- At most 32 SSTables of similar size are compacted together
        'min_threshold': 4 -- At least 4 SSTables of similar size must exist
    }
    AND compression = { -- reduces the size of these SSTables by encoding the data into smaller chunks
        'chunk_length_in_kb': 64,
        'class': 'LZ4Compressor'
    }
    AND gc_grace_seconds = 864000;  -- 10 days

-- =====================================================================
-- SECONDARY INDEXES USING SAI (STORAGE-ATTACHED INDEXING)
-- =====================================================================

-- IMPORTANT: HCD supports two types of indexes:
-- 1. Traditional Secondary Indexes (2i): Maximum 1 per table
-- 2. Storage-Attached Indexes (SAI): Maximum 50 per table
--
-- SAI Best Practices:
-- - Index HIGH cardinality columns (many unique values)
-- - AVOID low cardinality columns (few unique values like status, gateway)
-- - Low cardinality indexes waste space and don't improve performance
-- - For low cardinality queries, use separate partitioned tables instead

-- HIGH CARDINALITY - Good for SAI (many unique values per column)
CREATE CUSTOM INDEX IF NOT EXISTS sai_transactions_timestamp 
    ON transactions (timestamp) USING 'StorageAttachedIndex';

CREATE CUSTOM INDEX IF NOT EXISTS sai_transactions_merchant 
    ON transactions (merchant_id) USING 'StorageAttachedIndex';

CREATE CUSTOM INDEX IF NOT EXISTS sai_transactions_country 
    ON transactions (location_country) USING 'StorageAttachedIndex';

CREATE CUSTOM INDEX IF NOT EXISTS sai_transactions_risk_score 
    ON transactions (risk_score) USING 'StorageAttachedIndex';

-- Note: risk_score is continuous (0.0-1.0), so has high cardinality
-- Useful for range queries like: WHERE risk_score > 0.8

-- LOW CARDINALITY - DO NOT INDEX with SAI (few unique values)
-- These columns commented out - use partitioned tables instead:

-- CREATE CUSTOM INDEX sai_transactions_status      -- Only 3-4 values: approved/declined/pending
-- CREATE CUSTOM INDEX sai_transactions_gateway     -- Only 3 values: VISA/MASTERCARD/AMEX
-- CREATE CUSTOM INDEX sai_transactions_currency    -- Only 5-10 values: USD/EUR/GBP/etc

-- For low cardinality queries, use these alternative patterns:
-- 1. Separate tables partitioned by the low cardinality field
-- 2. Filter in application layer after retrieving by high cardinality field
-- 3. Use ALLOW FILTERING sparingly (only for small result sets)

-- Example of better approach for status queries:
-- CREATE TABLE transactions_by_status (
--     status TEXT,
--     timestamp TIMESTAMP,
--     transaction_id UUID,
--     PRIMARY KEY ((status), timestamp, transaction_id)
-- );

-- Cardinality Guidelines:
-- High Cardinality (Good for SAI):
--   - UUIDs, timestamps, merchant_ids, card_numbers
--   - Columns where each value appears in < 5% of rows
--   - Columns with 100+ distinct values
--
-- Low Cardinality (Bad for SAI):
--   - Boolean fields, status codes, categories
--   - Columns where each value appears in > 10% of rows
--   - Columns with < 20 distinct values

-- =====================================================================
-- TIME-SERIES TABLES FOR ANALYTICS AND REPORTING
-- =====================================================================

-- Daily transaction aggregates (for reporting and analytics)
CREATE TABLE IF NOT EXISTS daily_transaction_stats (
    stat_date DATE,
    merchant_id TEXT,
    currency TEXT,
    transaction_count BIGINT,
    total_amount DECIMAL,
    avg_amount DECIMAL,
    min_amount DECIMAL,
    max_amount DECIMAL,
    total_processing_fees DECIMAL,
    approved_count BIGINT,
    declined_count BIGINT,
    fraud_count BIGINT,
    last_updated TIMESTAMP,
    PRIMARY KEY ((stat_date), merchant_id, currency)
) WITH 
    CLUSTERING ORDER BY (merchant_id ASC, currency ASC)
    AND compaction = {
        'class': 'TimeWindowCompactionStrategy',
        'compaction_window_unit': 'DAYS',
        'compaction_window_size': 1
    }
    AND default_time_to_live = 7776000;  -- 90 days retention

-- Hourly transaction stats for real-time monitoring
-- Note: Using regular columns (not COUNTER) for flexibility
-- Populate via batch aggregation jobs, not real-time updates
CREATE TABLE IF NOT EXISTS hourly_transaction_stats (
    stat_hour TIMESTAMP,
    gateway_id TEXT,
    transaction_count BIGINT,
    total_amount DECIMAL,
    avg_risk_score FLOAT,
    high_risk_count BIGINT,
    declined_count BIGINT,
    last_updated TIMESTAMP,
    PRIMARY KEY ((stat_hour), gateway_id)
) WITH 
    CLUSTERING ORDER BY (gateway_id ASC)
    AND compaction = {
        'class': 'TimeWindowCompactionStrategy',
        'compaction_window_unit': 'HOURS',
        'compaction_window_size': 1
    }
    AND default_time_to_live = 604800;  -- 7 days retention

-- Alternative: Hourly stats with COUNTER columns (if you only need increments)
-- Limitation: COUNTER tables can only have partition keys, clustering keys, and COUNTER columns
-- Cannot include total_amount, avg_risk_score, or last_updated

-- CREATE TABLE IF NOT EXISTS hourly_transaction_stats_counters (
--     stat_hour TIMESTAMP,
--     gateway_id TEXT,
--     transaction_count COUNTER,
--     high_risk_count COUNTER,
--     declined_count COUNTER,
--     PRIMARY KEY ((stat_hour), gateway_id)
-- ) WITH CLUSTERING ORDER BY (gateway_id ASC);

-- =====================================================================
-- FRAUD DETECTION AND MONITORING TABLES
-- =====================================================================

-- High-risk transactions for fraud review
CREATE TABLE IF NOT EXISTS high_risk_transactions (
    risk_bucket TEXT,  -- 'critical', 'high', 'medium', 'low'
    timestamp TIMESTAMP,
    transaction_id UUID,
    card_number TEXT,
    merchant_id TEXT,
    amount DECIMAL,
    risk_score FLOAT,
    risk_factors LIST<TEXT>,
    review_status TEXT,  -- 'pending', 'approved', 'blocked'
    reviewer_id TEXT,
    reviewed_at TIMESTAMP,
    PRIMARY KEY ((risk_bucket), timestamp, transaction_id)
) WITH 
    CLUSTERING ORDER BY (timestamp DESC, transaction_id ASC)
    AND compaction = {
        'class': 'TimeWindowCompactionStrategy',
        'compaction_window_unit': 'DAYS',
        'compaction_window_size': 7
    }
    AND default_time_to_live = 2592000;  -- 30 days retention

-- =====================================================================
-- CARD LEDGER TABLE (for atomicity requirements)
-- =====================================================================

-- Sequence counters for atomic operations
CREATE TABLE IF NOT EXISTS sequence_counters (
    card_number TEXT,
    ledger_date TEXT,
    counter_value COUNTER,
    PRIMARY KEY (card_number, ledger_date)
);

-- Card balance and transaction ledger
CREATE TABLE IF NOT EXISTS card_ledger (
    card_number TEXT,
    ledger_date TEXT,
    sequence_number INT,
    entry_id TEXT,
    transaction_id TEXT,
    entry_type TEXT,
    amount DECIMAL,
    running_balance DECIMAL,
    timestamp TIMESTAMP,
    reference_id TEXT,
    description TEXT,
    PRIMARY KEY ((card_number, ledger_date), sequence_number)
) WITH 
    CLUSTERING ORDER BY (sequence_number ASC)
    AND compaction = {
        'class': 'SizeTieredCompactionStrategy'
    };

-- =====================================================================
-- MATERIALIZED VIEWS FOR OPTIMIZED QUERIES
-- =====================================================================

-- Note: Materialized views are DISABLED by default in HCD
-- To enable: set 'enable_materialized_views: true' in cassandra.yaml
-- 
-- With SAI indexes enabled (which support up to 50 indexes per table),
-- materialized views are generally NOT needed for most use cases.
-- SAI provides flexible querying without the write overhead of materialized views.
--
-- If you need materialized views for specific denormalization patterns,
-- uncomment below and enable them in cassandra.yaml first.

-- Transactions by merchant (alternative to SAI index)
-- CREATE MATERIALIZED VIEW IF NOT EXISTS transactions_by_merchant AS
--     SELECT transaction_id, merchant_id, merchant_name, timestamp, amount, currency, status
--     FROM transactions
--     WHERE transaction_id IS NOT NULL 
--     AND merchant_id IS NOT NULL 
--     AND timestamp IS NOT NULL
--     PRIMARY KEY ((merchant_id), timestamp, transaction_id)
--     WITH CLUSTERING ORDER BY (timestamp DESC, transaction_id ASC);

-- Transactions by status and time (alternative to SAI index)
-- CREATE MATERIALIZED VIEW IF NOT EXISTS transactions_by_status_time AS
--     SELECT transaction_id, status, timestamp, amount, merchant_id, risk_score
--     FROM transactions
--     WHERE transaction_id IS NOT NULL 
--     AND status IS NOT NULL 
--     AND timestamp IS NOT NULL
--     PRIMARY KEY ((status), timestamp, transaction_id)
--     WITH CLUSTERING ORDER BY (timestamp DESC, transaction_id ASC);

-- =====================================================================
-- USER DEFINED TYPES (UDTs) FOR COMPLEX DATA
-- =====================================================================

-- Location information UDT
CREATE TYPE IF NOT EXISTS location_info (
    country TEXT,
    city TEXT,
    state TEXT,
    postal_code TEXT,
    latitude FLOAT,
    longitude FLOAT,
    timezone TEXT
);

-- Risk assessment UDT
CREATE TYPE IF NOT EXISTS risk_assessment (
    score FLOAT,
    level TEXT,
    factors LIST<TEXT>,
    model_version TEXT,
    assessed_at TIMESTAMP
);

-- Enhanced transactions table with UDTs (optional advanced version)
CREATE TABLE IF NOT EXISTS transactions_v2 (
    transaction_id UUID PRIMARY KEY,
    card_number TEXT,
    merchant_id TEXT,
    merchant_name TEXT,
    amount DECIMAL,
    currency TEXT,
    transaction_type TEXT,
    gateway_id TEXT,
    timestamp TIMESTAMP,
    location frozen<location_info>,
    risk frozen<risk_assessment>,
    status TEXT,
    auth_code TEXT,
    processing_fee DECIMAL,
    metadata MAP<TEXT, TEXT>,
    created_at TIMESTAMP
) WITH 
    compaction = {
        'class': 'SizeTieredCompactionStrategy',
        'max_threshold': 32,
        'min_threshold': 4
    }
    AND compression = {
        'chunk_length_in_kb': 64,
        'class': 'LZ4Compressor'
    };

-- =====================================================================
-- PERFORMANCE MONITORING TABLE
-- =====================================================================

-- Track ingestion performance metrics
CREATE TABLE IF NOT EXISTS ingestion_metrics (
    metric_date DATE,
    metric_hour INT,
    gateway_id TEXT,
    transactions_per_second FLOAT,
    error_rate FLOAT,
    avg_latency_ms FLOAT,
    p95_latency_ms FLOAT,
    p99_latency_ms FLOAT,
    total_transactions BIGINT,
    successful_transactions BIGINT,
    failed_transactions BIGINT,
    recorded_at TIMESTAMP,
    PRIMARY KEY ((metric_date), metric_hour, gateway_id)
) WITH 
    CLUSTERING ORDER BY (metric_hour ASC, gateway_id ASC)
    AND default_time_to_live = 2592000;  -- 30 days retention

-- =====================================================================
-- SCHEMA DESIGN NOTES AND BEST PRACTICES
-- =====================================================================

/*
Schema Design Notes:

1. PRIMARY KEY DESIGN:
   - Main transactions table uses UUID as partition key for even distribution
   - Time-series tables use date/time as partition key for temporal queries
   - Materialized views provide alternative access patterns

2. HCD INDEXING WITH SAI:
   - HCD supports two index types: Traditional 2i (1 per table) and SAI (50 per table)
   - SAI (Storage-Attached Indexing) is the recommended approach
   - SAI provides better performance, lower storage overhead, and more flexibility
   - Supports complex queries: inequalities, OR predicates, nested boolean logic
   - Indexes are created at ingest time for immediate query coverage
   - Requires Cassandra 4.x/5.0+ (included in HCD)

3. SAI PERFORMANCE CONSIDERATIONS:
   - Works best with fast block storage (NVMe SSDs, EBS)
   - Requires adequate vCPU for write-heavy indexed workloads
   - Lower storage overhead compared to traditional 2i indexes
   - Only create indexes on columns you frequently query
   - Monitor per-node data density (certified up to 150TB/node, but verify for indexed workloads)

4. SAI QUERY CAPABILITIES:
   - Equality: WHERE merchant_id = 'MER_123'
   - Inequality: WHERE risk_score > 0.8
   - Multiple conditions: WHERE status = 'approved' AND amount > 1000
   - OR predicates: WHERE status = 'declined' OR status = 'failed'
   - Collection queries: WHERE tags CONTAINS 'fraud'
   - Text search with Lucene analyzers (HCD version dependent)

5. MULTI-TABLE WRITES:
   - Write to main table + high_risk_transactions in real-time
   - Use batch statements with retry logic for consistency
   - Implement Dead Letter Queue for failed writes
   - Aggregate stats tables via periodic batch jobs (not real-time)
   - SAI indexes updated automatically on write

6. COMPACTION:
   - SizeTiered for write-heavy main table
   - TimeWindow for time-series data with TTL
   - Adjust based on actual workload patterns

7. TTL STRATEGY:
   - Longer retention for main transactions
   - Shorter TTL for aggregated stats and metrics
   - Configurable based on compliance requirements

8. CONSISTENCY:
   - Use LOCAL_QUORUM for balanced performance and consistency
   - Consider QUORUM for critical ledger operations

9. PERFORMANCE CONSIDERATIONS:
   - Batch size optimization (50-100 transactions per batch)
   - Connection pooling and prepared statements
   - Monitor partition sizes and hotspots
   - SAI provides efficient query performance across multiple indexed columns

10. AGGREGATIONS:
    - Don't update stats tables in real-time (causes bottlenecks)
    - Use periodic batch jobs to calculate aggregations
    - Alternative: Use COUNTER tables for simple increments only

11. MATERIALIZED VIEWS vs SAI:
    - With SAI, materialized views may be less necessary
    - Views still useful for frequently accessed denormalized data patterns
    - Views have write overhead (each write updates base table + all views)
    - For very high-throughput: SAI indexes + base table may be sufficient
    - Evaluate based on your specific query patterns and performance requirements

12. STORAGE REQUIREMENTS:
    - Maintain 20% free disk space for compactions and indexing
    - SAI uses less storage than traditional 2i but still adds overhead
    - Monitor disk usage as you add indexes
    - Fast local storage (NVMe) recommended for best SAI performance
*/