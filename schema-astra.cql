-- AstraDB Schema for Transaction Data Ingest POC
-- This file contains all CQL statements needed to set up the database schema

-- =====================================================================
-- KEYSPACE CREATION (Optional - can be created via AstraDB dashboard)
-- =====================================================================

-- Note: Keyspaces are typically created through the AstraDB dashboard
-- If you need to create programmatically, use DataStax DevOps API

-- =====================================================================
-- MAIN TRANSACTIONS TABLE
-- =====================================================================

CREATE TABLE IF NOT EXISTS transactions (
    -- Primary Key
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
    -- Optimize for write-heavy workloads
    CLUSTERING ORDER BY (transaction_id ASC)
    AND compaction = {
        'class': 'SizeTieredCompactionStrategy',
        'max_threshold': 32,
        'min_threshold': 4
    }
    AND compression = {
        'chunk_length_in_kb': 64,
        'class': 'LZ4Compressor'
    }
    AND gc_grace_seconds = 864000  -- 10 days
    AND bloom_filter_fp_chance = 0.01;

-- =====================================================================
-- SECONDARY INDEXES FOR QUERY PATTERNS
-- =====================================================================

-- Index for time-based queries (fraud monitoring, reporting)
CREATE INDEX IF NOT EXISTS idx_transactions_timestamp 
ON transactions (timestamp);

-- Index for merchant-based queries
CREATE INDEX IF NOT EXISTS idx_transactions_merchant 
ON transactions (merchant_id);

-- Index for status-based queries (monitoring failed transactions)
CREATE INDEX IF NOT EXISTS idx_transactions_status 
ON transactions (status);

-- Index for gateway-based analysis
CREATE INDEX IF NOT EXISTS idx_transactions_gateway 
ON transactions (gateway_id);

-- Index for currency-based queries
CREATE INDEX IF NOT EXISTS idx_transactions_currency 
ON transactions (currency);

-- Index for risk-based queries (fraud detection)
CREATE INDEX IF NOT EXISTS idx_transactions_risk_score 
ON transactions (risk_score);

-- Index for location-based queries
CREATE INDEX IF NOT EXISTS idx_transactions_country 
ON transactions (location_country);

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

-- =====================================================================
-- FRAUD DETECTION AND MONITORING TABLES
-- =====================================================================

-- High-risk transactions for fraud review
CREATE TABLE IF NOT EXISTS high_risk_transactions (
    risk_bucket TEXT,  -- 'critical', 'high', 'medium'
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

-- Card balance and transaction ledger
CREATE TABLE IF NOT EXISTS card_ledger (
    card_number TEXT,
    ledger_date DATE,
    transaction_id UUID,
    transaction_type TEXT,
    amount DECIMAL,
    running_balance DECIMAL,
    timestamp TIMESTAMP,
    reference_transaction_id UUID,
    description TEXT,
    PRIMARY KEY ((card_number, ledger_date), timestamp, transaction_id)
) WITH 
    CLUSTERING ORDER BY (timestamp DESC, transaction_id ASC)
    AND compaction = {
        'class': 'TimeWindowCompactionStrategy',
        'compaction_window_unit': 'DAYS',
        'compaction_window_size': 30
    };

-- =====================================================================
-- MATERIALIZED VIEWS FOR OPTIMIZED QUERIES
-- =====================================================================

-- Transactions by merchant for analytics
CREATE MATERIALIZED VIEW IF NOT EXISTS transactions_by_merchant AS
    SELECT transaction_id, merchant_id, merchant_name, timestamp, amount, currency, status
    FROM transactions
    WHERE transaction_id IS NOT NULL 
    AND merchant_id IS NOT NULL 
    AND timestamp IS NOT NULL
    PRIMARY KEY ((merchant_id), timestamp, transaction_id)
    WITH CLUSTERING ORDER BY (timestamp DESC, transaction_id ASC);

-- Recent transactions by status for monitoring
CREATE MATERIALIZED VIEW IF NOT EXISTS transactions_by_status_time AS
    SELECT transaction_id, status, timestamp, amount, merchant_id, risk_score
    FROM transactions
    WHERE transaction_id IS NOT NULL 
    AND status IS NOT NULL 
    AND timestamp IS NOT NULL
    PRIMARY KEY ((status), timestamp, transaction_id)
    WITH CLUSTERING ORDER BY (timestamp DESC, transaction_id ASC);

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
-- COMMENTS AND DOCUMENTATION
-- =====================================================================

/*
Schema Design Notes:

1. PRIMARY KEY DESIGN:
   - Main transactions table uses UUID as partition key for even distribution
   - Time-series tables use date/time as partition key for temporal queries
   - Materialized views provide alternative access patterns

2. INDEXING STRATEGY:
   - Secondary indexes on frequently queried fields
   - Avoid over-indexing to maintain write performance
   - Consider query patterns when adding new indexes

3. COMPACTION:
   - SizeTiered for write-heavy main table
   - TimeWindow for time-series data with TTL
   - Adjust based on actual workload patterns

4. TTL STRATEGY:
   - Longer retention for main transactions
   - Shorter TTL for aggregated stats and metrics
   - Configurable based on compliance requirements

5. CONSISTENCY:
   - Use LOCAL_QUORUM for balanced performance and consistency
   - Consider QUORUM for critical ledger operations

6. PERFORMANCE CONSIDERATIONS:
   - Batch size optimization (50-100 transactions per batch)
   - Connection pooling and prepared statements
   - Monitor partition sizes and hotspots
*/