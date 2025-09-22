# Cassandra POC Test Suite

This repository contains three performance tests for Cassandra/HCD:
1. **Atomicity Tests** - Validates ACID guarantees for ledger operations
2. **Transaction Ingest** - High-scale transaction ingestion (2000+ TPS)  
3. **Read Query Performance** - Optimized read queries (≤50ms p95 latency)

## Prerequisites

### 1. Install Java 11
```bash
# macOS
brew install openjdk@11

# Ubuntu/Debian
sudo apt update
sudo apt install openjdk-11-jdk

# Verify installation
java -version
```

### 2. Install Conda/Miniconda
```bash
# Download and install Miniconda
curl -O https://repo.anaconda.com/miniconda/Miniconda3-latest-MacOSX-x86_64.sh
bash Miniconda3-latest-MacOSX-x86_64.sh
```

### 3. Create Python Environment
```bash
# Create conda environment with Python 3.11
conda create -n cassandra-poc python=3.11 -y
conda activate cassandra-poc

# Install required packages
pip install cassandra-driver python-dotenv
```

## HCD Installation

### 1. Download HCD
```bash
# Create installation directory
mkdir -p ~/hcd
cd ~/hcd

# Download HCD tarball (replace with latest version)
curl -O https://downloads.datastax.com/hcd/hcd-1.2.0-bin.tar.gz

# Extract
tar -xzf hcd-1.2.0-bin.tar.gz
cd hcd-1.2.0
```

### 2. Configure HCD
```bash
# Edit cassandra.yaml
vim conf/cassandra.yaml

# Key settings to verify/modify:
# cluster_name: 'Test Cluster'
# listen_address: localhost
# rpc_address: localhost
# seed_provider: 127.0.0.1
# authenticator: AllowAllAuthenticator (for testing)
```

### 3. Start HCD
```bash
# Start Cassandra
bin/cassandra -f

# In another terminal, verify it's running
bin/cqlsh
```

### 4. Create Schema
```bash
# Run the schema file to create keyspace and all tables
bin/cqlsh -f /path/to/your/schema.cql

# Or if the schema.cql is in your project directory:
bin/cqlsh -f ~/your-project/schema.cql

# Verify keyspace and tables were created
bin/cqlsh -e "DESCRIBE KEYSPACES;"
bin/cqlsh -k transactions -e "DESCRIBE TABLES;"
```

## Environment Configuration

Create a `.env` file in the project directory:

```bash
# HCD Configuration
USE_LOCAL_HCD=true
HCD_CONTACT_POINTS=127.0.0.1
HCD_PORT=9042
HCD_USERNAME=cassandra
HCD_PASSWORD=cassandra
HCD_KEYSPACE=transactions
HCD_DATACENTER=datacenter1

# Test Configuration
TARGET_TPS=2000
BATCH_SIZE=50
NUM_WORKERS=4
TEST_DURATION_SECONDS=300
READ_TEST_DURATION=180
```

## Running the Tests

### 1. Atomicity Tests
Tests ACID guarantees for financial ledger operations.

```bash
conda activate cassandra-poc
python atomicity_tests_poc.py
```

**Expected Results:**
- 100,000 single entry operations
- Zero duplicates/missing records
- Perfect balance consistency
- All atomicity guarantees validated

### 2. Transaction Ingest
High-throughput transaction ingestion test.

```bash
python transaction_injest.py
```

**Expected Results:**
- 2000+ TPS sustained throughput
- <0.01% error rate
- Concurrent worker performance
- Zero data loss

### 3. Read Query Performance
Optimized read query performance test.

```bash
python read_query_performance.py
```

**Expected Results:**
- 100,000 queries executed
- ≤50ms p95 latency (typically <5ms)
- 100% success rate
- 600+ QPS sustained

## Test Results Interpretation

### Success Criteria
- **Atomicity**: ✅ All operations atomic, zero inconsistencies
- **Ingest**: ✅ Target TPS achieved, error rate <0.01%
- **Read Performance**: ✅ P95 latency ≤50ms, 100% success rate

### Troubleshooting

**Connection Issues:**
```bash
# Check HCD is running
ps aux | grep cassandra

# Check port is open
netstat -an | grep 9042

# Restart HCD if needed
pkill -f cassandra
bin/cassandra -f
```

**Performance Issues:**
```bash
# Check system resources
top
df -h

# Increase JVM heap (in cassandra-env.sh)
MAX_HEAP_SIZE="2G"
HEAP_NEWSIZE="400M"
```

**Schema Issues:**
```sql
-- Drop and recreate keyspace if needed
DROP KEYSPACE transactions;
CREATE KEYSPACE transactions 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
```

## Architecture Notes

- **Atomicity**: Uses lightweight transactions (LWT) and sequence counters
- **Ingest**: Optimized batch processing with concurrent workers  
- **Reads**: PRIMARY KEY lookups with LOCAL_ONE consistency
- **Schema**: Time-partitioned tables for optimal performance

## Hardware Requirements

**Minimum:**
- 4 CPU cores
- 8GB RAM
- 20GB disk space

**Recommended:**
- 8+ CPU cores
- 16GB+ RAM
- SSD storage
