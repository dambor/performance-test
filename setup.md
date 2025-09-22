# AstraDB Transaction Ingest POC Setup Guide

## Prerequisites

1. **Python 3.8+** installed
2. **AstraDB Database** created (free tier available at [astra.datastax.com](https://astra.datastax.com))
3. **Secure Connect Bundle** downloaded from your AstraDB dashboard
4. **Application Token** generated in AstraDB dashboard

## Quick Start

### 1. Clone/Download Files
```bash
# Create project directory
mkdir astradb-transaction-poc
cd astradb-transaction-poc

# Copy all the provided files to this directory:
# - transaction_ingest.py
# - schema.cql  
# - requirements.txt
# - .env
# - SETUP.md (this file)
```

### 2. Install Dependencies
```bash
# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install requirements
pip install -r requirements.txt
```

### 3. Configure AstraDB Connection

#### Step 3.1: Create Database
1. Go to [astra.datastax.com](https://astra.datastax.com)
2. Create a new database:
   - **Name**: `transactions-poc`
   - **Keyspace**: `transactions`
   - **Provider**: Your preferred cloud provider
   - **Region**: Closest to your location

#### Step 3.2: Download Secure Connect Bundle
1. In your AstraDB dashboard, click on your database
2. Go to "Settings" → "Downloads"
3. Download the "Secure Connect Bundle"
4. Save it as `secure-connect-transactions.zip` in your project directory

#### Step 3.3: Generate Application Token
1. In AstraDB dashboard, go to "Settings" → "Application Tokens"
2. Click "Generate Token"
3. Select role: **Database Administrator**
4. Copy the **full token string** (starts with "AstraCS:")

#### Step 3.4: Configure Environment Variables
Edit the `.env` file and replace the placeholder values:

```bash
# Update these with your actual values
ASTRA_DB_SECURE_CONNECT_BUNDLE=./secure-connect-transactions.zip
ASTRA_DB_TOKEN=AstraCS:your-actual-token-here
ASTRA_DB_KEYSPACE=transactions
```

**Important:** Make sure to copy the complete token that starts with "AstraCS:" - this is the single authentication token that replaces the old client ID/secret approach.

### 4. Initialize Database Schema
The schema will be automatically created when you run the POC. However, you can also create it manually:

```bash
# Optional: Create schema manually using cqlsh (if you have it installed)
# cqlsh -b secure-connect-transactions.zip -u your-client-id -p your-client-secret -f schema.cql
```

### 5. Run the POC
```bash
python transaction_ingest.py
```

## Expected Output

The POC will run for 5 minutes (300 seconds) by default and output:

```
2025-09-18 10:00:00 - __main__ - INFO - Starting POC with configuration:
2025-09-18 10:00:00 - __main__ - INFO -   Target TPS: 2000
2025-09-18 10:00:00 - __main__ - INFO -   Batch Size: 50
2025-09-18 10:00:00 - __main__ - INFO -   Workers: 4
2025-09-18 10:00:00 - __main__ - INFO -   Duration: 300s
2025-09-18 10:00:00 - __main__ - INFO - Connected to AstraDB successfully
2025-09-18 10:00:05 - __main__ - INFO - Performance Update - TPS: 2150.1 (avg: 2089.3), Error Rate: 0.002%, Total: 10447, Target Met: True
```

## Configuration Options

You can customize the POC behavior by editing the `.env` file:

| Variable | Default | Description |
|----------|---------|-------------|
| `TARGET_TPS` | 2000 | Target transactions per second |
| `BATCH_SIZE` | 50 | Number of transactions per batch |
| `NUM_WORKERS` | 4 | Number of concurrent workers |
| `TEST_DURATION_SECONDS` | 300 | Test duration in seconds |
| `LOG_LEVEL` | INFO | Logging level (DEBUG, INFO, WARNING, ERROR) |

## Troubleshooting

### Common Issues

#### 1. Connection Errors
```
cassandra.cluster.NoHostAvailable: ('Unable to connect to any servers', {...})
```

**Solutions:**
- Verify your secure connect bundle path is correct
- Check that your Client ID and Client Secret are correct
- Ensure your database is running (not hibernated)
- Try downloading a fresh secure connect bundle

#### 2. Authentication Errors
```
cassandra.AuthenticationFailed: Failed to authenticate to X.X.X.X:9042: Bad credentials
```

**Solutions:**
- Regenerate your application token in AstraDB dashboard
- Verify the token in your `.env` file starts with "AstraCS:"
- Ensure you copied the complete token string
- Ensure the token has Database Administrator role

#### 3. Keyspace Not Found
```
InvalidRequest: Keyspace 'transactions' does not exist
```

**Solutions:**
- Create the keyspace in your AstraDB dashboard
- Or update `ASTRA_DB_KEYSPACE` in `.env` to match your existing keyspace

#### 4. Schema Creation Issues
```
InvalidRequest: unconfigured table transactions
```

**Solutions:**
- Ensure the keyspace exists
- Check that the schema.cql file is in the same directory
- Run with DEBUG logging to see detailed schema creation logs

#### 5. Low Performance
If you're not hitting the target TPS:

**Solutions:**
- Increase `NUM_WORKERS` (try 6-8)
- Adjust `BATCH_SIZE` (try 25-100)
- Check your network connection to AstraDB
- Monitor AstraDB dashboard for any throttling

### Performance Tuning

#### For Higher Throughput:
```bash
# In .env file
TARGET_TPS=3000
BATCH_SIZE=75
NUM_WORKERS=6
```

#### For Lower Latency:
```bash
# In .env file
BATCH_SIZE=25
NUM_WORKERS=8
```

#### For Debugging:
```bash
# In .env file
LOG_LEVEL=DEBUG
```

## Architecture Overview

### Components:
1. **TransactionGenerator**: Creates realistic transaction data
2. **AstraDBClient**: Handles database connections and operations
3. **PerformanceMonitor**: Tracks TPS, error rates, and latency
4. **TransactionIngestPOC**: Orchestrates the entire test

### Database Schema:
- **Primary Table**: `transactions` - Main transaction storage
- **Indexes**: Optimized for time-based, merchant, and status queries
- **Analytics Tables**: Daily/hourly aggregates for reporting
- **Fraud Tables**: High-risk transaction tracking
- **Ledger Table**: Card balance tracking with ACID properties

### Performance Optimizations:
- Prepared statements for faster execution
- Batch processing to reduce network overhead
- Connection pooling and compression
- Optimal consistency levels (LOCAL_QUORUM)
- Time-series optimized compaction strategies

## Next Steps

After successful POC completion:

1. **Scale Testing**: Increase workers and TPS targets
2. **Add Monitoring**: Integrate with monitoring tools
3. **Implement Fraud Detection**: Add real-time fraud scoring
4. **Add Reporting**: Create analytical queries and dashboards
5. **Production Hardening**: Add retry logic, circuit breakers, health checks

## Support

- **AstraDB Documentation**: https://docs.datastax.com/en/astra/
- **Cassandra Python Driver**: https://docs.datastax.com/en/developer/python-driver/
- **DataStax Community**: https://community.datastax.com/

## Security Notes

⚠️ **Important Security Practices:**

1. **Never commit `.env` file** - Add it to `.gitignore`
2. **Use environment-specific credentials** - Different tokens for dev/prod
3. **Rotate tokens regularly** - Generate new tokens periodically
4. **Use least-privilege access** - Only grant necessary permissions
5. **Secure the connect bundle** - Treat it as a password

```bash
# Add to .gitignore
echo ".env" >> .gitignore
echo "secure-connect-*.zip" >> .gitignore
```