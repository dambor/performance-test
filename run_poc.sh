#!/bin/bash

# AstraDB Transaction Ingest POC Runner
# This script sets up and runs the transaction ingest POC

set -e  # Exit on any error

echo "🚀 AstraDB Transaction Ingest POC Runner"
echo "========================================="

# Check if .env file exists
if [ ! -f ".env" ]; then
    echo "❌ Error: .env file not found!"
    echo "Please copy .env.template to .env and configure your AstraDB credentials"
    exit 1
fi

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "📦 Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "🔧 Activating virtual environment..."
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" ]]; then
    source venv/Scripts/activate
else
    source venv/bin/activate
fi

# Install requirements if needed
echo "📋 Installing requirements..."
pip install -q -r requirements.txt

# Check if schema file exists
if [ ! -f "schema.cql" ]; then
    echo "⚠️  Warning: schema.cql file not found. Will use inline schema creation."
fi

# Load environment variables and validate
echo "🔍 Validating configuration..."
source .env

required_vars=("ASTRA_DB_TOKEN" "ASTRA_DB_SECURE_CONNECT_BUNDLE" "ASTRA_DB_KEYSPACE")
missing_vars=()

for var in "${required_vars[@]}"; do
    if [ -z "${!var}" ]; then
        missing_vars+=("$var")
    fi
done

if [ ${#missing_vars[@]} -ne 0 ]; then
    echo "❌ Error: Missing required environment variables:"
    printf '%s\n' "${missing_vars[@]}"
    echo "Please check your .env file configuration"
    exit 1
fi

# Validate token format
if [[ ! $ASTRA_DB_TOKEN == AstraCS:* ]]; then
    echo "❌ Error: Invalid token format"
    echo "AstraDB tokens should start with 'AstraCS:'"
    echo "Please check your ASTRA_DB_TOKEN in the .env file"
    exit 1
fi

# Check if secure connect bundle exists
if [ ! -f "$ASTRA_DB_SECURE_CONNECT_BUNDLE" ]; then
    echo "❌ Error: Secure connect bundle not found: $ASTRA_DB_SECURE_CONNECT_BUNDLE"
    echo "Please download your secure connect bundle from AstraDB dashboard"
    exit 1
fi

echo "✅ Configuration validated"
echo ""
echo "Configuration Summary:"
echo "  Target TPS: ${TARGET_TPS:-2000}"
echo "  Batch Size: ${BATCH_SIZE:-50}"
echo "  Workers: ${NUM_WORKERS:-4}"
echo "  Duration: ${TEST_DURATION_SECONDS:-300}s"
echo "  Keyspace: $ASTRA_DB_KEYSPACE"
echo ""

# Prompt for confirmation
read -p "🎯 Start the POC test? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Test cancelled"
    exit 0
fi

echo "🏁 Starting POC test..."
echo "----------------------------------------"

# Run the POC
python transaction_ingest.py

echo ""
echo "✅ POC test completed!"
echo ""
echo "📊 Next Steps:"
echo "  1. Review the performance results above"
echo "  2. Check your AstraDB dashboard for data ingestion"
echo "  3. Adjust configuration in .env for different test scenarios"
echo "  4. Scale up workers and TPS for higher throughput testing"