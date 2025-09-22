#!/usr/bin/env python3
"""
Simple test to isolate the connection issue
"""

import os
import asyncio
import logging
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def test_sync_connection():
    """Test synchronous connection to AstraDB"""
    try:
        logger.info("Testing synchronous connection...")
        
        # Get config
        secure_connect_bundle = os.getenv("ASTRA_DB_SECURE_CONNECT_BUNDLE")
        token = os.getenv("ASTRA_DB_TOKEN")
        keyspace = os.getenv("ASTRA_DB_KEYSPACE", "default")
        
        logger.info(f"Bundle: {secure_connect_bundle}")
        logger.info(f"Keyspace: {keyspace}")
        logger.info(f"Token starts with: {token[:20]}...")
        
        # Create auth provider
        auth_provider = PlainTextAuthProvider('token', token)
        
        # Create cluster
        cluster = Cluster(
            cloud={'secure_connect_bundle': secure_connect_bundle},
            auth_provider=auth_provider,
            load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy()),
            connect_timeout=10,
            control_connection_timeout=10
        )
        
        # Connect
        session = cluster.connect(keyspace)
        logger.info("Connected successfully!")
        
        # Test a simple query
        result = session.execute("SELECT release_version FROM system.local")
        for row in result:
            logger.info(f"Cassandra version: {row.release_version}")
        
        # Test table creation
        session.execute("""
            CREATE TABLE IF NOT EXISTS test_table (
                id UUID PRIMARY KEY,
                name TEXT,
                created_at TIMESTAMP
            )
        """)
        logger.info("Test table created successfully")
        
        # Cleanup
        session.execute("DROP TABLE IF EXISTS test_table")
        cluster.shutdown()
        logger.info("Connection test completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Connection test failed: {e}")
        return False

if __name__ == "__main__":
    success = test_sync_connection()
    if success:
        print("✅ Connection test passed")
    else:
        print("❌ Connection test failed")
