"""
Transaction Data Ingest POC for AstraDB/HCD
High-scale card transaction ingestion system
Target: 2000 TPS with <0.01% error rate
"""

import asyncio
import json
import logging
import os
import time
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from decimal import Decimal
from typing import List, Dict, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor
import statistics

import aiohttp
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from cassandra.query import PreparedStatement
from cassandra import ConsistencyLevel
from dotenv import load_dotenv
import random

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Optional: Try to import pydantic for enhanced validation, but don't require it
try:
    from pydantic import BaseModel, Field
    PYDANTIC_AVAILABLE = True
    logger.info("Pydantic available for enhanced validation")
except ImportError:
    PYDANTIC_AVAILABLE = False
    logger.info("Pydantic not available, using basic dataclass validation")

@dataclass
class Transaction:
    """Card transaction data model"""
    transaction_id: str
    card_number: str
    merchant_id: str
    merchant_name: str
    amount: Decimal
    currency: str
    transaction_type: str  # purchase, refund, authorization
    pos_terminal_id: Optional[str]
    gateway_id: str
    timestamp: datetime
    location_country: str
    location_city: str
    location_lat: Optional[float]
    location_lng: Optional[float]
    status: str  # pending, approved, declined
    risk_score: float
    mcc_code: str  # Merchant Category Code
    auth_code: Optional[str]
    processing_fee: Decimal
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for database insertion"""
        data = asdict(self)
        # Convert Decimal to float for JSON serialization
        data['amount'] = float(self.amount)
        data['processing_fee'] = float(self.processing_fee)
        # Convert datetime to ISO string
        data['timestamp'] = self.timestamp.isoformat()
        return data

class TransactionGenerator:
    """Generate realistic transaction data for testing"""
    
    def __init__(self):
        self.merchants = [
            ("AMAZON.COM", "5399", "US", "Seattle"),
            ("WALMART", "5411", "US", "Bentonville"),
            ("MCDONALDS", "5814", "US", "Chicago"),
            ("SHELL STATION", "5541", "US", "Houston"),
            ("TARGET", "5310", "US", "Minneapolis"),
            ("STARBUCKS", "5814", "US", "Seattle"),
            ("UBER", "4121", "US", "San Francisco"),
            ("NETFLIX", "7841", "US", "Los Gatos"),
        ]
        
        self.currencies = ["USD", "EUR", "GBP", "CAD", "AUD"]
        self.gateways = ["VISA_GATEWAY", "MASTERCARD_GATEWAY", "AMEX_GATEWAY"]
        
    def generate_transaction(self) -> Transaction:
        """Generate a single realistic transaction"""
        merchant_name, mcc, country, city = random.choice(self.merchants)
        
        return Transaction(
            transaction_id=str(uuid.uuid4()),
            card_number=f"****-****-****-{random.randint(1000, 9999)}",
            merchant_id=f"MER_{random.randint(100000, 999999)}",
            merchant_name=merchant_name,
            amount=Decimal(str(random.uniform(5.99, 999.99))).quantize(Decimal('0.01')),
            currency=random.choice(self.currencies),
            transaction_type=random.choice(["purchase", "refund", "authorization"]),
            pos_terminal_id=f"POS_{random.randint(1000, 9999)}" if random.random() > 0.3 else None,
            gateway_id=random.choice(self.gateways),
            timestamp=datetime.now(timezone.utc),
            location_country=country,
            location_city=city,
            location_lat=random.uniform(25.0, 50.0) if random.random() > 0.1 else None,
            location_lng=random.uniform(-125.0, -70.0) if random.random() > 0.1 else None,
            status=random.choice(["approved"] * 90 + ["declined"] * 8 + ["pending"] * 2),
            risk_score=random.uniform(0.0, 1.0),
            mcc_code=mcc,
            auth_code=f"AUTH_{random.randint(100000, 999999)}" if random.random() > 0.1 else None,
            processing_fee=Decimal(str(random.uniform(0.10, 2.50))).quantize(Decimal('0.01'))
        )

class DatabaseClient:
    """Database client supporting both AstraDB and local HCD"""
    
    def __init__(self, **config):
        self.config = config
        self.session = None
        self.prepared_statements = {}
        
    def connect(self):
        """Establish connection to database"""
        try:
            if self.config.get("connection_type") == "local_hcd":
                self._connect_to_hcd()
            else:
                self._connect_to_astradb()
            
            # Configure session for maximum throughput
            self.session.default_consistency_level = ConsistencyLevel.LOCAL_QUORUM
            self.session.default_timeout = 5
            
            self.create_schema()
            self.prepare_statements()
            
            logger.info(f"Connected to {self.config.get('connection_type', 'AstraDB')} successfully")
            
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def _connect_to_hcd(self):
        """Connect to local HCD instance"""
        auth_provider = PlainTextAuthProvider(
            username=self.config["username"],
            password=self.config["password"]
        )
        
        cluster = Cluster(
            contact_points=self.config["contact_points"],
            port=self.config["port"],
            auth_provider=auth_provider,
            load_balancing_policy=DCAwareRoundRobinPolicy(local_dc=self.config["datacenter"]),
            compression=True,
            protocol_version=4,
            control_connection_timeout=10,
            connect_timeout=10,
            executor_threads=16,
            max_schema_agreement_wait=0,
            no_compact=True
        )
        
        self.session = cluster.connect(self.config["keyspace"])
    
    def _connect_to_astradb(self):
        """Connect to AstraDB instance"""
        auth_provider = PlainTextAuthProvider('token', self.config["token"])
        
        cluster = Cluster(
            cloud={
                'secure_connect_bundle': self.config["secure_connect_bundle"]
            },
            auth_provider=auth_provider,
            load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy()),
            compression=True,
            protocol_version=4,
            control_connection_timeout=10,
            connect_timeout=10,
            executor_threads=16,
            max_schema_agreement_wait=0,
            no_compact=True
        )
        
        self.session = cluster.connect(self.config["keyspace"])
    
    def create_schema(self):
        """Create the transactions table with optimized schema"""
        # Read schema from external file
        schema_file = os.path.join(os.path.dirname(__file__), 'schema.cql')
        
        try:
            with open(schema_file, 'r') as f:
                schema_content = f.read()
            
            # Split by semicolon and execute each statement
            statements = [stmt.strip() for stmt in schema_content.split(';') if stmt.strip() and not stmt.strip().startswith('--')]
            
            for statement in statements:
                # Skip comments and empty lines
                if statement.startswith('/*') or statement.startswith('--') or not statement:
                    continue
                
                try:
                    self.session.execute(statement)
                    logger.debug(f"Executed: {statement[:50]}...")
                except Exception as e:
                    # Log but don't fail on IF NOT EXISTS statements
                    if "already exists" in str(e).lower():
                        logger.debug(f"Schema object already exists: {e}")
                    else:
                        logger.warning(f"Schema execution warning: {e}")
            
            logger.info("Database schema created/verified successfully")
            
        except FileNotFoundError:
            logger.warning("schema.cql not found, using inline schema creation")
            self._create_basic_schema()
        except Exception as e:
            logger.error(f"Failed to create schema: {e}")
            raise
    
    def _create_basic_schema(self):
        """Fallback basic schema creation if schema.cql is not found"""
        create_table_cql = """
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id UUID PRIMARY KEY,
            card_number TEXT,
            merchant_id TEXT,
            merchant_name TEXT,
            amount DECIMAL,
            currency TEXT,
            transaction_type TEXT,
            pos_terminal_id TEXT,
            gateway_id TEXT,
            timestamp TIMESTAMP,
            location_country TEXT,
            location_city TEXT,
            location_lat FLOAT,
            location_lng FLOAT,
            status TEXT,
            risk_score FLOAT,
            mcc_code TEXT,
            auth_code TEXT,
            processing_fee DECIMAL,
            created_at TIMESTAMP
        ) WITH CLUSTERING ORDER BY (transaction_id ASC)
        AND compaction = {
            'class': 'SizeTieredCompactionStrategy',
            'max_threshold': 32,
            'min_threshold': 4
        }
        AND compression = {
            'chunk_length_in_kb': 64,
            'class': 'LZ4Compressor'
        }
        """
        
        # Create basic indexes
        create_indexes = [
            "CREATE INDEX IF NOT EXISTS idx_transactions_timestamp ON transactions (timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_transactions_merchant ON transactions (merchant_id)",
            "CREATE INDEX IF NOT EXISTS idx_transactions_status ON transactions (status)",
            "CREATE INDEX IF NOT EXISTS idx_transactions_gateway ON transactions (gateway_id)"
        ]
        
        self.session.execute(create_table_cql)
        logger.info("Basic transactions table created successfully")
        
        for index_cql in create_indexes:
            self.session.execute(index_cql)
        logger.info("Basic database indexes created successfully")
    
    def prepare_statements(self):
        """Prepare CQL statements for better performance"""
        insert_cql = """
        INSERT INTO transactions (
            transaction_id, card_number, merchant_id, merchant_name, amount,
            currency, transaction_type, pos_terminal_id, gateway_id, timestamp,
            location_country, location_city, location_lat, location_lng, status,
            risk_score, mcc_code, auth_code, processing_fee, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        self.prepared_statements['insert_transaction'] = self.session.prepare(insert_cql)
        logger.info("Prepared statements created successfully")
    
    def insert_transaction(self, transaction: Transaction) -> bool:
        """Insert a single transaction"""
        try:
            bound_stmt = self.prepared_statements['insert_transaction'].bind([
                uuid.UUID(transaction.transaction_id),
                transaction.card_number,
                transaction.merchant_id,
                transaction.merchant_name,
                transaction.amount,
                transaction.currency,
                transaction.transaction_type,
                transaction.pos_terminal_id,
                transaction.gateway_id,
                transaction.timestamp,
                transaction.location_country,
                transaction.location_city,
                transaction.location_lat,
                transaction.location_lng,
                transaction.status,
                transaction.risk_score,
                transaction.mcc_code,
                transaction.auth_code,
                transaction.processing_fee,
                datetime.now(timezone.utc)
            ])
            
            self.session.execute(bound_stmt)
            return True
            
        except Exception as e:
            logger.error(f"Failed to insert transaction {transaction.transaction_id}: {e}")
            return False
    
    def batch_insert_transactions(self, transactions: List[Transaction]) -> Tuple[int, int]:
        """Insert multiple transactions using individual async writes for better performance"""
        success_count = 0
        error_count = 0
        
        for transaction in transactions:
            try:
                bound_stmt = self.prepared_statements['insert_transaction'].bind([
                    uuid.UUID(transaction.transaction_id),
                    transaction.card_number,
                    transaction.merchant_id,
                    transaction.merchant_name,
                    transaction.amount,
                    transaction.currency,
                    transaction.transaction_type,
                    transaction.pos_terminal_id,
                    transaction.gateway_id,
                    transaction.timestamp,
                    transaction.location_country,
                    transaction.location_city,
                    transaction.location_lat,
                    transaction.location_lng,
                    transaction.status,
                    transaction.risk_score,
                    transaction.mcc_code,
                    transaction.auth_code,
                    transaction.processing_fee,
                    datetime.now(timezone.utc)
                ])
                
                self.session.execute(bound_stmt)
                success_count += 1
                
            except Exception as e:
                logger.error(f"Failed to insert transaction {transaction.transaction_id}: {e}")
                error_count += 1
        
        return success_count, error_count
    
    def close(self):
        """Close the database connection"""
        if self.session:
            self.session.cluster.shutdown()
            logger.info("Database connection closed")

class PerformanceMonitor:
    """Monitor and report performance metrics"""
    
    def __init__(self, target_tps=2000):
        self.target_tps = target_tps
        self.reset_metrics()
    
    def reset_metrics(self):
        """Reset all performance metrics"""
        self.start_time = time.time()
        self.total_transactions = 0
        self.successful_transactions = 0
        self.failed_transactions = 0
        self.batch_latencies = []
        self.current_tps = 0
    
    def record_batch(self, batch_size: int, success_count: int, error_count: int, batch_time: float):
        """Record metrics for a batch of transactions"""
        self.total_transactions += batch_size
        self.successful_transactions += success_count
        self.failed_transactions += error_count
        self.batch_latencies.append(batch_time)
        
        # Calculate current TPS over the last second
        current_time = time.time()
        elapsed = current_time - self.start_time
        if elapsed > 0:
            self.current_tps = self.successful_transactions / elapsed
    
    def get_current_stats(self) -> Dict:
        """Get current performance statistics"""
        elapsed_time = time.time() - self.start_time
        
        if elapsed_time == 0:
            return {"status": "initializing"}
        
        avg_tps = self.successful_transactions / elapsed_time
        error_rate = (self.failed_transactions / max(self.total_transactions, 1)) * 100
        
        latency_stats = {}
        if self.batch_latencies:
            latency_stats = {
                "avg_batch_latency": statistics.mean(self.batch_latencies),
                "p95_batch_latency": statistics.quantiles(self.batch_latencies, n=20)[18] if len(self.batch_latencies) >= 20 else max(self.batch_latencies),
                "min_batch_latency": min(self.batch_latencies),
                "max_batch_latency": max(self.batch_latencies)
            }
        
        return {
            "elapsed_time": elapsed_time,
            "total_transactions": self.total_transactions,
            "successful_transactions": self.successful_transactions,
            "failed_transactions": self.failed_transactions,
            "current_tps": self.current_tps,
            "average_tps": avg_tps,
            "error_rate": error_rate,
            "target_met": avg_tps >= self.target_tps and error_rate < 0.01,
            **latency_stats
        }

class TransactionIngestPOC:
    """Main POC class for transaction data ingestion"""
    
    def __init__(self, db_config: Dict, target_tps: int = 2000, batch_size: int = 50):
        self.db_config = db_config
        self.target_tps = target_tps
        self.batch_size = batch_size
        self.db_client = DatabaseClient(**db_config)
        self.transaction_generator = TransactionGenerator()
        self.performance_monitor = PerformanceMonitor(target_tps)
        self.running = False
    
    def initialize(self):
        """Initialize the POC"""
        logger.info("Initializing Transaction Ingest POC...")
        self.db_client.connect()
        logger.info("POC initialized successfully")
    
    async def run_ingest_worker(self, worker_id: int):
        """Individual worker for transaction ingestion"""
        logger.info(f"Starting ingest worker {worker_id}")
        
        while self.running:
            try:
                # Generate batch of transactions
                batch_start = time.time()
                transactions = [
                    self.transaction_generator.generate_transaction()
                    for _ in range(self.batch_size)
                ]
                
                # Insert batch directly (already concurrent)
                success_count, error_count = self.db_client.batch_insert_transactions(transactions)
                batch_time = time.time() - batch_start
                
                # Record performance metrics
                self.performance_monitor.record_batch(
                    self.batch_size, success_count, error_count, batch_time
                )
                
                # Minimal sleep to prevent CPU spinning
                await asyncio.sleep(0.001)
                
            except Exception as e:
                logger.error(f"Worker {worker_id} error: {e}")
                await asyncio.sleep(0.1)
    
    async def run_performance_reporter(self):
        """Report performance metrics periodically"""
        while self.running:
            stats = self.performance_monitor.get_current_stats()
            
            if "elapsed_time" in stats:
                logger.info(
                    f"Performance Update - "
                    f"TPS: {stats['current_tps']:.1f} "
                    f"(avg: {stats['average_tps']:.1f}), "
                    f"Error Rate: {stats['error_rate']:.3f}%, "
                    f"Total: {stats['total_transactions']}, "
                    f"Target Met: {stats['target_met']}"
                )
            
            await asyncio.sleep(5)  # Report every 5 seconds
    
    async def run_test(self, duration_seconds: int = 300, num_workers: int = 4):
        """Run the ingestion test"""
        logger.info(f"Starting {duration_seconds}s ingestion test with {num_workers} workers")
        logger.info(f"Target: {self.target_tps} TPS, Batch size: {self.batch_size}")
        
        self.running = True
        self.performance_monitor.reset_metrics()
        
        # Start worker tasks
        workers = [
            asyncio.create_task(self.run_ingest_worker(i))
            for i in range(num_workers)
        ]
        
        # Start performance reporter
        reporter = asyncio.create_task(self.run_performance_reporter())
        
        try:
            # Run for specified duration
            await asyncio.sleep(duration_seconds)
            
        finally:
            # Cleanup
            self.running = False
            
            # Wait for workers to finish
            for worker in workers:
                worker.cancel()
            reporter.cancel()
            
            await asyncio.gather(*workers, reporter, return_exceptions=True)
        
        # Final performance report
        final_stats = self.performance_monitor.get_current_stats()
        logger.info("=== FINAL PERFORMANCE REPORT ===")
        for key, value in final_stats.items():
            logger.info(f"{key}: {value}")
        
        return final_stats
    
    def cleanup(self):
        """Cleanup resources"""
        self.db_client.close()
        logger.info("POC cleanup completed")

# Example configuration and usage
def load_config_from_env() -> Dict:
    """Load configuration from environment variables"""
    
    # Check if using local HCD or AstraDB
    use_local_hcd = os.getenv("USE_LOCAL_HCD", "false").lower() == "true"
    
    if use_local_hcd:
        return {
            "connection_type": "local_hcd",
            "contact_points": os.getenv("HCD_CONTACT_POINTS", "127.0.0.1").split(","),
            "port": int(os.getenv("HCD_PORT", "9042")),
            "username": os.getenv("HCD_USERNAME", "cassandra"),
            "password": os.getenv("HCD_PASSWORD", "cassandra"),
            "keyspace": os.getenv("HCD_KEYSPACE", "transactions"),
            "datacenter": os.getenv("HCD_DATACENTER", "datacenter1")
        }
    else:
        return {
            "connection_type": "astradb",
            "secure_connect_bundle": os.getenv("ASTRA_DB_SECURE_CONNECT_BUNDLE"),
            "token": os.getenv("ASTRA_DB_TOKEN"),
            "keyspace": os.getenv("ASTRA_DB_KEYSPACE", "transactions")
        }

async def main():
    """Main function to run the POC"""
    
    # Load configuration from environment
    db_config = load_config_from_env()
    
    # Validate configuration based on connection type
    if db_config.get("connection_type") == "local_hcd":
        required_configs = ["contact_points", "username", "password", "keyspace"]
        missing_configs = [key for key in required_configs if not db_config.get(key)]
        
        if missing_configs:
            logger.error(f"Missing required HCD environment variables: {missing_configs}")
            logger.error("Set USE_LOCAL_HCD=true and configure: HCD_CONTACT_POINTS, HCD_USERNAME, HCD_PASSWORD, HCD_KEYSPACE")
            return
        
        logger.info("Using local HCD configuration")
        logger.info(f"Contact Points: {db_config['contact_points']}")
        logger.info(f"Keyspace: {db_config['keyspace']}")
        
    else:
        required_configs = ["secure_connect_bundle", "token", "keyspace"]
        missing_configs = [key for key in required_configs if not db_config.get(key)]
        
        if missing_configs:
            logger.error(f"Missing required AstraDB environment variables: {missing_configs}")
            logger.error("Please check your .env file and ensure all required variables are set")
            logger.error("Required variables: ASTRA_DB_SECURE_CONNECT_BUNDLE, ASTRA_DB_TOKEN, ASTRA_DB_KEYSPACE")
            return
        
        # Validate token format
        if not db_config["token"].startswith("AstraCS:"):
            logger.error("Invalid token format. AstraDB tokens should start with 'AstraCS:'")
            logger.error("Please check your ASTRA_DB_TOKEN in the .env file")
            return
        
        logger.info("Using AstraDB configuration")
        logger.info(f"Keyspace: {db_config['keyspace']}")
    
    # Load POC configuration from environment
    target_tps = int(os.getenv("TARGET_TPS", 2000))
    batch_size = int(os.getenv("BATCH_SIZE", 50))
    num_workers = int(os.getenv("NUM_WORKERS", 4))
    test_duration = int(os.getenv("TEST_DURATION_SECONDS", 300))
    
    logger.info(f"Starting POC with configuration:")
    logger.info(f"  Target TPS: {target_tps}")
    logger.info(f"  Batch Size: {batch_size}")
    logger.info(f"  Workers: {num_workers}")
    logger.info(f"  Duration: {test_duration}s")
    logger.info(f"  Authentication: {'HCD' if db_config.get('connection_type') == 'local_hcd' else 'Token-based'}")
    
    # Initialize POC
    poc = TransactionIngestPOC(
        db_config=db_config,
        target_tps=target_tps,
        batch_size=batch_size
    )
    
    try:
        # Initialize
        poc.initialize()
        
        # Run test
        results = await poc.run_test(
            duration_seconds=test_duration, 
            num_workers=num_workers
        )
        
        # Check if targets were met
        if results["target_met"]:
            print("✅ SUCCESS: Target metrics achieved!")
            print(f"   Average TPS: {results['average_tps']:.1f} (target: {target_tps})")
            print(f"   Error Rate: {results['error_rate']:.3f}% (target: <0.01%)")
        else:
            print("❌ TARGETS NOT MET")
            print(f"   Average TPS: {results['average_tps']:.1f} (target: {target_tps})")
            print(f"   Error Rate: {results['error_rate']:.3f}% (target: <0.01%)")
        
        # Additional metrics
        print(f"\n📊 Detailed Results:")
        print(f"   Total Transactions: {results['total_transactions']:,}")
        print(f"   Successful: {results['successful_transactions']:,}")
        print(f"   Failed: {results['failed_transactions']:,}")
        print(f"   Duration: {results['elapsed_time']:.1f}s")
        
        if 'avg_batch_latency' in results:
            print(f"   Avg Batch Latency: {results['avg_batch_latency']*1000:.1f}ms")
            print(f"   P95 Batch Latency: {results['p95_batch_latency']*1000:.1f}ms")
        
    except Exception as e:
        logger.error(f"POC failed: {e}")
        raise
    
    finally:
        poc.cleanup()

if __name__ == "__main__":
    # Run the POC
    asyncio.run(main())