"""
Transaction Data Ingest POC for AstraDB/HCD - Multi-Table Version
High-scale card transaction ingestion system with multiple table writes
Target: 2000 TPS with <0.01% error rate across all tables
"""

import asyncio
import json
import logging
import os
import time
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, date
from decimal import Decimal
from typing import List, Dict, Optional, Tuple
from collections import deque
import statistics

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from cassandra.query import PreparedStatement, BatchStatement, BatchType
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

@dataclass
class Transaction:
    """Card transaction data model"""
    transaction_id: str
    card_number: str
    merchant_id: str
    merchant_name: str
    amount: Decimal
    currency: str
    transaction_type: str
    pos_terminal_id: Optional[str]
    gateway_id: str
    timestamp: datetime
    location_country: str
    location_city: str
    location_lat: Optional[float]
    location_lng: Optional[float]
    status: str
    risk_score: float
    mcc_code: str
    auth_code: Optional[str]
    processing_fee: Decimal
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for database insertion"""
        data = asdict(self)
        data['amount'] = float(self.amount)
        data['processing_fee'] = float(self.processing_fee)
        data['timestamp'] = self.timestamp.isoformat()
        return data
    
    def get_risk_bucket(self) -> str:
        """Determine risk bucket based on risk score"""
        if self.risk_score >= 0.8:
            return 'critical'
        elif self.risk_score >= 0.6:
            return 'high'
        elif self.risk_score >= 0.4:
            return 'medium'
        return 'low'

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

class DeadLetterQueue:
    """Simple in-memory Dead Letter Queue for failed transactions"""
    
    def __init__(self, max_size: int = 10000):
        self.queue = deque(maxlen=max_size)
        self.total_failed = 0
        
    def add(self, transaction: Transaction, error: str, retry_count: int = 0):
        """Add a failed transaction to the DLQ"""
        self.queue.append({
            'transaction': transaction,
            'error': str(error),
            'retry_count': retry_count,
            'failed_at': datetime.now(timezone.utc),
            'transaction_id': transaction.transaction_id
        })
        self.total_failed += 1
        
    def get_batch(self, size: int = 10) -> List[Dict]:
        """Get a batch of items to retry"""
        batch = []
        for _ in range(min(size, len(self.queue))):
            if self.queue:
                batch.append(self.queue.popleft())
        return batch
    
    def size(self) -> int:
        """Get current queue size"""
        return len(self.queue)
    
    def get_stats(self) -> Dict:
        """Get DLQ statistics"""
        return {
            'current_size': len(self.queue),
            'total_failed': self.total_failed
        }

class MultiTableDatabaseClient:
    """Database client with multi-table write support"""
    
    def __init__(self, **config):
        self.config = config
        self.session = None
        self.prepared_statements = {}
        self.dlq = DeadLetterQueue()
        
    def connect(self):
        """Establish connection to database"""
        try:
            if self.config.get("connection_type") == "local_hcd":
                self._connect_to_hcd()
            else:
                self._connect_to_astradb()
            
            self.session.default_consistency_level = ConsistencyLevel.LOCAL_QUORUM
            self.session.default_timeout = 10
            
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
    
    def prepare_statements(self):
        """Prepare CQL statements for all tables"""
        
        # Main transactions table
        insert_main = """
        INSERT INTO transactions (
            transaction_id, card_number, merchant_id, merchant_name, amount,
            currency, transaction_type, pos_terminal_id, gateway_id, timestamp,
            location_country, location_city, location_lat, location_lng, status,
            risk_score, mcc_code, auth_code, processing_fee, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        # High risk transactions table (only for risky transactions)
        insert_high_risk = """
        INSERT INTO high_risk_transactions (
            risk_bucket, timestamp, transaction_id, card_number, merchant_id,
            amount, risk_score, review_status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        self.prepared_statements['insert_main'] = self.session.prepare(insert_main)
        self.prepared_statements['insert_high_risk'] = self.session.prepare(insert_high_risk)
        
        logger.info("Prepared statements created for all tables")
    
    def _write_to_main_table(self, transaction: Transaction) -> bool:
        """Write to main transactions table"""
        try:
            bound_stmt = self.prepared_statements['insert_main'].bind([
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
            logger.error(f"Failed to write to main table: {e}")
            raise
    
    def _write_to_high_risk_table(self, transaction: Transaction) -> bool:
        """Write to high risk transactions table (if applicable)"""
        if transaction.risk_score < 0.6:
            return True  # Skip low/medium risk
        
        try:
            bound_stmt = self.prepared_statements['insert_high_risk'].bind([
                transaction.get_risk_bucket(),
                transaction.timestamp,
                uuid.UUID(transaction.transaction_id),
                transaction.card_number,
                transaction.merchant_id,
                transaction.amount,
                transaction.risk_score,
                'pending'
            ])
            self.session.execute(bound_stmt)
            return True
        except Exception as e:
            logger.error(f"Failed to write to high_risk table: {e}")
            raise
    
    def _update_hourly_stats(self, transaction: Transaction) -> bool:
        """
        Note: Hourly stats aggregation removed due to schema constraints.
        In production, use a separate batch job to aggregate from main table,
        or modify schema to use COUNTER columns (which have limitations).
        """
        # Skipping hourly stats for this POC
        # Alternative: Run periodic aggregation job
        return True
    
    def write_transaction_multi_table(self, transaction: Transaction, retry_count: int = 0) -> Tuple[bool, Optional[str]]:
        """
        Write transaction to multiple tables with retry logic
        Returns: (success, error_message)
        """
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                # Write to all tables sequentially
                # Using UNLOGGED BATCH for better performance (tables have different partition keys)
                
                # 1. Main table (critical)
                self._write_to_main_table(transaction)
                
                # 2. High risk table (if applicable)
                self._write_to_high_risk_table(transaction)
                
                # Note: Hourly stats skipped in this POC
                # In production: use batch aggregation jobs or COUNTER columns
                
                return True, None
                
            except Exception as e:
                if attempt < max_retries - 1:
                    # Exponential backoff
                    wait_time = (2 ** attempt) * 0.1  # 0.1s, 0.2s, 0.4s
                    time.sleep(wait_time)
                    logger.warning(f"Retry {attempt + 1}/{max_retries} for transaction {transaction.transaction_id}")
                else:
                    # Final retry failed - add to DLQ
                    error_msg = f"Failed after {max_retries} retries: {str(e)}"
                    self.dlq.add(transaction, error_msg, retry_count)
                    return False, error_msg
        
        return False, "Max retries exceeded"
    
    def batch_insert_transactions(self, transactions: List[Transaction]) -> Tuple[int, int]:
        """
        Insert multiple transactions to all tables
        Returns: (success_count, error_count)
        """
        success_count = 0
        error_count = 0
        
        for transaction in transactions:
            success, error = self.write_transaction_multi_table(transaction)
            if success:
                success_count += 1
            else:
                error_count += 1
        
        return success_count, error_count
    
    def get_dlq_stats(self) -> Dict:
        """Get Dead Letter Queue statistics"""
        return self.dlq.get_stats()
    
    async def process_dlq(self):
        """Background task to reprocess failed transactions from DLQ"""
        while True:
            try:
                if self.dlq.size() > 0:
                    batch = self.dlq.get_batch(size=10)
                    logger.info(f"Reprocessing {len(batch)} transactions from DLQ")
                    
                    for item in batch:
                        transaction = item['transaction']
                        retry_count = item['retry_count']
                        
                        if retry_count >= 10:
                            logger.error(f"Transaction {transaction.transaction_id} failed 10 times, giving up")
                            continue
                        
                        success, error = self.write_transaction_multi_table(transaction, retry_count + 1)
                        if not success:
                            logger.warning(f"DLQ retry failed for {transaction.transaction_id}: {error}")
                
                await asyncio.sleep(5)  # Process DLQ every 5 seconds
                
            except Exception as e:
                logger.error(f"DLQ processing error: {e}")
                await asyncio.sleep(5)
    
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
        self.tables_written = {
            'main': 0,
            'high_risk': 0,
            'hourly_stats': 0
        }
    
    def record_batch(self, batch_size: int, success_count: int, error_count: int, batch_time: float):
        """Record metrics for a batch of transactions"""
        self.total_transactions += batch_size
        self.successful_transactions += success_count
        self.failed_transactions += error_count
        self.batch_latencies.append(batch_time)
        
        # Estimate tables written (each success writes to 2-3 tables)
        self.tables_written['main'] += success_count
        self.tables_written['hourly_stats'] += success_count
        
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
            "tables_written": self.tables_written,
            **latency_stats
        }

class TransactionIngestPOC:
    """Main POC class for multi-table transaction data ingestion"""
    
    def __init__(self, db_config: Dict, target_tps: int = 2000, batch_size: int = 50):
        self.db_config = db_config
        self.target_tps = target_tps
        self.batch_size = batch_size
        self.db_client = MultiTableDatabaseClient(**db_config)
        self.transaction_generator = TransactionGenerator()
        self.performance_monitor = PerformanceMonitor(target_tps)
        self.running = False
    
    def initialize(self):
        """Initialize the POC"""
        logger.info("Initializing Multi-Table Transaction Ingest POC...")
        self.db_client.connect()
        logger.info("POC initialized successfully")
        logger.info("Writing to tables: transactions + high_risk_transactions")
        logger.info("Note: Hourly stats aggregation should be done via batch jobs")
    
    async def run_ingest_worker(self, worker_id: int):
        """Individual worker for transaction ingestion"""
        logger.info(f"Starting ingest worker {worker_id}")
        
        while self.running:
            try:
                batch_start = time.time()
                transactions = [
                    self.transaction_generator.generate_transaction()
                    for _ in range(self.batch_size)
                ]
                
                # Insert to multiple tables
                success_count, error_count = self.db_client.batch_insert_transactions(transactions)
                batch_time = time.time() - batch_start
                
                self.performance_monitor.record_batch(
                    self.batch_size, success_count, error_count, batch_time
                )
                
                await asyncio.sleep(0.001)
                
            except Exception as e:
                logger.error(f"Worker {worker_id} error: {e}")
                await asyncio.sleep(0.1)
    
    async def run_performance_reporter(self):
        """Report performance metrics periodically"""
        while self.running:
            stats = self.performance_monitor.get_current_stats()
            dlq_stats = self.db_client.get_dlq_stats()
            
            if "elapsed_time" in stats:
                logger.info(
                    f"Performance - "
                    f"TPS: {stats['current_tps']:.1f} "
                    f"(avg: {stats['average_tps']:.1f}), "
                    f"Error: {stats['error_rate']:.3f}%, "
                    f"Total: {stats['total_transactions']}, "
                    f"DLQ: {dlq_stats['current_size']}, "
                    f"Target: {'✓' if stats['target_met'] else '✗'}"
                )
            
            await asyncio.sleep(5)
    
    async def run_test(self, duration_seconds: int = 300, num_workers: int = 4):
        """Run the ingestion test"""
        logger.info(f"Starting {duration_seconds}s multi-table ingestion test with {num_workers} workers")
        logger.info(f"Target: {self.target_tps} TPS, Batch size: {self.batch_size}")
        
        self.running = True
        self.performance_monitor.reset_metrics()
        
        # Start workers
        workers = [
            asyncio.create_task(self.run_ingest_worker(i))
            for i in range(num_workers)
        ]
        
        # Start DLQ processor
        dlq_processor = asyncio.create_task(self.db_client.process_dlq())
        
        # Start reporter
        reporter = asyncio.create_task(self.run_performance_reporter())
        
        try:
            await asyncio.sleep(duration_seconds)
        finally:
            self.running = False
            
            for worker in workers:
                worker.cancel()
            dlq_processor.cancel()
            reporter.cancel()
            
            await asyncio.gather(*workers, dlq_processor, reporter, return_exceptions=True)
        
        # Final report
        final_stats = self.performance_monitor.get_current_stats()
        dlq_stats = self.db_client.get_dlq_stats()
        
        logger.info("=== FINAL MULTI-TABLE PERFORMANCE REPORT ===")
        for key, value in final_stats.items():
            logger.info(f"{key}: {value}")
        logger.info(f"DLQ Statistics: {dlq_stats}")
        
        return final_stats
    
    def cleanup(self):
        """Cleanup resources"""
        self.db_client.close()
        logger.info("POC cleanup completed")

def load_config_from_env() -> Dict:
    """Load configuration from environment variables"""
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
    db_config = load_config_from_env()
    
    target_tps = int(os.getenv("TARGET_TPS", 2000))
    batch_size = int(os.getenv("BATCH_SIZE", 50))
    num_workers = int(os.getenv("NUM_WORKERS", 4))
    test_duration = int(os.getenv("TEST_DURATION_SECONDS", 300))
    
    logger.info(f"Multi-Table POC Configuration:")
    logger.info(f"  Target TPS: {target_tps}")
    logger.info(f"  Batch Size: {batch_size}")
    logger.info(f"  Workers: {num_workers}")
    logger.info(f"  Duration: {test_duration}s")
    logger.info(f"  Tables: transactions + high_risk_transactions")
    logger.info(f"  Note: Stats aggregation via batch jobs recommended")
    
    poc = TransactionIngestPOC(
        db_config=db_config,
        target_tps=target_tps,
        batch_size=batch_size
    )
    
    try:
        poc.initialize()
        results = await poc.run_test(
            duration_seconds=test_duration, 
            num_workers=num_workers
        )
        
        dlq_stats = poc.db_client.get_dlq_stats()
        
        if results["target_met"]:
            print("✅ SUCCESS: Multi-table target metrics achieved!")
            print(f"   Average TPS: {results['average_tps']:.1f} (target: {target_tps})")
            print(f"   Error Rate: {results['error_rate']:.3f}% (target: <0.01%)")
        else:
            print("❌ TARGETS NOT MET")
            print(f"   Average TPS: {results['average_tps']:.1f} (target: {target_tps})")
            print(f"   Error Rate: {results['error_rate']:.3f}% (target: <0.01%)")
        
        print(f"\n📊 Multi-Table Results:")
        print(f"   Total Transactions: {results['total_transactions']:,}")
        print(f"   Successful: {results['successful_transactions']:,}")
        print(f"   Failed: {results['failed_transactions']:,}")
        print(f"   DLQ Size: {dlq_stats['current_size']}")
        print(f"   Total Failed Ever: {dlq_stats['total_failed']}")
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
    asyncio.run(main())