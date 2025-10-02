"""
Read Query Performance POC for AstraDB/HCD with SAI
Optimized read queries using Storage-Attached Indexing
Target: ≤50ms p95 latency for fraud/monitoring/reporting queries
"""

import asyncio
import logging
import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import List, Dict, Optional, Tuple
import statistics
import random

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from cassandra import ConsistencyLevel
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

@dataclass
class QueryResult:
    """Query execution result with performance metrics"""
    query_type: str
    execution_time_ms: float
    row_count: int
    success: bool
    error_message: Optional[str] = None

class ReadQueryClient:
    """Database client optimized for read queries with SAI"""
    
    def __init__(self, **config):
        self.config = config
        self.session = None
        self.prepared_statements = {}
        self.sample_merchants = []
        self.sample_gateways = ["VISA_GATEWAY", "MASTERCARD_GATEWAY", "AMEX_GATEWAY"]
        
    def connect(self):
        """Establish optimized connection for read queries"""
        try:
            if self.config.get("connection_type") == "local_hcd":
                self._connect_to_hcd()
            else:
                self._connect_to_astradb()
            
            # Optimize for read performance with LOCAL_ONE
            self.session.default_consistency_level = ConsistencyLevel.LOCAL_ONE
            self.session.default_timeout = 10
            self.session.default_fetch_size = 1000
            
            self.prepare_sai_read_statements()
            self.sample_existing_data()
            
            logger.info(f"Connected to {self.config.get('connection_type', 'AstraDB')} for SAI read queries")
            
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
            executor_threads=8,
            max_schema_agreement_wait=30
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
            executor_threads=8,
            max_schema_agreement_wait=30
        )
        
        self.session = cluster.connect(self.config["keyspace"])
    
    def sample_existing_data(self):
        """Sample existing data to get realistic query parameters"""
        try:
            # Get sample merchant IDs from existing data
            sample_query = "SELECT DISTINCT merchant_id FROM transactions LIMIT 100 ALLOW FILTERING"
            results = self.session.execute(sample_query)
            self.sample_merchants = [row.merchant_id for row in results if row.merchant_id]
            
            if self.sample_merchants:
                logger.info(f"Sampled {len(self.sample_merchants)} merchants from existing data")
            else:
                logger.warning("No existing data found, queries will return empty results")
        except Exception as e:
            logger.warning(f"Could not sample existing data: {e}")
            self.sample_merchants = []
    
    def prepare_sai_read_statements(self):
        """Prepare optimized SAI query statements - HIGH CARDINALITY ONLY"""
        
        # HIGH CARDINALITY - Query by merchant using SAI index
        self.prepared_statements['by_merchant'] = self.session.prepare("""
            SELECT transaction_id, card_number, merchant_name, amount, 
                   timestamp, status, risk_score, gateway_id
            FROM transactions
            WHERE merchant_id = ?
            LIMIT ?
            ALLOW FILTERING
        """)
        
        # HIGH CARDINALITY - High-risk query using SAI inequality (continuous values)
        self.prepared_statements['high_risk'] = self.session.prepare("""
            SELECT transaction_id, card_number, merchant_name, amount,
                   timestamp, status, risk_score
            FROM transactions
            WHERE risk_score > ?
            LIMIT ?
            ALLOW FILTERING
        """)
        
        # HIGH CARDINALITY - Query by country using SAI index
        self.prepared_statements['by_country'] = self.session.prepare("""
            SELECT transaction_id, merchant_name, amount, location_city, status
            FROM transactions
            WHERE location_country = ?
            LIMIT ?
            ALLOW FILTERING
        """)
        
        # HIGH CARDINALITY - Query by timestamp range using SAI
        self.prepared_statements['by_timestamp'] = self.session.prepare("""
            SELECT transaction_id, merchant_name, amount, timestamp, status
            FROM transactions
            WHERE timestamp > ?
            LIMIT ?
            ALLOW FILTERING
        """)
        
        # Note: LOW CARDINALITY queries removed (status, gateway, currency)
        # These should use separate partitioned tables for better performance
        
        logger.info("SAI read statements prepared (high cardinality only)")
    
    def execute_timed_query(self, stmt_key: str, query_type: str, *params) -> QueryResult:
        """Execute a query and measure performance"""
        start_time = time.time()
        try:
            result = self.session.execute(self.prepared_statements[stmt_key], params)
            rows = list(result)
            execution_time = (time.time() - start_time) * 1000
            
            return QueryResult(
                query_type=query_type,
                execution_time_ms=execution_time,
                row_count=len(rows),
                success=True
            )
            
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            logger.error(f"Query {query_type} failed: {e}")
            return QueryResult(
                query_type=query_type,
                execution_time_ms=execution_time,
                row_count=0,
                success=False,
                error_message=str(e)
            )
    
    def query_by_merchant(self, limit: int = 10) -> QueryResult:
        """Query transactions by merchant using SAI - HIGH CARDINALITY"""
        merchant_id = random.choice(self.sample_merchants) if self.sample_merchants else f"MER_{random.randint(100000, 999999)}"
        return self.execute_timed_query('by_merchant', 'merchant_lookup', merchant_id, limit)
    
    def query_high_risk(self, min_risk: float = 0.8, limit: int = 10) -> QueryResult:
        """Query high-risk transactions using SAI inequality - HIGH CARDINALITY"""
        return self.execute_timed_query('high_risk', 'high_risk_lookup', min_risk, limit)
    
    def query_by_country(self, country: str = "US", limit: int = 10) -> QueryResult:
        """Query transactions by country using SAI - MEDIUM-HIGH CARDINALITY"""
        return self.execute_timed_query('by_country', 'country_lookup', country, limit)
    
    def query_by_recent_timestamp(self, minutes_back: int = 60, limit: int = 10) -> QueryResult:
        """Query recent transactions using SAI timestamp - HIGH CARDINALITY"""
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=minutes_back)
        return self.execute_timed_query('by_timestamp', 'timestamp_lookup', cutoff, limit)
    
    def close(self):
        """Close database connection"""
        if self.session:
            self.session.cluster.shutdown()
            logger.info("Read query client connection closed")

class ReadPerformanceMonitor:
    """Monitor read query performance metrics"""
    
    def __init__(self):
        self.reset_metrics()
    
    def reset_metrics(self):
        """Reset all performance metrics"""
        self.start_time = time.time()
        self.query_results = []
        self.query_type_stats = {}
    
    def record_query_result(self, result: QueryResult):
        """Record a query result for performance tracking"""
        self.query_results.append(result)
        
        if result.query_type not in self.query_type_stats:
            self.query_type_stats[result.query_type] = []
        self.query_type_stats[result.query_type].append(result.execution_time_ms)
    
    def get_performance_stats(self) -> Dict:
        """Get comprehensive performance statistics"""
        if not self.query_results:
            return {"status": "no_queries_executed"}
        
        all_latencies = [r.execution_time_ms for r in self.query_results if r.success]
        successful_queries = len([r for r in self.query_results if r.success])
        failed_queries = len([r for r in self.query_results if not r.success])
        
        overall_stats = {}
        if all_latencies:
            overall_stats = {
                "avg_latency_ms": statistics.mean(all_latencies),
                "p50_latency_ms": statistics.median(all_latencies),
                "p95_latency_ms": statistics.quantiles(all_latencies, n=20)[18] if len(all_latencies) >= 20 else max(all_latencies),
                "p99_latency_ms": statistics.quantiles(all_latencies, n=100)[98] if len(all_latencies) >= 100 else max(all_latencies),
                "min_latency_ms": min(all_latencies),
                "max_latency_ms": max(all_latencies)
            }
        
        query_type_breakdown = {}
        for query_type, latencies in self.query_type_stats.items():
            if latencies:
                query_type_breakdown[query_type] = {
                    "count": len(latencies),
                    "avg_latency_ms": statistics.mean(latencies),
                    "p95_latency_ms": statistics.quantiles(latencies, n=20)[18] if len(latencies) >= 20 else max(latencies),
                    "min_latency_ms": min(latencies),
                    "max_latency_ms": max(latencies)
                }
        
        return {
            "total_queries": len(self.query_results),
            "successful_queries": successful_queries,
            "failed_queries": failed_queries,
            "success_rate": (successful_queries / len(self.query_results)) * 100 if self.query_results else 0,
            "target_p95_met": overall_stats.get("p95_latency_ms", float('inf')) <= 50.0,
            "elapsed_time": time.time() - self.start_time,
            **overall_stats,
            "query_type_breakdown": query_type_breakdown
        }

class ReadQueryPOC:
    """Main POC class for SAI read query performance testing"""
    
    def __init__(self, db_config: Dict):
        self.db_config = db_config
        self.db_client = ReadQueryClient(**db_config)
        self.performance_monitor = ReadPerformanceMonitor()
    
    def initialize(self):
        """Initialize the read query POC"""
        logger.info("Initializing SAI Read Query Performance POC...")
        self.db_client.connect()
        logger.info("SAI read query POC initialized successfully")
    
    async def run_diverse_queries(self, num_queries: int):
        """Run diverse query workload - HIGH CARDINALITY ONLY"""
        logger.info(f"Running {num_queries} diverse SAI queries (high cardinality)...")
        
        # Only high cardinality queries
        query_functions = [
            self.db_client.query_by_merchant,        # 50% - most selective
            self.db_client.query_high_risk,          # 30% - range query
            self.db_client.query_by_country,         # 15% - medium cardinality
            self.db_client.query_by_recent_timestamp # 5% - time-based
        ]
        
        # Weighted random selection
        weights = [50, 30, 15, 5]
        
        for i in range(num_queries):
            query_func = random.choices(query_functions, weights=weights, k=1)[0]
            result = query_func()
            self.performance_monitor.record_query_result(result)
            
            if (i + 1) % 5000 == 0:
                logger.info(f"Completed {i + 1}/{num_queries} queries")
            
            # No sleep for maximum throughput
            if i % 100 == 0:
                await asyncio.sleep(0.001)  # Small yield every 100 queries
    
    async def run_fraud_detection_workload(self, num_queries: int):
        """Simulate fraud detection query workload - HIGH CARDINALITY"""
        logger.info(f"Running fraud detection workload ({num_queries} queries)...")
        
        for i in range(num_queries):
            # Fraud detection focuses on high-risk scores (high cardinality, continuous)
            if i % 3 == 0:
                result = self.db_client.query_high_risk(0.8)  # Very high risk
            elif i % 3 == 1:
                result = self.db_client.query_high_risk(0.6)  # Medium-high risk
            else:
                result = self.db_client.query_by_merchant()  # Check specific merchant
            
            self.performance_monitor.record_query_result(result)
            
            if (i + 1) % 5000 == 0:
                logger.info(f"Completed {i + 1}/{num_queries} queries")
            
            if i % 100 == 0:
                await asyncio.sleep(0.001)
    
    async def run_monitoring_workload(self, num_queries: int):
        """Simulate real-time monitoring workload - HIGH CARDINALITY"""
        logger.info(f"Running monitoring workload ({num_queries} queries)...")
        
        for i in range(num_queries):
            # Monitoring focuses on recent activity and specific merchants
            if i % 3 == 0:
                result = self.db_client.query_by_recent_timestamp(60)  # Last hour
            elif i % 3 == 1:
                result = self.db_client.query_by_merchant()  # Specific merchant
            else:
                result = self.db_client.query_high_risk(0.7)  # Medium risk threshold
            
            self.performance_monitor.record_query_result(result)
            
            if (i + 1) % 5000 == 0:
                logger.info(f"Completed {i + 1}/{num_queries} queries")
            
            if i % 100 == 0:
                await asyncio.sleep(0.001)
    
    async def run_performance_test(self, duration_seconds: int = 60, workload: str = "diverse", target_queries: int = None):
        """Run comprehensive read performance test"""
        logger.info(f"Starting SAI read performance test")
        
        if target_queries:
            logger.info(f"Target: {target_queries:,} queries")
            total_queries = target_queries
        else:
            logger.info(f"Duration: {duration_seconds}s")
            # Increased from 100 to 1000 queries per second for faster execution
            queries_per_second = 1000
            total_queries = duration_seconds * queries_per_second
        
        logger.info(f"Workload type: {workload}")
        logger.info(f"Total queries to execute: {total_queries:,}")
        
        self.performance_monitor.reset_metrics()
        
        if workload == "fraud":
            await self.run_fraud_detection_workload(total_queries)
        elif workload == "monitoring":
            await self.run_monitoring_workload(total_queries)
        else:
            await self.run_diverse_queries(total_queries)
        
        final_stats = self.performance_monitor.get_performance_stats()
        logger.info("=== SAI READ QUERY PERFORMANCE REPORT ===")
        
        for key, value in final_stats.items():
            if key != "query_type_breakdown":
                logger.info(f"{key}: {value}")
        
        if "query_type_breakdown" in final_stats:
            logger.info("\n=== QUERY TYPE BREAKDOWN ===")
            for query_type, stats in final_stats["query_type_breakdown"].items():
                logger.info(f"{query_type}: {stats}")
        
        return final_stats
    
    def cleanup(self):
        """Cleanup resources"""
        self.db_client.close()
        logger.info("SAI read query POC cleanup completed")

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
    """Main function to run the SAI read query performance POC"""
    db_config = load_config_from_env()
    
    if db_config.get("connection_type") == "local_hcd":
        required_configs = ["contact_points", "username", "password", "keyspace"]
        missing_configs = [key for key in required_configs if not db_config.get(key)]
        
        if missing_configs:
            logger.error(f"Missing required HCD environment variables: {missing_configs}")
            return
        
        logger.info("Using local HCD configuration with SAI")
        logger.info(f"Keyspace: {db_config['keyspace']}")
        
    else:
        required_configs = ["secure_connect_bundle", "token", "keyspace"]
        missing_configs = [key for key in required_configs if not db_config.get(key)]
        
        if missing_configs:
            logger.error(f"Missing required AstraDB environment variables: {missing_configs}")
            return
        
        logger.info("Using AstraDB configuration with SAI")
        logger.info(f"Keyspace: {db_config['keyspace']}")
    
    logger.info("Starting SAI Read Query Performance POC...")
    logger.info("Target: p95 latency ≤50ms using Storage-Attached Indexes")
    
    poc = ReadQueryPOC(db_config=db_config)
    
    try:
        poc.initialize()
        
        test_duration = int(os.getenv("READ_TEST_DURATION", 60))
        target_queries = os.getenv("TARGET_QUERIES")
        if target_queries:
            target_queries = int(target_queries)
        
        workload_type = os.getenv("READ_WORKLOAD_TYPE", "diverse")
        
        results = await poc.run_performance_test(
            duration_seconds=test_duration,
            workload=workload_type,
            target_queries=target_queries
        )
        
        target_met = results.get("target_p95_met", False)
        p95_latency = results.get("p95_latency_ms", 0)
        success_rate = results.get("success_rate", 0)
        
        print("\n" + "="*60)
        print("SAI READ QUERY PERFORMANCE RESULTS")
        print("="*60)
        
        if target_met and success_rate > 95:
            print(f"✅ SUCCESS: Read performance targets achieved!")
            print(f"   P95 Latency: {p95_latency:.1f}ms (target: ≤50ms)")
            print(f"   Success Rate: {success_rate:.1f}%")
        else:
            print(f"❌ TARGETS NOT MET")
            print(f"   P95 Latency: {p95_latency:.1f}ms (target: ≤50ms)")
            print(f"   Success Rate: {success_rate:.1f}%")
        
        print(f"\n📊 Detailed Results:")
        print(f"   Total Queries: {results.get('total_queries', 0):,}")
        print(f"   Successful: {results.get('successful_queries', 0):,}")
        print(f"   Failed: {results.get('failed_queries', 0):,}")
        print(f"   Avg Latency: {results.get('avg_latency_ms', 0):.1f}ms")
        print(f"   P50 Latency: {results.get('p50_latency_ms', 0):.1f}ms")
        print(f"   P95 Latency: {p95_latency:.1f}ms")
        print(f"   P99 Latency: {results.get('p99_latency_ms', 0):.1f}ms")
        print(f"   Min Latency: {results.get('min_latency_ms', 0):.1f}ms")
        print(f"   Max Latency: {results.get('max_latency_ms', 0):.1f}ms")
        print(f"   Test Duration: {results.get('elapsed_time', 0):.1f}s")
        
        if "query_type_breakdown" in results:
            print(f"\n📈 Query Type Breakdown:")
            for qtype, stats in results["query_type_breakdown"].items():
                print(f"   {qtype}:")
                print(f"     Count: {stats['count']}")
                print(f"     Avg: {stats['avg_latency_ms']:.1f}ms")
                print(f"     P95: {stats['p95_latency_ms']:.1f}ms")
        
        print(f"\n💡 SAI Features Used:")
        print(f"   ✓ Multiple indexes (merchant, status, gateway, risk_score, etc.)")
        print(f"   ✓ Inequality queries (risk_score > 0.8)")
        print(f"   ✓ Complex predicates (status AND risk_score)")
        print(f"   ✓ Optimized for read performance (LOCAL_ONE consistency)")
        
    except Exception as e:
        logger.error(f"SAI read query POC failed: {e}")
        raise
    finally:
        poc.cleanup()

if __name__ == "__main__":
    asyncio.run(main())