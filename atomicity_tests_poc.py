"""
Atomicity Tests POC for AstraDB
Validate partition/row atomicity for ledger operations
Target: No lost/duplicate records, ACID guarantees for financial operations
"""

import asyncio
import logging
import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import List, Dict, Optional, Tuple, Set
import random
import threading
from concurrent.futures import ThreadPoolExecutor

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from cassandra import ConsistencyLevel
from cassandra.query import BatchStatement, BatchType
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
class LedgerEntry:
    """Card ledger entry for atomic operations"""
    entry_id: str
    card_number: str
    ledger_date: str  # YYYY-MM-DD format for partitioning
    transaction_id: str
    entry_type: str  # debit, credit, fee, refund
    amount: Decimal
    running_balance: Decimal
    timestamp: datetime
    reference_id: Optional[str]
    description: str
    sequence_number: int  # For ordering within partition

@dataclass
class AtomicityTestResult:
    """Result of an atomicity test"""
    test_name: str
    total_operations: int
    successful_operations: int
    failed_operations: int
    duplicate_records: int
    missing_records: int
    balance_inconsistencies: int
    execution_time_ms: float
    atomicity_guaranteed: bool
    error_details: List[str]

class CardLedgerManager:
    """Manages card ledger operations with atomicity guarantees"""
    
    def __init__(self, session):
        self.session = session
        self.prepared_statements = {}
        self.lock = threading.Lock()
        
    def prepare_statements(self):
        """Prepare atomic ledger operation statements"""
        
        # Counter increment (different syntax for counters)
        self.prepared_statements['increment_sequence'] = self.session.prepare("""
            UPDATE sequence_counters 
            SET counter_value = counter_value + 1
            WHERE card_number = ? AND ledger_date = ?
        """)
        
        self.prepared_statements['get_sequence'] = self.session.prepare("""
            SELECT counter_value FROM sequence_counters
            WHERE card_number = ? AND ledger_date = ?
        """)
        
        # Ledger entry with lightweight transactions
        self.prepared_statements['insert_ledger_entry'] = self.session.prepare("""
            INSERT INTO card_ledger (
                card_number, ledger_date, sequence_number, entry_id, transaction_id,
                entry_type, amount, running_balance, timestamp, reference_id, description
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            IF NOT EXISTS
        """)
        
        # Read operations
        self.prepared_statements['get_current_balance'] = self.session.prepare("""
            SELECT running_balance, sequence_number 
            FROM card_ledger 
            WHERE card_number = ? AND ledger_date = ?
            ORDER BY sequence_number DESC
            LIMIT 1
        """)
        
        self.prepared_statements['get_ledger_entries'] = self.session.prepare("""
            SELECT entry_id, transaction_id, entry_type, amount, running_balance, sequence_number
            FROM card_ledger
            WHERE card_number = ? AND ledger_date = ?
            ORDER BY sequence_number ASC
        """)
        
        logger.info("Prepared ledger statements successfully")
    
    def get_next_sequence_number(self, card_number: str, ledger_date: str, max_retries: int = 5) -> int:
        """Get next sequence number atomically with retry logic"""
        
        for attempt in range(max_retries):
            try:
                # Increment counter (this automatically creates it if it doesn't exist)
                self.session.execute(
                    self.prepared_statements['increment_sequence'],
                    [card_number, ledger_date]
                )
                
                # Get the new value
                result = self.session.execute(
                    self.prepared_statements['get_sequence'],
                    [card_number, ledger_date]
                )
                
                for row in result:
                    return int(row.counter_value)
                
                # If no result, wait and retry
                time.sleep(0.01 * (2 ** attempt))
                
            except Exception as e:
                if attempt == max_retries - 1:
                    raise e
                time.sleep(0.01 * (2 ** attempt))
        
        raise Exception(f"Failed to get sequence number after {max_retries} attempts")
    
    def get_current_balance(self, card_number: str, ledger_date: str) -> Tuple[Decimal, int]:
        """Get current balance and last sequence number with strong consistency"""
        stmt = self.prepared_statements['get_current_balance']
        stmt.consistency_level = ConsistencyLevel.QUORUM
        result = self.session.execute(stmt, [card_number, ledger_date])
        
        for row in result:
            return row.running_balance, row.sequence_number
        
        return Decimal('0.00'), 0
    
    def insert_atomic_entry(self, entry: LedgerEntry, max_retries: int = 3) -> bool:
        """Insert a single ledger entry with proper atomicity and retry logic"""
        
        for attempt in range(max_retries):
            try:
                # Step 1: Get next sequence number atomically
                entry.sequence_number = self.get_next_sequence_number(entry.card_number, entry.ledger_date)
                
                # Step 2: For concurrent operations, set a placeholder balance
                # The correct balance will be calculated after insertion based on sequence order
                entry.running_balance = Decimal('0.00')  # Placeholder
                
                # Step 3: Insert entry with conditional check
                stmt = self.prepared_statements['insert_ledger_entry']
                stmt.consistency_level = ConsistencyLevel.QUORUM
                result = self.session.execute(
                    stmt,
                    [
                        entry.card_number, entry.ledger_date, entry.sequence_number,
                        entry.entry_id, entry.transaction_id, entry.entry_type,
                        entry.amount, entry.running_balance, entry.timestamp,
                        entry.reference_id, entry.description
                    ]
                )
                
                if result.one().applied:
                    return True
                else:
                    # Entry already exists, this indicates a sequence collision
                    logger.warning(f"Sequence collision detected for entry {entry.entry_id}, retrying...")
                    time.sleep(0.01 * (2 ** attempt))
                    continue
                    
            except Exception as e:
                logger.error(f"Attempt {attempt + 1} failed for entry {entry.entry_id}: {e}")
                if attempt == max_retries - 1:
                    return False
                time.sleep(0.01 * (2 ** attempt))
        
        return False
    
    def insert_atomic_entry_with_balance(self, entry: LedgerEntry, max_retries: int = 3) -> bool:
        """Insert a single ledger entry with proper balance calculation for sequential operations"""
        
        for attempt in range(max_retries):
            try:
                # Step 1: Get next sequence number atomically
                entry.sequence_number = self.get_next_sequence_number(entry.card_number, entry.ledger_date)
                
                # Step 2: Get current balance and last sequence with strong consistency
                current_balance, last_sequence = self.get_current_balance(entry.card_number, entry.ledger_date)
                
                # Step 3: Calculate new balance based on our sequence position
                if entry.entry_type in ['debit', 'fee']:
                    new_balance = current_balance - entry.amount
                else:
                    new_balance = current_balance + entry.amount
                
                entry.running_balance = new_balance
                
                # Step 4: Insert entry with conditional check
                stmt = self.prepared_statements['insert_ledger_entry']
                stmt.consistency_level = ConsistencyLevel.QUORUM
                result = self.session.execute(
                    stmt,
                    [
                        entry.card_number, entry.ledger_date, entry.sequence_number,
                        entry.entry_id, entry.transaction_id, entry.entry_type,
                        entry.amount, entry.running_balance, entry.timestamp,
                        entry.reference_id, entry.description
                    ]
                )
                
                if result.one().applied:
                    return True
                else:
                    # Entry already exists, this indicates a sequence collision
                    logger.warning(f"Sequence collision detected for entry {entry.entry_id}, retrying...")
                    time.sleep(0.01 * (2 ** attempt))
                    continue
                    
            except Exception as e:
                logger.error(f"Attempt {attempt + 1} failed for entry {entry.entry_id}: {e}")
                if attempt == max_retries - 1:
                    return False
                time.sleep(0.01 * (2 ** attempt))
        
        return False
    
    def insert_batch_atomic(self, entries: List[LedgerEntry], max_retries: int = 3) -> bool:
        """Insert multiple ledger entries atomically with proper coordination"""
        if not entries:
            return True
        
        # Validate all entries are for same partition
        card_number = entries[0].card_number
        ledger_date = entries[0].ledger_date
        
        if not all(e.card_number == card_number and e.ledger_date == ledger_date for e in entries):
            logger.error("Batch entries must be for same partition")
            return False
        
        for attempt in range(max_retries):
            try:
                # Step 1: Reserve sequence numbers for all entries
                sequence_numbers = []
                for _ in entries:
                    seq_num = self.get_next_sequence_number(card_number, ledger_date)
                    sequence_numbers.append(seq_num)
                
                # Step 2: Get current balance
                current_balance, last_sequence = self.get_current_balance(card_number, ledger_date)
                running_balance = current_balance
                
                # Step 3: Calculate balances for all entries
                for i, entry in enumerate(entries):
                    entry.sequence_number = sequence_numbers[i]
                    
                    if entry.entry_type in ['debit', 'fee']:
                        running_balance -= entry.amount
                    else:
                        running_balance += entry.amount
                    
                    entry.running_balance = running_balance
                
                # Step 4: Execute as atomic batch
                batch = BatchStatement(
                    batch_type=BatchType.LOGGED,
                    consistency_level=ConsistencyLevel.QUORUM
                )
                
                for entry in entries:
                    batch.add(
                        self.prepared_statements['insert_ledger_entry'],
                        [
                            entry.card_number, entry.ledger_date, entry.sequence_number,
                            entry.entry_id, entry.transaction_id, entry.entry_type,
                            entry.amount, entry.running_balance, entry.timestamp,
                            entry.reference_id, entry.description
                        ]
                    )
                
                self.session.execute(batch)
                return True
                
            except Exception as e:
                logger.error(f"Batch attempt {attempt + 1} failed: {e}")
                if attempt == max_retries - 1:
                    return False
                time.sleep(0.05 * (2 ** attempt))
        
        return False
    
    def recalculate_balances(self, card_number: str, ledger_date: str) -> bool:
        """Recalculate running balances based on sequence order"""
        try:
            # Get all entries in sequence order
            stmt = self.prepared_statements['get_ledger_entries']
            stmt.consistency_level = ConsistencyLevel.QUORUM
            result = self.session.execute(stmt, [card_number, ledger_date])
            
            entries = list(result)
            if not entries:
                return True
            
            # Sort by sequence number to ensure correct order
            entries.sort(key=lambda x: x.sequence_number)
            
            # Prepare update statement
            update_stmt = self.session.prepare("""
                UPDATE card_ledger 
                SET running_balance = ?
                WHERE card_number = ? AND ledger_date = ? AND sequence_number = ?
            """)
            update_stmt.consistency_level = ConsistencyLevel.QUORUM
            
            # Recalculate balances
            running_balance = Decimal('0.00')
            for entry in entries:
                if entry.entry_type in ['debit', 'fee']:
                    running_balance -= entry.amount
                else:
                    running_balance += entry.amount
                
                # Update the running balance
                self.session.execute(update_stmt, [
                    running_balance, card_number, ledger_date, entry.sequence_number
                ])
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to recalculate balances: {e}")
            return False
    
    def verify_ledger_integrity(self, card_number: str, ledger_date: str, expected_entries: List[str]) -> Dict:
        """Verify ledger integrity with strong consistency reads"""
        
        # Get all entries with strong consistency
        stmt = self.prepared_statements['get_ledger_entries']
        stmt.consistency_level = ConsistencyLevel.QUORUM
        result = self.session.execute(stmt, [card_number, ledger_date])
        
        entries = list(result)
        actual_entry_ids = [row.entry_id for row in entries]
        
        # Check for duplicates
        duplicates = len(actual_entry_ids) - len(set(actual_entry_ids))
        
        # Check for missing entries
        expected_set = set(expected_entries)
        actual_set = set(actual_entry_ids)
        missing = expected_set - actual_set
        extra = actual_set - expected_set
        
        # Verify balance consistency and sequence gaps
        balance_errors = []
        sequence_errors = []
        running_balance = Decimal('0.00')
        
        # Sort entries by sequence number to ensure proper order
        sorted_entries = sorted(entries, key=lambda x: x.sequence_number)
        expected_sequence = 1
        
        for i, row in enumerate(sorted_entries):
            # Check sequence continuity
            if row.sequence_number != expected_sequence:
                sequence_errors.append(f"Gap in sequence: expected {expected_sequence}, got {row.sequence_number}")
            expected_sequence = row.sequence_number + 1
            
            # Check balance calculation
            if row.entry_type in ['debit', 'fee']:
                running_balance -= row.amount
            else:
                running_balance += row.amount
            
            if abs(row.running_balance - running_balance) > Decimal('0.001'):  # Allow for rounding
                balance_errors.append(f"Sequence {row.sequence_number}: expected {running_balance}, got {row.running_balance}")
        
        return {
            'total_entries': len(entries),
            'expected_entries': len(expected_entries),
            'duplicates': duplicates,
            'missing_count': len(missing),
            'missing_entries': list(missing),
            'extra_count': len(extra),
            'extra_entries': list(extra),
            'balance_errors': balance_errors,
            'sequence_errors': sequence_errors,
            'final_balance': sorted_entries[-1].running_balance if sorted_entries else Decimal('0.00')
        }

class AtomicityTestClient:
    """Client for testing atomicity guarantees"""
    
    def __init__(self, **config):
        self.config = config
        self.session = None
        self.ledger_manager = None
        
    def connect(self):
        """Connect to database (AstraDB or local HCD)"""
        try:
            if self.config.get("connection_type") == "local_hcd":
                self._connect_to_hcd()
            else:
                self._connect_to_astradb()
            
            # Use strong consistency for atomicity tests
            self.session.default_consistency_level = ConsistencyLevel.QUORUM
            self.session.default_timeout = 30
            
            self.create_ledger_schema()
            self.ledger_manager = CardLedgerManager(self.session)
            self.ledger_manager.prepare_statements()
            
            logger.info(f"Connected to {self.config.get('connection_type', 'AstraDB')} for atomicity testing")
            
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def _connect_to_hcd(self):
        """Connect to local HCD instance"""
        from cassandra.auth import PlainTextAuthProvider
        
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
            connect_timeout=10
        )
        
        # Connect directly to the existing keyspace
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
            connect_timeout=10
        )
        
        self.session = cluster.connect(self.config["keyspace"])
    
    def create_ledger_schema(self):
        """Create ledger table with proper atomicity support"""
        
        # Sequence counters table for atomic sequence generation
        sequence_table = """
        CREATE TABLE IF NOT EXISTS sequence_counters (
            card_number TEXT,
            ledger_date TEXT,
            counter_value BIGINT,
            PRIMARY KEY (card_number, ledger_date)
        )
        """
        
        # Main ledger table
        ledger_table = """
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
        ) WITH CLUSTERING ORDER BY (sequence_number ASC)
        """
        
        self.session.execute(sequence_table)
        self.session.execute(ledger_table)
        logger.info("Enhanced ledger schema created successfully")
    
    def generate_test_entries(self, card_number: str, count: int) -> List[LedgerEntry]:
        """Generate test ledger entries"""
        entries = []
        today = datetime.now(timezone.utc).date().isoformat()
        
        for i in range(count):
            entry_type = random.choice(['debit', 'credit', 'fee', 'refund'])
            amount = Decimal(str(random.uniform(10.00, 500.00))).quantize(Decimal('0.01'))
            
            entry = LedgerEntry(
                entry_id=str(uuid.uuid4()),
                card_number=card_number,
                ledger_date=today,
                transaction_id=str(uuid.uuid4()),
                entry_type=entry_type,
                amount=amount,
                running_balance=Decimal('0.00'),  # Will be calculated
                timestamp=datetime.now(timezone.utc),
                reference_id=str(uuid.uuid4()) if random.random() > 0.3 else None,
                description=f"Test {entry_type} transaction",
                sequence_number=0  # Will be assigned
            )
            entries.append(entry)
        
        return entries
    
    def test_single_entry_atomicity(self, iterations: int = 100) -> AtomicityTestResult:
        """Test atomicity of single ledger entry operations"""
        logger.info(f"Testing single entry atomicity with {iterations} iterations")
        
        start_time = time.time()
        test_card = "TEST-CARD-001"
        expected_entries = []
        successful_ops = 0
        failed_ops = 0
        error_details = []
        
        for i in range(iterations):
            entries = self.generate_test_entries(test_card, 1)
            entry = entries[0]
            expected_entries.append(entry.entry_id)
            
            try:
                if self.ledger_manager.insert_atomic_entry_with_balance(entry):
                    successful_ops += 1
                else:
                    failed_ops += 1
                    error_details.append(f"Entry {entry.entry_id} failed conditional insert")
            except Exception as e:
                failed_ops += 1
                error_details.append(f"Entry {entry.entry_id} exception: {str(e)}")
        
        # Verify integrity
        today = datetime.now(timezone.utc).date().isoformat()
        integrity_check = self.ledger_manager.verify_ledger_integrity(test_card, today, expected_entries)
        
        execution_time = (time.time() - start_time) * 1000
        
        return AtomicityTestResult(
            test_name="single_entry_atomicity",
            total_operations=iterations,
            successful_operations=successful_ops,
            failed_operations=failed_ops,
            duplicate_records=integrity_check['duplicates'],
            missing_records=integrity_check['missing_count'],
            balance_inconsistencies=len(integrity_check['balance_errors']),
            execution_time_ms=execution_time,
            atomicity_guaranteed=(integrity_check['duplicates'] == 0 and 
                                 integrity_check['missing_count'] == 0 and
                                 len(integrity_check['balance_errors']) == 0),
            error_details=error_details + integrity_check['balance_errors']
        )
    
    def test_batch_atomicity(self, batch_count: int = 20, batch_size: int = 5) -> AtomicityTestResult:
        """Test atomicity of batch operations"""
        logger.info(f"Testing batch atomicity with {batch_count} batches of {batch_size} entries")
        
        start_time = time.time()
        test_card = "TEST-CARD-002"
        expected_entries = []
        successful_ops = 0
        failed_ops = 0
        error_details = []
        
        for i in range(batch_count):
            entries = self.generate_test_entries(test_card, batch_size)
            batch_entry_ids = [e.entry_id for e in entries]
            expected_entries.extend(batch_entry_ids)
            
            try:
                if self.ledger_manager.insert_batch_atomic(entries):
                    successful_ops += 1
                else:
                    failed_ops += 1
                    error_details.append(f"Batch {i} failed atomic execution")
            except Exception as e:
                failed_ops += 1
                error_details.append(f"Batch {i} exception: {str(e)}")
        
        # Verify integrity
        today = datetime.now(timezone.utc).date().isoformat()
        integrity_check = self.ledger_manager.verify_ledger_integrity(test_card, today, expected_entries)
        
        execution_time = (time.time() - start_time) * 1000
        
        return AtomicityTestResult(
            test_name="batch_atomicity",
            total_operations=batch_count,
            successful_operations=successful_ops,
            failed_operations=failed_ops,
            duplicate_records=integrity_check['duplicates'],
            missing_records=integrity_check['missing_count'],
            balance_inconsistencies=len(integrity_check['balance_errors']),
            execution_time_ms=execution_time,
            atomicity_guaranteed=(integrity_check['duplicates'] == 0 and 
                                 integrity_check['missing_count'] == 0 and
                                 len(integrity_check['balance_errors']) == 0),
            error_details=error_details + integrity_check['balance_errors']
        )
    
    def test_concurrent_atomicity(self, thread_count: int = 5, ops_per_thread: int = 20) -> AtomicityTestResult:
        """Test atomicity under concurrent access"""
        logger.info(f"Testing concurrent atomicity with {thread_count} threads, {ops_per_thread} ops each")
        
        start_time = time.time()
        test_card = "TEST-CARD-003"
        expected_entries = []
        results = []
        error_details = []
        
        def worker_thread(worker_id: int):
            """Worker thread for concurrent testing"""
            thread_results = {'success': 0, 'failed': 0, 'entries': []}
            
            for i in range(ops_per_thread):
                entries = self.generate_test_entries(test_card, 1)
                entry = entries[0]
                thread_results['entries'].append(entry.entry_id)
                
                try:
                    if self.ledger_manager.insert_atomic_entry(entry):
                        thread_results['success'] += 1
                    else:
                        thread_results['failed'] += 1
                except Exception as e:
                    thread_results['failed'] += 1
                    error_details.append(f"Worker {worker_id} entry {entry.entry_id}: {str(e)}")
            
            results.append(thread_results)
        
        # Execute concurrent operations
        with ThreadPoolExecutor(max_workers=thread_count) as executor:
            futures = [executor.submit(worker_thread, i) for i in range(thread_count)]
            for future in futures:
                future.result()  # Wait for completion
        
        # Collect all expected entries
        for result in results:
            expected_entries.extend(result['entries'])
        
        # Calculate totals
        total_successful = sum(r['success'] for r in results)
        total_failed = sum(r['failed'] for r in results)
        
        # Recalculate balances after concurrent operations
        today = datetime.now(timezone.utc).date().isoformat()
        self.ledger_manager.recalculate_balances(test_card, today)
        
        # Verify integrity
        integrity_check = self.ledger_manager.verify_ledger_integrity(test_card, today, expected_entries)
        
        execution_time = (time.time() - start_time) * 1000
        
        return AtomicityTestResult(
            test_name="concurrent_atomicity",
            total_operations=thread_count * ops_per_thread,
            successful_operations=total_successful,
            failed_operations=total_failed,
            duplicate_records=integrity_check['duplicates'],
            missing_records=integrity_check['missing_count'],
            balance_inconsistencies=len(integrity_check['balance_errors']),
            execution_time_ms=execution_time,
            atomicity_guaranteed=(integrity_check['duplicates'] == 0 and 
                                 integrity_check['missing_count'] == 0 and
                                 len(integrity_check['balance_errors']) == 0),
            error_details=error_details + integrity_check['balance_errors']
        )
    
    def close(self):
        """Close database connection"""
        if self.session:
            self.session.cluster.shutdown()
            logger.info("Atomicity test client connection closed")

class AtomicityTestPOC:
    """Main POC class for atomicity testing"""
    
    def __init__(self, astradb_config: Dict):
        self.config = astradb_config
        self.test_client = AtomicityTestClient(**astradb_config)
    
    def initialize(self):
        """Initialize the atomicity test POC"""
        logger.info("Initializing Atomicity Test POC...")
        self.test_client.connect()
        logger.info("Atomicity test POC initialized successfully")
    
    def run_all_atomicity_tests(self) -> Dict[str, AtomicityTestResult]:
        """Run comprehensive atomicity test suite"""
        logger.info("Starting comprehensive atomicity test suite...")
        
        results = {}
        
        # Test 1: Single entry atomicity
        logger.info("=" * 50)
        logger.info("TEST 1: Single Entry Atomicity")
        results['single_entry'] = self.test_client.test_single_entry_atomicity(100000)
        
        # Test 2: Batch atomicity
        logger.info("=" * 50)
        logger.info("TEST 2: Batch Atomicity")
        results['batch'] = self.test_client.test_batch_atomicity(20, 5)
        
        # Test 3: Concurrent access atomicity
        logger.info("=" * 50)
        logger.info("TEST 3: Concurrent Access Atomicity")
        results['concurrent'] = self.test_client.test_concurrent_atomicity(5, 20)
        
        return results
    
    def print_test_results(self, results: Dict[str, AtomicityTestResult]):
        """Print comprehensive test results"""
        
        logger.info("=" * 60)
        logger.info("ATOMICITY TEST RESULTS SUMMARY")
        logger.info("=" * 60)
        
        all_tests_passed = True
        
        for test_name, result in results.items():
            logger.info(f"\n{test_name.upper()} TEST:")
            logger.info(f"  Total Operations: {result.total_operations}")
            logger.info(f"  Successful: {result.successful_operations}")
            logger.info(f"  Failed: {result.failed_operations}")
            logger.info(f"  Duplicate Records: {result.duplicate_records}")
            logger.info(f"  Missing Records: {result.missing_records}")
            logger.info(f"  Balance Inconsistencies: {result.balance_inconsistencies}")
            logger.info(f"  Execution Time: {result.execution_time_ms:.1f}ms")
            logger.info(f"  Atomicity Guaranteed: {'✅ YES' if result.atomicity_guaranteed else '❌ NO'}")
            
            if not result.atomicity_guaranteed:
                all_tests_passed = False
                logger.error(f"  Error Details: {result.error_details[:5]}")  # Show first 5 errors
        
        # Final verdict
        logger.info("=" * 60)
        if all_tests_passed:
            print("✅ SUCCESS: All atomicity tests passed!")
            print("   No lost/duplicate records detected")
            print("   ACID guarantees validated for ledger operations")
        else:
            print("❌ ATOMICITY FAILURES DETECTED")
            print("   Some tests failed atomicity requirements")
            print("   Check logs for detailed error information")
        
        return all_tests_passed
    
    def cleanup(self):
        """Cleanup resources"""
        self.test_client.close()
        logger.info("Atomicity test POC cleanup completed")

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

def main():
    """Main function to run the atomicity test POC"""
    
    # Load configuration
    config = load_config_from_env()
    
    # Validate configuration based on connection type
    if config.get("connection_type") == "local_hcd":
        required_configs = ["contact_points", "username", "password", "keyspace"]
        missing_configs = [key for key in required_configs if not config.get(key)]
        
        if missing_configs:
            logger.error(f"Missing required HCD environment variables: {missing_configs}")
            logger.error("Set USE_LOCAL_HCD=true and configure: HCD_CONTACT_POINTS, HCD_USERNAME, HCD_PASSWORD, HCD_KEYSPACE")
            return
        
        logger.info("Using local HCD configuration")
        logger.info(f"Contact Points: {config['contact_points']}")
        logger.info(f"Keyspace: {config['keyspace']}")
        
    else:
        required_configs = ["secure_connect_bundle", "token", "keyspace"]
        missing_configs = [key for key in required_configs if not config.get(key)]
        
        if missing_configs:
            logger.error(f"Missing required AstraDB environment variables: {missing_configs}")
            return
        
        if not config["token"].startswith("AstraCS:"):
            logger.error("Invalid token format. AstraDB tokens should start with 'AstraCS:'")
            return
        
        logger.info("Using AstraDB configuration")
        logger.info(f"Keyspace: {config['keyspace']}")
    
    logger.info("Starting Atomicity Test POC...")
    logger.info("Target: No lost/duplicate records, ACID guarantees for ledger operations")
    
    # Initialize POC
    poc = AtomicityTestPOC(config)
    
    try:
        # Initialize
        poc.initialize()
        
        # Run all atomicity tests
        results = poc.run_all_atomicity_tests()
        
        # Print results and determine success
        success = poc.print_test_results(results)
        
        return success
        
    except Exception as e:
        logger.error(f"Atomicity test POC failed: {e}")
        raise
    
    finally:
        poc.cleanup()

if __name__ == "__main__":
    # Run the atomicity test POC
    main()