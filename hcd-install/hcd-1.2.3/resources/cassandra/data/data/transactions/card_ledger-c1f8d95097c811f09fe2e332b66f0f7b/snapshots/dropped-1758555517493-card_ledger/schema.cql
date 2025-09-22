CREATE TABLE IF NOT EXISTS transactions.card_ledger (
    card_number text,
    ledger_date date,
    timestamp timestamp,
    transaction_id uuid,
    amount decimal,
    description text,
    reference_transaction_id uuid,
    running_balance decimal,
    transaction_type text,
    PRIMARY KEY ((card_number, ledger_date), timestamp, transaction_id)
) WITH ID = c1f8d950-97c8-11f0-9fe2-e332b66f0f7b
    AND CLUSTERING ORDER BY (timestamp DESC, transaction_id ASC)
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND cdc = false
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy', 'compaction_window_size': '30', 'compaction_window_unit': 'DAYS', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND memtable = {}
    AND crc_check_chance = 1.0
    AND default_time_to_live = 0
    AND extensions = {}
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair = 'BLOCKING'
    AND speculative_retry = '99p';
