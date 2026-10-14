SELECT committed_at,
       snapshot_id,
       summary['flink-cdc.checkpoint-id'] AS checkpoint_id,
       summary['flink-cdc.batch-index']   AS batch_index
FROM lake.crm.customers.snapshots
ORDER BY committed_at;
