SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT ar.replica_server_name AS server_name,
			drs.is_primary_replica,
       adc.database_name,
       ag.name AS ag_name,
       drs.is_local,
       drs.synchronization_state_desc AS sync_desc,
       drs.synchronization_health_desc AS sync_health,
       drs.last_redone_time,
       drs.log_send_queue_size,
       drs.log_send_rate,
       drs.redo_queue_size,
       drs.redo_rate,
       (drs.redo_queue_size / drs.redo_rate) / 60.0 AS estimated_redo_completion_time_min,
       drs.last_commit_time
  FROM sys.dm_hadr_database_replica_states AS drs
 INNER JOIN sys.availability_databases_cluster AS adc
    ON drs.group_id          = adc.group_id
   AND drs.group_database_id = adc.group_database_id
 INNER JOIN sys.availability_groups AS ag
    ON ag.group_id           = drs.group_id
 INNER JOIN sys.availability_replicas AS ar
    ON drs.group_id          = ar.group_id
   AND drs.replica_id        = ar.replica_id
 --WHERE drs.is_local = 1 and drs.is_primary_replica <> 1
	--and adc.database_name in ('babel','stp')
 ORDER BY ag.name, ar.replica_server_name, adc.database_name;
