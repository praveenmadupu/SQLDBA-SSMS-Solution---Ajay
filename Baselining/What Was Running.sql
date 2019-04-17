SELECT  DENSE_RANK()OVER(ORDER BY collection_Time ASC) AS CollectionBatch, [collection_time], [TimeInMinutes], [dd hh:mm:ss.mss], [dd hh:mm:ss.mss (avg)], [session_id], [sql_text], [sql_command], [login_name], 
		[wait_info], [tasks], [tran_log_writes], [CPU], [tempdb_allocations], [tempdb_current], [blocking_session_id], 
		[blocked_session_count], [reads], [writes], [context_switches], [physical_io], [physical_reads], [query_plan], [locks], 
		[used_memory], [status], [tran_start_time], [open_tran_count], [percent_complete], [host_name], [database_name], [program_name], 
		[additional_info], [start_time], [login_time], [request_id]
FROM [DBA].[dbo].WhoIsActive_ResultSets AS r
WHERE r.collection_Time >= '2019-04-09 04:48:00.000'
AND r.collection_Time <= getdate()
AND r.program_name <> 'Microsoft® Windows® Operating System'
ORDER BY collection_Time desc, [TimeInMinutes] desc
GO