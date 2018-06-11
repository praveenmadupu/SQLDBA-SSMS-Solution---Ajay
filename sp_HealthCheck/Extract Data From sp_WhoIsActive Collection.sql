use dba;
;with tQueries AS
(
	select [TimeInMinutes] = (cast(LEFT([dd hh:mm:ss.mss],2) as int) * 24 * 60)
			+ (cast(SUBSTRING([dd hh:mm:ss.mss],4,2) as int) * 60)
			+ cast(SUBSTRING([dd hh:mm:ss.mss],7,2) as int)
			,*
			,DENSE_RANK()OVER(ORDER BY collection_time ASC) AS CollectionBatchNO
	from [dbo].[WhoIsActive_ResultSets] as r
	where (r.collection_time >= '2018-06-06 14:30:00.000' and r.collection_time <= '2018-06-06 22:50:00.000')
)
select	CollectionBatchNO, collection_time, TimeInMinutes, [dd hh:mm:ss.mss], [dd hh:mm:ss.mss (avg)], session_id, sql_text, sql_command, login_name, wait_info, tasks, tran_log_writes, CPU, tempdb_allocations, tempdb_current, blocking_session_id, blocked_session_count, reads, writes, context_switches, physical_io, physical_reads, locks, used_memory, status, tran_start_time, open_tran_count, percent_complete, host_name, database_name, program_name, additional_info, start_time, login_time, request_id 
from tQueries
where  blocking_session_id IS NOT NULL OR blocked_session_count <> 0
order by collection_time ASC