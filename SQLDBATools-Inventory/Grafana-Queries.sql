USE DBA
GO
-- collection_time BETWEEN master.dbo.utc2local($__timeFrom()) AND master.dbo.utc2local($__timeTo())
-- master.dbo.local2utc(collection_time) as time

SELECT /* Grafana => PLE */ top 1 page_life_expectancy FROM DBA.dbo.dm_os_performance_counters ORDER BY collection_time desc;

SELECT /* Grafana => PLE Against Time */
  master.dbo.local2utc(collection_time) as time,
  page_life_expectancy
FROM
  DBA.dbo.dm_os_performance_counters
WHERE 
  collection_time BETWEEN master.dbo.utc2local($__timeFrom()) AND master.dbo.utc2local($__timeTo())
ORDER BY
  time;

select /* Grafana => CPU Against Time */
		master.dbo.local2utc(collection_time) as time,
		system_cpu_utilization as OS,
		sql_cpu_utilization as [SqlServer]
from DBA..dm_os_ring_buffers
--where collection_time BETWEEN master.dbo.utc2local($__timeFrom()) AND master.dbo.utc2local($__timeTo())
order by time asc;

select top 1 master.dbo.local2utc(collection_time) as time,	system_cpu_utilization as CPU from DBA..dm_os_ring_buffers order by time DESC


select top 1 collection_time, cast(available_physical_memory_gb*1024 as decimal(20,0)) as available_physical_memory from [dbo].[dm_os_sys_memory] order by collection_time desc
select top 1 collection_time as time,
		available_physical_memory = case when available_physical_memory_gb >= 1.0 then cast(
from DBA.[dbo].[dm_os_sys_memory] order by collection_time desc

select top 1 collection_time as time, cast(([SQL Server Memory Usage (MB)]*1.0)/1024 as decimal(20,2)) as SqlServer_Physical_Memory_GB from DBA.[dbo].[dm_os_process_memory] order by collection_time desc
select top 1 collection_time as time, cast((page_fault_count*8.0)/1024/1024 as decimal(20,2)) as page_fault_gb from DBA.[dbo].[dm_os_process_memory] order by collection_time desc

select top 1 collection_time as time, cast((total_server_memory_mb*100.0)/target_server_memory_mb as decimal(20,0)) as sql_server_memory_utilization from DBA.[dbo].[dm_os_performance_counters] order by collection_time desc

select cast([collection_time] as smalldatetime) as [time],[dd hh:mm:ss.mss],[login_name],[wait_info],CAST(REPLACE([CPU],',','') AS BIGINT) as [CPU],CAST(REPLACE([reads],',','') AS BIGINT) as reads,CAST(REPLACE([writes],',','') AS BIGINT) as [writes],CAST(REPLACE([used_memory],',','') AS BIGINT) as [used_memory],[host_name],[database_name],[program_name],[sql_command]
from DBA.dbo.WhoIsActive_ResultSets
--where collection_time BETWEEN master.dbo.utc2local($__timeFrom()) AND master.dbo.utc2local($__timeTo())
order by [time] desc, [TimeInMinutes] desc


;with t_active_results as 
(	select collection_time, count(*) as active_requests 
	from DBA.dbo.WhoIsActive_ResultSets
	where collection_time BETWEEN master.dbo.utc2local($__timeFrom()) AND master.dbo.utc2local($__timeTo())
	group by collection_time
)
select master.dbo.local2utc(collection_time) as [time], active_requests 
from t_active_results 


;WITH T_Active_Requests AS
(
--	Query to find what's is running on server
SELECT	s.session_id
FROM	sys.dm_exec_sessions AS s
LEFT JOIN sys.dm_exec_requests AS r ON r.session_id = s.session_id
OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) AS st
OUTER APPLY sys.dm_exec_query_plan(r.plan_handle) AS bqp
OUTER APPLY sys.dm_exec_text_query_plan(r.plan_handle,r.statement_start_offset, r.statement_end_offset) as sqp
WHERE	s.session_id != @@SPID
	AND (	(CASE	WHEN	s.session_id IN (select ri.blocking_session_id from sys.dm_exec_requests as ri )
					--	Get sessions involved in blocking (including system sessions)
					THEN	1
					ELSE	0
			END) = 1
			OR
			(CASE	WHEN	s.session_id > 50
							AND r.session_id IS NOT NULL -- either some part of session has active request
							AND ISNULL(open_resultset_count,0) > 0 -- some result is open
							AND NOT (s.status = 'sleeping' AND r.status IN ('background','sleeping'))
					THEN	1
					ELSE	0
			END) = 1
			OR
			(CASE	WHEN	s.session_id > 50
							AND ISNULL(r.open_transaction_count,0) > 0
					THEN	1
					ELSE	0
			END) = 1
		)		
)
SELECT SYSUTCDATETIME() as time, COUNT(*) as Counts
FROM T_Active_Requests




;with t_page_faults as
(
	select collection_time, page_fault_count, LAG(page_fault_count) OVER ( ORDER BY collection_time ) as page_fault_count__prev
			,LAG(collection_time) OVER ( ORDER BY collection_time ) as collection_time_prev			
	from DBA.[dbo].[dm_os_process_memory] 
	--where collection_time BETWEEN master.dbo.utc2local($__timeFrom()) AND master.dbo.utc2local($__timeTo())
)
select	collection_time, page_fault_count, DATEDIFF(second,collection_time_prev,collection_time) as interval_seconds,
		page_faults_in_interval =
		(case when page_fault_count__prev is null then 0
			 when page_fault_count < page_fault_count__prev then page_fault_count
			 when page_fault_count >= page_fault_count__prev then page_fault_count-page_fault_count__prev
			 else null
			 end)
from t_page_faults
order by collection_time asc



SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET QUOTED_IDENTIFIER OFF 
DECLARE @sql varchar(max) = "
SELECT TOP 1 DATEADD(hour, DATEDIFF(hour, GETDATE(), GETUTCDATE()), date_time) AS time,
       'CPU' AS metric,
       100 - record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') AS value
       --,record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') as [System Idle],
       --,record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') as [SQL Server],
       --,100 - record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') - record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') AS [Other]
FROM   (
        SELECT   DATEADD(ms, -1 * ((SELECT cpu_ticks/(cpu_ticks/ms_ticks) FROM sys.dm_os_sys_info) - [timestamp]), GetDate()) as date_time,
                 CONVERT(xml, record) AS record
        FROM     sys.dm_os_ring_buffers
        WHERE    ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR'
        AND      record LIKE '%<SystemHealth>%'
       ) AS xml_data
ORDER BY date_time DESC;
"
SET QUOTED_IDENTIFIER ON
IF ('$Server' = SERVERPROPERTY('ServerName'))
BEGIN
  EXEC (@sql);
END;
ELSE
BEGIN
  EXEC (@sql) AT $Server;
END;



USE DBA
select collection_time as time, cntr_value as [Batch Requests/sec]
from DBA.dbo.dm_os_performance_counters
where 1 = 1
--and collection_time BETWEEN master.dbo.utc2local($__timeFrom()) AND master.dbo.utc2local($__timeTo()) 
and object_name = 'SQLServer:SQL Statistics' and counter_name in ('Batch Requests/sec')
order by time asc