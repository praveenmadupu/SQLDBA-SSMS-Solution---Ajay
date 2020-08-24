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



select master.dbo.local2utc(collection_time) as time, counter_name, cntr_value
from DBA.dbo.dm_os_performance_counters as pc
where 1 = 1
and pc.object_name = 'SQLServer:Memory Manager' and counter_name not in ('Memory Grants Pending')
--and collection_time BETWEEN master.dbo.utc2local($__timeFrom()) AND master.dbo.utc2local($__timeTo())


select master.dbo.local2utc(collection_time) as time, counter_name, cntr_value
from DBA.dbo.dm_os_performance_counters as pc
where 1 = 1
and pc.object_name = 'SQLServer:SQL Statistics' --and counter_name not in ('Memory Grants Pending')
--and collection_time BETWEEN master.dbo.utc2local($__timeFrom()) AND master.dbo.utc2local($__timeTo())


select master.dbo.local2utc(collection_time) as time, counter_name, cntr_value
from DBA.dbo.dm_os_performance_counters as pc
where 1 = 1
and pc.object_name = 'SQLServer:Buffer Manager' --and counter_name not in ('Memory Grants Pending')
--and collection_time BETWEEN master.dbo.utc2local($__timeFrom()) AND master.dbo.utc2local($__timeTo())


select distinct object_name, counter_name, instance_name, cntr_type from DBA..dm_os_performance_counters as pc where object_name = 'SQLServer:Buffer Manager'

select * from sys.dm_os_performance_counters as pc 
where --object_name like 'SQLServer:Buffer Manager%'
pc.counter_name like '%available%'
--or pc.counter_name like 'SQL Compilations/sec%'
--or pc.counter_name like 'SQL Re-Compilations/sec%'
/*
SQL Attention rate
SQL Compilations/sec
SQL Re-Compilations/sec
*/

select master.dbo.local2utc(collection_time) as time, cast(free_memory_kb/1024 as decimal(20,2)) as [Available Mbytes], used_page_file_mb = cast(used_page_file_kb*1024 as decimal(30,2))
from DBA.[dbo].[dm_os_sys_memory]
where 1 = 1
--and collection_time BETWEEN master.dbo.utc2local($__timeFrom()) AND master.dbo.utc2local($__timeTo())
--and server_name = $server_name
order by time asc


select rtrim(object_name) as object_name, rtrim(counter_name) as counter_name, rtrim(instance_name) as instance_name, rtrim(cntr_type) as cntr_type
from sys.dm_os_performance_counters as pc
where rtrim(object_name) like 'SQLServer:Transactions%'
--
EXCEPT
--
select distinct rtrim(object_name) as object_name, rtrim(counter_name) as counter_name, rtrim(instance_name) as instance_name, rtrim(cntr_type) as cntr_type
from DBA..dm_os_performance_counters as pc
where rtrim(object_name) like 'SQLServer:Transactions'

;with t_counters as (
	select distinct top 100 rtrim(object_name) as object_name, rtrim(counter_name) as counter_name, rtrim(cntr_type) as cntr_type
	from sys.dm_os_performance_counters
	where	(	object_name like 'SQLServer:Transactions%'
			and
				( counter_name like 'Free Space in tempdb (KB)%'
				  or
				  counter_name like 'Longest Transaction Running Time%'
				  or
				  counter_name like 'Transactions%'
				  or
				  counter_name like 'Version Store Size (KB)%'
				)
			)
	order by cntr_type, object_name, counter_name
)
select		--*,
		'			or
			( [object_name] like '''+object_name+'%'' and [counter_name] like '''+counter_name+'%'' )'
from t_counters
--where cntr_type = '272696576'
order by cntr_type, object_name, counter_name


select distinct object_name, counter_name, instance_name
from dbo.dm_os_performance_counters
order by object_name, counter_name, instance_name


SELECT --TOP(5)
		--@current_time as collection_time,
		[type] AS memory_clerk,
		SUM(pages_kb) / 1024 AS size_mb
--INTO DBA..dm_os_memory_clerks
FROM sys.dm_os_memory_clerks WITH (NOLOCK)
GROUP BY [type]
HAVING (SUM(pages_kb) / 1024) > 0
ORDER BY SUM(pages_kb) DESC

SELECT  *
FROM sys.dm_os_memory_cache_counters
order by pages_kb DESC

SELECT objtype, cacheobjtype, 
  AVG(usecounts) AS Avg_UseCount, 
  SUM(refcounts) AS AllRefObjects, 
  SUM(CAST(size_in_bytes AS bigint))/1024/1024 AS Size_MB
FROM sys.dm_exec_cached_plans
--WHERE objtype = 'Adhoc' AND usecounts = 1
GROUP BY objtype, cacheobjtype;

-- ====================================================================================================

select top 1 collection_time as time
        ,available_physical_memory_gb*1024 as decimal(20,0)) as available_physical_memory 
from DBA.[dbo].[dm_os_sys_memory]
order by collection_time desc

-- ====================================================================================================
DECLARE @server_name varchar(256);
set @server_name = 'MSI';

select collection_time as time, (available_physical_memory_kb*1.0)/1024 as [Available Memory]
from DBA.[dbo].[dm_os_sys_memory]
where 1 = 1
--and collection_time BETWEEN master.dbo.utc2local($__timeFrom()) AND master.dbo.utc2local($__timeTo())
--and pc.server_name = @server_name
order by collection_time desc

-- ====================================================================================================

select *
from DBA..WhoIsActive_ResultSets as r
where r.collection_time >= '2020-08-23 16:30:00'

-- ====================================================================================================

DECLARE @server_name varchar(256);
set @server_name = 'MSI';

select master.dbo.local2utc(pc.collection_time) as time, pc.cntr_value as [Available MBytes]
from DBA.dbo.dm_os_performance_counters_nonsql as pc
where 1 = 1
--and collection_time BETWEEN master.dbo.utc2local($__timeFrom()) AND master.dbo.utc2local($__timeTo())
and pc.server_name = @server_name
and pc.[object_name] = 'Memory'
and pc.counter_name = 'Available MBytes'

-- ====================================================================================================

DECLARE @server_name varchar(256);
set @server_name = 'MSI';

select master.dbo.local2utc(pc.collection_time) as time, pc.counter_name, CEILING(pc.cntr_value) as cntr_value
from DBA.dbo.dm_os_performance_counters_nonsql as pc
where 1 = 1
--and collection_time BETWEEN master.dbo.utc2local($__timeFrom()) AND master.dbo.utc2local($__timeTo())
and pc.server_name = @server_name
and pc.[object_name] = 'Memory'
and pc.counter_name in ('Pages Input/Sec','Pages/Sec')

-- ====================================================================================================

DECLARE @server_name varchar(256);
set @server_name = 'MSI';

select master.dbo.local2utc(pc.collection_time) as time, pc.counter_name, CEILING(pc.cntr_value) as cntr_value
from DBA.dbo.dm_os_performance_counters_nonsql as pc
where 1 = 1
--and collection_time BETWEEN master.dbo.utc2local($__timeFrom()) AND master.dbo.utc2local($__timeTo())
and pc.server_name = @server_name
and pc.[object_name] = 'Paging File'
and pc.counter_name in ('% Usage','% Usage Peak')


select top 1 collection_time as time, (available_physical_memory_kb*1.0)/1024 as [Available Memory]
from DBA.[dbo].[dm_os_sys_memory]
order by collection_time desc

-- ====================================================================================================

DECLARE @server_name varchar(256);
set @server_name = 'MSI';

select master.dbo.local2utc(pc.collection_time) as time, pc.instance_name + '\ --- ' + pc.counter_name as instance_name, CAST(pc.cntr_value AS FLOAT) as cntr_value
from DBA.dbo.dm_os_performance_counters_nonsql as pc
where 1 = 1
--and collection_time BETWEEN master.dbo.utc2local($__timeFrom()) AND master.dbo.utc2local($__timeTo())
and collection_time >= DATEADD(hour,-2,getdate())
and pc.server_name = @server_name
and pc.[object_name] = 'LogicalDisk'
and counter_name in ('Avg. Disk sec/Read','Avg. Disk sec/Write')
and ( instance_name <> '_Total' and instance_name not like 'HarddiskVolume%' )

-- ====================================================================================================

select * from [dbo].[CounterDetails]
where ObjectName = 'Network Interface' 
and CounterName in ('Avg. Disk sec/Read','Avg. Disk sec/Write')

-- ====================================================================================================

DECLARE @server_name varchar(256);
set @server_name = 'MSI';

select --master.dbo.local2utc(pc.collection_time) as time, pc.instance_name + '\ --- ' + pc.counter_name as instance_name, CAST(pc.cntr_value AS FLOAT) as cntr_value
		*
from DBA.dbo.dm_os_performance_counters_nonsql as pc
where 1 = 1
--and collection_time BETWEEN master.dbo.utc2local($__timeFrom()) AND master.dbo.utc2local($__timeTo())
and collection_time >= DATEADD(hour,-2,getdate())
and pc.server_name = @server_name
and pc.[object_name] = 'LogicalDisk'
and counter_name in ('Disk Bytes/sec')
and ( instance_name <> '_Total' and instance_name not like 'HarddiskVolume%' )


-- ====================================================================================================

DECLARE @server_name varchar(256);
set @server_name = 'MSI';

select master.dbo.local2utc(pc.collection_time) as time, pc.instance_name --+ '\ --- ' + pc.counter_name as instance_name
		,CAST(pc.cntr_value AS FLOAT) as cntr_value
		--*
from DBA.dbo.dm_os_performance_counters_nonsql as pc
where 1 = 1
--and collection_time BETWEEN master.dbo.utc2local($__timeFrom()) AND master.dbo.utc2local($__timeTo())
and collection_time >= DATEADD(hour,-2,getdate())
and pc.server_name = @server_name
and pc.[object_name] = 'Network Interface'
and counter_name in ('Bytes Total/sec')
--and ( instance_name <> '_Total' and instance_name not like 'HarddiskVolume%' )

-- ====================================================================================================
GO


--set nocount on;
DECLARE @server_name varchar(256);
DECLARE @start_time datetime2;
DECLARE @end_time datetime2;
set @server_name = 'MSI';
--set @start_time = master.dbo.utc2local($__timeFrom());
--set @end_time = master.dbo.utc2local($__timeTo());
set @start_time = DATEADD(MINUTE,-60,GETDATE());
set @end_time = GETDATE();

DECLARE @verbose bit = 1;

if OBJECT_ID('tempdb..#wait_stats_range') is not null
	drop table tempdb..#wait_stats_range;

select IDENTITY(int,1,1) as id, point_in_time
into #wait_stats_range
from (
	SELECT si.wait_stats_cleared_time as point_in_time
	FROM [DBA].dbo.dm_os_sys_info AS si
	WHERE 1 = 1
	--and pc.server_name = @server_name
	and collection_time BETWEEN @start_time AND @end_time
	--
	union
	--
	select point_in_time
	from (values (@start_time),(@end_time)) Times (point_in_time)
) as ranges
order by point_in_time asc;

if @verbose = 1
begin
	select * from #wait_stats_range;
	select count(distinct CollectionTime) as WaitStats_Sample_Counts from [DBA].[dbo].[WaitStats];
end

IF OBJECT_ID('tempdb..#Wait_Stats_Delta') IS NOT NULL
	DROP TABLE #Wait_Stats_Delta;
CREATE TABLE #Wait_Stats_Delta
(
	--[id_T1] [int] NULL,
	--[id_T2] [int] NULL,
	--[point_in_time_T1] [datetime2](7) NULL,
	--[point_in_time_T2] [datetime2](7) NULL,
	--[CollectionTime_T1] [datetime2](7) NULL,
	--[CollectionTime_T2] [datetime2](7) NULL,
	[CollectionTime] [datetime2](7) NOT NULL,
	[CollectionTime_Duration_Seconds] [int] NOT NULL,
	[WaitType] [nvarchar](120) NOT NULL,
	[Wait_S] [decimal](15, 2) NULL,
	[Resource_S] [decimal](15, 2) NULL,
	[Signal_S] [decimal](15, 2) NULL,
	[WaitCount] [bigint] NULL,
	[Percentage] [decimal](10,2) NULL,
	[AvgWait_S] [decimal](35, 22) NULL,
	[AvgRes_S] [decimal](35, 22) NULL,
	[AvgSig_S] [decimal](35, 22) NULL
)

declare @l_id int
		,@l_point_in_time datetime2
		,@l_counter int = 1
		,@l_counter_max int;

select @l_counter_max = max(id) from #wait_stats_range;

--if @verbose = 1
--	select [@l_counter] = @l_counter, [@l_counter_max] = @l_counter_max;

while @l_counter < @l_counter_max -- execute for N-1 times
begin
	select @l_point_in_time = point_in_time from #wait_stats_range as tr where tr.id = @l_counter;

	if @verbose = 1
		select [@l_counter] = @l_counter, [@l_counter_max] = @l_counter_max,  [@l_point_in_time] = @l_point_in_time;

	;WITH T1 AS (
		select id = tr.id, point_in_time = tr.point_in_time, 
				ws.CollectionTime, 
				ws.WaitType, 
				ws.Wait_S, ws.Resource_S, ws.Signal_S, ws.WaitCount, ws.Percentage, ws.AvgWait_S, ws.AvgRes_S, ws.AvgSig_S
		from [DBA].[dbo].[WaitStats] AS ws with (nolock) full join #wait_stats_range as tr on tr.id = @l_counter
		where ws.CollectionTime = (	case when @l_counter = 1
										 then (select MIN(wsi.CollectionTime) as CollectionTime from [DBA].[dbo].[WaitStats] as wsi with (nolock) where wsi.CollectionTime >= tr.point_in_time)
										 when @l_counter = @l_counter_max - 1
										 then ISNULL( (select MIN(wsi.CollectionTime) as CollectionTime from [DBA].[dbo].[WaitStats] as wsi with (nolock) where wsi.CollectionTime >= tr.point_in_time),
												(select MAX(wsi.CollectionTime) as CollectionTime from [DBA].[dbo].[WaitStats] as wsi with (nolock) where wsi.CollectionTime <= tr.point_in_time)
											  )
										else (select MIN(wsi.CollectionTime) as CollectionTime from [DBA].[dbo].[WaitStats] as wsi with (nolock) where wsi.CollectionTime > tr.point_in_time)
										end
								  )
	)
	,T2 AS (
		select id = tr.id, point_in_time = tr.point_in_time, 
				ws.CollectionTime, 
				ws.WaitType, 
				ws.Wait_S, ws.Resource_S, ws.Signal_S, ws.WaitCount, ws.Percentage, ws.AvgWait_S, ws.AvgRes_S, ws.AvgSig_S
		from [DBA].[dbo].[WaitStats] AS ws with (nolock) full join #wait_stats_range as tr on tr.id = @l_counter+1
		where ws.CollectionTime = (	case when @l_counter = 1 AND (@l_counter <> (@l_counter_max - 1))
										 then (select MAX(wsi.CollectionTime) as CollectionTime from [DBA].[dbo].[WaitStats] as wsi with (nolock) where wsi.CollectionTime < tr.point_in_time)
										 when @l_counter = 1 AND (@l_counter = (@l_counter_max - 1))
										 then (select MAX(wsi.CollectionTime) as CollectionTime from [DBA].[dbo].[WaitStats] as wsi with (nolock) where wsi.CollectionTime <= tr.point_in_time)
										 when @l_counter = @l_counter_max - 1
										 then (select MAX(wsi.CollectionTime) as CollectionTime from [DBA].[dbo].[WaitStats] as wsi with (nolock) where wsi.CollectionTime <= tr.point_in_time)
										else (select MAX(wsi.CollectionTime) as CollectionTime from [DBA].[dbo].[WaitStats] as wsi with (nolock) where wsi.CollectionTime < tr.point_in_time)
										end
								  )
	)
	,T_Waits_Delta AS (
		SELECT CollectionTime, CollectionTime_Duration_Seconds, WaitType, Wait_S, Resource_S, Signal_S, WaitCount,
				[Percentage],
				AvgWait_S, AvgRes_S, AvgSig_S
		--INTO tempdb..Wait_Stats_Delta
		FROM (
				SELECT --id_T1 = T1.id, id_T2 = T2.id,
						--point_in_time_T1 = T1.point_in_time, point_in_time_T2 = T2.point_in_time,
						--CollectionTime_T1 = T1.CollectionTime, CollectionTime_T2 = T2.CollectionTime,
						CollectionTime = T2.CollectionTime,
						CollectionTime_Duration_Seconds = DATEDIFF(second,T1.CollectionTime,T2.CollectionTime),
						WaitType = COALESCE(T1.WaitType,T2.WaitType),
						Wait_S = ISNULL(T2.Wait_S,0.0) - ISNULL(T1.Wait_S,0.0),
						Resource_S = ISNULL(T2.Resource_S,0.0) - ISNULL(T1.Resource_S,0.0),
						Signal_S = ISNULL(T2.Signal_S,0.0) - ISNULL(T1.Signal_S,0.0),
						WaitCount = ISNULL(T2.WaitCount,0.0) - ISNULL(T1.WaitCount,0.0),
						[Percentage] = NULL, --ISNULL(T2.Wait_S,0.0) - ISNULL(T1.Wait_S,0.0),
						AvgWait_S = CASE WHEN (ISNULL(T2.WaitCount,0.0) - ISNULL(T1.WaitCount,0.0)) = 0 THEN (ISNULL(T2.Wait_S,0.0) - ISNULL(T1.Wait_S,0.0))
										 ELSE (ISNULL(T2.Wait_S,0.0) - ISNULL(T1.Wait_S,0.0)) / (ISNULL(T2.WaitCount,0.0) - ISNULL(T1.WaitCount,0.0))
										 END,
						AvgRes_S = CASE WHEN (ISNULL(T2.WaitCount,0.0) - ISNULL(T1.WaitCount,0.0)) = 0 THEN (ISNULL(T2.Resource_S,0.0) - ISNULL(T1.Resource_S,0.0))
										ELSE (ISNULL(T2.Resource_S,0.0) - ISNULL(T1.Resource_S,0.0)) / (ISNULL(T2.WaitCount,0.0) - ISNULL(T1.WaitCount,0.0))
										END,
						AvgSig_S = CASE WHEN (ISNULL(T2.WaitCount,0.0) - ISNULL(T1.WaitCount,0.0)) = 0 THEN (ISNULL(T2.Signal_S,0.0) - ISNULL(T1.Signal_S,0.0))
										ELSE (ISNULL(T2.Signal_S,0.0) - ISNULL(T1.Signal_S,0.0)) / (ISNULL(T2.WaitCount,0.0) - ISNULL(T1.WaitCount,0.0))
										END
				FROM T1 full outer join T2 on T2.WaitType = T1.WaitType
			) as waits
		WHERE 1 = 1
			AND CollectionTime_Duration_Seconds > 0.0
			--AND Wait_S >= 0.0
	)
	INSERT #Wait_Stats_Delta
	SELECT CollectionTime, CollectionTime_Duration_Seconds, WaitType, Wait_S, Resource_S, Signal_S, WaitCount,
				[Percentage] = (Wait_S*100.0)/Total_Wait_S,
				AvgWait_S, AvgRes_S, AvgSig_S
	FROM T_Waits_Delta as d
	JOIN (select sum(i.Wait_S) as Total_Wait_S from T_Waits_Delta as i) as t ON 1 = 1
	ORDER BY Wait_S DESC;	

	set @l_counter += 1;
end

select CollectionTime as time, CollectionTime_Duration_Seconds as Duration_S, WaitType, Wait_S, Resource_S, Signal_S, WaitCount, [Percentage], AvgWait_S, AvgRes_S, AvgSig_S
from #Wait_Stats_Delta;


GO

