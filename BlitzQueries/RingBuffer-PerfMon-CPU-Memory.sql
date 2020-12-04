--	https://www.sqlskills.com/blogs/jonathan/identifying-external-memory-pressure-with-dm_os_ring_buffers-and-ring_buffer_resource_monitor/
--exec sp_WhoIsActive

;with t_PerfMon as
(
	--Total amount of RAM consumed by database data (Buffer Pool). This should be the highest usage of Memory on the server.
	Select SQLBufferPoolUsedMemoryMB = (Select SUM(pages_kb)/1024 AS [SPA Mem, Mb] FROM sys.dm_os_memory_clerks WITH (NOLOCK) Where type = 'MEMORYCLERK_SQLBUFFERPOOL')
		   --Total amount of RAM used by SQL Server memory clerks (includes Buffer Pool)
		   , SQLAllMemoryClerksUsedMemoryMB = (Select SUM(pages_kb)/1024 AS [SPA Mem, Mb] FROM sys.dm_os_memory_clerks WITH (NOLOCK))
		   --How long in seconds since data was removed from the Buffer Pool, to be replaced with data from disk. (Key indicator of memory pressure when below 300 consistently)
		   ,[PageLifeExpectancy] = (SELECT cntr_value FROM sys.dm_os_performance_counters WITH (NOLOCK) WHERE [object_name] LIKE N'%Buffer Manager%' AND counter_name = N'Page life expectancy' )
		   --How many memory operations are Pending (should always be 0, anything above 0 for extended periods of time is a very high sign of memory pressure)
		   ,[MemoryGrantsPending] = (SELECT cntr_value FROM sys.dm_os_performance_counters WITH (NOLOCK) WHERE [object_name] LIKE N'%Memory Manager%' AND counter_name = N'Memory Grants Pending' )
		   --How many memory operations are Outstanding (should always be 0, anything above 0 for extended periods of time is a very high sign of memory pressure)
		   ,[MemoryGrantsOutstanding] = (SELECT cntr_value FROM sys.dm_os_performance_counters WITH (NOLOCK) WHERE [object_name] LIKE N'%Memory Manager%' AND counter_name = N'Memory Grants Outstanding' )
)
select  convert(datetime,sysutcdatetime()) as [Current-Time-UTC], 'Memory-Status' as RunningQuery, [MemoryGrantsPending] as [**M/r-Grants-Pending**], [PageLifeExpectancy],
		cast(sm.total_physical_memory_kb * 1.0 / 1024 / 1024 as numeric(20,0)) as SqlServer_Process_memory_gb, 
		cast(sm.available_physical_memory_kb * 1.0 / 1024 / 1024 as numeric(20,2)) as available_physical_memory_gb, 
		cast((sm.total_page_file_kb - sm.available_page_file_kb) * 1.0 / 1024 / 1024 as numeric(20,0)) as used_page_file_gb,
		cast(sm.system_cache_kb * 1.0 / 1024 /1024 as numeric(20,2)) as system_cache_gb, 
		cast((sm.available_physical_memory_kb - sm.system_cache_kb) * 1.0 / 1024 as numeric(20,2)) as free_memory_mb,
		cast(page_fault_count*8.0/1024/1024 as decimal(20,2)) as page_fault_gb,
		[MemoryGrantsOutstanding], SQLBufferPoolUsedMemoryMB, SQLAllMemoryClerksUsedMemoryMB
from sys.dm_os_sys_memory as sm
full outer join sys.dm_os_process_memory as pm on 1 = 1
full outer join t_PerfMon as pfm on 1 = 1;


SELECT	--top 3 
		--[Event-Duration] = master.dbo.time2duration(EventTime,'datetime'), 
		convert(datetime,sysutcdatetime()) as [Current-Time-UTC], 'CPU-Ring-Buffer' as RunningQuery,
		EventTime,  (select cast(count(IIF(dos.status = 'VISIBLE ONLINE','sql',NULL)) as varchar)+' / '+cast(count(IIF(dos.status IN ('VISIBLE ONLINE','VISIBLE OFFLINE'),'all',NULL)) as varchar) from sys.dm_os_schedulers dos) as [Allocated-Schedulers],
		CASE WHEN system_cpu_utilization_post_sp2 IS NOT NULL THEN system_cpu_utilization_post_sp2 ELSE system_cpu_utilization_pre_sp2 END AS system_cpu_utilization,  
		CASE WHEN sql_cpu_utilization_post_sp2 IS NOT NULL THEN sql_cpu_utilization_post_sp2 ELSE sql_cpu_utilization_pre_sp2 END AS sql_cpu_utilization 
FROM  (	SELECT	record.value('(Record/@id)[1]', 'int') AS record_id,
				DATEADD (ms, -1 * (ts_now - [timestamp]), GETDATE()) AS EventTime,
				100-record.value('(Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') AS system_cpu_utilization_post_sp2, 
				record.value('(Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') AS sql_cpu_utilization_post_sp2,
				100-record.value('(Record/SchedluerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') AS system_cpu_utilization_pre_sp2,
				record.value('(Record/SchedluerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') AS sql_cpu_utilization_pre_sp2
		FROM (	SELECT	timestamp, CONVERT (xml, record) AS record, cpu_ticks / (cpu_ticks/ms_ticks) as ts_now
				FROM sys.dm_os_ring_buffers cross apply sys.dm_os_sys_info
				WHERE ring_buffer_type = 'RING_BUFFER_SCHEDULER_MONITOR'
				AND record LIKE '%<SystemHealth>%'
			 ) AS t 
	  ) AS t
WHERE EventTime >= DATEADD(minute,-20,getdate())
ORDER BY EventTime desc;


/* Begin Code to find Resource Pool Scheduler Affinity */
set nocount on;
if OBJECT_ID('tempdb..#resource_pool') is not null	drop table #resource_pool;
if OBJECT_ID('tempdb..#temp') is not null	drop table #temp;

create table #resource_pool (rpoolname sysname, scheduler_id int, cpu_id int);
create table #temp (name sysname, pool_id int, scheduler_mask bigint);

insert into #temp
select rp.name,rp.pool_id,pa.scheduler_mask 
from sys.dm_resource_governor_resource_pools rp 
left join sys.resource_governor_resource_pool_affinity pa on rp.pool_id=pa.pool_id
where rp.pool_id>2;

--select * from #temp

if not exists (select * from #temp where scheduler_mask is not null)
	print 'WARNING: No Scheduler Affinity Defined';
else
begin
	while((select count(1) from #temp) > 0)
	Begin
	declare @intvalue numeric,@rpoolname sysname
	declare @vsresult varchar(64)
	declare @inti numeric
	DECLARE @counter int=0
	select @inti = 64, @vsresult = ''
	select top 1 @intvalue = scheduler_mask,@rpoolname = name from #temp
	while @inti>0
	  begin
	  if(@intvalue %2 =1)
	  BEGIN
		insert into #resource_pool(rpoolname,scheduler_id) values(@rpoolname,@counter)
	  END
		select @intvalue = convert(bigint, (@intvalue / 2)), @inti=@inti-1
		set @counter = @counter+1
	  end
	  delete from #temp where name= @rpoolname
	End

	update rpl
	set rpl.cpu_id = dos.cpu_id
	from sys.dm_os_schedulers dos inner join #resource_pool rpl
	on dos.scheduler_id=rpl.scheduler_id
end

-- Insert schedulers NOT assigned to Any Pool, and still utilized by SQL Server
insert into #resource_pool
select 'REST' as rpoolname, dos.scheduler_id,dos.cpu_id 
from sys.dm_os_schedulers dos
left join #resource_pool rpl on dos.scheduler_id = rpl.scheduler_id 
where rpl.scheduler_id is NULL and dos.status = 'VISIBLE ONLINE';
--select * from #resource_pool

/* End Code to find Resource Pool Scheduler Affinity */


declare @object_name varchar(255);
set @object_name = (case when @@SERVICENAME = 'MSSQLSERVER' then 'SQLServer' else 'MSSQL$'+@@SERVICENAME end);
;WITH T_Pools AS (
	SELECT /* counter that require Fraction & Base */
			'Resource Pool CPU %' as RunningQuery,
			rtrim(fr.instance_name) as [Pool], 
			[% CPU @Server-Level] = convert(numeric(20,1),case when bs.cntr_value <> 0 then (100*((fr.cntr_value*1.0)/bs.cntr_value)) else fr.cntr_value end),		
			[% Schedulers@Total] = case when rp.Scheduler_Count <> 0 then convert(numeric(20,1),((rp.Scheduler_Count*1.0)/(select count(1) as cpu_counts from sys.dm_os_schedulers as dos where dos.status IN ('VISIBLE ONLINE','VISIBLE OFFLINE')))*100) else NULL end,	
			[% Schedulers@Sql] = case when rp.Scheduler_Count <> 0 then convert(numeric(20,1),((rp.Scheduler_Count*1.0)/(select count(1) as cpu_counts from sys.dm_os_schedulers as dos where dos.status = 'VISIBLE ONLINE'))*100) else NULL end,	
			[Assigned Schedulers] = case when rp.Scheduler_Count <> 0 then rp.Scheduler_Count else null end
	FROM sys.dm_os_performance_counters as fr
	OUTER APPLY
		(	SELECT * FROM sys.dm_os_performance_counters as bs 
			WHERE bs.cntr_type = 1073939712 /* PERF_LARGE_RAW_BASE  */ 
			AND bs.[object_name] = fr.[object_name] 
			AND (	REPLACE(LOWER(RTRIM(bs.counter_name)),' base','') = REPLACE(LOWER(RTRIM(fr.counter_name)),' ratio','')
				OR
				REPLACE(LOWER(RTRIM(bs.counter_name)),' base','') = LOWER(RTRIM(fr.counter_name))
				)
			AND bs.instance_name = fr.instance_name
		) as bs
	OUTER APPLY (	SELECT COUNT(*) as Scheduler_Count FROM #resource_pool AS rp WHERE rp.rpoolname = rtrim(fr.instance_name)	) as rp
	WHERE fr.cntr_type = 537003264 /* PERF_LARGE_RAW_FRACTION */
		--and fr.cntr_value > 0.0
		and
		(
			( fr.[object_name] like (@object_name+':Resource Pool Stats%') and fr.counter_name like 'CPU usage %' )
		)
)
SELECT RunningQuery, convert(datetime,sysutcdatetime()) as [Current-Time-UTC], [Pool], 
		[% CPU @Pool-Level] = CASE WHEN [Assigned Schedulers] IS NULL THEN NULL WHEN [% Schedulers@Total] <> 0 THEN CONVERT(NUMERIC(20,2),([% CPU @Server-Level]*100.0)/[% Schedulers@Total]) ELSE [% CPU @Server-Level] END,
		[% CPU @Server-Level], [% Schedulers@Total],		
		[% Schedulers@Sql], [Assigned Schedulers]
FROM T_Pools
WHERE NOT ([Assigned Schedulers] IS NULL AND [% CPU @Server-Level] = 0)
ORDER BY [% CPU @Pool-Level] desc, [% CPU @Server-Level] desc --, [% CPU @Pool-Level] desc;
go

--SELECT scheduler_id,count(*) FROM #resource_pool AS rp group by scheduler_id

DECLARE @pool_name sysname = 'REST';
IF (SELECT count(distinct rpoolname) FROM #resource_pool) < 2
	SET @pool_name = NULL;
;WITH T_Requests AS 
(
	SELECT [Pool], s.program_name, r.session_id, r.request_id
	FROM  sys.dm_exec_requests r
	JOIN	sys.dm_exec_sessions s ON s.session_id = r.session_id
	OUTER APPLY
		(	select rgrp.name as [Pool]
			from sys.resource_governor_workload_groups rgwg 
			join sys.resource_governor_resource_pools rgrp ON rgwg.pool_id = rgrp.pool_id
			where rgwg.group_id = s.group_id
		) rp
	WHERE s.is_user_process = 1	
		AND login_name NOT LIKE '%sqlexec%'
		AND (@pool_name is null or [Pool] = @pool_name )
)
,T_Programs_Tasks_Total AS
(
	SELECT	[Pool], r.program_name,
			[active_request_counts] = COUNT(*),
			[num_tasks] = SUM(t.tasks)
	FROM  T_Requests as r
	OUTER APPLY (	select count(*) AS tasks, count(distinct t.scheduler_id) as schedulers 
								from sys.dm_os_tasks t where r.session_id = t.session_id and r.request_id = t.request_id
							) t
	GROUP  BY [Pool], r.program_name
)
,T_Programs_Schedulers AS
(
	SELECT [Pool], r.program_name, [num_schedulers] = COUNT(distinct t.scheduler_id)
	FROM T_Requests as r
	JOIN sys.dm_os_tasks t
		ON t.session_id = r.session_id AND t.request_id = r.request_id
	GROUP BY [Pool], program_name
)
SELECT RunningQuery = (COALESCE(@pool_name,'ALL')+'-POOL/')+'Active Requests/program',
		ptt.[Pool],
		ptt.program_name, ptt.active_request_counts, ptt.num_tasks, ps.num_schedulers, 
		[scheduler_percent] = case when @pool_name is not null then Floor(ps.num_schedulers * 100.0 / rp.Scheduler_Count)
									else Floor(ps.num_schedulers * 100.0 / (select count(*) from sys.dm_os_schedulers as os where os.status = 'VISIBLE ONLINE'))
									end
FROM	T_Programs_Tasks_Total as ptt
JOIN	T_Programs_Schedulers as ps
	ON ps.program_name = ptt.program_name
OUTER APPLY (	SELECT COUNT(*) as Scheduler_Count FROM #resource_pool AS rp WHERE rp.rpoolname = ptt.[Pool]	) as rp
ORDER  BY [scheduler_percent] desc, active_request_counts desc, [num_tasks] desc;
GO


/*
DECLARE @pool_name sysname = 'REST';
SELECT	[RunningQuery] = COALESCE(@pool_name+'-','ALL-')+'Pool/Schedulers/Program',
				des.program_name,
        [schedulers_used] = COUNT(DISTINCT der.scheduler_id),
        [schedulers_used_percent] = FLOOR(COUNT(DISTINCT der.scheduler_id)*100.0/(SELECT COUNT(1) FROM sys.dm_os_schedulers WHERE status = 'VISIBLE ONLINE'))
FROM sys.dm_exec_sessions des
INNER JOIN sys.dm_exec_requests der ON des.session_id = der.session_id
INNER JOIN sys.resource_governor_workload_groups rgwg ON des.group_id = rgwg.group_id
INNER JOIN sys.resource_governor_resource_pools rgrp ON rgwg.pool_id = rgrp.pool_id
WHERE des.is_user_process = 1
AND des.login_name NOT LIKE '%sqlexec%'
AND (@pool_name is null or rgrp.name = @pool_name )
GROUP BY des.program_name
ORDER BY schedulers_used_percent DESC;
GO
*/

/*
SELECT	[RunningQuery] = 'Active Request/login',
				s.login_name,
				[active_request_counts] = COUNT(*),
				[num_schedulers] = Count(distinct r.scheduler_id),
				[num_tasks] = SUM(t.tasks),
				[scheduler_percent] = Floor(Count(distinct r.scheduler_id) * 100.0 / (select count(*) from sys.dm_os_schedulers as os where os.status = 'VISIBLE ONLINE'))
FROM   sys.dm_exec_requests r
JOIN sys.dm_exec_sessions s ON s.session_id = r.session_id
OUTER APPLY (select count(*) AS tasks from sys.dm_os_tasks t where r.session_id = t.session_id and r.request_id = t.request_id) t
WHERE  s.is_user_process = 1
       AND login_name NOT LIKE '%sqlexec%'
GROUP  BY s.login_name
ORDER  BY num_schedulers desc, [scheduler_percent] desc, active_request_counts desc ,[num_tasks] desc;



IF (SELECT count(distinct rpoolname) FROM #resource_pool) < 2 /* When Scheduler Affinity is not set for Resource Governor Pool */
BEGIN
	SELECT	[RunningQuery] = 'Active Request/Program',
					s.program_name,
					[active_request_counts] = COUNT(*),
					[num_schedulers] = Count(distinct r.scheduler_id),
					[num_tasks] = SUM(t.tasks),
					[scheduler_percent] = Floor(Count(distinct r.scheduler_id) * 100.0 / (select count(*) from sys.dm_os_schedulers as os where os.status = 'VISIBLE ONLINE'))
	FROM   sys.dm_exec_requests r
	JOIN sys.dm_exec_sessions s ON s.session_id = r.session_id
	OUTER APPLY (select count(*) AS tasks from sys.dm_os_tasks t where r.session_id = t.session_id and r.request_id = t.request_id) t
	WHERE  s.is_user_process = 1
		   AND login_name NOT LIKE '%sqlexec%'
	GROUP  BY s.program_name
	ORDER  BY num_schedulers desc, [scheduler_percent] desc, active_request_counts desc ,[num_tasks] desc;
END
*/

/*
select *
from sys.dm_exec_sessions es
join sys.dm_exec_requests er
on er.session_id = es.session_id
where login_name = ''

select rgwg.*, rgrp.*
from sys.resource_governor_workload_groups rgwg
join sys.resource_governor_resource_pools rgrp ON rgwg.pool_id = rgrp.pool_id
*/

