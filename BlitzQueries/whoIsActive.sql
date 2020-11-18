EXEC sp_WhoIsActive @get_outer_command = 1, @get_avg_time=1 ,@get_task_info=2
					--,@find_block_leaders=1 , @get_additional_info=1, 
					--,@get_transaction_info=1 , @get_task_info=2, @get_additional_info=1, 	
					--,@get_full_inner_text=1
					--,@get_locks=1
					--,@get_plans=1
					--,@sort_order = '[CPU] DESC'					
					--,@filter_type = 'login' ,@filter = 'Lab\adwivedi'
					--,@filter_type = 'program' ,@filter = 'Test-Parallelism.py'
					--,@filter_type = 'database' ,@filter = 'facebook'
					--,@sort_order = '[reads] desc'

--kill 814 with statusonly
					
					
/*
EXEC sp_WhoIsActive @filter_type = 'login' ,@filter = 'Lab\adwivedi'
					,@output_column_list = '[session_id][percent_complete][sql_text][login_name][wait_info][blocking_session_id][start_time]'

EXEC sp_WhoIsActive @filter_type = 'session' ,@filter = '174'
					,@output_column_list = '[session_id][percent_complete][sql_text][login_name][wait_info][blocking_session_id][start_time]'

--	EXEC sp_WhoIsActive @destination_table = 'DBA.dbo.WhoIsActive_ResultSets'

*/

/* Begin Code to find Resource Pool Scheduler Affinity */
if OBJECT_ID('tempdb..#resource_pool') is not null	drop table #resource_pool;
if OBJECT_ID('tempdb..#temp') is not null	drop table #temp;

create table #resource_pool (rpoolname sysname, scheduler_id int, cpu_id int);
create table #temp (name sysname, pool_id int, scheduler_mask bigint);

insert into #temp
select rp.name,rp.pool_id,pa.scheduler_mask 
from sys.dm_resource_governor_resource_pools rp 
join sys.resource_governor_resource_pool_affinity pa on rp.pool_id=pa.pool_id
where rp.pool_id>2;

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

insert into #resource_pool
select 'REST' as rpoolname, dos.scheduler_id,dos.cpu_id from sys.dm_os_schedulers dos
left join #resource_pool rpl on dos.scheduler_id = rpl.scheduler_id 
where rpl.scheduler_id is NULL and dos.status = 'VISIBLE ONLINE';

/* End Code to find Resource Pool Scheduler Affinity */

declare @object_name varchar(255);
set @object_name = (case when @@SERVICENAME = 'MSSQLSERVER' then 'SQLServer' else 'MSSQL$'+@@SERVICENAME end);
SELECT /* counter that require Fraction & Base */
		'Resource Pool CPU %' as RunningQuery,
		rtrim(fr.instance_name) as [Pool], 
		[% CPU] = convert(numeric(20,1),case when bs.cntr_value <> 0 then (100*((fr.cntr_value*1.0)/bs.cntr_value)) else fr.cntr_value end),
		[% Assigned Schedulers] = convert(numeric(20,1),((rp.Scheduler_Count*1.0)/(select count(1) as cpu_counts from sys.dm_os_schedulers as dos where dos.status = 'VISIBLE ONLINE'))*100),
		[Assigned Schedulers] = rp.Scheduler_Count		
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
	and fr.cntr_value > 0.0
	and
	(
		( fr.[object_name] like (@object_name+':Resource Pool Stats%') and fr.counter_name like 'CPU usage %' )
	)
ORDER BY [% CPU] desc;
go


DECLARE @pool_name sysname --= 'REST';
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

--OUTER APPLY (	SELECT * FROM #resource_pool AS rp WHERE rp.rpoolname = rtrim(fr.instance_name)	) as rp

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

