EXEC sp_WhoIsActive @get_full_inner_text=1, @get_transaction_info=1, @get_task_info=2, @get_locks=1, @get_avg_time=1, @get_additional_info=1,@find_block_leaders=1, @get_outer_command =1	
					,@get_plans=2
					--,@filter_type = 'session' ,@filter = '325'
					--,@filter_type = 'login' ,@filter = 'Corporate\RichMedia'
					--,@sort_order = '[reads] desc'

--	EXEC sp_WhoIsActive @help = 1;

EXEC sp_healthcheck @p_getExecutionPlan = 1;
--	EXEC [dbo].[sp_HealthCheck] '?'

/*
$instance = 'ANN1VESPDB01';
$excelPath = "C:\Temp\$instance.xlsx";
$sqlQuery = @" 
exec sp_whoIsActive @get_plans=1, @get_full_inner_text=1, 
                    @get_transaction_info=1, @get_task_info=2, 
                    @get_locks=1, @get_avg_time=1, @get_additional_info=1,
                    @find_block_leaders=1
"@;

Invoke-Sqlcmd -ServerInstance $instance -Query $sqlQuery | Export-Excel $excelPath -Show;
*/

/*

select r.*, th.threads, st.text
		,SUBSTRING(st.text, (r.statement_start_offset/2)+1,   
        ((CASE r.statement_end_offset  
          WHEN -1 THEN DATALENGTH(st.text)  
         ELSE r.statement_end_offset  
         END - r.statement_start_offset)/2) + 1) AS statement_text 
		-- ,qp.query_plan 		
		--,cast(tp.query_plan as xml) as statement_query_plan

from sys.dm_exec_requests as r 
	outer apply sys.dm_exec_sql_text(r.sql_handle) as st
	OUTER APPLY (select count(*) as threads from sys.dm_os_tasks as t where t.session_id = r.session_id) as th
	--outer apply sys.dm_exec_query_plan(r.plan_handle) as qp 
	--OUTER APPLY sys.dm_exec_text_query_plan(r.plan_handle, r.statement_start_offset, r.statement_end_offset) as tp
	--where r.session_id = 325

select * from sys.dm_os_tasks as t
	--where t.session_id = 325
*/

/*
https://www.brentozar.com/archive/2014/11/many-cpus-parallel-query-using-sql-server/
*/

/*
SELECT  top 10 [srvName] = @@servername, DENSE_RANK()OVER(ORDER BY collection_Time ASC) AS CollectionBatch, [collection_time], [TimeInMinutes], [dd hh:mm:ss.mss], [session_id], [sql_text], [sql_command], [login_name], 
		[wait_info], [tasks], [tran_log_writes], [CPU], [tempdb_allocations], [tempdb_current], [blocking_session_id], 
		[blocked_session_count], [reads], [writes], [context_switches], [physical_io], [physical_reads], [query_plan], [locks], 
		[used_memory], [status], [tran_start_time], [open_tran_count], [percent_complete], [host_name], [database_name], [program_name], 
		[additional_info], [start_time], [login_time], [request_id]
		--sql_handle = additional_info.value('(/additional_info/sql_handle)[1]','varchar(500)'),			
		,query_hash = query_plan.value('(/ShowPlanXML/BatchSequence/Batch/Statements/StmtSimple/@QueryHash)[1]', 'varchar(500)')
		,query_plan_hash = query_plan.value('(/ShowPlanXML/BatchSequence/Batch/Statements/StmtSimple/@QueryPlanHash)[1]', 'varchar(500)')
FROM [DBA].[dbo].WhoIsActive_ResultSets AS r
WHERE (CASE WHEN REPLACE(REPLACE(TRY_CONVERT(varchar(max),r.sql_text),char(10),''),char(13),'') like '%INSERT INTO DBO.![DELTA!_Music!_%'  ESCAPE '!' THEN 1 ELSE 0 END) = 1
AND (CASE WHEN REPLACE(REPLACE(TRY_CONVERT(varchar(max),r.sql_text),char(10),''),char(13),'') like '%SELECT N.![ReleaseID!], N.![AlbumID!], N.![MediaFormatAttributeID!], N.![ProductFormAttributeID!]%'  ESCAPE '!' THEN 1 ELSE 0 END) = 1
--AND [database_name] LIKE 'RoviMusicShipping_UK_%'
ORDER BY [TimeInMinutes] desc
GO
*/