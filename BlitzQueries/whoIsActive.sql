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