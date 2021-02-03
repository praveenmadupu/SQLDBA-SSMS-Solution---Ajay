declare @sql_text_fragment_filter nvarchar(200) --= 'settlement_booking_dat'
declare @get_plans bit = 1;

--	Query to find what's is running on server
;WITH T_Requests AS 
(
	select  Concat
					(
							RIGHT('00'+CAST(ISNULL((datediff(second,COALESCE(r.start_time, s.last_request_start_time),GETDATE()) / 3600 / 24), 0) AS VARCHAR(2)),2)
							,' '
							,RIGHT('00'+CAST(ISNULL(datediff(second,COALESCE(r.start_time, s.last_request_start_time),GETDATE()) / 3600  % 24, 0) AS VARCHAR(2)),2)
							,':'
							,RIGHT('00'+CAST(ISNULL(datediff(second,COALESCE(r.start_time, s.last_request_start_time),GETDATE()) / 60 % 60, 0) AS VARCHAR(2)),2)
							,':'
							,RIGHT('00'+CAST(ISNULL(datediff(second,COALESCE(r.start_time, s.last_request_start_time),GETDATE()) % 3600 % 60, 0) AS VARCHAR(2)),2)
					) as [dd hh:mm:ss]
			,s.session_id
			,st.text as sql_command
			/*
			,SUBSTRING(st.text, (r.statement_start_offset/2)+1,   
					((CASE r.statement_end_offset WHEN -1 THEN DATALENGTH(st.text)  
					ELSE r.statement_end_offset END - r.statement_start_offset)/2) + 1) AS sql_text
			*/
			,r.command as command
			,s.login_name as login_name
			,db_name(COALESCE(r.database_id,s.database_id)) as database_name
			,[program_name] = CASE	WHEN	s.program_name like 'SQLAgent - TSQL JobStep %'
					THEN	(	select	top 1 'SQL Job = '+j.name 
								from msdb.dbo.sysjobs (nolock) as j
								inner join msdb.dbo.sysjobsteps (nolock) AS js on j.job_id=js.job_id
								where right(cast(js.job_id as nvarchar(50)),10) = RIGHT(substring(s.program_name,30,34),10) 
							) + ' ( '+SUBSTRING(LTRIM(RTRIM(s.program_name)), CHARINDEX(': Step ',LTRIM(RTRIM(s.program_name)))+2,LEN(LTRIM(RTRIM(s.program_name)))-CHARINDEX(': Step ',LTRIM(RTRIM(s.program_name)))-2)+' )'
					ELSE	s.program_name
					END
			,(case when r.wait_time = 0 then null else r.wait_type end) as wait_type
			,r.wait_time as wait_time
			,(SELECT CASE
					WHEN pageid = 1 OR pageid % 8088 = 0 THEN 'PFS'
					WHEN pageid = 2 OR pageid % 511232 = 0 THEN 'GAM'
					WHEN pageid = 3 OR (pageid - 1) % 511232 = 0 THEN 'SGAM'
					WHEN pageid IS NULL THEN NULL
					ELSE 'Not PFS/GAM/SGAM' END
					FROM (SELECT CASE WHEN r.[wait_type] LIKE 'PAGE%LATCH%' AND r.[wait_resource] LIKE '%:%'
					THEN CAST(RIGHT(r.[wait_resource], LEN(r.[wait_resource]) - CHARINDEX(':', r.[wait_resource], LEN(r.[wait_resource])-CHARINDEX(':', REVERSE(r.[wait_resource])))) AS INT)
					ELSE NULL END AS pageid) AS latch_pageid
			) AS wait_resource_type
			,null as tempdb_allocations
			,null as tempdb_current
			,ISNULL(r.blocking_session_id,0) as blocking_session_id
			,r.logical_reads as reads
			,r.writes as writes
			,r.cpu_time
			,granted_query_memory = CASE WHEN ((CAST(r.granted_query_memory AS numeric(20,2))*8)/1024/1024) >= 1.0
											THEN CAST(((CAST(r.granted_query_memory AS numeric(20,2))*8)/1024/1024) AS VARCHAR(23)) + ' GB'
											WHEN ((CAST(r.granted_query_memory AS numeric(20,2))*8)/1024) >= 1.0
											THEN CAST(((CAST(r.granted_query_memory AS numeric(20,2))*8)/1024) AS VARCHAR(23)) + ' MB'
											ELSE CAST((CAST(r.granted_query_memory AS numeric(20,2))*8) AS VARCHAR(23)) + ' KB'
											END
			,COALESCE(r.status, s.status) as status
			,s.open_transaction_count
			,s.host_name as host_name
			,COALESCE(r.start_time, s.last_request_start_time) as start_time
			,s.login_time as login_time
			,r.statement_start_offset ,r.statement_end_offset
			--,[BatchQueryPlan] = case when @get_plans = 1 then bqp.query_plan else null end
			,[SqlQueryPlan] = case when @get_plans = 1 then CAST(sqp.query_plan AS xml) else null end
			,GETUTCDATE() as collection_time
	FROM	sys.dm_exec_sessions AS s
	LEFT JOIN sys.dm_exec_requests AS r ON r.session_id = s.session_id
	OUTER APPLY (select dec.most_recent_sql_handle as [sql_handle] from sys.dm_exec_connections dec where dec.session_id = s.session_id) AS dec
	OUTER APPLY sys.dm_exec_sql_text(COALESCE(r.sql_handle,dec.sql_handle)) AS st
	OUTER APPLY sys.dm_exec_query_plan(r.plan_handle) AS bqp
	OUTER APPLY sys.dm_exec_text_query_plan(r.plan_handle,r.statement_start_offset, r.statement_end_offset) as sqp
	WHERE	s.session_id != @@SPID
		AND (	(CASE	WHEN	s.session_id IN (select ri.blocking_session_id from sys.dm_exec_requests as ri)
						--	Get sessions involved in blocking (including system sessions)
						THEN	1
						WHEN	r.blocking_session_id IS NOT NULL AND r.blocking_session_id <> 0
						THEN	1
						ELSE	0
				END) = 1
				OR
				(CASE	WHEN	s.session_id > 50
								AND r.session_id IS NOT NULL -- either some part of session has active request
								--AND ISNULL(open_resultset_count,0) > 0 -- some result is open
								AND s.status <> 'sleeping'
						THEN	1
						ELSE	0
				END) = 1
				OR
				(CASE	WHEN	s.session_id > 50 AND s.open_transaction_count <> 0
						THEN	1
						ELSE	0
				END) = 1
			)		
)
SELECT * FROM T_Requests AS r
WHERE ( @sql_text_fragment_filter is null or len(@sql_text_fragment_filter) = 0 )
		or (	r.sql_command like ('%'+@sql_text_fragment_filter+'%')
			 )
ORDER BY start_time asc;

--exec sp_WhoIsActive