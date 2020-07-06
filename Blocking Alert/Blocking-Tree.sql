IF OBJECT_ID('tempdb..#SysProcesses') IS NOT NULL
	DROP TABLE #SysProcesses;
select  Concat
        (
            RIGHT('00'+CAST(ISNULL((datediff(second,er.start_time,GETDATE()) / 3600 / 24), 0) AS VARCHAR(2)),2)
            ,' '
            ,RIGHT('00'+CAST(ISNULL(datediff(second,er.start_time,GETDATE()) / 3600  % 24, 0) AS VARCHAR(2)),2)
            ,':'
            ,RIGHT('00'+CAST(ISNULL(datediff(second,er.start_time,GETDATE()) / 60 % 60, 0) AS VARCHAR(2)),2)
            ,':'
            ,RIGHT('00'+CAST(ISNULL(datediff(second,er.start_time,GETDATE()) % 3600 % 60, 0) AS VARCHAR(2)),2)
        ) as [dd hh:mm:ss]
		,r.spid as session_id
		,t.text as sql_command
		,SUBSTRING(t.text, (r.stmt_start/2)+1,   
        ((CASE r.stmt_end WHEN -1 THEN DATALENGTH(t.text)  
				ELSE r.stmt_end END - r.stmt_start)/2) + 1) AS sql_text
		--,r.cmd
		,r.loginame as login_name
		,db_name(r.dbid) as database_name
		,r.program_name
		,(case when r.waittime = 0 then null else r.lastwaittype end) as wait_type
		,r.waittime as wait_time
		,null as tempdb_allocations
		,null as tempdb_current
		,r.blocked as blocking_session_id
		,er.logical_reads as reads
		,er.writes as writes
		,r.physical_io
		,r.cpu
		,r.memusage
		,r.status
		,r.open_tran
		,r.hostname as host_name
		,er.start_time as start_time
		,r.login_time as login_time
		,GETDATE() as collection_time
INTO #SysProcesses
from sys.sysprocesses as r left join sys.dm_exec_requests as er
	on er.session_id = r.spid
CROSS APPLY sys.dm_exec_sql_text(r.SQL_HANDLE) as t;

--select top 2 * from #SysProcesses;

;WITH T_BLOCKERS AS
(
	-- Find block Leaders
	SELECT	[dd hh:mm:ss], [collection_time], [session_id], 
			[sql_text] = REPLACE(REPLACE(REPLACE(REPLACE(CAST(COALESCE([sql_command],[sql_text]) AS VARCHAR(MAX)),char(13),''),CHAR(10),''),'<?query --',''),'--?>',''), 
			[login_name], wait_type, r.wait_time, [blocking_session_id], null as [blocked_session_count],
			[status], open_tran, [host_name], [database_name], [program_name],
			r.cpu, r.[tempdb_allocations], r.[tempdb_current], r.[reads], r.[writes], r.[physical_io],
			[LEVEL] = CAST (REPLICATE ('0', 4-LEN (CAST (r.session_id AS VARCHAR))) + CAST (r.session_id AS VARCHAR) AS VARCHAR (1000))
	FROM	#SysProcesses AS r
	WHERE	(ISNULL(r.blocking_session_id,0) = 0 OR ISNULL(r.blocking_session_id,0) = r.session_id)
		AND EXISTS (SELECT * FROM #SysProcesses AS R2 WHERE R2.collection_time = r.collection_time AND ISNULL(R2.blocking_session_id,0) = r.session_id AND ISNULL(R2.blocking_session_id,0) <> R2.session_id)
	--	
	UNION ALL
	--
	SELECT	r.[dd hh:mm:ss], r.[collection_time], r.[session_id], 
			[sql_text] = REPLACE(REPLACE(REPLACE(REPLACE(CAST(COALESCE(r.[sql_command],r.[sql_text]) AS VARCHAR(MAX)),char(13),''),CHAR(10),''),'<?query --',''),'--?>',''), 
			r.[login_name], r.wait_type, r.wait_time, r.[blocking_session_id], null as [blocked_session_count],
			r.[status], r.open_tran, r.[host_name], r.[database_name], r.[program_name],
			r.cpu, r.[tempdb_allocations], r.[tempdb_current], r.[reads], r.[writes], r.[physical_io],
			CAST (B.LEVEL + RIGHT (CAST ((1000 + r.session_id) AS VARCHAR (100)), 4) AS VARCHAR (1000)) AS LEVEL
	FROM	#SysProcesses AS r
	INNER JOIN 
			T_BLOCKERS AS B
		ON	r.collection_time = B.collection_time
		AND	r.blocking_session_id = B.session_id
	WHERE	r.blocking_session_id <> r.session_id
)
--select * from T_BLOCKERS
	
SELECT	[dd hh:mm:ss], 
		[BLOCKING_TREE] = N'    ' + REPLICATE (N'|         ', LEN (LEVEL)/4 - 1) 
						+	CASE	WHEN (LEN(LEVEL)/4 - 1) = 0
									THEN 'HEAD -  '
									ELSE '|------  ' 
							END
						+	CAST (r.session_id AS NVARCHAR (10)) + N' ' + (CASE WHEN LEFT(r.[sql_text],1) = '(' THEN SUBSTRING(r.[sql_text],CHARINDEX('exec',r.[sql_text]),LEN(r.[sql_text]))  ELSE r.[sql_text] END),
		[session_id], [blocking_session_id], 
		--w.lock_text,
		[sql_commad] = CONVERT(XML, '<?query -- '+char(13)
						+ (CASE WHEN LEFT([sql_text],1) = '(' THEN SUBSTRING([sql_text],CHARINDEX('exec',[sql_text]),LEN([sql_text]))  ELSE [sql_text] END)
						+ char(13)+'--?>')
		,[login_name], [program_name], [database_name], wait_type, wait_time, status, [blocked_session_count], r.open_tran
		,r.cpu, r.[reads], r.[writes], r.[physical_io]
		,[host_name]
FROM	T_BLOCKERS AS r
ORDER BY LEVEL ASC;

exec sp_WhoIsActive;
--EXEC sp_WhoIsActive @filter_type = 'session' ,@filter = '47'