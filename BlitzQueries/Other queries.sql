/*
	https://www.sqlshack.com/searching-the-sql-server-query-plan-cache/
	https://blog.sqlauthority.com/2014/07/29/sql-server-ssms-top-queries-by-cpu-and-io/
*/
--	Check when was the system started
SELECT	@@servername as SvrName,
		getdate() as CurrentDate, create_date as ServiceStartDate, 
		DATEDIFF(day,create_date, GETDATE()) as ServiceStartDays 
FROM sys.databases as d where d.name = 'tempdb';

--	Get Socket, Physical Core, and Logical Core count from SQL Server Error Log
EXEC master..xp_readerrorlog 0,1, N'Server process ID is'
/* Lock pages in Memory permission */
EXEC master..xp_readerrorlog 0,1, N'lock memory privilege was not granted.'
EXEC master..xp_readerrorlog 0,1, N'System Manufacturer:', N'System Model'
EXEC master..xp_readerrorlog 0,1, N'sockets',N'processors'
EXEC master..xp_readerrorlog 0,1, N'Instant File Initialization'
EXEC master..xp_readerrorlog 0,1, N'Server is listening on'
EXEC master..xp_readerrorlog 0,1, N'Dedicated admin connection support'

--	Hardware Information from SQL Server 2016
SELECT	i.cpu_count as [Logical_CPU_Count], physical_memory_kb/1024 as [Physical Memory(MB)], i.virtual_machine_type_desc
		,i.sqlserver_start_time
FROM	sys.dm_os_sys_info as i;

--	Check Enabled Trace Flags
DBCC TRACESTATUS(-1);

--	Check if compatibility Model of databases are up to date
SELECT * FROM sys.databases as d
	WHERE d.compatibility_level NOT IN (SELECT d1.compatibility_level FROM sys.databases as d1 WHERE d1.name = 'model');
	
--	Ad hoc queries, and m/r settings
select * from sys.configurations c where c.name in ('optimize for ad hoc workloads','max degree of parallelism','max server memory (MB)','min server memory (MB)')

--	Find DBCC commands 
select DB_NAME(ST.dbid) AS DBName, qs.execution_count, qs.query_hash, st.text from sys.dm_exec_query_stats qs
	cross apply sys.dm_exec_sql_text(qs.sql_handle) st
	where st.text like '%DBCC%'
	and ( DB_NAME(ST.dbid) not in ('master','tempdb')
		or dbid is null
		)
	and st.text not like '%blitz%'
	and st.text not like '%uhtdba%'

--	https://blog.sqlauthority.com/2010/05/14/sql-server-find-most-expensive-queries-using-dmv/
--	Find Most Expensive Queries
SELECT TOP 10 SUBSTRING(qt.TEXT, (qs.statement_start_offset/2)+1,
		((CASE qs.statement_end_offset
		WHEN -1 THEN DATALENGTH(qt.TEXT)
		ELSE qs.statement_end_offset
		END - qs.statement_start_offset)/2)+1),
		qs.execution_count,
		qs.total_logical_reads, qs.last_logical_reads,
		qs.total_logical_writes, qs.last_logical_writes,
		qs.total_worker_time,
		qs.last_worker_time,
		qs.total_elapsed_time/1000000 total_elapsed_time_in_S,
		qs.last_elapsed_time/1000000 last_elapsed_time_in_S,
		qs.last_execution_time,
		qp.query_plan
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt
CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) qp
ORDER BY qs.total_logical_reads DESC -- logical reads
-- ORDER BY qs.total_logical_writes DESC -- logical writes
-- ORDER BY qs.total_worker_time DESC -- CPU time

--	Performance – Top Queries by Average IO
SELECT TOP 100
		DB_NAME(st.dbid) AS database_name
		,creation_time
		, last_execution_time
		, total_logical_reads AS [LogicalReads] , total_logical_writes AS [LogicalWrites] , execution_count
		, total_logical_reads+total_logical_writes AS [AggIO] , (total_logical_reads+total_logical_writes)/(execution_count+0.0) AS [AvgIO] , st.TEXT
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(sql_handle) st
WHERE total_logical_reads+total_logical_writes > 0
AND sql_handle IS NOT NULL
ORDER BY [AggIO] DESC

--	Expensive Queries using cursor
;WITH XMLNAMESPACES
(DEFAULT 'http://schemas.microsoft.com/sqlserver/2004/07/showplan')
SELECT TOP 1000 DB_NAME(qt.dbid) AS DbName,
		SUBSTRING(qt.TEXT, (qs.statement_start_offset/2)+1,
		((CASE qs.statement_end_offset
		WHEN -1 THEN DATALENGTH(qt.TEXT)
		ELSE qs.statement_end_offset
		END - qs.statement_start_offset)/2)+1) as SQLStatement,
		qt.TEXT as BatchStatement,
		qs.execution_count,
		qs.total_logical_reads, qs.last_logical_reads,
		qs.total_logical_writes, qs.last_logical_writes,
		qs.total_worker_time,
		qs.last_worker_time,
		qs.total_elapsed_time/1000000 total_elapsed_time_in_S,
		qs.last_elapsed_time/1000000 last_elapsed_time_in_S,
		qs.last_execution_time,
		qp.query_plan
		,c.value('@StatementText', 'varchar(255)') AS StatementText
		,c.value('@StatementType', 'varchar(255)') AS StatementType
		,c.value('CursorPlan[1]/@CursorName', 'varchar(255)') AS CursorName
		,c.value('CursorPlan[1]/@CursorActualType', 'varchar(255)') AS CursorActualType
		,c.value('CursorPlan[1]/@CursorRequestedType', 'varchar(255)') AS CursorRequestedType
		,c.value('CursorPlan[1]/@CursorConcurrency', 'varchar(255)') AS CursorConcurrency
		,c.value('CursorPlan[1]/@ForwardOnly', 'varchar(255)') AS ForwardOnly
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt
CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) qp
INNER JOIN sys.dm_exec_cached_plans AS cp 
	ON cp.plan_handle = qs.plan_handle
CROSS APPLY qp.query_plan.nodes('//StmtCursor') t(c)
WHERE qp.query_plan.exist('//StmtCursor') = 1
--AND DB_NAME(qt.dbid) NOT IN ('uhtdba','msdb')
and ( qt.dbid is null or DB_NAME(qt.dbid) NOT IN ('uhtdba','msdb')
)
ORDER BY qs.total_logical_reads DESC -- logical reads
-- ORDER BY qs.total_logical_writes DESC -- logical writes
-- ORDER BY qs.total_worker_time DESC -- CPU time

/*	 Joins to Table-Valued Functions */
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
SELECT  st.text,
        qp.query_plan
FROM    (
    SELECT  TOP 50 *
    FROM    sys.dm_exec_query_stats
    ORDER BY total_worker_time DESC
) AS qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) AS qp
WHERE qp.query_plan.exist('//p:RelOp[contains(@LogicalOp, "Join")]/*/p:RelOp[(@LogicalOp[.="Table-valued function"])]') = 1
go

--	Queries with implicit conversion
	--	http://sqlblog.com/blogs/jonathan_kehayias/archive/2010/01/08/finding-implicit-column-conversions-in-the-plan-cache.aspx
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
DECLARE @dbname SYSNAME;
SET @dbname = QUOTENAME(DB_NAME());

WITH XMLNAMESPACES
    (DEFAULT 'http://schemas.microsoft.com/sqlserver/2004/07/showplan')
SELECT
	DB_NAME(st.dbid) as dbName,
    stmt.value('(@StatementText)[1]', 'varchar(max)') as SQLStatement,
    t.value('(ScalarOperator/Identifier/ColumnReference/@Schema)[1]', 'varchar(128)') as SchemaName,
    t.value('(ScalarOperator/Identifier/ColumnReference/@Table)[1]', 'varchar(128)') as TableName,
    t.value('(ScalarOperator/Identifier/ColumnReference/@Column)[1]', 'varchar(128)') as ColumnName,
    ic.DATA_TYPE AS ConvertFrom,
    ic.CHARACTER_MAXIMUM_LENGTH AS ConvertFromLength,
    t.value('(@DataType)[1]', 'varchar(128)') AS ConvertTo,
    t.value('(@Length)[1]', 'int') AS ConvertToLength,
    query_plan
FROM sys.dm_exec_cached_plans AS cp
INNER JOIN
		sys.dm_exec_query_stats qs
	ON cp.plan_handle = qs.plan_handle
CROSS APPLY sys.dm_exec_query_plan(cp.plan_handle) AS qp
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) as st
CROSS APPLY query_plan.nodes('/ShowPlanXML/BatchSequence/Batch/Statements/StmtSimple') AS batch(stmt)
CROSS APPLY stmt.nodes('.//Convert[@Implicit="1"]') AS n(t)
JOIN INFORMATION_SCHEMA.COLUMNS AS ic
    ON QUOTENAME(ic.TABLE_SCHEMA) = t.value('(ScalarOperator/Identifier/ColumnReference/@Schema)[1]', 'varchar(128)')
    AND QUOTENAME(ic.TABLE_NAME) = t.value('(ScalarOperator/Identifier/ColumnReference/@Table)[1]', 'varchar(128)')
    AND ic.COLUMN_NAME = t.value('(ScalarOperator/Identifier/ColumnReference/@Column)[1]', 'varchar(128)')
WHERE t.exist('ScalarOperator/Identifier/ColumnReference[@Database=sql:variable("@dbname")][@Schema!="[sys]"]') = 1;

/*	Adhoc queries against Database */
SELECT  DB_NAME(dbid) as dbName, usecounts, cacheobjtype, objtype, size_in_bytes/1024 as 'Size(KB)', TEXT, cp.plan_handle, qs.sql_handle, qs.creation_time, qs.total_logical_reads, qs.total_logical_writes, qs.query_hash, qs.query_plan_hash
FROM sys.dm_exec_cached_plans AS cp
JOIN sys.dm_exec_query_stats AS qs
ON qs.plan_handle = cp.plan_handle
CROSS APPLY sys.dm_exec_sql_text(cp.plan_handle) as st
WHERE objtype = 'Adhoc'
--AND DB_NAME(dbid) = 'WMS'
ORDER BY usecounts desc

/* Find queries with multiple plans */
SELECT q.PlanCount,
		q.DistinctPlanCount,
		st.text AS QueryText,
		qp.query_plan AS QueryPlan
FROM ( SELECT query_hash,
				COUNT(DISTINCT(query_hash)) AS DistinctPlanCount,
				COUNT(query_hash) AS PlanCount
		FROM sys.dm_exec_query_stats
		GROUP BY query_hash
	) AS q		
JOIN sys.dm_exec_query_stats qs ON q.query_hash = qs.query_hash
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) AS qp
WHERE PlanCount > 1
ORDER BY q.PlanCount DESC;


/* Find queries with multiple plans */
with tt as (
SELECT q.PlanCount,
		q.DistinctPlanCount,
		st.text AS QueryText,
		qp.query_plan AS QueryPlan
		FROM ( SELECT query_hash,
						COUNT(DISTINCT(query_hash)) AS DistinctPlanCount,
						COUNT(query_hash) AS PlanCount
				FROM sys.dm_exec_query_stats
				GROUP BY query_hash
			) AS q		
JOIN sys.dm_exec_query_stats qs ON q.query_hash = qs.query_hash
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) AS qp
WHERE PlanCount > 1
)
select * from tt where tt.QueryText not like 'select name as objectName from source where source_id%'
	and tt.QueryText not like 'INSERT INTO #group1_search_results (ROW_ID, master_title ,program_id,parent_program_id)%'
	and tt.QueryText not like 'INSERT INTO #group1_search_results_tv_source (source_id) SELECT TOP%'
	and tt.QueryText not like 'INSERT INTO #record_count SELECT COUNT(source_base.source_id)%'
	and tt.QueryText not like 'SELECT         program_base.[program_id] AS %'
order by PlanCount desc

--	How to examine IO subsystem latencies from within SQL Server
	--	https://www.sqlskills.com/blogs/paul/how-to-examine-io-subsystem-latencies-from-within-sql-server/
	--	https://sqlperformance.com/2015/03/io-subsystem/monitoring-read-write-latency
	--	https://www.brentozar.com/blitz/slow-storage-reads-writes/
SELECT
    [ReadLatency] =
        CASE WHEN [num_of_reads] = 0
            THEN 0 ELSE ([io_stall_read_ms] / [num_of_reads]) END,
    [WriteLatency] =
        CASE WHEN [num_of_writes] = 0
            THEN 0 ELSE ([io_stall_write_ms] / [num_of_writes]) END,
    [Latency] =
        CASE WHEN ([num_of_reads] = 0 AND [num_of_writes] = 0)
            THEN 0 ELSE ([io_stall] / ([num_of_reads] + [num_of_writes])) END,
    [AvgBPerRead] =
        CASE WHEN [num_of_reads] = 0
            THEN 0 ELSE ([num_of_bytes_read] / [num_of_reads]) END,
    [AvgBPerWrite] =
        CASE WHEN [num_of_writes] = 0
            THEN 0 ELSE ([num_of_bytes_written] / [num_of_writes]) END,
    [AvgBPerTransfer] =
        CASE WHEN ([num_of_reads] = 0 AND [num_of_writes] = 0)
            THEN 0 ELSE
                (([num_of_bytes_read] + [num_of_bytes_written]) /
                ([num_of_reads] + [num_of_writes])) END,
    LEFT ([mf].[physical_name], 2) AS [Drive],
    DB_NAME ([vfs].[database_id]) AS [DB],
    [mf].[physical_name]
FROM
    sys.dm_io_virtual_file_stats (NULL,NULL) AS [vfs]
JOIN sys.master_files AS [mf]
    ON [vfs].[database_id] = [mf].[database_id]
    AND [vfs].[file_id] = [mf].[file_id]
-- WHERE [vfs].[file_id] = 2 -- log files
-- ORDER BY [Latency] DESC
-- ORDER BY [ReadLatency] DESC
ORDER BY [WriteLatency] DESC;
GO


-- Current RAM share of SQL Server
select 32767/1024 [total gb], 30719/1024 [sql gb]
go


/*	active tables without clustered index	 */
SET NOCOUNT ON;
DECLARE @MinTableRowsThreshold [int]; 
SET @MinTableRowsThreshold = 5000; 
;WITH [TablesWithoutClusteredIndexes] --( [db_name], [table_name], [table_schema], [row_count] )
AS 
( 
	SELECT   DB_NAME() AS [db_name],
            t.[name] AS [table_name],
            SCHEMA_NAME(t.[schema_id]) AS [table_schema],
            SUM(ps.[row_count]) AS [row_count],
			SUM(us.[user_seeks]) AS  user_seeks,
            SUM( us.[user_scans]) AS user_scans,
            SUM(us.[user_lookups]) AS user_lookups,
            SUM( us.[user_updates]) AS user_updates
    FROM     [sys].[tables] t
            INNER JOIN [sys].[dm_db_partition_stats] ps
            ON ps.[object_id] = t.[object_id]
            INNER JOIN [sys].[dm_db_index_usage_stats] us
            ON ps.[object_id] = us.[object_id]
    WHERE    OBJECTPROPERTY(t.[object_id], N'TableHasClustIndex') = 0
            AND ps.[index_id] < 2
AND COALESCE(us.[user_seeks] ,
                us.[user_scans] ,
                us.[user_lookups] ,
                us.[user_updates]) IS NOT NULL
    GROUP BY t.[name] , t.[schema_id] 
)
    SELECT  *
    FROM    [TablesWithoutClusteredIndexes]
    WHERE   [row_count] > 5000;

use EBZPEGA
go

/* Find tables with forwarded records on Single Database */
SELECT
    OBJECT_NAME(ps.object_id) as TableName,
    i.name as IndexName,
    ps.index_type_desc,
    ps.page_count,
    ps.avg_fragmentation_in_percent,
    ps.forwarded_record_count
FROM sys.dm_db_index_physical_stats (DB_ID(), NULL, NULL, NULL, 'DETAILED') AS ps
INNER JOIN sys.indexes AS i
    ON ps.OBJECT_ID = i.OBJECT_ID  
    AND ps.index_id = i.index_id
WHERE forwarded_record_count > 0
go

/*	Find Forwarded Records using Cursor Method for VLDBs	*/
use Cosmo
go

IF OBJECT_ID('tempdb..#HeapFragTable') IS NOT NULL
	DROP TABLE #HeapFragTable;
CREATE table #HeapFragTable
(	dbName varchar(100), table_name varchar(100), forwarded_record_count int, avg_fragmentation_in_percent decimal(20,2), page_count bigint );
declare @c_ObjectID int

DECLARE curObjects CURSOR LOCAL FORWARD_ONLY FOR
		select o.object_id
		from sys.objects as o inner join sys.schemas as s on s.schema_id = o.schema_id
		inner join sys.indexes as i on i.object_id = o.object_id
		where o.type_desc = 'USER_TABLE'
		and i.type_desc = 'HEAP'
		
OPEN curObjects  

FETCH NEXT FROM curObjects INTO @c_ObjectID

WHILE @@FETCH_STATUS = 0  
BEGIN  
	PRINT	@c_ObjectID;
	insert into #HeapFragTable
	SELECT DB_NAME() AS dbName, OBJECT_NAME(object_id) AS table_name, forwarded_record_count, avg_fragmentation_in_percent, page_count
	FROM sys.dm_db_index_physical_stats (DB_ID(), @c_ObjectID, DEFAULT, DEFAULT, 'DETAILED');

	FETCH NEXT FROM curObjects INTO @c_ObjectID
END

CLOSE curObjects;  
DEALLOCATE curObjects; 

SELECT * FROM #HeapFragTable;

/* Find tables with forwarded records on All Databases*/
	--	https://www.brentozar.com/archive/2016/07/fix-forwarded-records/
EXEC sp_msForEachDB '
USE [?];
SELECT	DBName, O.*
FROM ( VALUES (DB_NAME()) ) DBs (DBName)
LEFT JOIN
	(
		SELECT	OBJECT_NAME(ps.object_id) as TableName,
				i.name as IndexName,
				ps.index_type_desc,
				ps.page_count,
				ps.avg_fragmentation_in_percent,
				ps.forwarded_record_count
		FROM	sys.dm_db_index_physical_stats (DB_ID(), NULL, NULL, NULL, ''DETAILED'') AS ps
		INNER JOIN sys.indexes AS i
			ON ps.OBJECT_ID = i.OBJECT_ID  
			AND ps.index_id = i.index_id
		WHERE forwarded_record_count > 0
	) AS O
	ON	1 = 1
'
go

use VDP

/*	 Joins to Table-Valued Functions */
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
SELECT  st.text,
        qp.query_plan
FROM    (
    SELECT  TOP 50 *
    FROM    sys.dm_exec_query_stats
    ORDER BY total_worker_time DESC
) AS qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) AS qp
WHERE qp.query_plan.exist('//p:RelOp[contains(@LogicalOp, "Join")]/*/p:RelOp[(@LogicalOp[.="Table-valued function"])]') = 1
go

/*	Finding Query Compilation Timeouts	*/
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
SELECT  CASE
              WHEN qs.statement_end_offset > 0
                     THEN substring(st.text, qs.statement_start_offset/2 + 1,
                                                       (qs.statement_end_offset-qs.statement_start_offset)/2)
              ELSE 'SQL Statement'
       END as timeout_statement,
       st.text AS batch,
       qp.query_plan
FROM    (
       SELECT  TOP 50 *
       FROM    sys.dm_exec_query_stats
       ORDER BY total_worker_time DESC
) AS qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) AS qp
WHERE qp.query_plan.exist('//p:StmtSimple/@StatementOptmEarlyAbortReason[.="TimeOut"]') = 1

--	Find all disabled indexes
EXEC sp_msForEachDB ' 
USE [?];
SELECT DB_NAME() as dbName, i.name AS Index_Name, i.index_id, i.type_desc, s.name AS [Schema_Name], o.name AS Table_Name
FROM sys.indexes i
JOIN sys.objects o on o.object_id = i.object_id
JOIN sys.schemas s on s.schema_id = o.schema_id
WHERE i.is_disabled = 1
ORDER BY
i.name;
'


--	Get Cumulative Waits on Server
	-- https://www.sqlskills.com/blogs/paul/wait-statistics-or-please-tell-me-where-it-hurts/
	-- Last updated November 27, 2017
;WITH [Waits] AS
    (SELECT
        [wait_type],
        [wait_time_ms] / 1000.0 AS [WaitS],
        ([wait_time_ms] - [signal_wait_time_ms]) / 1000.0 AS [ResourceS],
        [signal_wait_time_ms] / 1000.0 AS [SignalS],
        [waiting_tasks_count] AS [WaitCount],
        100.0 * [wait_time_ms] / SUM ([wait_time_ms]) OVER() AS [Percentage],
        ROW_NUMBER() OVER(ORDER BY [wait_time_ms] DESC) AS [RowNum]
    FROM sys.dm_os_wait_stats
    WHERE [wait_type] NOT IN (
        -- These wait types are almost 100% never a problem and so they are
        -- filtered out to avoid them skewing the results. Click on the URL
        -- for more information.
        N'BROKER_EVENTHANDLER', -- https://www.sqlskills.com/help/waits/BROKER_EVENTHANDLER
        N'BROKER_RECEIVE_WAITFOR', -- https://www.sqlskills.com/help/waits/BROKER_RECEIVE_WAITFOR
        N'BROKER_TASK_STOP', -- https://www.sqlskills.com/help/waits/BROKER_TASK_STOP
        N'BROKER_TO_FLUSH', -- https://www.sqlskills.com/help/waits/BROKER_TO_FLUSH
        N'BROKER_TRANSMITTER', -- https://www.sqlskills.com/help/waits/BROKER_TRANSMITTER
        N'CHECKPOINT_QUEUE', -- https://www.sqlskills.com/help/waits/CHECKPOINT_QUEUE
        N'CHKPT', -- https://www.sqlskills.com/help/waits/CHKPT
        N'CLR_AUTO_EVENT', -- https://www.sqlskills.com/help/waits/CLR_AUTO_EVENT
        N'CLR_MANUAL_EVENT', -- https://www.sqlskills.com/help/waits/CLR_MANUAL_EVENT
        N'CLR_SEMAPHORE', -- https://www.sqlskills.com/help/waits/CLR_SEMAPHORE
 
        -- Maybe comment these four out if you have mirroring issues
        N'DBMIRROR_DBM_EVENT', -- https://www.sqlskills.com/help/waits/DBMIRROR_DBM_EVENT
        N'DBMIRROR_EVENTS_QUEUE', -- https://www.sqlskills.com/help/waits/DBMIRROR_EVENTS_QUEUE
        N'DBMIRROR_WORKER_QUEUE', -- https://www.sqlskills.com/help/waits/DBMIRROR_WORKER_QUEUE
        N'DBMIRRORING_CMD', -- https://www.sqlskills.com/help/waits/DBMIRRORING_CMD
 
        N'DIRTY_PAGE_POLL', -- https://www.sqlskills.com/help/waits/DIRTY_PAGE_POLL
        N'DISPATCHER_QUEUE_SEMAPHORE', -- https://www.sqlskills.com/help/waits/DISPATCHER_QUEUE_SEMAPHORE
        N'EXECSYNC', -- https://www.sqlskills.com/help/waits/EXECSYNC
        N'FSAGENT', -- https://www.sqlskills.com/help/waits/FSAGENT
        N'FT_IFTS_SCHEDULER_IDLE_WAIT', -- https://www.sqlskills.com/help/waits/FT_IFTS_SCHEDULER_IDLE_WAIT
        N'FT_IFTSHC_MUTEX', -- https://www.sqlskills.com/help/waits/FT_IFTSHC_MUTEX
 
        -- Maybe comment these six out if you have AG issues
        N'HADR_CLUSAPI_CALL', -- https://www.sqlskills.com/help/waits/HADR_CLUSAPI_CALL
        N'HADR_FILESTREAM_IOMGR_IOCOMPLETION', -- https://www.sqlskills.com/help/waits/HADR_FILESTREAM_IOMGR_IOCOMPLETION
        N'HADR_LOGCAPTURE_WAIT', -- https://www.sqlskills.com/help/waits/HADR_LOGCAPTURE_WAIT
        N'HADR_NOTIFICATION_DEQUEUE', -- https://www.sqlskills.com/help/waits/HADR_NOTIFICATION_DEQUEUE
        N'HADR_TIMER_TASK', -- https://www.sqlskills.com/help/waits/HADR_TIMER_TASK
        N'HADR_WORK_QUEUE', -- https://www.sqlskills.com/help/waits/HADR_WORK_QUEUE
 
        N'KSOURCE_WAKEUP', -- https://www.sqlskills.com/help/waits/KSOURCE_WAKEUP
        N'LAZYWRITER_SLEEP', -- https://www.sqlskills.com/help/waits/LAZYWRITER_SLEEP
        N'LOGMGR_QUEUE', -- https://www.sqlskills.com/help/waits/LOGMGR_QUEUE
        N'MEMORY_ALLOCATION_EXT', -- https://www.sqlskills.com/help/waits/MEMORY_ALLOCATION_EXT
        N'ONDEMAND_TASK_QUEUE', -- https://www.sqlskills.com/help/waits/ONDEMAND_TASK_QUEUE
        N'PREEMPTIVE_XE_GETTARGETSTATE', -- https://www.sqlskills.com/help/waits/PREEMPTIVE_XE_GETTARGETSTATE
        N'PWAIT_ALL_COMPONENTS_INITIALIZED', -- https://www.sqlskills.com/help/waits/PWAIT_ALL_COMPONENTS_INITIALIZED
        N'PWAIT_DIRECTLOGCONSUMER_GETNEXT', -- https://www.sqlskills.com/help/waits/PWAIT_DIRECTLOGCONSUMER_GETNEXT
        N'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP', -- https://www.sqlskills.com/help/waits/QDS_PERSIST_TASK_MAIN_LOOP_SLEEP
        N'QDS_ASYNC_QUEUE', -- https://www.sqlskills.com/help/waits/QDS_ASYNC_QUEUE
        N'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP',
            -- https://www.sqlskills.com/help/waits/QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP
        N'QDS_SHUTDOWN_QUEUE', -- https://www.sqlskills.com/help/waits/QDS_SHUTDOWN_QUEUE
        N'REDO_THREAD_PENDING_WORK', -- https://www.sqlskills.com/help/waits/REDO_THREAD_PENDING_WORK
        N'REQUEST_FOR_DEADLOCK_SEARCH', -- https://www.sqlskills.com/help/waits/REQUEST_FOR_DEADLOCK_SEARCH
        N'RESOURCE_QUEUE', -- https://www.sqlskills.com/help/waits/RESOURCE_QUEUE
        N'SERVER_IDLE_CHECK', -- https://www.sqlskills.com/help/waits/SERVER_IDLE_CHECK
        N'SLEEP_BPOOL_FLUSH', -- https://www.sqlskills.com/help/waits/SLEEP_BPOOL_FLUSH
        N'SLEEP_DBSTARTUP', -- https://www.sqlskills.com/help/waits/SLEEP_DBSTARTUP
        N'SLEEP_DCOMSTARTUP', -- https://www.sqlskills.com/help/waits/SLEEP_DCOMSTARTUP
        N'SLEEP_MASTERDBREADY', -- https://www.sqlskills.com/help/waits/SLEEP_MASTERDBREADY
        N'SLEEP_MASTERMDREADY', -- https://www.sqlskills.com/help/waits/SLEEP_MASTERMDREADY
        N'SLEEP_MASTERUPGRADED', -- https://www.sqlskills.com/help/waits/SLEEP_MASTERUPGRADED
        N'SLEEP_MSDBSTARTUP', -- https://www.sqlskills.com/help/waits/SLEEP_MSDBSTARTUP
        N'SLEEP_SYSTEMTASK', -- https://www.sqlskills.com/help/waits/SLEEP_SYSTEMTASK
        N'SLEEP_TASK', -- https://www.sqlskills.com/help/waits/SLEEP_TASK
        N'SLEEP_TEMPDBSTARTUP', -- https://www.sqlskills.com/help/waits/SLEEP_TEMPDBSTARTUP
        N'SNI_HTTP_ACCEPT', -- https://www.sqlskills.com/help/waits/SNI_HTTP_ACCEPT
        N'SP_SERVER_DIAGNOSTICS_SLEEP', -- https://www.sqlskills.com/help/waits/SP_SERVER_DIAGNOSTICS_SLEEP
        N'SQLTRACE_BUFFER_FLUSH', -- https://www.sqlskills.com/help/waits/SQLTRACE_BUFFER_FLUSH
        N'SQLTRACE_INCREMENTAL_FLUSH_SLEEP', -- https://www.sqlskills.com/help/waits/SQLTRACE_INCREMENTAL_FLUSH_SLEEP
        N'SQLTRACE_WAIT_ENTRIES', -- https://www.sqlskills.com/help/waits/SQLTRACE_WAIT_ENTRIES
        N'WAIT_FOR_RESULTS', -- https://www.sqlskills.com/help/waits/WAIT_FOR_RESULTS
        N'WAITFOR', -- https://www.sqlskills.com/help/waits/WAITFOR
        N'WAITFOR_TASKSHUTDOWN', -- https://www.sqlskills.com/help/waits/WAITFOR_TASKSHUTDOWN
        N'WAIT_XTP_RECOVERY', -- https://www.sqlskills.com/help/waits/WAIT_XTP_RECOVERY
        N'WAIT_XTP_HOST_WAIT', -- https://www.sqlskills.com/help/waits/WAIT_XTP_HOST_WAIT
        N'WAIT_XTP_OFFLINE_CKPT_NEW_LOG', -- https://www.sqlskills.com/help/waits/WAIT_XTP_OFFLINE_CKPT_NEW_LOG
        N'WAIT_XTP_CKPT_CLOSE', -- https://www.sqlskills.com/help/waits/WAIT_XTP_CKPT_CLOSE
        N'XE_DISPATCHER_JOIN', -- https://www.sqlskills.com/help/waits/XE_DISPATCHER_JOIN
        N'XE_DISPATCHER_WAIT', -- https://www.sqlskills.com/help/waits/XE_DISPATCHER_WAIT
        N'XE_TIMER_EVENT' -- https://www.sqlskills.com/help/waits/XE_TIMER_EVENT
        )
    AND [waiting_tasks_count] > 0
    )
SELECT
    MAX ([W1].[wait_type]) AS [WaitType],
    CAST (MAX ([W1].[WaitS]) AS DECIMAL (16,2)) AS [Wait_S],
    CAST (MAX ([W1].[ResourceS]) AS DECIMAL (16,2)) AS [Resource_S],
    CAST (MAX ([W1].[SignalS]) AS DECIMAL (16,2)) AS [Signal_S],
    MAX ([W1].[WaitCount]) AS [WaitCount],
    CAST (MAX ([W1].[Percentage]) AS DECIMAL (5,2)) AS [Percentage],
    CAST ((MAX ([W1].[WaitS]) / MAX ([W1].[WaitCount])) AS DECIMAL (16,4)) AS [AvgWait_S],
    CAST ((MAX ([W1].[ResourceS]) / MAX ([W1].[WaitCount])) AS DECIMAL (16,4)) AS [AvgRes_S],
    CAST ((MAX ([W1].[SignalS]) / MAX ([W1].[WaitCount])) AS DECIMAL (16,4)) AS [AvgSig_S],
    CAST ('https://www.sqlskills.com/help/waits/' + MAX ([W1].[wait_type]) as XML) AS [Help/Info URL]
FROM [Waits] AS [W1]
INNER JOIN [Waits] AS [W2] ON [W2].[RowNum] <= [W1].[RowNum]
GROUP BY [W1].[RowNum]
HAVING SUM ([W2].[Percentage]) - MAX( [W1].[Percentage] ) < 95; -- percentage threshold
GO

/*	Fragmentation	*/
SELECT OBJECT_NAME(ips.OBJECT_ID)
 ,i.NAME
 ,ips.index_id
 ,index_type_desc
 ,avg_fragmentation_in_percent
 ,avg_page_space_used_in_percent
 ,page_count
FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'SAMPLED') ips
INNER JOIN sys.indexes i ON (ips.object_id = i.object_id)
 AND (ips.index_id = i.index_id)
ORDER BY avg_fragmentation_in_percent DESC

/* Verify if tempdb files are of same size/autogrowth	*/        
exec sp_helpdb 'tempdb';

/* Un-indexes Foreign Keys	*/
;WITH    fk_cte
          AS ( SELECT   OBJECT_NAME(fk.referenced_object_id) pk_table ,
                        c2.name pk_column ,
                        kc.name pk_index_name ,
                        OBJECT_NAME(fk.parent_object_id) fk_table ,
                        c.name fk_column ,
                        fk.name fk_name ,
                        CASE WHEN i.object_id IS NOT NULL THEN 1 ELSE 0 END does_fk_has_index ,
                        i.is_primary_key is_fk_a_pk_also ,
                        i.is_unique is_index_on_fk_unique ,
                        fk.*
               FROM     sys.foreign_keys fk
                        INNER JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
                        INNER JOIN sys.columns c ON c.object_id = fk.parent_object_id AND c.column_id = fkc.parent_column_id
                        LEFT  JOIN sys.columns c2 ON c2.object_id = fk.referenced_object_id AND c2.column_id = fkc.referenced_column_id
                        LEFT JOIN sys.key_constraints kc ON kc.parent_object_id = fk.referenced_object_id AND kc.type = 'PK'
                        LEFT JOIN sys.index_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                        LEFT JOIN sys.indexes i ON i.object_id = ic.object_id AND i.index_id = ic.index_id
             )
    SELECT  * FROM    fk_cte
       LEFT JOIN sys.dm_db_partition_stats ps on ps.object_id = fk_cte.parent_object_id and ps.index_id <= 1
    WHERE   does_fk_has_index = 0 -- and fk_table = 'LineItems'
    ORDER BY used_page_count desc


--	Find VLF Counts
EXEC [dbo].[usp_AnalyzeSpaceCapacity] @getLogInfo = 1;
/* SQL Server Logs giving Warnings for high VLF counts */
EXEC master..xp_readerrorlog 0,1, N'virtual log files which is excessive.'

--	Foreign Keys Not Trusted.
	--	https://BrentOzar.com/go/trust
EXEC sp_msForEachDB '
USE [?];
SELECT	*
FROM (values (DB_NAME())) as DBs(dbName)
LEFT JOIN
	(
		SELECT ''['' + s.name + ''].['' + o.name + '']'' AS TableName, ''['' + i.name + '']'' AS keyname
		from sys.foreign_keys i
		INNER JOIN sys.objects o ON i.parent_object_id = o.object_id
		INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
		WHERE i.is_not_trusted = 1 AND i.is_not_for_replication = 0
	) AS r
	ON	1 = 1
';
/*
The [SRA] database has foreign keys that were probably disabled, data was changed, and then the key was enabled again.  Simply enabling the key is not enough for the optimizer to use this key - we have to alter the table using the WITH CHECK CHECK CONSTRAINT parameter. (https://www.brentozar.com/blitz/foreign-key-trusted/ ). It turns out this can have a huge performance impact on queries, too, because SQL Server won’t use untrusted constraints to build better execution plans.
*/

--	Disk Latency Logs
$serverName = 'dbsep0456';
$rs = Get-ChildItem "\\$serverName\e$\MSSQL\MSSQL11.MSSQLSERVER\MSSQL\Log\ERRORLOG*" | Select-String -Pattern "I/O requests taking longer than" | fl Filename, Line | Out-File "C:\temp\$serverName-DiskLatencyLogs.txt"

--	Find Usage Stats for Table and Indexes
SELECT   OBJECT_NAME(S.[OBJECT_ID]) AS [OBJECT NAME], 
         I.[NAME] AS [INDEX NAME], 
         USER_SEEKS, 
         USER_SCANS, 
         USER_LOOKUPS, 
         USER_UPDATES 
FROM     SYS.DM_DB_INDEX_USAGE_STATS AS S 
         INNER JOIN SYS.INDEXES AS I 
           ON I.[OBJECT_ID] = S.[OBJECT_ID] 
              AND I.INDEX_ID = S.INDEX_ID 
WHERE    OBJECTPROPERTY(S.[OBJECT_ID],'IsUserTable') = 1
AND		OBJECT_NAME(S.[OBJECT_ID]) = '0071 - CCDI Data Table' 
