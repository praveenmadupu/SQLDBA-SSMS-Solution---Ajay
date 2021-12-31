/* Dashboard Query */
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
DECLARE @From datetime = master.dbo.fn_getETFromUTC('2021-12-31T01:37:21Z');
DECLARE @To datetime = master.dbo.fn_getETFromUTC('2021-12-31T04:37:21Z');
DECLARE @Server sysname = 'MyProdServer';
--DECLARE @From datetime = dateadd(ho
SELECT master.dbo.fn_getUTCFromET(time) AS time, wait_type, wait_time_ms_diff
from
(
select collection_time_et AS time, collection_id, wait_type, wait_time_ms_diff,
RANK() OVER (PARTITION BY collection_id ORDER BY wait_time_ms_diff DESC) AS column3_order
from DBA.dbo.wait_stats WITH (NOLOCK)
where server_name = @Server
and collection_time_et BETWEEN @From AND @To
and  waiting_tasks_count_diff is not null
) as a
where column3_order <= 5
order by wait_type, time
go



/* Powershell Code to Collect */
#Import-Module RequiredModuleName;

$CentralServer = 'DbaCentralServer';
$ConnectionTimeout = 30;
$StagingTableName = "DBA.dbo.wait_stats_staging";
$TableName = "DBA.dbo.wait_stats";
$RetentionDays = 365;

$ResultsFile = "$PsScriptRoot\..\Get-WaitStatistics.log";
$Now = (get-date -format G);

$sql_get_server_list = "
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT Dataserver
  FROM DBA.dbo.inventory
 WHERE IsActive = 1
   AND Monitor = 'Yes'
   AND ServerType = 'DB'
   AND Env IN ('PROD', 'UAT');
";

$sql_get_wait_stats = "
SELECT '${Now}' AS date_time_et,
       SERVERPROPERTY('ServerName') AS server_name,
       REPLACE(ws.[wait_type], ',', '') AS [wait_type],
       ws.[waiting_tasks_count],
       ws.[wait_time_ms],
       ws.[max_wait_time_ms],
       ws.[signal_wait_time_ms]
  FROM sys.dm_os_wait_stats ws
  --WHERE waiting_tasks_count+wait_time_ms+max_wait_time_ms+signal_wait_time_ms <> 0
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
        N'CXCONSUMER', -- https://www.sqlskills.com/help/waits/CXCONSUMER
 
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
        N'PARALLEL_REDO_DRAIN_WORKER', -- https://www.sqlskills.com/help/waits/PARALLEL_REDO_DRAIN_WORKER
        N'PARALLEL_REDO_LOG_CACHE', -- https://www.sqlskills.com/help/waits/PARALLEL_REDO_LOG_CACHE
        N'PARALLEL_REDO_TRAN_LIST', -- https://www.sqlskills.com/help/waits/PARALLEL_REDO_TRAN_LIST
        N'PARALLEL_REDO_WORKER_SYNC', -- https://www.sqlskills.com/help/waits/PARALLEL_REDO_WORKER_SYNC
        N'PARALLEL_REDO_WORKER_WAIT_WORK', -- https://www.sqlskills.com/help/waits/PARALLEL_REDO_WORKER_WAIT_WORK
        N'PREEMPTIVE_OS_FLUSHFILEBUFFERS', -- https://www.sqlskills.com/help/waits/PREEMPTIVE_OS_FLUSHFILEBUFFERS 
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
        N'SOS_WORK_DISPATCHER', -- https://www.sqlskills.com/help/waits/SOS_WORK_DISPATCHER
        N'SP_SERVER_DIAGNOSTICS_SLEEP', -- https://www.sqlskills.com/help/waits/SP_SERVER_DIAGNOSTICS_SLEEP
        N'SQLTRACE_BUFFER_FLUSH', -- https://www.sqlskills.com/help/waits/SQLTRACE_BUFFER_FLUSH
        N'SQLTRACE_INCREMENTAL_FLUSH_SLEEP', -- https://www.sqlskills.com/help/waits/SQLTRACE_INCREMENTAL_FLUSH_SLEEP
        N'SQLTRACE_WAIT_ENTRIES', -- https://www.sqlskills.com/help/waits/SQLTRACE_WAIT_ENTRIES
        N'VDI_CLIENT_OTHER', -- https://www.sqlskills.com/help/waits/VDI_CLIENT_OTHER
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
  ORDER BY waiting_tasks_count DESC;
";

$sql_create_tables = "
IF (OBJECT_ID('${StagingTableName}') IS NULL)
BEGIN
  CREATE TABLE ${StagingTableName}
  (
	[collection_time_et] [datetime] NOT NULL,
	[server_name] [sysname] NOT NULL,
	[wait_type] [nvarchar](60) NOT NULL,
	[waiting_tasks_count] [bigint] NOT NULL,
	[wait_time_ms] [bigint] NOT NULL,
	[max_wait_time_ms] [bigint] NOT NULL,
	[signal_wait_time_ms] [bigint] NOT NULL
  );
END;
IF (OBJECT_ID('${TableName}') IS NULL)
BEGIN
  CREATE TABLE ${TableName}
  (
	[collection_id] [int] NULL,
	[collection_time_et] [datetime] NOT NULL,
	[server_name] [sysname] NOT NULL,
	[wait_type] [nvarchar](60) NOT NULL,
	[waiting_tasks_count] [bigint] NOT NULL,
	[wait_time_ms] [bigint] NOT NULL,
	[max_wait_time_ms] [bigint] NOT NULL,
	[signal_wait_time_ms] [bigint] NOT NULL,
	[waiting_tasks_count_diff] [bigint] NULL,
	[wait_time_ms_diff] [bigint] NULL,
	[max_wait_time_ms_diff] [bigint] NULL,
	[signal_wait_time_ms_diff] [bigint] NULL,
	[wait_time_pct] [decimal](38, 10) NULL,
	[wait_time_pct_diff] [decimal](38, 10) NULL
);
CREATE NONCLUSTERED INDEX [ix_servername_collectid] ON ${TableName} ([server_name]) INCLUDE ([collection_id]);
END;
";

 $sql_truncate_staging ="
 -- $CentralServer
 TRUNCATE TABLE $StagingTableName;
 ";

 $sql_bulk_insert="
-- $CentralServer
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
BULK INSERT $StagingTableName
FROM '$ResultsFile'
WITH 
(
	BATCHSIZE = 100000,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
	FIRSTROW = 2
)
";

$sql_load_table="
-- $CentralServer
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
DECLARE @last_collection_id int, @current_collection_id int, @server_name sysname;
SELECT @server_name = server_name FROM ${StagingTableName};
--SELECT @last_collection_id = ISNULL(MAX(collection_id), 0) FROM ${TableName} WHERE server_name = @server_name;
--SELECT @last_collection_id = ISNULL(MAX(collection_id), 0) FROM DBA.dbo.wait_stats WITH (INDEX (ix_servername_collectid)) WHERE server_name = @server_name;
SELECT @last_collection_id = ISNULL(MAX(collection_id), 0) FROM DBA.dbo.wait_stats WITH (FORCESEEK) WHERE server_name = @server_name;
--SELECT @current_collection_id = @last_collection_id + 1;
SELECT @current_collection_id = ISNULL(MAX(collection_id)+1, 1) FROM ${TableName};
  
INSERT    ${TableName}
        (
		collection_id,
		collection_time_et,
		server_name,
		[wait_type],
		[waiting_tasks_count],
		[wait_time_ms],
		[max_wait_time_ms],
		[signal_wait_time_ms],
		[waiting_tasks_count_diff],
		[wait_time_ms_diff],
		[max_wait_time_ms_diff],
		[signal_wait_time_ms_diff],
		[wait_time_pct],
		[wait_time_pct_diff]
		)
SELECT  DISTINCT @current_collection_id AS collection_id,
        c.collection_time_et,
        c.server_name,
        c.[wait_type],
        c.[waiting_tasks_count],
        c.[wait_time_ms],
        c.[max_wait_time_ms],
        c.[signal_wait_time_ms],
        c.[waiting_tasks_count]-p.[waiting_tasks_count] AS [waiting_tasks_count_diff],
		c.[wait_time_ms]-p.[wait_time_ms] AS [wait_time_ms_diff],
		c.[max_wait_time_ms]-p.[max_wait_time_ms] AS [max_wait_time_ms_diff],
		c.[signal_wait_time_ms]-p.[signal_wait_time_ms] AS [signal_wait_time_ms_diff],
        100.*c.wait_time_ms/NULLIF(SUM(c.wait_time_ms) OVER(), 0) AS wait_time_pct, 
        100.*(c.wait_time_ms-p.wait_time_ms)/NULLIF(SUM(c.wait_time_ms-p.wait_time_ms) OVER(),0) AS wait_time_pct_diff
FROM      ${StagingTableName} c
--LEFT JOIN ${TableName} p ON c.wait_type = p.wait_type AND p.collection_id = @last_collection_id
LEFT JOIN ${TableName} p ON c.wait_type = p.wait_type AND @last_collection_id = p.collection_id
ORDER BY  c.[wait_type] DESC;
";

$sql_delete_old_data="
-- $CentralServer
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
DELETE FROM $TableName
WHERE date_time_et < DATEADD(day, -$RetentionDays, GETDATE());
";

Write-Host "Get Server List" -ForegroundColor Cyan;
Push-Location;
$Servers = (Invoke-Sqlcmd -MaxCharLength 80000 -ConnectionTimeout $ConnectionTimeout -QueryTimeout 0 -ServerInstance $CentralServer -Query $sql_get_server_list -Verbose);
Pop-Location;
#$Servers;

Push-Location;
Invoke-Sqlcmd -MaxCharLength 80000 -ConnectionTimeout $ConnectionTimeout -QueryTimeout 0 -ServerInstance $CentralServer -Query $sql_create_tables -Verbose;
Pop-Location;

foreach ($Server IN $Servers)
{
 $Server = $Server.Dataserver.ToString();
 Write-Host "`nServer: ${Server}" -ForegroundColor Cyan;
 $ServerNoSlash = $Server -replace "\\", "-";
 $ServerNoSlash = $Server -replace ",", "-";

 Write-Host "Get Wait Statistics" -ForegroundColor Cyan;
 Push-Location;
 Invoke-Sqlcmd -MaxCharLength 80000 -ConnectionTimeout $ConnectionTimeout -QueryTimeout 0 -ServerInstance $Server -Query $sql_get_wait_stats -Verbose | Export-Csv $ResultsFile -NoTypeInformation;
 Pop-Location;

 if ((Test-Path -Path ${ResultsFile}) -eq $True)
 {
  icacls ${ResultsFile} /grant Everyone:f;
 }

 (Get-Content $ResultsFile).replace('"', '') | Set-Content $ResultsFile;

 Write-Host "Truncate Staging Table" -ForegroundColor Cyan;
 Push-Location;
 Invoke-Sqlcmd -MaxCharLength 80000 -ConnectionTimeout $ConnectionTimeout -QueryTimeout 0 -ServerInstance $CentralServer -Query $sql_truncate_staging -Verbose;
 Pop-Location;

 Write-Host "Bulk Insert Staging Table" -ForegroundColor Cyan;
 Push-Location;
 Invoke-Sqlcmd -MaxCharLength 80000 -ConnectionTimeout $ConnectionTimeout -QueryTimeout 0 -ServerInstance $CentralServer -Query $sql_bulk_insert -Verbose;
 Pop-Location;

 Write-Host "Load Main Table" -ForegroundColor Cyan;
 Push-Location;
 Invoke-Sqlcmd -MaxCharLength 80000 -ConnectionTimeout $ConnectionTimeout -QueryTimeout 0 -ServerInstance $CentralServer -Query $sql_load_table -Verbose;
 Pop-Location;

}
