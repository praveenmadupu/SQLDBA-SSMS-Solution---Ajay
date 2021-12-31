#Remove-Variable * -ErrorAction SilentlyContinue; 
$Error.Clear();
cls

$Inventory = "inventory.lab.com";
$InventoryDb = "dbatools"
$Threads = 8

$modulePath  = $PSScriptRoot

$ErrorActionPreference = "Stop"
$WarningPreference = "Continue"
$VerbosePreference = "SilentlyContinue" # SilentlyContinue
$JobFailureNotifyThreshold = 4
$startTime = Get-Date
$Dtmm = $startTime.ToString('yyyy-MM-dd HH.mm.ss')
$Script = $MyInvocation.MyCommand.Name
if([String]::IsNullOrEmpty($Script)) {
    $Script = 'Run-SPWhoIsActive.ps1'
}
$LogFile = "C:\local\$($Script.Replace('.ps1','.log'))"

"`n`n`n`n`n`n{0} {1,-8} {2}" -f "($((Get-Date).ToString('yyyy-MM-dd HH:mm:ss')))","(START)","Execute script '$Script'.." | Write-Output

try
{
    $isCustomError = $false

    Import-Module dbatools, PoshRSJob;

    [int]$lastRunStatus = 0
    "{0} {1,-8} {2}" -f "($((Get-Date).ToString('yyyy-MM-dd HH:mm:ss')))","(INFO)","Create log file '$LogFile' if not exists.." | Write-Output
    if(-not (Test-Path $LogFile)) {
        New-Item -Path $LogFile -Force -ItemType File
        $lastRunStatus | Out-File -FilePath $LogFile -Append
        #Get-Content $LogFile
    }
    else {
        "{0} {1,-8} {2}" -f "($((Get-Date).ToString('yyyy-MM-dd HH:mm:ss')))","(INFO)","Get last run status from log file '$LogFile'.." | Write-Output
        [int]$lastRunStatus = (Get-Content $LogFile | select -First 1| Out-String ) # First line contains continous failure state
        "{0} {1,-8} {2}" -f "($((Get-Date).ToString('yyyy-MM-dd HH:mm:ss')))","(INFO)","Last run status was $lastRunStatus" | Write-Output
    }

    $tsqlSqlServers = @"
select i.Dataserver, i.FriendlyName
from $InventoryDb.dbo.database_server_inventory as i  with (nolock)
where IsActive = 1
"@
    "{0} {1,-8} {2}" -f "($((Get-Date).ToString('yyyy-MM-dd HH:mm:ss')))","(INFO)","Fetch Dataserver list from Inventory.." | Write-Output
    $resultSqlServers = Invoke-DbaQuery -SqlInstance $Inventory -Query $tsqlSqlServers -QueryTimeout 60 -EnableException -ErrorAction Stop

    $tsqlWhoIsActive = @"
--USE dba;
SET NOCOUNT ON;
SET QUOTED_IDENTIFIER ON;
SET ANSI_WARNINGS ON;

-- Parameters
declare @retention_day int = 15;
DECLARE @drop_recreate bit = 0;
DECLARE	@destination_table VARCHAR(4000) = 'dbo.who_is_active';
DECLARE	@staging_table VARCHAR(4000) = @destination_table+'_staging';
DECLARE @output_column_list VARCHAR(8000);

SET @output_column_list = '[collection_time][dd hh:mm:ss.mss][session_id][program_name][login_name][database_name]
						[CPU][CPU_delta][used_memory][used_memory_delta][open_tran_count][status][wait_info][sql_command]
                        [blocked_session_count][blocking_session_id][sql_text][%]';

-- Local Variables
DECLARE @rows_affected int = 0;
DECLARE @s VARCHAR(MAX);
DECLARE @collection_time datetime;

-- Step 01: Truncate/Create Staging table
IF ( (OBJECT_ID(@staging_table) IS NULL) OR (@drop_recreate = 1))
BEGIN
    IF (@drop_recreate = 1)
    BEGIN
		SET @s = 'if object_id('''+@staging_table+''') is not null drop table '+@staging_table;
	    EXEC(@s)
    END

	EXEC dbo.sp_WhoIsActive @get_outer_command=1, @get_task_info=2, @find_block_leaders=1, @get_plans=1, @get_avg_time=1, @get_additional_info=1, @delta_interval = 10
			,@output_column_list = @output_column_list
			,@return_schema = 1, @schema = @s OUTPUT;
	SET @s = REPLACE(@s, '<table_name>', @staging_table)
	EXEC(@s)
END
ELSE
BEGIN
	SET @s = 'TRUNCATE TABLE '+@staging_table;
	EXEC(@s);
	--select * from dbo.who_is_active_staging
END

-- Step 02: Create main table if Not Exists
IF ( (OBJECT_ID(@destination_table) IS NULL) OR (@drop_recreate = 1))
BEGIN
    IF (@drop_recreate = 1)
    BEGIN
		SET @s = 'if object_id('''+@destination_table+''') is not null drop table '+@destination_table;
	    EXEC(@s)
    END

	EXEC dbo.sp_WhoIsActive @get_outer_command=1, @get_task_info=2, @find_block_leaders=1, @get_plans=1, @get_avg_time=1, @get_additional_info=1, @delta_interval = 10
			,@output_column_list = @output_column_list
			,@return_schema = 1, @schema = @s OUTPUT;
	SET @s = REPLACE(@s, '<table_name>', @destination_table)

	DECLARE @insert_position int = CHARINDEX ( ',' , @s )
	SET @s = LEFT(@s, @insert_position)+'[host_cpu_percent] tinyint NOT NULL DEFAULT 0,[cpu_rank] smallint NOT NULL DEFAULT 0,[CPU_delta_percent] tinyint NOT NULL, [pool] varchar(30) NULL,'+RIGHT(@s,LEN(@s)-@insert_position)+';'
	SET @insert_position = CHARINDEX(');',@s);
	SET @s = LEFT(@s, @insert_position-1)+',[CPU_delta_all] bigint NOT NULL);'
	EXEC(@s)
END

--	Step 03: Add a clustered Index
IF NOT EXISTS (select * from sys.indexes i where i.type_desc = 'CLUSTERED' and i.object_id = OBJECT_ID(@destination_table))
BEGIN
	--EXEC('CREATE CLUSTERED INDEX [ci_who_is_active] ON '+@destination_table+' ( [collection_time] ASC, cpu_rank )')
	EXEC ('ALTER TABLE '+@destination_table+' ADD CONSTRAINT pk_who_is_active PRIMARY KEY CLUSTERED ( [collection_time] ASC, cpu_rank )')
END

-- Step 04: Purge Old data
set @s = 'DELETE FROM '+@destination_table+' where collection_time < DATEADD(day,-'+cast(@retention_day as varchar)+',getdate());'
EXEC(@s);

-- Step 05: Populate Staging table
EXEC dbo.sp_WhoIsActive @get_outer_command=1, @get_task_info=2, @find_block_leaders=1, @get_plans=1, @get_avg_time=1, @get_additional_info=1, @delta_interval = 10
			,@output_column_list = @output_column_list
			,@destination_table = @staging_table;

SET @rows_affected = @@ROWCOUNT;

-- Step 06: Update missing Query Plan
SET @s = '
update w
set query_plan = qp.query_plan
from '+@staging_table+' AS w
join sys.dm_exec_requests as r
on w.session_id = r.session_id and w.request_id = r.request_id
outer apply sys.dm_exec_text_query_plan(r.plan_handle, r.statement_start_offset, r.statement_end_offset) as qp
where w.collection_time = (select max(ri.collection_time) from '+@staging_table+' AS ri)
and w.query_plan IS NULL and qp.query_plan is not null;
';
--EXEC (@s);

-- Step 07: Populate Main table
DECLARE @columns VARCHAR(8000);
DECLARE @cpu_system int;
DECLARE @cpu_sql int;

SELECT @columns = COALESCE(@columns+','+QUOTENAME(c.COLUMN_NAME),QUOTENAME(c.COLUMN_NAME)) 
FROM INFORMATION_SCHEMA.COLUMNS c WHERE OBJECT_ID(c.TABLE_SCHEMA+'.'+TABLE_NAME) = OBJECT_ID(@staging_table)
ORDER BY c.ORDINAL_POSITION;

SELECT	@cpu_system = CASE WHEN system_cpu_utilization_post_sp2 IS NOT NULL THEN system_cpu_utilization_post_sp2 ELSE system_cpu_utilization_pre_sp2 END,  
		@cpu_sql = CASE WHEN sql_cpu_utilization_post_sp2 IS NOT NULL THEN sql_cpu_utilization_post_sp2 ELSE sql_cpu_utilization_pre_sp2 END
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
ORDER BY EventTime DESC OFFSET 0 ROWS FETCH FIRST 1 ROWS ONLY;

SET @s = '
INSERT '+@destination_table+'
([host_cpu_percent],[cpu_rank],[CPU_delta_percent],[pool],'+@columns+',[CPU_delta_all])';
IF EXISTS (select * from sys.resource_governor_configuration where is_enabled = 1)
BEGIN
	SET @s = @s + '
SELECT '+CONVERT(varchar,@cpu_system)+' as [host_cpu_percent],
		[cpu_rank] = DENSE_RANK()OVER(ORDER BY CPU_delta DESC, CPU DESC, start_time ASC, session_id), 
		[CPU_delta_percent] = CONVERT(tinyint,CASE WHEN SUM(CONVERT(bigint,REPLACE(CPU_delta,'','',''''))) over(partition by collection_time) = 0
											THEN 0
											ELSE ISNULL(CONVERT(int,REPLACE(CPU_delta,'','',''''))*100/SUM(CONVERT(bigint,REPLACE(CPU_delta,'','',''''))) over(partition by collection_time),0)
											END),
		[pool] = ISNULL(rg.pool,''REST''),
		'+@columns+',
		[CPU_delta_all] = ISNULL(SUM(CONVERT(bigint,REPLACE(CPU_delta,'','',''''))) over(partition by collection_time),0)
FROM '+@staging_table+' s
OUTER APPLY (	SELECT rp.name as [pool]
					FROM sys.dm_resource_governor_workload_groups wg
					JOIN sys.dm_resource_governor_resource_pools rp
					ON rp.pool_id = wg.pool_id
					WHERE wg.group_id = s.additional_info.value(''(/additional_info/group_id)[1]'',''int'')
		) rg
ORDER BY cpu_rank;';
END
ELSE
BEGIN
	SET @s = @s + '
SELECT '+CONVERT(varchar,@cpu_system)+' as [host_cpu_percent],
		[cpu_rank] = DENSE_RANK()OVER(ORDER BY CPU_delta DESC, CPU DESC, start_time ASC, session_id),
		[CPU_delta_percent] = CONVERT(tinyint,CASE WHEN SUM(CONVERT(bigint,REPLACE(CPU_delta,'','',''''))) over(partition by collection_time) = 0
											THEN 0
											ELSE ISNULL(CONVERT(int,REPLACE(CPU_delta,'','',''''))*100/SUM(CONVERT(bigint,REPLACE(CPU_delta,'','',''''))) over(partition by collection_time),0)
											END),
		[pool] = ''REST'',
		'+@columns+',
		[CPU_delta_all] = ISNULL(SUM(CONVERT(bigint,REPLACE(CPU_delta,'','',''''))) over(partition by collection_time),0)
FROM '+@staging_table+' s
ORDER BY cpu_rank;';
END
--PRINT @s
EXEC(@s);

-- Step 08: Return rows affected
select SERVERPROPERTY('MachineName') as SqlInstance, [rows_affected] = ISNULL(@rows_affected,0);
--select * from dbo.who_is_active w where w.collection_time = (select max(s.collection_time) from dbo.who_is_active s)
"@

    "{0} {1,-8} {2}" -f "($((Get-Date).ToString('yyyy-MM-dd HH:mm:ss')))","(INFO)","Start $Threads parallel RSJobs.." | Write-Output
    $jobs = $resultSqlServers | Start-RSJob -Name {"$($_.FriendlyName)"} -ScriptBlock {
        $ErrorActionPreference = "Stop"
        $sqlServer = $_
        #if(($sqlServer.FriendlyName) -in ('dbReplSrv1','CentralServerdr2')) {$x = 1/0}
        #$x = 1/0

        Import-Module dbatools;
        Invoke-DbaQuery -SqlInstance $sqlServer.Dataserver -Database 'dba' -Query $Using:tsqlWhoIsActive -EnableException
    } -Throttle $Threads

    "{0} {1,-8} {2}" -f "($((Get-Date).ToString('yyyy-MM-dd HH:mm:ss')))","(INFO)","Waiting for RSJobs to complete.." | Write-Output
    $jobs | Wait-RSJob -ShowProgress -Timeout 600 | Out-Null #| Format-Table -AutoSize | Out-String | Write-Host -ForegroundColor Cyan

    $jobs_timedout = @()
    $jobs_timedout += $jobs | Where-Object {$_.State -in ('NotStarted','Running','Stopping')}
    $jobs_success = @()
    $jobs_success += $jobs | Where-Object {$_.State -eq 'Completed' -and $_.HasErrors -eq $false}
    $jobs_fail = @()
    $jobs_fail += $jobs | Where-Object {$_.HasErrors -or $_.State -in @('Disconnected')}

    $jobsResult = @()
    $jobsResult += $jobs_success | Receive-RSJob

    if($jobs_success.Count -gt 0) {
        "{0} {1,-8} {2}" -f "($((Get-Date).ToString('yyyy-MM-dd HH:mm:ss')))","(RESULT)","sp_WhoIsActive result .." | Write-Output
        $jobsResult | Format-Table -AutoSize | Out-String | Write-Output
    }

    if($jobs_timedout.Count -gt 0)
    {
        "{0} {1,-8} {2}" -f "($((Get-Date).ToString('yyyy-MM-dd HH:mm:ss')))","(ERROR)","Some jobs timed out. Could not completed in 10 minutes." | Write-Output
        $jobs_timedout | Format-Table -AutoSize | Out-String | Write-Output
        "{0} {1,-8} {2}" -f "($((Get-Date).ToString('yyyy-MM-dd HH:mm:ss')))","(INFO)","Stop timedout jobs.." | Write-Output
        $jobs_timedout | Stop-RSJob
    }

    if($jobs_fail.Count -gt 0)
    {
        "{0} {1,-8} {2}" -f "($((Get-Date).ToString('yyyy-MM-dd HH:mm:ss')))","(ERROR)","Some jobs failed." | Write-Output
        $jobs_fail | Format-Table -AutoSize | Out-String | Write-Output
        "--"*20 | Write-Output
    }

    $jobs_exception = @()
    $jobs_exception += $jobs_timedout + $jobs_fail
    if($jobs_exception.Count -gt 0 ) {
        $alertHost = $jobs_exception | Select-Object -ExpandProperty Name -First 1
        $isCustomError = $true
        $errMessage = "`nBelow jobs either timed or failed-`n$($jobs_exception | Select-Object Name, State, HasErrors | Out-String)"
        [System.Collections.ArrayList]$jobErrMessages = @()
        $failCount = $jobs_fail.Count
        $failCounter = 0
        foreach($job in $jobs_fail) {
            $failCounter += 1
            $jobErrMessage = ''
            if($failCounter -eq 1) {
                $jobErrMessage = "`n$("_"*20)`n" | Write-Output
            }
            $jobErrMessage += "`nError Message for server [$($job.Name)] => `n`n$($job.Error | Out-String)"
            $jobErrMessage += "$("_"*20)`n`n" | Write-Output
            $jobErrMessages.Add($jobErrMessage) | Out-Null;
        }
        $errMessage += ($jobErrMessages -join '')
        throw $errMessage
    }

    #$y = 1/0

    $jobs | Remove-RSJob

    "{0} {1,-8} {2}" -f "($((Get-Date).ToString('yyyy-MM-dd HH:mm:ss')))","(INFO)","Script executed without any error." | Write-Output
    0 | Out-File $LogFile

    $timeSpan = New-TimeSpan -Start $startTime -End (Get-Date)
    "{0} {1,-8} {2}" -f "($((Get-Date).ToString('yyyy-MM-dd HH:mm:ss')))","(FINISH)","Total script time is `"$([Math]::Round($timeSpan.TotalSeconds))`" seconds" | Write-Output

}
catch
{
    $MyError = $_
    $lastRunStatus += 1
    $lastRunStatus | Out-File $LogFile
    "{0} {1,-8} {2}" -f "($((Get-Date).ToString('yyyy-MM-dd HH:mm:ss')))","(INFO)","Execution status saved in '$LogFile'." | Write-Output
    "{0} {1,-8} {2}" -f "($((Get-Date).ToString('yyyy-MM-dd HH:mm:ss')))","(INFO)","Script has failed for $lastRunStatus times continously." | Write-Output

    $Body = "The script '$Script' failed.`nFollowing are details -`n";
    $Body = $Body + "`n$('_'*40)`n$(if($isCustomError){$errMessage}else{$MyError|Out-String})$('_'*40)`n`n";
    $Body = $Body + "`nNote: This alert was generated by the [(dba) Run-SPWhoIsActive] job on CentralServer.";
    $Body = $Body + "`n`n/sub: dba-ops@arcesium.com";
    $Body = $Body + "`n/Service: Databases";
    $message = $Body;

    if([String]::IsNullOrEmpty($alertHost)) {
        $alertHost = $CentralServer
    }

    Write-Host "$Body" -ForegroundColor Red;
    if( $lastRunStatus -ge $JobFailureNotifyThreshold) {
        Raise-DbaAlert -Summary "Run-SPWhoIsActive" -Severity MEDIUM -Description $message -AlertSourceHost $alertHost -AlertTargetHost $alertHost
        "{0} {1,-8} {2}" -f "($((Get-Date).ToString('yyyy-MM-dd HH:mm:ss')))","(INFO)","Failure RADAR alert created." | Write-Output
    }

    throw $MyError
}