--	Check-SQLServerAvailability
SET nocount on;

declare @time datetime;
select @time = d.create_date
FROM sys.databases as d
WHERE d.name = 'tempdb';
--SELECT @@servername as srv, @time as server_uptime, DATEDIFF(MINUTE,@time,GETDATE()) AS [up_time(minutes)], DATEDIFF(HOUR,@time,GETDATE()) AS [up_time(hours)];

declare @start_time datetime, @end_time datetime, @err_msg_1 nvarchar(256) = null, @err_msg_2 nvarchar(256) = null;
--set @start_time = '2021-05-17 18:00:00.000' --  August 22, 2020 05:16:00
--set @time = DATEADD(HOUR,-1,getdate());
set @start_time = DATEADD(HOUR,-12,getdate());
--set @end_time = '2021-04-23 00:00:00.000';
set @end_time = GETDATE()
--set @end_time = DATEADD(minute,30*1,@start_time)
--set @err_msg_1 = 'Unable to open the physical file'
--set @err_msg_1 = 'There is insufficient system memory in resource pool'
--set @err_msg_1 = 'Internal'
--set @err_msg_1 = 'has been rejected due to breached concurrent connection limit';

--EXEC master.dbo.xp_enumerrorlogs
declare @NumErrorLogs int;
begin try
	exec master.dbo.xp_instance_regread N'HKEY_LOCAL_MACHINE', 
								N'Software\Microsoft\MSSQLServer\MSSQLServer',
								N'NumErrorLogs', 
								@NumErrorLogs OUTPUT;
end try
begin catch
end catch
if OBJECT_ID('tempdb..#errorlog') is not null	drop table #errorlog;
create table #errorlog (LogDate datetime2 not null, ProcessInfo varchar(200) not null, Text varchar(2000) not null);

declare @counter int = isnull(@NumErrorLogs,6);
while @counter >= 0
begin
	begin try
	insert #errorlog
	EXEC master.dbo.xp_readerrorlog @counter, 1, @err_msg_1, @err_msg_2, @start_time, @end_time, "asc";
	end try
	begin catch
		print 'error'
	end catch
	set @counter -= 1;

	if exists (select * from #errorlog where LogDate > @end_time)
		break;
end


select lower(convert(varchar,SERVERPROPERTY('MachineName'))) as ServerName,
		ROW_NUMBER()over(order by LogDate asc) as id,
		datediff(minute,LogDate,getdate()) as [-Time(min)],
			--master.dbo.time2duration(LogDate,'datetime') as [Log-Duration],
				*
--select left(Text,45) as Text, max(LogDate) as LogDate_max, min(LogDate) as LogDate_min, COUNT(*) as occurrences
from #errorlog as e
where 1 = 1
--and e.Text like '%has been rejected due to breached concurrent connection limit%'
and	e.ProcessInfo not in ('Backup')
--and e.ProcessInfo not in ('Logon')
--and e.Text not like 'Error: 18456, Severity: 14, State: 5.'
--and not (e.ProcessInfo = 'Backup' and (e.Text like 'Log was backed up%' or e.Text like 'Database backed up. %' or e.Text like 'BACKUP DATABASE successfully%') )
and e.Text not like 'Parallel redo is shutdown for database%'
and e.Text not like 'Parallel redo is started for database%'
--and e.Text not like 'Database % is a cloned database. This database should be used for diagnostic purposes only and is not supported for use in a production environment.'
--and e.Text not like 'DbMgrPartnerCommitPolicy::SetSyncState:%'
--and e.Text not like 'SQL Server blocked access to procedure ''sys.xp_cmdshell'' of component%'
--and e.Text not like 'DbMgrPartnerCommitPolicy::SetSyncAndRecoveryPoint:%'
--and e.Text not like 'Recovery completed for database %'
and e.Text not like 'CHECKDB for database % finished without errors%'
--and e.Text not like 'SQL Server blocked access to procedure%'
--and e.Text not like 'Always On: DebugTraceVarArgs AR %'
--and e.Text not like 'Login failed for user %'
and e.Text not like 'I/O is frozen on database%'
and e.Text not like 'I/O was resumed on database%'
and e.Text not like 'Attempting to load library ''%.dll'' into memory. This is an informational message only. No user action is required.'
--and e.Text not like 'AlwaysOn Availability Groups connection with secondary database terminated for primary database %'
and e.Text not like 'AlwaysOn Availability Groups connection with secondary database established for primary database %'
and e.Text not like 'AlwaysOn Availability Groups connection with primary database established for secondary database %'
and e.Text not like 'The recovery LSN % was identified for the database with ID %. This is an informational message only. No user action is required.'
and e.Text not like 'SQL Server blocked access to procedure ''sys.xp_cmdshell%'
--and e.Text like '%ALTER DATABASE%'
--and e.Text like '%rejected due to breached'
--group by left(Text,45) order by occurrences desc
order by id desc

--select *
--from #errorlog

/*
select create_date 
from sys.databases d
where d.name = 'tempdb'

*/