
declare @start_time datetime, @end_time datetime;
set @start_time = '2020-08-22 03:00:00.000' --  August 22, 2020 05:16:00
--set @start_time = DATEADD(minute,-120,getdate());
set @end_time = '2020-08-22 03:30:00.000';
--set @end_time = DATEADD(minute,30,@start_time)

declare @NumErrorLogs int;
exec master.dbo.xp_instance_regread N'HKEY_LOCAL_MACHINE', 
                            N'Software\Microsoft\MSSQLServer\MSSQLServer',
                            N'NumErrorLogs', 
                            @NumErrorLogs OUTPUT;
if OBJECT_ID('tempdb..#errorlog') is not null	drop table #errorlog;
create table #errorlog (LogDate datetime2 not null, ProcessInfo varchar(200) not null, Text varchar(2000) not null);

declare @counter int = isnull(@NumErrorLogs,6);
while @counter >= 0
begin
	insert #errorlog
	EXEC master.dbo.xp_readerrorlog @counter, 1, NULL, NULL, @start_time, @end_time, "asc";
	set @counter -= 1;

	if exists (select * from #errorlog where LogDate > @end_time)
		break;
end

select ROW_NUMBER()over(order by LogDate asc) as id,*
from #errorlog as e
where 1 = 1
--and e.Text like '%pricing%'
--and	e.ProcessInfo not in ('Backup','Logon')
--and not (e.ProcessInfo = 'Backup' and (e.Text like 'Log was backed up%' or e.Text like 'Database backed up. %' or e.Text like 'BACKUP DATABASE successfully%') )
--and e.Text not like 'DbMgrPartnerCommitPolicy::SetSyncState:%'
--and e.Text not like 'SQL Server blocked access to procedure%'
--and e.Text not like 'Login failed for user %'
--and e.Text not like 'AlwaysOn Availability Groups connection with secondary database terminated for primary database %'
--and e.Text not like 'AlwaysOn Availability Groups connection with secondary database established for primary database %'
--and e.Text like '%ALTER DATABASE%'
order by LogDate asc

/*
select create_date 
from sys.databases d
where d.name = 'tempdb'

*/