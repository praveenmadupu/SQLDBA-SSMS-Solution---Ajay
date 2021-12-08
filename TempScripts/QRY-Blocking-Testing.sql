USE DBA
GO

EXEC [dbo].[usp_get_blocking_alert] 
	@verbose = 2,
	@recipients = 'sqlagentservice@gmail.com',
	@threshold_minutes = 2,
	@delay_minutes = 1,
	@alert_key = 'Alert-SdtBlocking',
	@is_test_alert = 1
go

select *
from DBA.dbo.WhoIsActive w
where w.collection_time = (select max(i.collection_time) from DBA.dbo.WhoIsActive i)

-- exec sp_WhoIsActive @help = 1

exec msdb.dbo.sp_start_job @job_name = 'SQLWATCH-LOGGER-WHOISACTIVE'
go
