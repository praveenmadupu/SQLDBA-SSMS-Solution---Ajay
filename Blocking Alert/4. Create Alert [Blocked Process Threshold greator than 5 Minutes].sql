USE [msdb]
GO

DECLARE @p_job_id BINARY(16);
select @p_job_id = job_id from msdb..sysjobs_view as j where j.name = 'DBA - Log_With_sp_WhoIsActive';

/****** Object:  Alert [Blocked Process Threshold > 5 Minutes]    Script Date: 8/3/2018 4:33:14 PM ******/
EXEC msdb.dbo.sp_add_alert @name=N'Blocked Process Threshold > 5 Minutes', 
		@message_id=0, 
		@severity=0, 
		@enabled=1, 
		@delay_between_responses=0, 
		@include_event_description_in=1, 
		@notification_message=N'Kindly check session details involved in blocking by using below query.

SELECT DENSE_RANK()OVER(ORDER BY collection_time ASC) AS CollectionBatchNO, *
  FROM [DBA].[dbo].[WhoIsActive_ResultSets] as r
  WHERE r.blocking_session_id IS NOT NULL OR r.blocked_session_count > 0;

Thanks & Regards,
SQL Alerts', 
		@category_name=N'[Uncategorized]', 
		@wmi_namespace=N'\\.\root\Microsoft\SqlServer\ServerEvents\MSSQLSERVER', 
		@wmi_query=N'SELECT * FROM BLOCKED_PROCESS_REPORT Where Duration >= 300000', 
		@job_id = @p_job_id
GO


