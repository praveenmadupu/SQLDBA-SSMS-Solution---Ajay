USE [msdb]
GO

/****** Object:  Job [(dba) Collect Metrics]    Script Date: 06-Sep-20 9:57:17 PM ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [Data Collector]    Script Date: 06-Sep-20 9:57:17 PM ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Data Collector' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'Data Collector'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'(dba) Collect Metrics', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'Collect CPU/Memory metrics', 
		@category_name=N'Data Collector', 
		@owner_login_name=N'sa', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Collect Metrics - dm_os_sys_memory]    Script Date: 06-Sep-20 9:57:17 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Collect Metrics - dm_os_sys_memory', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'set nocount on;
exec DBA..usp_collect_performance_metrics @metrics = ''dm_os_sys_memory'';
--exec msdb..sp_start_job @job_name = ''(dba) Collect Performance Metrics - 2'';', 
		@database_name=N'DBA', 
		@flags=8
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Collect Metrics - dm_os_process_memory]    Script Date: 06-Sep-20 9:57:17 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Collect Metrics - dm_os_process_memory', 
		@step_id=2, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'exec DBA..usp_collect_performance_metrics @metrics = ''dm_os_process_memory'';', 
		@database_name=N'master', 
		@flags=8
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Collect Metrics - dm_os_performance_counters]    Script Date: 06-Sep-20 9:57:17 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Collect Metrics - dm_os_performance_counters', 
		@step_id=3, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'exec DBA..usp_collect_performance_metrics @metrics = ''dm_os_performance_counters'';', 
		@database_name=N'master', 
		@flags=8
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Collect Metrics - dm_os_ring_buffers]    Script Date: 06-Sep-20 9:57:17 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Collect Metrics - dm_os_ring_buffers', 
		@step_id=4, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'exec msdb..sp_start_job @job_name = ''(dba) Collect Metrics - dm_os_ring_buffers''', 
		@database_name=N'master', 
		@flags=8
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Collect Metrics - dm_os_performance_counters_sampling]    Script Date: 06-Sep-20 9:57:17 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Collect Metrics - dm_os_performance_counters_sampling', 
		@step_id=5, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'exec msdb..sp_start_job @job_name = ''(dba) Collect Metrics - dm_os_performance_counters_sampling'';', 
		@database_name=N'master', 
		@flags=8
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'(dba) Collect Performance Metrics - 2 - 01', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=2, 
		@freq_subday_interval=10, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20200815, 
		@active_end_date=99991231, 
		@active_start_time=0, 
		@active_end_time=235959, 
		@schedule_uid=N'12a61bc4-5fa1-47cf-b68b-a1b628d665f4'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO

/****** Object:  Job [(dba) Collect Metrics - dm_os_performance_counters_deprecated_features]    Script Date: 06-Sep-20 9:57:17 PM ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [Data Collector]    Script Date: 06-Sep-20 9:57:17 PM ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Data Collector' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'Data Collector'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'(dba) Collect Metrics - dm_os_performance_counters_deprecated_features', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'No description available.', 
		@category_name=N'Data Collector', 
		@owner_login_name=N'sa', 
		@notify_email_operator_name=N'dba', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Collect Metrics - Deprecated Features]    Script Date: 06-Sep-20 9:57:17 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Collect Metrics - Deprecated Features', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'exec DBA..usp_collect_performance_metrics @metrics = ''dm_os_performance_counters_deprecated_features'';', 
		@database_name=N'master', 
		@flags=8
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'(db) Collect Metrics - dm_os_performance_counters_deprecated_features', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=8, 
		@freq_subday_interval=4, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20200823, 
		@active_end_date=99991231, 
		@active_start_time=0, 
		@active_end_time=235959, 
		@schedule_uid=N'4caf120c-46ba-4dc7-839c-561628435bfe'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO

/****** Object:  Job [(dba) Collect Metrics - dm_os_performance_counters_sampling]    Script Date: 06-Sep-20 9:57:17 PM ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [Data Collector]    Script Date: 06-Sep-20 9:57:17 PM ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Data Collector' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'Data Collector'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'(dba) Collect Metrics - dm_os_performance_counters_sampling', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@category_name=N'Data Collector', 
		@owner_login_name=N'sa', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [(dba) Collect Metrics]    Script Date: 06-Sep-20 9:57:17 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'(dba) Collect Metrics', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'exec DBA..usp_collect_performance_metrics @metrics = ''dm_os_performance_counters_sampling'';', 
		@database_name=N'master', 
		@flags=8
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO

/****** Object:  Job [(dba) Collect Metrics - dm_os_ring_buffers]    Script Date: 06-Sep-20 9:57:17 PM ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [Data Collector]    Script Date: 06-Sep-20 9:57:17 PM ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Data Collector' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'Data Collector'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'(dba) Collect Metrics - dm_os_ring_buffers', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'Collect CPU/Memory metrics', 
		@category_name=N'Data Collector', 
		@owner_login_name=N'sa', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Collect-Metrics - dm_os_ring_buffers]    Script Date: 06-Sep-20 9:57:17 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Collect-Metrics - dm_os_ring_buffers', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'exec DBA..usp_collect_performance_metrics @metrics = ''dm_os_ring_buffers'';', 
		@database_name=N'DBA', 
		@flags=8
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO

/****** Object:  Job [(dba) Collect Metrics - dm_os_sys_info]    Script Date: 06-Sep-20 9:57:17 PM ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [Data Collector]    Script Date: 06-Sep-20 9:57:17 PM ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Data Collector' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'Data Collector'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'(dba) Collect Metrics - dm_os_sys_info', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'Collect SqlServer start time', 
		@category_name=N'Data Collector', 
		@owner_login_name=N'sa', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Collect Metrics - dm_os_sys_info]    Script Date: 06-Sep-20 9:57:17 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Collect Metrics - dm_os_sys_info', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'SET NOCOUNT ON;
INSERT INTO DBA.dbo.dm_os_sys_info
SELECT collection_time = getdate(), 
		server_name = @@SERVERNAME, 
		sqlserver_start_time, 
		wait_stats_cleared_time = ws.[Date/TimeCleared],
		cpu_count, physical_memory_kb, max_workers_count, virtual_machine_type, softnuma_configuration_desc, socket_count, cores_per_socket, numa_node_count
		--,WS.*
--INTO DBA..dm_os_sys_info
FROM sys.dm_os_sys_info as si
OUTER APPLY
	(	SELECT	[wait_type],
				[wait_time_ms],
				[Date/TimeCleared] = CAST(DATEADD(ms,-[wait_time_ms],getdate()) as smalldatetime),
				CASE
				WHEN [wait_time_ms] < 1000 THEN CAST([wait_time_ms] AS VARCHAR(15)) + '' ms''
				WHEN [wait_time_ms] between 1000 and 60000 THEN CAST(([wait_time_ms]/1000) AS VARCHAR(15)) + '' seconds''
				WHEN [wait_time_ms] between 60001 and 3600000 THEN CAST(([wait_time_ms]/60000) AS VARCHAR(15)) + '' minutes''
				WHEN [wait_time_ms] between 3600001 and 86400000 THEN CAST(([wait_time_ms]/3600000) AS VARCHAR(15)) + '' hours''
				WHEN [wait_time_ms] > 86400000 THEN CAST(([wait_time_ms]/86400000) AS VARCHAR(15)) + '' days''
		END [TimeSinceCleared]
		FROM [sys].[dm_os_wait_stats]
		WHERE [wait_type] = ''SQLTRACE_INCREMENTAL_FLUSH_SLEEP''
	) AS ws
WHERE NOT EXISTS (select * from DBA.dbo.dm_os_sys_info as i where i.sqlserver_start_time = si.sqlserver_start_time and i.wait_stats_cleared_time = ws.[Date/TimeCleared])

', 
		@database_name=N'master', 
		@flags=8
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'(dba) Collect Metrics - dm_os_sys_info', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=2, 
		@freq_subday_interval=10, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20200824, 
		@active_end_date=99991231, 
		@active_start_time=0, 
		@active_end_time=235959, 
		@schedule_uid=N'8d73cb10-d879-429a-975e-5ee1388326ad'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO

/****** Object:  Job [(dba) Collect Metrics - NonSqlServer Perfmon Counters]    Script Date: 06-Sep-20 9:57:17 PM ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [Data Collector]    Script Date: 06-Sep-20 9:57:17 PM ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Data Collector' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'Data Collector'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'(dba) Collect Metrics - NonSqlServer Perfmon Counters', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'powershell.exe -ExecutionPolicy Bypass .  ''D:\MSSQL15.MSSQLSERVER\MSSQL\Perfmon\perfmon-collector-push-to-sqlserver.ps1'';

https://docs.microsoft.com/en-us/windows/win32/perfctrs/counterdata?redirectedfrom=MSDN', 
		@category_name=N'Data Collector', 
		@owner_login_name=N'sa', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [truncate table [dbo].[CounterData]]    Script Date: 06-Sep-20 9:57:17 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'truncate table [dbo].[CounterData]', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'if not exists (select * from [dbo].[DisplayToID] where RunID = 0)
begin
	truncate table [dbo].[CounterData];
	truncate table [dbo].[CounterDetails];
end', 
		@database_name=N'DBA', 
		@flags=8
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [NonSqlServer Perfmon Counters]    Script Date: 06-Sep-20 9:57:17 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'NonSqlServer Perfmon Counters', 
		@step_id=2, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'CmdExec', 
		@command=N'powershell.exe -ExecutionPolicy Bypass .  ''D:\MSSQL15.MSSQLSERVER\MSSQL\Perfmon\perfmon-collector-push-to-sqlserver.ps1'';', 
		@flags=8
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Populate dbo.dm_os_performance_counters_nonsql]    Script Date: 06-Sep-20 9:57:17 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Populate dbo.dm_os_performance_counters_nonsql', 
		@step_id=3, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'INSERT into dbo.dm_os_performance_counters_nonsql
(collection_time, server_name, [object_name], counter_name, instance_name, cntr_value, cntr_type, id)
select	collection_time = dbo.perfmon2local(dt.CounterDateTime)
		,server_name = REPLACE(MachineName,''\\'','''')
		,[object_name] = dtls.ObjectName
		,counter_name = dtls.CounterName
		,instance_name = dtls.InstanceName
		,cntr_value = AVG(CounterValue)
		,cntr_type = dtls.CounterType
		,id = ROW_NUMBER()OVER(ORDER BY getdate())
FROM dbo.CounterData as dt -- GUID, CounterID, RecordIndex
   JOIN dbo.CounterDetails as dtls ON dtls.CounterID = dt.CounterID --CounterID
   --JOIN dbo.DisplayToID as di ON di.GUID = dt.GUID -- GUID (pk), DisplayString (nci)
--where dtls.ObjectName = ''Process''
--and dtls.CounterName = ''% Processor Time''
--and dtls.InstanceName = ''sqlservr''
GROUP BY dbo.perfmon2local(dt.CounterDateTime), REPLACE(MachineName,''\\'',''''), 
				dtls.ObjectName, dtls.CounterName, dtls.InstanceName, dtls.CounterType
ORDER BY collection_time, server_name, [object_name], counter_name;

update [dbo].[DisplayToID]
set RunID = 1
where RunID = 0;', 
		@database_name=N'DBA', 
		@flags=8
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'(dba) Collect Metrics - NonSqlServer Perfmon Counters', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=2, 
		@freq_subday_interval=20, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20200823, 
		@active_end_date=99991231, 
		@active_start_time=0, 
		@active_end_time=235959, 
		@schedule_uid=N'aaf78c03-6d6b-46c6-81ca-21d6b32ac508'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO

/****** Object:  Job [(dba) Collect Metrics - Purge Tables & Files]    Script Date: 06-Sep-20 9:57:17 PM ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [Data Collector]    Script Date: 06-Sep-20 9:57:17 PM ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Data Collector' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'Data Collector'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'(dba) Collect Metrics - Purge Tables & Files', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'No description available.', 
		@category_name=N'Data Collector', 
		@owner_login_name=N'sa', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Delete PerfMon Files]    Script Date: 06-Sep-20 9:57:17 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Delete PerfMon Files', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'CmdExec', 
		@command=N'powershell.exe -ExecutionPolicy Bypass .  ''D:\MSSQL15.MSSQLSERVER\MSSQL\Perfmon\perfmon-remove-imported-files.ps1'';', 
		@flags=8
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Purge [dbo].[dm_os_memory_clerks]]    Script Date: 06-Sep-20 9:57:17 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Purge [dbo].[dm_os_memory_clerks]', 
		@step_id=2, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'set nocount on;
declare @retention_days int;
set @retention_days = 90;

declare @r int;
set @r = 1;

while @r > 0
begin
	delete top (10000) [dbo].[dm_os_memory_clerks] where collection_time < DATEADD(day,-@retention_days,GETDATE());

	set @r = @@ROWCOUNT;
end', 
		@database_name=N'DBA', 
		@flags=8
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Purge [dbo].[dm_os_performance_counters]]    Script Date: 06-Sep-20 9:57:17 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Purge [dbo].[dm_os_performance_counters]', 
		@step_id=3, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'set nocount on;
declare @retention_days int;
set @retention_days = 90;

declare @r int;
set @r = 1;

while @r > 0
begin
	delete top (10000) [dbo].[dm_os_performance_counters] where collection_time < DATEADD(day,-@retention_days,GETDATE());

	set @r = @@ROWCOUNT;
end', 
		@database_name=N'DBA', 
		@flags=8
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Purge [dbo].[dm_os_performance_counters_nonsql]]    Script Date: 06-Sep-20 9:57:17 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Purge [dbo].[dm_os_performance_counters_nonsql]', 
		@step_id=4, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'set nocount on;
declare @retention_days int;
set @retention_days = 90;

declare @r int;
set @r = 1;

while @r > 0
begin
	delete top (10000) [dbo].[dm_os_performance_counters_nonsql] where collection_time < DATEADD(day,-@retention_days,GETDATE());

	set @r = @@ROWCOUNT;
end', 
		@database_name=N'DBA', 
		@flags=8
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Purge [dbo].[dm_os_process_memory]]    Script Date: 06-Sep-20 9:57:17 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Purge [dbo].[dm_os_process_memory]', 
		@step_id=5, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'set nocount on;
declare @retention_days int;
set @retention_days = 90;

declare @r int;
set @r = 1;

while @r > 0
begin
	delete top (10000) [dbo].[dm_os_process_memory] where collection_time < DATEADD(day,-@retention_days,GETDATE());

	set @r = @@ROWCOUNT;
end', 
		@database_name=N'DBA', 
		@flags=8
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Purge [dbo].[dm_os_ring_buffers]]    Script Date: 06-Sep-20 9:57:17 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Purge [dbo].[dm_os_ring_buffers]', 
		@step_id=6, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'set nocount on;
declare @retention_days int;
set @retention_days = 90;

declare @r int;
set @r = 1;

while @r > 0
begin
	delete top (10000) [dbo].[dm_os_ring_buffers] where collection_time < DATEADD(day,-@retention_days,GETDATE());

	set @r = @@ROWCOUNT;
end', 
		@database_name=N'DBA', 
		@flags=8
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Purge [dbo].[dm_os_sys_info]]    Script Date: 06-Sep-20 9:57:17 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Purge [dbo].[dm_os_sys_info]', 
		@step_id=7, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'set nocount on;
declare @retention_days int;
set @retention_days = 90;

declare @r int;
set @r = 1;

while @r > 0
begin
	delete top (10000) [dbo].[dm_os_sys_info] where collection_time < DATEADD(day,-@retention_days,GETDATE());

	set @r = @@ROWCOUNT;
end', 
		@database_name=N'DBA', 
		@flags=8
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Purge [dbo].[dm_os_sys_memory]]    Script Date: 06-Sep-20 9:57:17 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Purge [dbo].[dm_os_sys_memory]', 
		@step_id=8, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'set nocount on;
declare @retention_days int;
set @retention_days = 90;

declare @r int;
set @r = 1;

while @r > 0
begin
	delete top (10000) [dbo].[dm_os_sys_memory] where collection_time < DATEADD(day,-@retention_days,GETDATE());

	set @r = @@ROWCOUNT;
end', 
		@database_name=N'DBA', 
		@flags=8
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Purge [dbo].[WaitStats]]    Script Date: 06-Sep-20 9:57:17 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Purge [dbo].[WaitStats]', 
		@step_id=9, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'set nocount on;
declare @retention_days int;
set @retention_days = 90;

declare @r int;
set @r = 1;

while @r > 0
begin
	delete top (10000) [dbo].[WaitStats] where collection_time < DATEADD(day,-@retention_days,GETDATE());

	set @r = @@ROWCOUNT;
end', 
		@database_name=N'DBA', 
		@flags=8
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'(dba) Collect Metrics - Purge Tables & Files', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=8, 
		@freq_subday_interval=4, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20200823, 
		@active_end_date=99991231, 
		@active_start_time=0, 
		@active_end_time=235959, 
		@schedule_uid=N'f68ed208-9e03-4fd6-a44f-fea2c99ad468'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO

/****** Object:  Job [(dba) Collect Metrics - Wait Stats]    Script Date: 06-Sep-20 9:57:17 PM ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [Data Collector]    Script Date: 06-Sep-20 9:57:17 PM ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Data Collector' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'Data Collector'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'(dba) Collect Metrics - Wait Stats', 
		@enabled=1, 
		@notify_level_eventlog=2, 
		@notify_level_email=2, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'No description available.', 
		@category_name=N'Data Collector', 
		@owner_login_name=N'sa', 
		@notify_email_operator_name=N'dba', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Collect Metrics - Wait Stats]    Script Date: 06-Sep-20 9:57:17 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Collect Metrics - Wait Stats', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'SET NOCOUNT ON;
-- Last updated February 26, 2019
;WITH [Waits] AS (
	SELECT
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
        N''BROKER_EVENTHANDLER'', -- https://www.sqlskills.com/help/waits/BROKER_EVENTHANDLER
        N''BROKER_RECEIVE_WAITFOR'', -- https://www.sqlskills.com/help/waits/BROKER_RECEIVE_WAITFOR
        N''BROKER_TASK_STOP'', -- https://www.sqlskills.com/help/waits/BROKER_TASK_STOP
        N''BROKER_TO_FLUSH'', -- https://www.sqlskills.com/help/waits/BROKER_TO_FLUSH
        N''BROKER_TRANSMITTER'', -- https://www.sqlskills.com/help/waits/BROKER_TRANSMITTER
        N''CHECKPOINT_QUEUE'', -- https://www.sqlskills.com/help/waits/CHECKPOINT_QUEUE
        N''CHKPT'', -- https://www.sqlskills.com/help/waits/CHKPT
        N''CLR_AUTO_EVENT'', -- https://www.sqlskills.com/help/waits/CLR_AUTO_EVENT
        N''CLR_MANUAL_EVENT'', -- https://www.sqlskills.com/help/waits/CLR_MANUAL_EVENT
        N''CLR_SEMAPHORE'', -- https://www.sqlskills.com/help/waits/CLR_SEMAPHORE
        N''CXCONSUMER'', -- https://www.sqlskills.com/help/waits/CXCONSUMER
        -- Maybe comment these four out if you have mirroring issues
        N''DBMIRROR_DBM_EVENT'', -- https://www.sqlskills.com/help/waits/DBMIRROR_DBM_EVENT
        N''DBMIRROR_EVENTS_QUEUE'', -- https://www.sqlskills.com/help/waits/DBMIRROR_EVENTS_QUEUE
        N''DBMIRROR_WORKER_QUEUE'', -- https://www.sqlskills.com/help/waits/DBMIRROR_WORKER_QUEUE
        N''DBMIRRORING_CMD'', -- https://www.sqlskills.com/help/waits/DBMIRRORING_CMD
        N''DIRTY_PAGE_POLL'', -- https://www.sqlskills.com/help/waits/DIRTY_PAGE_POLL
        N''DISPATCHER_QUEUE_SEMAPHORE'', -- https://www.sqlskills.com/help/waits/DISPATCHER_QUEUE_SEMAPHORE
        N''EXECSYNC'', -- https://www.sqlskills.com/help/waits/EXECSYNC
        N''FSAGENT'', -- https://www.sqlskills.com/help/waits/FSAGENT
        N''FT_IFTS_SCHEDULER_IDLE_WAIT'', -- https://www.sqlskills.com/help/waits/FT_IFTS_SCHEDULER_IDLE_WAIT
        N''FT_IFTSHC_MUTEX'', -- https://www.sqlskills.com/help/waits/FT_IFTSHC_MUTEX
        -- Maybe comment these six out if you have AG issues
        N''HADR_CLUSAPI_CALL'', -- https://www.sqlskills.com/help/waits/HADR_CLUSAPI_CALL
        N''HADR_FILESTREAM_IOMGR_IOCOMPLETION'', -- https://www.sqlskills.com/help/waits/HADR_FILESTREAM_IOMGR_IOCOMPLETION
        N''HADR_LOGCAPTURE_WAIT'', -- https://www.sqlskills.com/help/waits/HADR_LOGCAPTURE_WAIT
        N''HADR_NOTIFICATION_DEQUEUE'', -- https://www.sqlskills.com/help/waits/HADR_NOTIFICATION_DEQUEUE
        N''HADR_TIMER_TASK'', -- https://www.sqlskills.com/help/waits/HADR_TIMER_TASK
        N''HADR_WORK_QUEUE'', -- https://www.sqlskills.com/help/waits/HADR_WORK_QUEUE
        N''KSOURCE_WAKEUP'', -- https://www.sqlskills.com/help/waits/KSOURCE_WAKEUP
        N''LAZYWRITER_SLEEP'', -- https://www.sqlskills.com/help/waits/LAZYWRITER_SLEEP
        N''LOGMGR_QUEUE'', -- https://www.sqlskills.com/help/waits/LOGMGR_QUEUE
        N''MEMORY_ALLOCATION_EXT'', -- https://www.sqlskills.com/help/waits/MEMORY_ALLOCATION_EXT
        N''ONDEMAND_TASK_QUEUE'', -- https://www.sqlskills.com/help/waits/ONDEMAND_TASK_QUEUE
        N''PARALLEL_REDO_DRAIN_WORKER'', -- https://www.sqlskills.com/help/waits/PARALLEL_REDO_DRAIN_WORKER
        N''PARALLEL_REDO_LOG_CACHE'', -- https://www.sqlskills.com/help/waits/PARALLEL_REDO_LOG_CACHE
        N''PARALLEL_REDO_TRAN_LIST'', -- https://www.sqlskills.com/help/waits/PARALLEL_REDO_TRAN_LIST
        N''PARALLEL_REDO_WORKER_SYNC'', -- https://www.sqlskills.com/help/waits/PARALLEL_REDO_WORKER_SYNC
        N''PARALLEL_REDO_WORKER_WAIT_WORK'', -- https://www.sqlskills.com/help/waits/PARALLEL_REDO_WORKER_WAIT_WORK
        N''PREEMPTIVE_OS_FLUSHFILEBUFFERS'', -- https://www.sqlskills.com/help/waits/PREEMPTIVE_OS_FLUSHFILEBUFFERS
        N''PREEMPTIVE_XE_GETTARGETSTATE'', -- https://www.sqlskills.com/help/waits/PREEMPTIVE_XE_GETTARGETSTATE
        N''PWAIT_ALL_COMPONENTS_INITIALIZED'', -- https://www.sqlskills.com/help/waits/PWAIT_ALL_COMPONENTS_INITIALIZED
        N''PWAIT_DIRECTLOGCONSUMER_GETNEXT'', -- https://www.sqlskills.com/help/waits/PWAIT_DIRECTLOGCONSUMER_GETNEXT
        N''QDS_PERSIST_TASK_MAIN_LOOP_SLEEP'', -- https://www.sqlskills.com/help/waits/QDS_PERSIST_TASK_MAIN_LOOP_SLEEP
        N''QDS_ASYNC_QUEUE'', -- https://www.sqlskills.com/help/waits/QDS_ASYNC_QUEUE
        N''QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP'',
            -- https://www.sqlskills.com/help/waits/QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP
        N''QDS_SHUTDOWN_QUEUE'', -- https://www.sqlskills.com/help/waits/QDS_SHUTDOWN_QUEUE
        N''REDO_THREAD_PENDING_WORK'', -- https://www.sqlskills.com/help/waits/REDO_THREAD_PENDING_WORK
        N''REQUEST_FOR_DEADLOCK_SEARCH'', -- https://www.sqlskills.com/help/waits/REQUEST_FOR_DEADLOCK_SEARCH
        N''RESOURCE_QUEUE'', -- https://www.sqlskills.com/help/waits/RESOURCE_QUEUE
        N''SERVER_IDLE_CHECK'', -- https://www.sqlskills.com/help/waits/SERVER_IDLE_CHECK
        N''SLEEP_BPOOL_FLUSH'', -- https://www.sqlskills.com/help/waits/SLEEP_BPOOL_FLUSH
        N''SLEEP_DBSTARTUP'', -- https://www.sqlskills.com/help/waits/SLEEP_DBSTARTUP
        N''SLEEP_DCOMSTARTUP'', -- https://www.sqlskills.com/help/waits/SLEEP_DCOMSTARTUP
        N''SLEEP_MASTERDBREADY'', -- https://www.sqlskills.com/help/waits/SLEEP_MASTERDBREADY
        N''SLEEP_MASTERMDREADY'', -- https://www.sqlskills.com/help/waits/SLEEP_MASTERMDREADY
        N''SLEEP_MASTERUPGRADED'', -- https://www.sqlskills.com/help/waits/SLEEP_MASTERUPGRADED
        N''SLEEP_MSDBSTARTUP'', -- https://www.sqlskills.com/help/waits/SLEEP_MSDBSTARTUP
        N''SLEEP_SYSTEMTASK'', -- https://www.sqlskills.com/help/waits/SLEEP_SYSTEMTASK
        N''SLEEP_TASK'', -- https://www.sqlskills.com/help/waits/SLEEP_TASK
        N''SLEEP_TEMPDBSTARTUP'', -- https://www.sqlskills.com/help/waits/SLEEP_TEMPDBSTARTUP
        N''SNI_HTTP_ACCEPT'', -- https://www.sqlskills.com/help/waits/SNI_HTTP_ACCEPT
        N''SOS_WORK_DISPATCHER'', -- https://www.sqlskills.com/help/waits/SOS_WORK_DISPATCHER
        N''SP_SERVER_DIAGNOSTICS_SLEEP'', -- https://www.sqlskills.com/help/waits/SP_SERVER_DIAGNOSTICS_SLEEP
        N''SQLTRACE_BUFFER_FLUSH'', -- https://www.sqlskills.com/help/waits/SQLTRACE_BUFFER_FLUSH
        N''SQLTRACE_INCREMENTAL_FLUSH_SLEEP'', -- https://www.sqlskills.com/help/waits/SQLTRACE_INCREMENTAL_FLUSH_SLEEP
        N''SQLTRACE_WAIT_ENTRIES'', -- https://www.sqlskills.com/help/waits/SQLTRACE_WAIT_ENTRIES
        N''VDI_CLIENT_OTHER'', -- https://www.sqlskills.com/help/waits/VDI_CLIENT_OTHER
        N''WAIT_FOR_RESULTS'', -- https://www.sqlskills.com/help/waits/WAIT_FOR_RESULTS
        N''WAITFOR'', -- https://www.sqlskills.com/help/waits/WAITFOR
        N''WAITFOR_TASKSHUTDOWN'', -- https://www.sqlskills.com/help/waits/WAITFOR_TASKSHUTDOWN
        N''WAIT_XTP_RECOVERY'', -- https://www.sqlskills.com/help/waits/WAIT_XTP_RECOVERY
        N''WAIT_XTP_HOST_WAIT'', -- https://www.sqlskills.com/help/waits/WAIT_XTP_HOST_WAIT
        N''WAIT_XTP_OFFLINE_CKPT_NEW_LOG'', -- https://www.sqlskills.com/help/waits/WAIT_XTP_OFFLINE_CKPT_NEW_LOG
        N''WAIT_XTP_CKPT_CLOSE'', -- https://www.sqlskills.com/help/waits/WAIT_XTP_CKPT_CLOSE
        N''XE_DISPATCHER_JOIN'', -- https://www.sqlskills.com/help/waits/XE_DISPATCHER_JOIN
        N''XE_DISPATCHER_WAIT'', -- https://www.sqlskills.com/help/waits/XE_DISPATCHER_WAIT
        N''XE_TIMER_EVENT'' -- https://www.sqlskills.com/help/waits/XE_TIMER_EVENT
        )
    AND [waiting_tasks_count] > 0
)
INSERT INTO dbo.WaitStats
 ( [Collection_Time], RowNum, [WaitType] , [Wait_S] , [Resource_S] , [Signal_S] , [WaitCount] , [Percentage] )
SELECT [Collection_Time] = GETDATE(), RowNum,
		w.wait_type, w.WaitS, w.ResourceS, w.SignalS, w.WaitCount, w.Percentage 
FROM [Waits] AS w
ORDER BY [Collection_Time], WaitS DESC, ResourceS DESC, SignalS DESC
 /*
SELECT 
	W1.[Collection_Time],
    MAX ([W1].[WaitType]) AS [WaitType],
    CAST (MAX ([W1].[Wait_S]) AS DECIMAL (16,2)) AS [Wait_S],
    CAST (MAX ([W1].[Resource_S]) AS DECIMAL (16,2)) AS [Resource_S],
    CAST (MAX ([W1].[Signal_S]) AS DECIMAL (16,2)) AS [Signal_S],
    MAX ([W1].[WaitCount]) AS [WaitCount],
    CAST (MAX ([W1].[Percentage]) AS DECIMAL (5,2)) AS [Percentage],
    CAST (MAX ([W1].[AvgWait_S]) AS DECIMAL (16,4)) AS [AvgWait_S],
    CAST (MAX ([W1].[AvgRes_S]) AS DECIMAL (16,4)) AS [AvgRes_S],
    CAST (MAX ([W1].[AvgSig_S]) AS DECIMAL (16,4)) AS [AvgSig_S]
    --,MAX([Help/Info URL]) AS [Help/Info URL]
FROM dbo.WaitStats AS [W1]
INNER JOIN dbo.WaitStats AS [W2] ON [W2].[RowNum] <= [W1].[RowNum] AND W1.Collection_Time = W2.Collection_Time
GROUP BY W1.Collection_Time, [W1].[RowNum]
HAVING SUM ([W2].[Percentage]) - MAX( [W1].[Percentage] ) < 99 -- percentage threshold
ORDER BY Collection_Time, Percentage desc
*/', 
		@database_name=N'DBA', 
		@flags=8
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'(dba) Collect Metrics - Wait Stats', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=4, 
		@freq_subday_interval=1, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20200820, 
		@active_end_date=99991231, 
		@active_start_time=0, 
		@active_end_time=235959, 
		@schedule_uid=N'edf61f10-a94c-4a92-b2b4-c0baee7043ff'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO

/****** Object:  Job [(dba) Collect Metrics - Wait Stats Clearing]    Script Date: 06-Sep-20 9:57:17 PM ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [Data Collector]    Script Date: 06-Sep-20 9:57:17 PM ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Data Collector' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'Data Collector'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'(dba) Collect Metrics - Wait Stats Clearing', 
		@enabled=0, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'Montly Clear the Wait Stats', 
		@category_name=N'Data Collector', 
		@owner_login_name=N'sa', 
		@notify_email_operator_name=N'dba', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Wait Stats Clearing]    Script Date: 06-Sep-20 9:57:17 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Wait Stats Clearing', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'DBCC SQLPERF
(
     "sys.dm_os_wait_stats" , CLEAR 
)   
WITH NO_INFOMSGS ;', 
		@database_name=N'master', 
		@flags=8
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'(dba) Collect Metrics - Wait Stats Clearing', 
		@enabled=1, 
		@freq_type=16, 
		@freq_interval=1, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20200820, 
		@active_end_date=99991231, 
		@active_start_time=0, 
		@active_end_time=235959, 
		@schedule_uid=N'e4a954b8-3b9e-4e2d-931a-18470c1d7f28'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO


