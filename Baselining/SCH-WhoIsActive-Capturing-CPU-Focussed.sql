USE DBA;
go

SET NOCOUNT ON;
SET QUOTED_IDENTIFIER ON;
SET ANSI_WARNINGS ON;

-- Parameters
DECLARE @retention_day int = 15;
DECLARE @drop_recreate bit = 0;
DECLARE	@destination_table VARCHAR(4000) = 'dbo.who_is_active';
DECLARE	@staging_table VARCHAR(4000) = @destination_table+'_staging';
DECLARE @output_column_list VARCHAR(8000);
DECLARE @output VARCHAR(8000);
DECLARE @send_error_mail bit = 1;
DECLARE @threshold_continous_failure tinyint = 3;
DECLARE @is_test_alert bit = 0;
DECLARE @recipients varchar(500) = 'dba@gmail.com';

SET @output_column_list = '[collection_time][dd hh:mm:ss.mss][session_id][program_name][login_name][database_name]
						[CPU][CPU_delta][used_memory][used_memory_delta][open_tran_count][status][wait_info][sql_command]
                        [blocked_session_count][blocking_session_id][sql_text][%]';

SET @output = 'Declare local variables'+CHAR(10);
-- Local Variables
DECLARE @rows_affected int = 0;
DECLARE @s VARCHAR(MAX);
DECLARE @collection_time datetime;
DECLARE @columns VARCHAR(8000);
DECLARE @cpu_system int;
DECLARE @cpu_sql int;

-- Variables for Try/Catch Block
DECLARE @profile_name varchar(200);
DECLARE	@_errorNumber int,
		@_errorSeverity int,
		@_errorState int,
		@_errorLine int,
		@_errorMessage nvarchar(4000);

BEGIN TRY
	SET @output += '<br>Start Try Block..'+CHAR(10);

	-- Step 01: Truncate/Create Staging table
	IF ( (OBJECT_ID(@staging_table) IS NULL) OR (@drop_recreate = 1))
	BEGIN
		SET @output += '<br>Inside Step 01: Create Staging table..'+CHAR(10);
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
		SET @output += '<br>Inside Step 01: Truncate Staging table..'+CHAR(10);
		SET @s = 'TRUNCATE TABLE '+@staging_table;
		EXEC(@s);
		--select * from dbo.who_is_active_staging
	END
	
	-- Step 02: Create main table if Not Exists
	IF ( (OBJECT_ID(@destination_table) IS NULL) OR (@drop_recreate = 1))
	BEGIN
		SET @output += '<br>Inside Step 02: Create main table if Not Exists..'+CHAR(10);
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
		SET @output += '<br>Inside Step 03: Add a clustered Index..'+CHAR(10);
		--EXEC('CREATE CLUSTERED INDEX [ci_who_is_active] ON '+@destination_table+' ( [collection_time] ASC, cpu_rank )')
		EXEC ('ALTER TABLE '+@destination_table+' ADD CONSTRAINT pk_who_is_active PRIMARY KEY CLUSTERED ( [collection_time] ASC, cpu_rank )')
	END

	-- Step 04: Purge Old data
	SET @output += '<br>Execute Step 04: Purge Old data..'+CHAR(10);
	SET @s = 'DELETE FROM '+@destination_table+' where collection_time < DATEADD(day,-'+cast(@retention_day as varchar)+',getdate());'
	EXEC(@s);

	-- Step 05: Create Log Table
	SET @output += '<br>Execute Step 04: Create Log Table dbo.CommandLogWhoIsActive..'+CHAR(10);
	IF OBJECT_ID('dbo.CommandLogWhoIsActive') IS NULL
	BEGIN
		SET @s = '
IF OBJECT_ID(''dbo.CommandLogWhoIsActive'') IS NULL
	create table dbo.CommandLogWhoIsActive (collection_time datetime2 default SYSDATETIME() not null, status varchar(30) not null, message nvarchar(max) null);
IF OBJECT_ID(''pk_CommandLogWhoIsActive'') IS NULL
	alter table dbo.CommandLogWhoIsActive add constraint pk_CommandLogWhoIsActive primary key clustered (collection_time);
'
		EXEC (@s);
	END

	-- Step 06: Populate Staging table
	SET @output += '<br>Execute Step 06: Populate Staging table..'+CHAR(10);
	EXEC dbo.sp_WhoIsActive @get_outer_command=1, @get_task_info=2, @find_block_leaders=1, @get_plans=1, @get_avg_time=1, @get_additional_info=1, @delta_interval = 10
				,@output_column_list = @output_column_list
				,@destination_table = @staging_table;
	SET @output += '<br>Set @rows_affected..'+CHAR(10);
	SET @rows_affected = ISNULL(@@ROWCOUNT,0);
	
	-- Step 07: Update missing Query Plan
	SET @output += '<br>Execute Step 07: Update missing Query Plan..'+CHAR(10);
	SET @s = '
	update w
	set query_plan = qp.query_plan
	from '+@staging_table+' AS w
	join sys.dm_exec_sessions as s 
	on s.session_id = w.session_id and s.host_name = w.host_name and s.program_name = w.program_name and s.login_name = w.login_name and s.database_id = DB_ID(w.database_name)
	join sys.dm_exec_requests as r
	on s.session_id = r.session_id and w.request_id = r.request_id 
	outer apply sys.dm_exec_text_query_plan(r.plan_handle, r.statement_start_offset, r.statement_end_offset) as qp
	where w.collection_time = (select max(ri.collection_time) from '+@staging_table+' AS ri)
	and w.query_plan IS NULL and qp.query_plan is not null;
	';
	--PRINT @s
	BEGIN TRY
		EXEC (@s);
	END TRY
	BEGIN CATCH
		PRINT 'ERROR => '+CHAR(10)+Error_Message();
		SET @output += '<br>ERROR => '+CHAR(10)+Error_Message()+CHAR(10);
	END CATCH

	-- Step 08: Populate Main table
	SET @output += '<br>Execute Step 08: Populate Main table..'+CHAR(10);
	
	SELECT @columns = COALESCE(@columns+','+QUOTENAME(c.COLUMN_NAME),QUOTENAME(c.COLUMN_NAME)) 
	FROM INFORMATION_SCHEMA.COLUMNS c WHERE OBJECT_ID(c.TABLE_SCHEMA+'.'+TABLE_NAME) = OBJECT_ID(@staging_table)
	ORDER BY c.ORDINAL_POSITION;

	SET @output += '<br>Fetch @cpu_system & @cpu_sql..'+CHAR(10);
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
	
	SET @output += '<br>Calculate cpu_rank, CPU_delta_percent, pool & CPU_delta_all..'+CHAR(10);
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
												ELSE ISNULL(CONVERT(bigint,REPLACE(CPU_delta,'','',''''))*100/SUM(CONVERT(bigint,REPLACE(CPU_delta,'','',''''))) over(partition by collection_time),0)
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
												ELSE ISNULL(CONVERT(bigint,REPLACE(CPU_delta,'','',''''))*100/SUM(CONVERT(bigint,REPLACE(CPU_delta,'','',''''))) over(partition by collection_time),0)
												END),
			[pool] = ''REST'',
			'+@columns+',
			[CPU_delta_all] = ISNULL(SUM(CONVERT(bigint,REPLACE(CPU_delta,'','',''''))) over(partition by collection_time),0)
	FROM '+@staging_table+' s
	ORDER BY cpu_rank;';
	END
	SET @output += '<br>Populate Main table..'+CHAR(10);
	--PRINT @s
	EXEC(@s);
	
	-- Step 09: Return rows affected
	SET @output += '<br>Execute Step 09: Return rows affected..'+CHAR(10);
	PRINT '[rows_affected] = '+CONVERT(varchar,ISNULL(@rows_affected,0));
	SET @output += '<br>FINISH. Script executed without error.'+CHAR(10);

	-- Step 10: Make Success log entry
	SET @output += '<br>Execute Step 10: Make Success log entry..'+CHAR(10);
	SET @s = 'INSERT dbo.CommandLogWhoIsActive (status)	SELECT [status] = ''Success''';
	EXEC (@s);
	
END TRY  -- Perform main logic inside Try/Catch
BEGIN CATCH
	PRINT @output

	DECLARE @_tableHTML  NVARCHAR(MAX);  
	DECLARE @_subject nvarchar(1000);
	DECLARE @_job_name nvarchar(500);
	DECLARE @_continous_failures tinyint = 0;

	SELECT @_job_name = '(dba) Run-SPWhoIsActive';
	SET @_subject = '[The job failed.] SQL Server Job System: '''+@_job_name+''' completed on \\'+@@SERVERNAME+'.'
	IF @is_test_alert = 1
		SET @_subject = 'TestAlert - '+@_subject;

	SELECT @_errorNumber	 = Error_Number()
				,@_errorSeverity = Error_Severity()
				,@_errorState	 = Error_State()
				,@_errorLine	 = Error_Line()
				,@_errorMessage	 = Error_Message();

	SET @_tableHTML =
		N'Sql Agent job '''+@_job_name+''' has failed @'+ CONVERT(nvarchar(30),getdate(),121) +'.'+
		N'<br><br>Error Number: ' + convert(varchar, @_errorNumber) + 
		N'<br>Line Number: ' + convert(varchar, @_errorLine) +
		N'<br>Error Message: <br>"' + @_errorMessage +
		N'<br><br>Kindly resolve the job failure based on above error message.'+
		N'<br><br>Below is Job Output till now -><br><br>'+@output+
		N'<br><br>Regards,'+
		N'<br>Job [(dba) Run-SPWhoIsActive]';

	INSERT dbo.CommandLogWhoIsActive (status, message)
	SELECT [status] = 'Failure', [message] = @_errorMessage;

	IF OBJECT_ID('tempdb..#CommandLogWhoIsActive') IS NOT NULL
		DROP TABLE #CommandLogWhoIsActive;
	SELECT * 
	INTO #CommandLogWhoIsActive
	FROM dbo.CommandLogWhoIsActive 
	ORDER BY collection_time DESC 
	OFFSET 0 ROWS FETCH FIRST @threshold_continous_failure ROWS ONLY;

	SELECT @_continous_failures = COUNT(*) FROM #CommandLogWhoIsActive WHERE [status] = 'Failure';

	IF @send_error_mail = 1 AND @_continous_failures >= @threshold_continous_failure
	BEGIN
		SELECT @profile_name = p.name
		FROM msdb.dbo.sysmail_profile p 
		JOIN msdb.dbo.sysmail_principalprofile pp ON pp.profile_id = p.profile_id AND pp.is_default = 1
		JOIN msdb.dbo.sysmail_profileaccount pa ON p.profile_id = pa.profile_id 
		JOIN msdb.dbo.sysmail_account a ON pa.account_id = a.account_id 
		JOIN msdb.dbo.sysmail_server s ON a.account_id = s.account_id;

		EXEC msdb.dbo.sp_send_dbmail
				@recipients = @recipients,
				@profile_name = @profile_name,
				@subject = @_subject,
				@body = @_tableHTML,
				@body_format = 'HTML';
	END
	ELSE
		PRINT '@threshold_continous_failure not satified';

END CATCH

IF @_errorMessage IS NOT NULL
	THROW 50000, @_errorMessage, 1;
GO