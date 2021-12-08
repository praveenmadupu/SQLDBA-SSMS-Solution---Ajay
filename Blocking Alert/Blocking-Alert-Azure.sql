USE DBA
GO

IF OBJECT_ID('dbo.usp_get_blocking_alert') IS NULL
	EXEC('CREATE PROCEDURE [dbo].[usp_get_blocking_alert] AS SELECT 1 AS [Dummy];')
GO

ALTER PROCEDURE [dbo].[usp_get_blocking_alert] 
		@recipients varchar(1000) = 'dba@company.com',
		@threshold_minutes INT = 2,
		@delay_minutes int = 15,
		@verbose tinyint = 0,
		@alert_key varchar(100) = 'Sdt-AlertBlocking',
		@help BIT = 0
AS
BEGIN 
	/*
		Version:		0.2
		Created By:		Ajay Kumar Dwivedi
		Purpose:		To have custom alerting system for Consistant Blocking
		Modifications:	2021-Dec-06 - Enhacement module to made it standard code, and implement Auto Clear
	*/
	SET NOCOUNT ON;

	--	Global Variables
	IF @verbose > 0
		PRINT 'Inside usp_get_blocking_alert.'+char(10)+char(9)+'Declaring local variables..';
	DECLARE @_collection_time datetime;
	DECLARE @_latest_collection_time datetime;
	DECLARE @_second_latest_collection_time datetime;
	DECLARE @_default_html_style VARCHAR(100) = 'GreenBackgroundHeader';
	DECLARE @_mail_html  NVARCHAR(MAX) ;
	DECLARE @_subject VARCHAR(1000);
	DECLARE @_table_name VARCHAR(125);
	DECLARE @_column_list_4_table_header VARCHAR(MAX);
	DECLARE @_column_list_4_table_data VARCHAR(MAX);
	DECLARE @_css_style_green_background_header VARCHAR(MAX);
	DECLARE @_html_body VARCHAR(MAX);
	DECLARE @_sql_string VARCHAR(MAX);
	DECLARE @_data_4_table_data TABLE ( TableData VARCHAR(MAX) );
	DECLARE @_query_filter VARCHAR(2000);
	DECLARE @_send_mail bit = 0;
	DECLARE @_is_blocking_found bit = 0;
	DECLARE @_is_consistent_blocking_found bit = 0;
	DECLARE @_last_sent_blocking_active datetime;
	DECLARE @_last_sent_blocking_cleared datetime;

	-- Store details in Variables 
	SET @_collection_time = GETDATE();

	IF OBJECT_ID('dbo.sdt_blocking_alert') IS NULL
		CREATE TABLE [dbo].[sdt_blocking_alert]
		(
			[collection_time] datetime NULL,
			--[TimeInMinutes] bigint NULL,
			[dd hh:mm:ss.mss] varchar(2000),
			[blocking_tree] [nvarchar](max) NULL,
			[session_id] [smallint] NULL,
			[blocking_session_id] [smallint] NULL,
			--[sql_text] [xml] NULL,
			[host_name] varchar(128) NULL,
			[database_name] varchar(128) NULL,
			[login_name] varchar(128) NULL,
			[program_name] varchar(128) NULL,
			[wait_info] varchar(4000) NULL,
			--[blocked_session_count] varchar(30) NULL,
			--[locks] [xml] NULL,
			--[tran_start_time] smalldatetime NULL,
			[open_tran_count] smallint NULL,
			--[additional_info] [xml] NULL,
			[cpu] [varchar](30) NULL,
			[tempdb_allocations] [varchar](30) NULL,
			[tempdb_current] [varchar](30) NULL,
			[reads] [varchar](30) NULL,
			[writes] [varchar](30) NULL,
			--[physical_io] [varchar](30) NULL,
			[physical_reads] [varchar](30) NULL
		);
	ELSE
		TRUNCATE TABLE [dbo].[sdt_blocking_alert];

	--	Get latest 2 collection_time
	IF(@verbose > 0)
		PRINT 'Get latest 2 collection_time';
	SELECT @_latest_collection_time = MAX(r.collection_time) FROM dbo.WhoIsActive as r WHERE r.collection_time <= @_collection_time;
	SELECT @_second_latest_collection_time = MAX(r.collection_time) FROM dbo.WhoIsActive as r 
		WHERE r.collection_time < @_latest_collection_time AND DATEDIFF(mi,r.collection_time,@_latest_collection_time) >= @threshold_minutes;
	
	IF(@verbose > 1)
	BEGIN
		PRINT '@_collection_time = '''+CONVERT(nvarchar(30),@_collection_time,121)+'''';
		PRINT '@_latest_collection_time= '''+CONVERT(nvarchar(30),@_latest_collection_time,121)+'''';
		PRINT '@_second_latest_collection_time = '''+CONVERT(nvarchar(30),@_second_latest_collection_time,121)+'''';
	END

	-- Get blocking details b/w latest 2 collection_time
	IF(@verbose > 0)
		PRINT 'Create table #WhoIsActive_Filtered'
	IF OBJECT_ID('tempdb..#WhoIsActive_Filtered') IS NOT NULL
		DROP TABLE #WhoIsActive_Filtered;
	SELECT	DENSE_RANK()OVER(ORDER BY collection_time ASC) AS CollectionBatch, *
	INTO	#WhoIsActive_Filtered
	FROM	dbo.WhoIsActive as r
	WHERE	r.collection_time IN (@_latest_collection_time, @_second_latest_collection_time )
		-- and line item is either victim, or blocker
		AND	(r.blocking_session_id IS NOT NULL OR r.session_id IN (SELECT h.blocking_session_id FROM dbo.WhoIsActive h WHERE h.collection_time = r.collection_time AND h.blocking_session_id IS NOT NULL));

	IF @verbose > 1
	BEGIN
		PRINT 'SELECT * FROM #WhoIsActive_Filtered;';
		SELECT [RunningQuery] = 'SELECT * FROM #WhoIsActive_Filtered',* FROM #WhoIsActive_Filtered;
	END

	-- Set blocking flag
	IF EXISTS ( SELECT * FROM #WhoIsActive_Filtered WHERE CollectionBatch = 2)
		SET @_is_blocking_found = 1;

	IF @_is_blocking_found = 1 /* If Blocking Found, Check for consistent blocking */
	BEGIN
		IF @verbose > 0
			PRINT 'Blocking found. Checking is its consistent..';

		-- Check is consistent blocking records are found by comparing both Batches
		IF(@verbose > 0)
			PRINT 'Creating table #WhoIsActive_Filtered_Blocking_SingleLine';
		IF OBJECT_ID('tempdb..#WhoIsActive_Filtered_Blocking_SingleLine') IS NOT NULL
			DROP TABLE #WhoIsActive_Filtered_Blocking_SingleLine;
		SELECT	c.CollectionBatch, c.collection_time, c.session_id, c.blocking_session_id, p.collection_time as previous_collection_time
		INTO	#WhoIsActive_Filtered_Blocking_SingleLine
		FROM #WhoIsActive_Filtered as c, #WhoIsActive_Filtered as p
		WHERE	c.CollectionBatch = 2 AND p.CollectionBatch = 1
			AND	c.session_id = p.session_id AND c.blocking_session_id = p.blocking_session_id
			AND	c.duration_minutes >= @threshold_minutes 
			AND c.login_name = p.login_name AND c.program_name = p.program_name AND c.database_name = p.database_name AND c.host_name = p.host_name

		IF @verbose > 1
		BEGIN
			PRINT 'SELECT * FROM #WhoIsActive_Filtered_Blocking_SingleLine'
			SELECT [RunningQuery] = 'SELECT * FROM #WhoIsActive_Filtered_Blocking_SingleLine', * FROM #WhoIsActive_Filtered_Blocking_SingleLine;
		END

		-- Delete Batches That are not valid for Consistent Blocking
		IF(@verbose > 0)
			PRINT 'Deleting non-consistant blocking records from #WhoIsActive_Filtered';
		DELETE r
		FROM #WhoIsActive_Filtered as r
		WHERE CollectionBatch = 1
			OR (CollectionBatch = 2 AND session_id NOT IN (SELECT b.session_id FROM #WhoIsActive_Filtered_Blocking_SingleLine as b) AND session_id NOT IN (SELECT b.blocking_session_id FROM #WhoIsActive_Filtered_Blocking_SingleLine as b));

		IF(@verbose > 1)
		BEGIN
			PRINT 'SELECT /* Consistent Blocking */ * FROM #WhoIsActive_Filtered';
			SELECT [RunningQuery] = 'SELECT /* Consistent Blocking */ * FROM #WhoIsActive_Filtered', * FROM #WhoIsActive_Filtered;
		END

		-- Create Blocking Tree
		IF(@verbose > 0)
			PRINT 'Populate table [dbo].sdt_blocking_alert using CTE';
		;WITH T_JobCaptures AS
		(
			SELECT [dd hh:mm:ss.mss], [session_id], [sql_text], [login_name], [wait_info], [CPU], [tempdb_allocations], [tempdb_current], [blocking_session_id], [reads], [writes], [physical_reads], [used_memory], [status], [open_tran_count], [percent_complete], [host_name], [database_name], [program_name], [start_time], [login_time], [request_id], [collection_time]
				,[sql_query] = REPLACE(REPLACE(REPLACE(REPLACE(CAST(COALESCE([sql_text],null) AS VARCHAR(MAX)),char(13),''),CHAR(10),''),'<?query --',''),'--?>','')
				,[LEVEL] = CAST (REPLICATE ('0', 4-LEN (CAST (r.session_id AS VARCHAR))) + CAST (r.session_id AS VARCHAR) AS VARCHAR (1000))
			FROM #WhoIsActive_Filtered as r
			WHERE (ISNULL(r.blocking_session_id,0) = 0 OR ISNULL(r.blocking_session_id,0) = r.session_id)
			AND EXISTS (SELECT * FROM #WhoIsActive_Filtered AS R2 WHERE R2.collection_time = r.collection_time AND ISNULL(R2.blocking_session_id,0) = r.session_id AND ISNULL(R2.blocking_session_id,0) <> R2.session_id)
			--
			UNION ALL
			--
			SELECT r.[dd hh:mm:ss.mss], r.[session_id], r.[sql_text], r.[login_name], r.[wait_info], r.[CPU], r.[tempdb_allocations], r.[tempdb_current], r.[blocking_session_id], r.[reads], r.[writes], r.[physical_reads], r.[used_memory], r.[status], r.[open_tran_count], r.[percent_complete], r.[host_name], r.[database_name], r.[program_name], r.[start_time], r.[login_time], r.[request_id], r.[collection_time]
				,[sql_query] = REPLACE(REPLACE(REPLACE(REPLACE(CAST(COALESCE(r.[sql_text],NULL) AS VARCHAR(MAX)),char(13),''),CHAR(10),''),'<?query --',''),'--?>','')
				,[LEVEL] = CAST (b.LEVEL + RIGHT (CAST ((1000 + r.session_id) AS VARCHAR (100)), 4) AS VARCHAR (1000))
			FROM T_JobCaptures AS b
			INNER JOIN #WhoIsActive_Filtered as r
				ON r.collection_time = b.collection_time
				AND	r.blocking_session_id = b.session_id
			WHERE	r.blocking_session_id <> r.session_id
		)
		INSERT [dbo].sdt_blocking_alert
		SELECT	[collection_time], [dd hh:mm:ss.mss],
				[BLOCKING_TREE] = N'    ' + REPLICATE (N'|         ', LEN (LEVEL)/4 - 1) 
								+	CASE	WHEN (LEN(LEVEL)/4 - 1) = 0
											THEN 'HEAD -  '
											ELSE '|------  ' 
									END
								+	CAST (r.session_id AS NVARCHAR (10)) + N' ' + (CASE WHEN LEFT(r.[sql_query],1) = '(' THEN SUBSTRING(r.[sql_query],CHARINDEX('exec',r.[sql_query]),LEN(r.[sql_query]))  ELSE r.[sql_query] END),
				[session_id], [blocking_session_id], 				
				--[sql_text], 
				[host_name], [database_name], [login_name], [program_name],	[wait_info],  
				--[locks], 
				[open_tran_count] --,additional_info
				,r.[CPU], r.[tempdb_allocations], r.[tempdb_current], r.[reads], r.[writes], r.[physical_reads] --, r.[query_plan]
		FROM	T_JobCaptures as r
		ORDER BY r.collection_time, LEVEL ASC;
							
		IF(@verbose > 1)
		BEGIN
			PRINT 'SELECT * FROM [dbo].sdt_blocking_alert;';
			SELECT 'dbo.sdt_blocking_alert' AS RunningTable, * FROM [dbo].sdt_blocking_alert;
		END
		
		IF EXISTS(SELECT * FROM dbo.sdt_blocking_alert)
		BEGIN
			SET @_is_consistent_blocking_found = 1;
			IF @verbose > 0
				PRINT 'Consistent blocking found.'
		END
	END

	--EXEC msdb.dbo.sp_send_dbmail 
	--			@recipients = @recipients,  
	--			@subject = @_subject,  
	--			@body = @_mail_html,  
	--			@body_format = 'HTML' ; 

	/* 
	Check if Consistent Blocking, then based on Continous Threshold & Delay, send mail
	Check if No Error & No Consistent Blocking, then clear the Blocking alert if active
	*/

	IF @verbose > 0
		PRINT 'Dynamically fetch @alert_key ..'
	SET @alert_key = '(dba) Run-WhoIsActive';
	IF program_name() LIKE 'SQLAgent - TSQL JobStep (Job %'
		EXEC sp_executesql	@stmt = N'SELECT @alert_key_OUTPUT = name FROM msdb.dbo.sysjobs WHERE job_id = CONVERT(uniqueidentifier, $(ESCAPE_NONE(JOBID)))',
							@params = N'@alert_key_OUTPUT nvarchar(500) OUTPUT',
							@alert_key_OUTPUT = @alert_key OUTPUT;
	--PRINT '"'+@alert_key+'"';
	
	IF @verbose > 0
		PRINT 'Get Last @_last_sent_blocking_active & @_last_sent_blocking_cleared..';
	SELECT @_last_sent_blocking_active = MAX(si.sent_date) FROM msdb..sysmail_sentitems si WHERE si.subject LIKE ('% - !['+@alert_key+'!] - ![ACTIVE!]') ESCAPE '!';
	SELECT @_last_sent_blocking_cleared = MAX(si.sent_date) FROM msdb..sysmail_sentitems si WHERE si.subject LIKE ('% - !['+@alert_key+'!] - ![CLEARED!]') ESCAPE '!';

	IF @verbose > 0
	BEGIN
		PRINT '@_last_sent_blocking_active => '+ISNULL(CONVERT(nvarchar(30),@_last_sent_blocking_active,121),'');
		PRINT '@_last_sent_blocking_cleared => '+ISNULL(CONVERT(nvarchar(30),@_last_sent_blocking_cleared,121),'');
		PRINT '@_is_consistent_blocking_found => '+CONVERT(varchar,@_is_consistent_blocking_found);
		PRINT '@_is_consistent_blocking_found => '+CONVERT(varchar,@_is_consistent_blocking_found);
	END

	--RETURN;
	-- Check if Blocking, @delay_minutes is breached
	IF @_is_consistent_blocking_found = 1
		AND ( (@_last_sent_blocking_active IS NULL) OR (DATEDIFF(MINUTE,@_last_sent_blocking_active,GETDATE()) >= @_last_sent_blocking_cleared) )
	BEGIN
		-- Generate HTML table Headers/Rows

		IF(@verbose > 0)
			PRINT 'Inside Generate HTML table Headers/Rows';
		SET @_table_name = 'dbo.sdt_blocking_alert';

		-- Get table headers <th> data for Table <table>
		IF(@verbose > 0)
			PRINT 'Set value for @_column_list_4_table_header';
		SELECT	@_column_list_4_table_header = COALESCE(@_column_list_4_table_header ,'') + ('<th>'+COLUMN_NAME+'</th>'+CHAR(13)+CHAR(10))
		FROM	INFORMATION_SCHEMA.COLUMNS as c
		WHERE	TABLE_SCHEMA+'.'+c.TABLE_NAME = @_table_name
			AND	c.COLUMN_NAME NOT IN ('ID');

		IF(@verbose > 0)
			PRINT '@_column_list_4_table_header => ' + @_column_list_4_table_header;

		-- Get row (tr) data for Table <table>
		IF(@verbose > 0)
			PRINT 'Set Value for @_column_list_4_table_data';
		SELECT	@_column_list_4_table_data = COALESCE(@_column_list_4_table_data+', '''','+CHAR(13)+CHAR(10) ,'') + 
				('td = '+CASE WHEN COLUMN_NAME = 'BLOCKING_TREE' THEN 'LEFT(ISNULL('+COLUMN_NAME+','' ''),150)'
							WHEN COLUMN_NAME = 'dd hh:mm:ss.mss' THEN 'LEFT(LTRIM(RTRIM('+QUOTENAME(COLUMN_NAME)+')),18)'
							WHEN DATA_TYPE = 'xml' THEN 'ISNULL(LEFT(CAST('+COLUMN_NAME+' AS varchar(max)),150),'' '')'
							WHEN DATA_TYPE NOT LIKE '%char' AND IS_NULLABLE = 'YES' THEN 'ISNULL(CAST('+COLUMN_NAME+' AS varchar(125)),'' '')'
							WHEN DATA_TYPE NOT LIKE '%char' THEN 'CAST('+COLUMN_NAME+' AS VARCHAR(125))'
							WHEN IS_NULLABLE = 'YES' THEN 'ISNULL('+COLUMN_NAME+','' '')'
							ELSE COLUMN_NAME
							END)
		FROM	INFORMATION_SCHEMA.COLUMNS as c
		WHERE	TABLE_SCHEMA+'.'+c.TABLE_NAME = @_table_name
			AND	c.COLUMN_NAME NOT IN ('ID');

		IF(@verbose > 0)
			PRINT '@_column_list_4_table_data => ' + @_column_list_4_table_data;

		IF(@verbose > 0)
			PRINT 'Populating table @_data_4_table_data';
		SET @_sql_string = N'
			SELECT CAST ( ( SELECT '+@_column_list_4_table_data+'
							FROM '+@_table_name+'
							WHERE 1 = 1 '+ISNULL(@_query_filter,'')+'
							FOR XML PATH(''tr''), TYPE   
				) AS NVARCHAR(MAX) )';

		INSERT @_data_4_table_data
		EXEC (@_sql_string);

		IF(@verbose > 0)
		BEGIN
			PRINT 'SELECT * FROM @_data_4_table_data;'
			SELECT '@_data_4_table_data' as RunningTable, * FROM @_data_4_table_data;
		END

		IF(@verbose > 0)
			PRINT 'SELECT @_column_list_4_table_data = TableData FROM @_data_4_table_data;'
		SELECT @_column_list_4_table_data = TableData FROM @_data_4_table_data;
		IF(@verbose > 0)
			PRINT '@_column_list_4_table_data => '+@_column_list_4_table_data;

		IF(@verbose > 0)
			PRINT 'Setting value for @_subject'
		SET @_subject = 'Consistent Blocking for more than '+cast(@threshold_minutes as varchar(5))+' minutes - '+cast(@_collection_time as varchar(100));


		SET @_css_style_green_background_header = N'
		<style>
		.GreenBackgroundHeader {
			font-family: "Trebuchet MS", Arial, Helvetica, sans-serif;
			border-collapse: collapse;
			width: 100%;
		}

		.GreenBackgroundHeader td, .GreenBackgroundHeader th {
			border: 1px solid #ddd;
			padding: 8px;
		}

		.GreenBackgroundHeader tr:nth-child(even){background-color: #f2f2f2;}

		.GreenBackgroundHeader tr:hover {background-color: #ddd;}

		.GreenBackgroundHeader th {
			padding-top: 12px;
			padding-bottom: 12px;
			text-align: left;
			background-color: #4CAF50;
			color: white;
		}
		</style>';

		SET @_html_body = N'<H1>'+@_subject+'</H1>' +  
				N'<table border="1" class="'+@_default_html_style+'">' +  
				N'<tr>'+@_column_list_4_table_header+'</tr>' +  
				+@_column_list_4_table_data+
				N'</table>' ;  

		SET @_html_body = @_html_body + '
		<p>
		<br><br>
		Thanks & Regards,<br>
		SQL Alerts<br>
		-- Alert Coming from SQL Agent Job [DBA - Blocking Alert]<br>
		</p>
		';

		SET @_mail_html =  @_css_style_green_background_header + @_html_body;
	END
			

END
