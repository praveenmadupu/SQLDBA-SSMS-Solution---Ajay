USE DBA
GO
SET NOCOUNT ON;

--	Global Variables
DECLARE @p_BlockingThresholdTime_Minutes INT = 5;
DECLARE @p_recipients VARCHAR(255);

--	Get sp_whoisactive data for last 2 executions within last 2 hours
DECLARE @Filter4WhoIsActiveData_Hours SMALLINT = 1;
DECLARE @p_DefaultHTMLStyle VARCHAR(100) = 'GreenBackgroundHeader';
DECLARE @mailHTML  NVARCHAR(MAX) ;
DECLARE @subject VARCHAR(200);
DECLARE @tableName VARCHAR(125);
DECLARE @columnList4TableHeader VARCHAR(MAX);
DECLARE @columnList4TableData VARCHAR(MAX);
DECLARE @cssStyle_GreenBackgroundHeader VARCHAR(MAX);
DECLARE @htmlBody VARCHAR(MAX);
DECLARE @sqlString VARCHAR(MAX);
DECLARE @data4TableData TABLE ( TableData VARCHAR(MAX) );
DECLARE @queryFilter VARCHAR(2000);

IF OBJECT_ID('DBA..DBABlockersTable') IS NULL
	CREATE TABLE DBA.[dbo].[DBABlockersTable]
	(
		[collection_time] smalldatetime NULL,
		[BLOCKING_TREE] [nvarchar](max) NULL,
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
		[CPU] [varchar](30) NULL,
		[tempdb_allocations] [varchar](30) NULL,
		[tempdb_current] [varchar](30) NULL,
		[reads] [varchar](30) NULL,
		[writes] [varchar](30) NULL,
		--[physical_io] [varchar](30) NULL,
		[physical_reads] [varchar](30) NULL
	);
ELSE
	TRUNCATE TABLE DBA.[dbo].[DBABlockersTable];

IF OBJECT_ID('tempdb..#WhoIsActive_Filtered') IS NOT NULL
	DROP TABLE #WhoIsActive_Filtered;
SELECT	DENSE_RANK()OVER(ORDER BY collection_Time ASC) AS CollectionBatch, *
INTO	#WhoIsActive_Filtered
FROM	dbo.WhoIsActive_ResultSets as r
WHERE	r.collection_time >= DATEADD(HH,-@Filter4WhoIsActiveData_Hours,GETDATE())
	AND	(SELECT COUNT(DISTINCT i.collection_time) FROM dbo.WhoIsActive_ResultSets as i WHERE i.collection_time >= DATEADD(HH,-@Filter4WhoIsActiveData_Hours,GETDATE()) AND i.collection_time >= r.collection_time) <= 2
	AND	(r.blocking_session_id IS NOT NULL OR r.session_id IN (SELECT i.blocking_session_id FROM dbo.WhoIsActive_ResultSets i WHERE i.collection_time = r.collection_time AND i.blocking_session_id IS NOT NULL))

IF OBJECT_ID('tempdb..#WhoIsActive_Filtered_Blocking_SingleLine') IS NOT NULL
	DROP TABLE #WhoIsActive_Filtered_Blocking_SingleLine;
SELECT	c.CollectionBatch, c.collection_time, c.session_id, c.blocking_session_id, p.collection_time as previous_collection_time
INTO	#WhoIsActive_Filtered_Blocking_SingleLine
FROM #WhoIsActive_Filtered as c, #WhoIsActive_Filtered as p
WHERE	c.CollectionBatch = 2 AND p.CollectionBatch = 1
	AND	c.session_id = p.session_id AND c.blocking_session_id = p.blocking_session_id
	AND	c.TimeInMinutes >= @p_BlockingThresholdTime_Minutes;

IF OBJECT_ID('tempdb..#WhoIsActive_Filtered_Blocking') IS NOT NULL
	DROP TABLE #WhoIsActive_Filtered_Blocking;
SELECT	r.*
INTO	#WhoIsActive_Filtered_Blocking
FROM	#WhoIsActive_Filtered as r, #WhoIsActive_Filtered_Blocking_SingleLine AS b
WHERE	r.collection_time = b.collection_time
	AND (r.session_id = b.session_id OR r.session_id = b.blocking_session_id);

--select * from #WhoIsActive_Filtered_Blocking;


;WITH T_JobCaptures AS
(
	SELECT [dd hh:mm:ss.mss], [session_id], [sql_text], [login_name], [wait_info], [CPU], [tempdb_allocations], [tempdb_current], [blocking_session_id], [reads], [writes], [physical_reads], [used_memory], [status], [open_tran_count], [percent_complete], [host_name], [database_name], [program_name], [start_time], [login_time], [request_id], [collection_time]
		,[sql_query] = REPLACE(REPLACE(REPLACE(REPLACE(CAST(COALESCE([sql_text],null) AS VARCHAR(MAX)),char(13),''),CHAR(10),''),'<?query --',''),'--?>','')
		,[LEVEL] = CAST (REPLICATE ('0', 4-LEN (CAST (r.session_id AS VARCHAR))) + CAST (r.session_id AS VARCHAR) AS VARCHAR (1000))
	FROM #WhoIsActive_Filtered_Blocking as r
	WHERE (ISNULL(r.blocking_session_id,0) = 0 OR ISNULL(r.blocking_session_id,0) = r.session_id)
	AND EXISTS (SELECT * FROM #WhoIsActive_Filtered_Blocking AS R2 WHERE R2.collection_Time = r.collection_Time AND ISNULL(R2.blocking_session_id,0) = r.session_id AND ISNULL(R2.blocking_session_id,0) <> R2.session_id)
	--
	UNION ALL
	--
	SELECT r.[dd hh:mm:ss.mss], r.[session_id], r.[sql_text], r.[login_name], r.[wait_info], r.[CPU], r.[tempdb_allocations], r.[tempdb_current], r.[blocking_session_id], r.[reads], r.[writes], r.[physical_reads], r.[used_memory], r.[status], r.[open_tran_count], r.[percent_complete], r.[host_name], r.[database_name], r.[program_name], r.[start_time], r.[login_time], r.[request_id], r.[collection_time]
		,[sql_query] = REPLACE(REPLACE(REPLACE(REPLACE(CAST(COALESCE(r.[sql_text],NULL) AS VARCHAR(MAX)),char(13),''),CHAR(10),''),'<?query --',''),'--?>','')
		,[LEVEL] = CAST (b.LEVEL + RIGHT (CAST ((1000 + r.session_id) AS VARCHAR (100)), 4) AS VARCHAR (1000))
	FROM T_JobCaptures AS b
	INNER JOIN #WhoIsActive_Filtered_Blocking as r
		ON r.collection_time = B.collection_time
		AND	r.blocking_session_id = B.session_id
	WHERE	r.blocking_session_id <> r.session_id
)
INSERT [dbo].DBABlockersTable
SELECT	[collection_time], 
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
							

IF EXISTS(SELECT * FROM dbo.DBABlockersTable)
BEGIN
	SET @tableName = 'dbo.DBABlockersTable';

	-- Get table headers <th> data for Table <table>
	SELECT	@columnList4TableHeader = COALESCE(@columnList4TableHeader ,'') + ('<th>'+COLUMN_NAME+'</th>'+CHAR(13)+CHAR(10))
	FROM	INFORMATION_SCHEMA.COLUMNS as c
	WHERE	TABLE_SCHEMA+'.'+c.TABLE_NAME = @tableName
		AND	c.COLUMN_NAME NOT IN ('ID');

	-- Get row (tr) data for Table <table>
	SELECT	@columnList4TableData = COALESCE(@columnList4TableData+', '''','+CHAR(13)+CHAR(10) ,'') + 
			('td = '+CASE WHEN COLUMN_NAME = 'BLOCKING_TREE' THEN 'LEFT(ISNULL('+COLUMN_NAME+','' ''),150)'
						WHEN DATA_TYPE = 'xml' THEN 'ISNULL(LEFT(CAST('+COLUMN_NAME+' AS varchar(max)),150),'' '')'
						WHEN DATA_TYPE NOT LIKE '%char' AND IS_NULLABLE = 'YES' THEN 'ISNULL(CAST('+COLUMN_NAME+' AS varchar(125)),'' '')'
						WHEN DATA_TYPE NOT LIKE '%char' THEN 'CAST('+COLUMN_NAME+' AS VARCHAR(125))'
						WHEN IS_NULLABLE = 'YES' THEN 'ISNULL('+COLUMN_NAME+','' '')'
						ELSE COLUMN_NAME
						END)
	FROM	INFORMATION_SCHEMA.COLUMNS as c
	WHERE	TABLE_SCHEMA+'.'+c.TABLE_NAME = @tableName
		AND	c.COLUMN_NAME NOT IN ('ID');

	SET @sqlString = N'
		SELECT CAST ( ( SELECT '+@columnList4TableData+'
						FROM '+@tableName+'
						WHERE 1 = 1 '+ISNULL(@queryFilter,'')+'
						FOR XML PATH(''tr''), TYPE   
			) AS NVARCHAR(MAX) )';

	INSERT @data4TableData
	EXEC (@sqlString);

	SELECT @columnList4TableData = TableData FROM @data4TableData;

	SET @subject = 'Blockers - '+cast(getdate() as varchar(100));

	SET @cssStyle_GreenBackgroundHeader = N'
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

	SET @htmlBody = N'<H1>'+@subject+'</H1>' +  
			N'<table border="1" class="'+@p_DefaultHTMLStyle+'">' +  
			N'<tr>'+@columnList4TableHeader+'</tr>' +  
			+@columnList4TableData+
			N'</table>' ;  

	SET @htmlBody = @htmlBody + '
	<p>
	<br><br>
	Thanks & Regards,<br>
	SQL Alerts<br>
	ajay.dwivedi2007@gmail.com<br>
	-- Alert Coming from SQL Agent Job [DBA Log Walk Alerts]<br>
	</p>
	';

	SET @mailHTML =  @cssStyle_GreenBackgroundHeader + @htmlBody;

	IF (@p_recipients IS NULL) 
	BEGIN
		SET @p_recipients = 'ajay.dwivedi2007@gmail.com';
	END

	EXEC msdb.dbo.sp_send_dbmail 
		@recipients = @p_recipients,  
		@subject = @subject,  
		@body = @mailHTML,  
		@body_format = 'HTML' ; 
END
ELSE
	PRINT 'No Blocking Detected'