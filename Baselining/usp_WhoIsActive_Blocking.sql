USE [DBA]
GO

IF NOT EXISTS(SELECT * FROM sys.indexes WHERE name='NCI_WhoIsActive_ResultSets__blocking_session_id' AND object_id = OBJECT_ID('dbo.WhoIsActive_ResultSets'))
	CREATE NONCLUSTERED INDEX [NCI_WhoIsActive_ResultSets__blocking_session_id]
		ON [dbo].[WhoIsActive_ResultSets] ([blocking_session_id]) INCLUDE ([session_id],[collection_time],[program_name],[TimeInMinutes])
GO
--	DROP INDEX [NCI_WhoIsActive_ResultSets__blocking_session_id]	ON [dbo].[WhoIsActive_ResultSets]

IF OBJECT_ID('dbo.usp_WhoIsActive_Blocking') IS NULL
	EXEC('CREATE PROCEDURE dbo.usp_WhoIsActive_Blocking AS SELECT 1 AS Dummy')
GO

ALTER PROCEDURE dbo.usp_WhoIsActive_Blocking 
	@p_Collection_time_Start datetime = NULL, @p_Collection_time_End datetime = NULL, @p_Program_Name nvarchar(256) = NULL,
	@p_Verbose bit = 0
AS 
BEGIN
	/*	Created By:			Ajay Dwivedi (ajay.dwivedi2007@gmail.com)
		Version:			0.0
		Updates:			May 12, 2019 - Get Blocking Details
	*/
	SET NOCOUNT ON;

	IF @p_Verbose = 1
		PRINT 'Evaluating values of @p_Collection_time_Start and @p_Collection_time_End';

	IF @p_Collection_time_Start IS NULL AND @p_Collection_time_End IS NULL
		SELECT	@p_Collection_time_Start = DATEADD(minute,-120,getdate()), @p_Collection_time_End = GETDATE();
	ELSE IF @p_Collection_time_Start IS NULL
		SELECT @p_Collection_time_Start = DATEADD(minute,-120,@p_Collection_time_End);
	
	IF @p_Collection_time_End IS NULL AND @p_Collection_time_Start IS NOT NULL
		SELECT	@p_Collection_time_End = DBA.dbo.fn_GetNextCollectionTime(@p_Collection_time_Start);

	IF @p_Verbose = 1
	BEGIN
		PRINT '@p_Collection_time_Start = '''+CAST(@p_Collection_time_Start AS VARCHAR(35))+'''';
		PRINT '@p_Collection_time_End = '''+CAST(@p_Collection_time_End AS VARCHAR(35))+'''';
	END
			

	;WITH T_BLOCKERS AS
	(
		-- Find block Leaders
		SELECT	[collection_time], [TimeInMinutes], [session_id], 
				[sql_text] = REPLACE(REPLACE(REPLACE(REPLACE(CAST([sql_text] AS VARCHAR(MAX)),char(13),''),CHAR(10),''),'<?query --',''),'--?>',''), 
				[login_name], [wait_info], [blocking_session_id],
				[status], [open_tran_count], [host_name], [database_name], [program_name],
				r.[CPU], r.[tempdb_allocations], r.[tempdb_current], r.[reads], r.[writes], r.[physical_reads],
				[LEVEL] = CAST (REPLICATE ('0', 4-LEN (CAST (r.session_id AS VARCHAR))) + CAST (r.session_id AS VARCHAR) AS VARCHAR (1000))
		FROM	[dbo].WhoIsActive_ResultSets AS r
		WHERE	(r.collection_time >= @p_Collection_time_Start AND r.collection_time <= @p_Collection_time_End)
			AND	(r.blocking_session_id IS NULL OR r.blocking_session_id = r.session_id)
			AND EXISTS (SELECT R2.session_id FROM [dbo].WhoIsActive_ResultSets AS R2 
						WHERE R2.collection_Time = r.collection_Time AND R2.blocking_session_id IS NOT NULL 
							AND R2.blocking_session_id = r.session_id AND R2.blocking_session_id <> R2.session_id
							AND (@p_Program_Name IS NULL OR R2.program_name = @p_Program_Name)
						)
		--	
		UNION ALL
		--
		SELECT	r.[collection_time], r.[TimeInMinutes], r.[session_id], 
				[sql_text] = REPLACE(REPLACE(REPLACE(REPLACE(CAST(r.[sql_text] AS VARCHAR(MAX)),char(13),''),CHAR(10),''),'<?query --',''),'--?>',''), 
				r.[login_name], r.[wait_info], r.[blocking_session_id], 
				r.[status], r.[open_tran_count], r.[host_name], r.[database_name], r.[program_name],
				r.[CPU], r.[tempdb_allocations], r.[tempdb_current], r.[reads], r.[writes], r.[physical_reads],
				CAST (B.LEVEL + RIGHT (CAST ((1000 + r.session_id) AS VARCHAR (100)), 4) AS VARCHAR (1000)) AS LEVEL
		FROM	[dbo].WhoIsActive_ResultSets AS r
		INNER JOIN 
				T_BLOCKERS AS B
			ON	r.collection_time = B.collection_time
			AND	r.blocking_session_id = B.session_id
		WHERE	r.blocking_session_id <> r.session_id
	)
	--select * from T_BLOCKERS	
	SELECT	--TOP 100 PERCENT 
			[collection_time], 
			[BLOCKING_TREE] = N'    ' + REPLICATE (N'|         ', LEN (LEVEL)/4 - 1) 
							+	CASE	WHEN (LEN(LEVEL)/4 - 1) = 0
										THEN 'HEAD -  '
										ELSE '|------  ' 
								END
							+	CAST (r.session_id AS NVARCHAR (10)) + N' ' + (CASE WHEN LEFT(r.[sql_text],1) = '(' THEN SUBSTRING(r.[sql_text],CHARINDEX('exec',r.[sql_text]),LEN(r.[sql_text]))  ELSE r.[sql_text] END),
			[session_id], [blocking_session_id], 
			--w.[WaitTime(Seconds)],
			w.lock_text,
			[sql_commad] = CONVERT(XML, '<?query -- '+char(13)
							+ (CASE WHEN LEFT([sql_text],1) = '(' THEN SUBSTRING([sql_text],CHARINDEX('exec',[sql_text]),LEN([sql_text]))  ELSE [sql_text] END)
							+ char(13)+'--?>'), 
			[host_name], [database_name], [login_name], [program_name],	[wait_info], [open_tran_count]
			,r.[CPU], r.[tempdb_allocations], r.[tempdb_current], r.[reads], r.[writes], r.[physical_reads] --, r.[query_plan]
			--,[Blocking_Order] = DENSE_RANK()OVER(ORDER BY collection_time, LEVEL ASC)
	FROM	T_BLOCKERS AS r
	OUTER APPLY
		(	
			select	lock_text,								
					--[WaitTime(Seconds)] =
					--		CAST(SUBSTRING(lock_text,
					--			CHARINDEX(':',lock_text)+1,
					--			CHARINDEX('ms',lock_text)-(CHARINDEX(':',lock_text)+1)
					--		) AS BIGINT)/1000
					[WaitTime(Seconds)] =
							CASE WHEN CHARINDEX(':',lock_text) = 0
									THEN CAST(SUBSTRING(lock_text, CHARINDEX('(',lock_text)+1, CHARINDEX('ms',lock_text)-(CHARINDEX('(',lock_text)+1)) AS BIGINT)/1000
									ELSE CAST(SUBSTRING(lock_text, CHARINDEX(':',lock_text)+1, CHARINDEX('ms',lock_text)-(CHARINDEX(':',lock_text)+1)) AS BIGINT)/1000
									END
								
			from (
				SELECT	[lock_text] = CASE	WHEN r.[wait_info] IS NULL OR CHARINDEX('LCK',r.[wait_info]) = 0
											THEN NULL
											WHEN CHARINDEX(',',r.[wait_info]) = 0
											THEN r.[wait_info]
											WHEN CHARINDEX(',',LEFT(r.[wait_info],  CHARINDEX(',',r.[wait_info],CHARINDEX('LCK_',r.[wait_info]))-1   )) <> 0
											THEN REVERSE(LEFT(	REVERSE(LEFT(r.[wait_info],  CHARINDEX(',',r.[wait_info],CHARINDEX('LCK_',r.[wait_info]))-1)),
															CHARINDEX(',',REVERSE(LEFT(r.[wait_info],  CHARINDEX(',',r.[wait_info],CHARINDEX('LCK_',r.[wait_info]))-1)))-1
														))
											ELSE LEFT(r.[wait_info],  CHARINDEX(',',r.[wait_info],CHARINDEX('LCK_',r.[wait_info]))-1   )
											END
			) as wi
		) AS w
	ORDER BY collection_time, LEVEL ASC;
	--ORDER BY [Blocking_Order];		
END
GO

--	EXEC DBA.dbo.usp_WhoIsActive_Blocking @p_Collection_time_Start = 'May 12 2019 11:30AM', @p_Collection_time_End = 'May 12 2019 01:30PM';

