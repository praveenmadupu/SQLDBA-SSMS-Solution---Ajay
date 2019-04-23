SET NOCOUNT ON
GO

DECLARE @p_collection_time datetime2
SET @p_collection_time = '2019-04-23 02:45:09.033';

IF OBJECT_ID('tempdb..#T') IS NOT NULL
	DROP TABLE #T;

;WITH t_processes AS
(
	SELECT  SPID = [session_id], BLOCKED = ISNULL([blocking_session_id],0), 
			[BATCH] = REPLACE(REPLACE(REPLACE(REPLACE(CAST(COALESCE([sql_command],[sql_text]) AS VARCHAR(MAX)),char(13),''),CHAR(10),''),'<?query --',''),'--?>','')
	FROM [DBA].[dbo].WhoIsActive_ResultSets AS r
	WHERE r.collection_Time = @p_collection_time
)
SELECT	SPID, BLOCKED,
		[BATCH] = CASE WHEN LEFT([BATCH],1) = '(' THEN SUBSTRING([BATCH],CHARINDEX('exec',[BATCH]),LEN([BATCH])) ELSE [BATCH] END
INTO #T
FROM	t_processes;

;WITH BLOCKERS (SPID, BLOCKED, LEVEL, BATCH) AS
(
	SELECT	SPID,
			BLOCKED,
			[LEVEL] = CAST (REPLICATE ('0', 4-LEN (CAST (SPID AS VARCHAR))) + CAST (SPID AS VARCHAR) AS VARCHAR (1000)),	
			BATCH 
	FROM	#T R
	WHERE	(BLOCKED = 0 OR BLOCKED = SPID)
		AND EXISTS (SELECT * FROM #T R2 WHERE R2.BLOCKED = R.SPID AND R2.BLOCKED <> R2.SPID)
	--
	UNION ALL
	--
	SELECT	R.SPID,
			R.BLOCKED,
			CAST (BLOCKERS.LEVEL + RIGHT (CAST ((1000 + R.SPID) AS VARCHAR (100)), 4) AS VARCHAR (1000)) AS LEVEL,
			R.BATCH 
	FROM	#T AS R
	INNER JOIN BLOCKERS 
		ON	R.BLOCKED = BLOCKERS.SPID 
	WHERE	R.BLOCKED > 0 AND R.BLOCKED <> R.SPID
)
SELECT	[BLOCKING_TREE] = N'    ' + REPLICATE (N'|         ', LEN (LEVEL)/4 - 1) 
						+	CASE	WHEN (LEN(LEVEL)/4 - 1) = 0
									THEN 'HEAD -  '
									ELSE '|------  ' 
							END
						+	CAST (SPID AS NVARCHAR (10)) + N' ' + BATCH
FROM BLOCKERS ORDER BY LEVEL ASC;

;WITH T_ResultSet AS
(
	SELECT  [collection_time], [TimeInMinutes], [session_id], 
			[sql_text] = REPLACE(REPLACE(REPLACE(REPLACE(CAST(COALESCE([sql_command],[sql_text]) AS VARCHAR(MAX)),char(13),''),CHAR(10),''),'<?query --',''),'--?>',''), 
			[login_name], 
			[wait_info], [blocking_session_id], [blocked_session_count], [locks], 
			[status], [tran_start_time], [open_tran_count], [host_name], [database_name], [program_name]		
	FROM [DBA].[dbo].WhoIsActive_ResultSets AS r
	WHERE r.collection_Time = @p_collection_time
)
SELECT	[collection_time], [session_id], [blocking_session_id], 
		w.[WaitTime(Seconds)],
		--[sql_text] = (CASE WHEN LEFT([sql_text],1) = '(' THEN SUBSTRING([sql_text],CHARINDEX('exec',[sql_text]),LEN([sql_text]))  ELSE [sql_text] END), 
		[sql_commad] = CONVERT(XML, '<?query -- '+char(13)
						+ (CASE WHEN LEFT([sql_text],1) = '(' THEN SUBSTRING([sql_text],CHARINDEX('exec',[sql_text]),LEN([sql_text]))  ELSE [sql_text] END)
						+ char(13)+'--?>'), 
		[host_name], [database_name], [login_name], [program_name],
		[wait_info], [blocked_session_count], [locks], 
		[tran_start_time], [open_tran_count]
FROM	T_ResultSet AS r
OUTER APPLY
	(	
		select	lock_text,								
				[WaitTime(Seconds)] =
						CAST(SUBSTRING(lock_text,
							CHARINDEX(':',lock_text)+1,
							CHARINDEX('ms',lock_text)-(CHARINDEX(':',lock_text)+1)
						) AS BIGINT)/1000
								
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