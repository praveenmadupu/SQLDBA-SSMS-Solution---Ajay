SET NOCOUNT ON
GO

-- Declare Variables
DECLARE @T_CollectionTimes TABLE (collection_time datetime2);
DECLARE @c_collection_time datetime2;

-- Populate table & Variables
INSERT @T_CollectionTimes
SELECT DISTINCT collection_time FROM [DBA].[dbo].WhoIsActive_ResultSets 
	WHERE collection_time >= '2019-04-25 01:45:00.000' AND collection_time <= '2019-04-25 02:15:00.000';

DECLARE cur_CollectionTimes CURSOR LOCAL FAST_FORWARD READ_ONLY FOR SELECT collection_time FROM @T_CollectionTimes;
OPEN cur_CollectionTimes;
FETCH NEXT FROM cur_CollectionTimes INTO @c_collection_time;

WHILE @@FETCH_STATUS = 0
BEGIN
	PRINT 'Starting @c_collection_time = '+cast(@c_collection_time as varchar(50));
	
	;WITH T_BLOCKERS AS
	(
		-- Find block Leaders
		SELECT	[collection_time], [TimeInMinutes], [session_id], 
				[sql_text] = REPLACE(REPLACE(REPLACE(REPLACE(CAST(COALESCE([sql_command],[sql_text]) AS VARCHAR(MAX)),char(13),''),CHAR(10),''),'<?query --',''),'--?>',''), 
				[login_name], [wait_info], [blocking_session_id], [blocked_session_count], [locks], 
				[status], [tran_start_time], [open_tran_count], [host_name], [database_name], [program_name], additional_info,
				r.[CPU], r.[tempdb_allocations], r.[tempdb_current], r.[reads], r.[writes], r.[physical_io], r.[physical_reads], r.[query_plan],
				[LEVEL] = CAST (REPLICATE ('0', 4-LEN (CAST (r.session_id AS VARCHAR))) + CAST (r.session_id AS VARCHAR) AS VARCHAR (1000))
		FROM	[DBA].[dbo].WhoIsActive_ResultSets AS r
		WHERE	r.collection_Time = @c_collection_time
			AND	(ISNULL(r.blocking_session_id,0) = 0 OR ISNULL(r.blocking_session_id,0) = r.session_id)
			AND EXISTS (SELECT * FROM [DBA].[dbo].WhoIsActive_ResultSets AS R2 WHERE R2.collection_Time = r.collection_Time AND ISNULL(R2.blocking_session_id,0) = r.session_id AND ISNULL(R2.blocking_session_id,0) <> R2.session_id)
		--	
		UNION ALL
		--
		SELECT	r.[collection_time], r.[TimeInMinutes], r.[session_id], 
				[sql_text] = REPLACE(REPLACE(REPLACE(REPLACE(CAST(COALESCE(r.[sql_command],r.[sql_text]) AS VARCHAR(MAX)),char(13),''),CHAR(10),''),'<?query --',''),'--?>',''), 
				r.[login_name], r.[wait_info], r.[blocking_session_id], r.[blocked_session_count], r.[locks], 
				r.[status], r.[tran_start_time], r.[open_tran_count], r.[host_name], r.[database_name], r.[program_name], r.additional_info,
				r.[CPU], r.[tempdb_allocations], r.[tempdb_current], r.[reads], r.[writes], r.[physical_io], r.[physical_reads], r.[query_plan],
				CAST (B.LEVEL + RIGHT (CAST ((1000 + r.session_id) AS VARCHAR (100)), 4) AS VARCHAR (1000)) AS LEVEL
		FROM	[DBA].[dbo].WhoIsActive_ResultSets AS r
		INNER JOIN 
				T_BLOCKERS AS B
			ON	r.collection_time = B.collection_time
			AND	r.blocking_session_id = B.session_id
		WHERE	r.blocking_session_id <> r.session_id
	)
	--select * from T_BLOCKERS
	
	SELECT	[collection_time], 
			[BLOCKING_TREE] = N'    ' + REPLICATE (N'|         ', LEN (LEVEL)/4 - 1) 
							+	CASE	WHEN (LEN(LEVEL)/4 - 1) = 0
										THEN 'HEAD -  '
										ELSE '|------  ' 
								END
							+	CAST (r.session_id AS NVARCHAR (10)) + N' ' + (CASE WHEN LEFT(r.[sql_text],1) = '(' THEN SUBSTRING(r.[sql_text],CHARINDEX('exec',r.[sql_text]),LEN(r.[sql_text]))  ELSE r.[sql_text] END),
			[session_id], [blocking_session_id], 
			w.[WaitTime(Seconds)],
			[sql_commad] = CONVERT(XML, '<?query -- '+char(13)
							+ (CASE WHEN LEFT([sql_text],1) = '(' THEN SUBSTRING([sql_text],CHARINDEX('exec',[sql_text]),LEN([sql_text]))  ELSE [sql_text] END)
							+ char(13)+'--?>'), 
			[host_name], [database_name], [login_name], [program_name],	[wait_info], [blocked_session_count], [locks], [tran_start_time], [open_tran_count], additional_info
			,r.[CPU], r.[tempdb_allocations], r.[tempdb_current], r.[reads], r.[writes], r.[physical_io], r.[physical_reads], r.[query_plan]
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
	ORDER BY LEVEL ASC;
	

	PRINT '		Ended @c_collection_time = '+cast(@c_collection_time as varchar(50));
	FETCH NEXT FROM cur_CollectionTimes INTO @c_collection_time;
END

CLOSE cur_CollectionTimes;
DEALLOCATE cur_CollectionTimes;
/*
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
*/