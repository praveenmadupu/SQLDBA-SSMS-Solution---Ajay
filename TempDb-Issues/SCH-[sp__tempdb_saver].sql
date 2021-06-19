USE [master]
GO

IF OBJECT_ID('master.dbo.sp__tempdb_saver') IS NULL
	EXEC('CREATE PROCEDURE dbo.sp__tempdb_saver AS select 1 as dummy;');
GO

ALTER PROCEDURE [dbo].[sp__tempdb_saver]
(
	 @data_used_pct_threshold tinyint = 90,
	 @kill_spids bit = 0,
	 @retention_days int = 15, /* Keep x days of data in history table */
	 @verbose tinyint = 1, /* 1 => messages, 2 => messages + table results */
	 @first_x_rows int = 10 /* Save top x rows in history table */
)
AS
BEGIN
	/*
		Purpose:	Kill sessions causing tempdb space utilization
		
		EXEC [dbo].[sp__tempdb_saver] @data_used_pct_threshold = 80, @kill_spids = 0, @verbose = 2, @first_x_rows = 5
	*/
	SET NOCOUNT ON;
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
	SET XACT_ABORT ON;
	SET ANSI_WARNINGS OFF;

	DECLARE @sql varchar(8000) = '', @sql_kill varchar(8000),
			@email_body varchar(max) = null,
			@email_subject nvarchar(255) = 'Tempdb Saver: ' + @@SERVERNAME,
			@data_used_pct_current decimal(5,2);

	IF (@verbose > 0)
		PRINT '('+convert(varchar, getdate(), 21)+') Creating table variableS @tempdbspace & @tempdbusage..';
	DECLARE @tempdbspace TABLE (database_name sysname, data_size_mb varchar(100), data_used_mb varchar(100), data_used_pct decimal(5,2), log_size_mb varchar(100), log_used_mb varchar(100), log_used_pct decimal(5,2), version_store_mb decimal(20,2))
	DECLARE @tempdbusage TABLE
	(
		[collection_time] [datetime] NOT NULL,
		[spid] [smallint] NULL,
		[login_name] [nvarchar](128) NOT NULL,
		[program_name] [nvarchar](128) NULL,
		[host_name] [nvarchar](128) NULL,
		[host_process_id] [int] NULL,
		[is_active_session] [int] NOT NULL,
		[open_transaction_count] [int] NOT NULL,
		[transaction_isolation_level] [varchar](15) NULL,
		[size_bytes] [bigint] NULL,
		[transaction_begin_time] [datetime] NULL,
		[is_snapshot] [int] NOT NULL,
		[log_bytes] [bigint] NULL,
		[log_rsvd] [bigint] NULL,
		[action_taken] [varchar](200) NULL
	);

	IF OBJECT_ID('dbo.tempdb_saver_history') IS NULL
	BEGIN
		SET @sql = '
		CREATE TABLE [dbo].[tempdb_saver_history]
		(
			[collection_time] [datetime] NOT NULL,
			[spid] [smallint] NULL,
			[login_name] [nvarchar](128) NULL,
			[program_name] [nvarchar](128) NULL,
			[host_name] [nvarchar](128) NULL,
			[host_process_id] [int] NULL,
			[is_active_session] [int] NULL,
			[open_transaction_count] [int] NULL,
			[transaction_isolation_level] [varchar](15) NULL,
			[size_bytes] [bigint] NULL,
			[transaction_begin_time] [datetime] NULL,
			[is_snapshot] [int] NULL,
			[log_bytes] [bigint] NULL,
			[log_rsvd] [bigint] NULL,
			[action_taken] [varchar](200) NULL
		);
		CREATE CLUSTERED INDEX ci_tempdb_saver_history ON dbo.tempdb_saver_history(collection_time);
		'
		IF (@verbose > 0)
		BEGIN
			PRINT '('+convert(varchar, getdate(), 21)+') Creating table dbo.tempdb_saver_history..'+CHAR(10)+CHAR(13);
			PRINT @sql;
		END

		EXEC (@sql)
	END

	IF (@verbose > 0)
		PRINT '('+convert(varchar, getdate(), 21)+') Populate table @tempdbspace..'	

	SET @sql = '
		--	Find used/free space in Database Files
		use [tempdb];
		;with t_files as (
			select [database_name] = DB_NAME(), type_desc,
					[size_mb] = SUM(size)/128.0, 
					[space_used_mb] = SUM(FILEPROPERTY(f.name, ''SpaceUsed''))/128.0
			from tempdb.sys.database_files f
			group by type_desc
		)
		select l.database_name, 
				[data_size_mb] = convert(numeric(38,2),d.size_mb), [data_used_mb] = convert(numeric(38,2),d.space_used_mb), [data_used_pct] = convert(numeric(38,2),(d.space_used_mb*100.0)/d.size_mb),
				[log_size_mb] = convert(numeric(38,2),l.size_mb), [log_used_mb] = convert(numeric(38,2),l.space_used_mb), [log_used_pct] = convert(numeric(38,2),(l.space_used_mb*100.0)/l.size_mb)
		from t_files l
		join t_files d on d.database_name = l.database_name and l.type_desc = ''LOG'' and d.type_desc <> l.type_desc'
	--PRINT (@sql);
	INSERT INTO @tempdbspace
	([database_name], data_size_mb, data_used_mb, data_used_pct, log_size_mb, log_used_mb, log_used_pct)
	EXEC (@sql);

	UPDATE @tempdbspace
	SET version_store_mb = (SELECT (SUM(version_store_reserved_page_count) / 128.0)	
							FROM tempdb.sys.dm_db_file_space_usage fsu with (nolock));

	IF (@verbose > 1)
	BEGIN
		PRINT '('+convert(varchar, getdate(), 21)+') select * from @tempdbspace..'
		select running_query, t.*
		from @tempdbspace t
		full outer join (values ('@tempdbspace') )dummy(running_query) on 1 = 1;
	END

	IF (@verbose > 0)
		PRINT '('+convert(varchar, getdate(), 21)+') Populate table @tempdbusage..'	
	SET @sql = '
	;WITH T_SnapshotTran
	AS (	
		SELECT	[s_tst].[session_id], --DB_NAME(s_tdt.database_id) as database_name,
				ISNULL(MIN([s_tdt].[database_transaction_begin_time]),MIN(DATEADD(SECOND,snp.elapsed_time_seconds,GETDATE()))) AS [begin_time],
				SUM([s_tdt].[database_transaction_log_bytes_used]) AS [log_bytes],
				SUM([s_tdt].[database_transaction_log_bytes_reserved]) AS [log_rsvd],
				MAX(CASE WHEN snp.elapsed_time_seconds IS NOT NULL THEN 1 ELSE 0 END) AS is_snapshot
		FROM sys.dm_tran_database_transactions [s_tdt]
		JOIN sys.dm_tran_session_transactions [s_tst]
			ON [s_tst].[transaction_id] = [s_tdt].[transaction_id]
		LEFT JOIN sys.dm_tran_active_snapshot_database_transactions snp
			ON snp.session_id = s_tst.session_id AND snp.transaction_id = s_tst.transaction_id
		--WHERE s_tdt.database_id = 2
		GROUP BY [s_tst].[session_id] --,s_tdt.database_id
	)
	,T_TempDbTrans AS 
	(
		SELECT	GETDATE() AS collection_time,
				des.session_id AS spid,
				des.original_login_name as login_name,  
				des.program_name,
				des.host_name,
				des.host_process_id,
				[is_active_session] = CASE WHEN er.request_id IS NOT NULL THEN 1 ELSE 0 END,
				des.open_transaction_count,
				[transaction_isolation_level] = (CASE des.transaction_isolation_level 
						WHEN 0 THEN ''Unspecified''
						WHEN 1 THEN ''ReadUncommitted''
						WHEN 2 THEN ''ReadCommitted''
						WHEN 3 THEN ''Repeatable''
						WHEN 4 THEN ''Serializable'' 
						WHEN 5 THEN ''Snapshot'' END ),
				[size_bytes] = case when tsu.session_id is not null /* if active request, then active request */
									then tsu.size_bytes
									else /* if no  active request, then session usage */
										 ((ssu.user_objects_alloc_page_count+ssu.internal_objects_alloc_page_count)-(ssu.internal_objects_dealloc_page_count+ssu.user_objects_dealloc_page_count+ssu.user_objects_deferred_dealloc_page_count))*8192
									end,
				[transaction_begin_time] = case when des.open_transaction_count > 0 then (case when ott.begin_time is not null then ott.begin_time when er.start_time is not null then er.start_time else des.last_request_start_time end) else er.start_time end,
				[is_snapshot] = CASE WHEN ISNULL(ott.is_snapshot,0) = 1 THEN 1
									 WHEN tasdt.is_snapshot = 1 THEN 1
									 ELSE ISNULL(ott.is_snapshot,0)
									 END,
				ott.[log_bytes], ott.log_rsvd,
				CONVERT(varchar(200),NULL) AS action_taken
		FROM       sys.dm_exec_sessions des
		LEFT JOIN sys.dm_db_session_space_usage ssu on ssu.session_id = des.session_id
		LEFT JOIN T_SnapshotTran ott ON ott.session_id = ssu.session_id
		LEFT JOIN sys.dm_exec_requests er ON er.session_id = des.session_id
		LEFT JOIN (	SELECT tsu.session_id, size_bytes = ( SUM((tsu.user_objects_alloc_page_count+tsu.internal_objects_alloc_page_count)-(tsu.user_objects_dealloc_page_count+tsu.internal_objects_dealloc_page_count)) )*8192 
						FROM sys.dm_db_task_space_usage tsu 
						WHERE ((tsu.user_objects_alloc_page_count+tsu.internal_objects_alloc_page_count)-(tsu.user_objects_dealloc_page_count+tsu.internal_objects_dealloc_page_count)) > 0
							--AND tsu.session_id = er.session_id
						GROUP BY tsu.session_id
					) as tsu
			ON tsu.session_id = des.session_id
		OUTER APPLY (select 1 as [is_snapshot] from sys.dm_tran_active_snapshot_database_transactions asdt where asdt.session_id = des.session_id) as tasdt
		WHERE des.session_id <> @@SPID --AND (er.request_id IS NOT NULL OR des.open_transaction_count > 0)
			--AND ssu.database_id = 2
	)
	SELECT top ('+CONVERT(varchar,@first_x_rows)+') *
	FROM T_TempDbTrans ot
	WHERE size_bytes > 0 OR is_active_session = 1 OR open_transaction_count > 0 OR  is_snapshot = 1
	'
	IF EXISTS (SELECT * FROM @tempdbspace s WHERE s.version_store_mb >= 0.30*CONVERT(numeric(20,2),data_used_mb))
		SET @sql = @sql + 'order by is_snapshot DESC, transaction_begin_time ASC;'+CHAR(10)
	ELSE
		SET @sql = @sql + 'order by size_bytes desc;'+CHAR(10)
	
	IF (@verbose > 1)
		PRINT @sql
	INSERT @tempdbusage
	EXEC (@sql);

	IF (@verbose > 1)
	BEGIN
		PRINT '('+convert(varchar, getdate(), 21)+') select * from @tempdbusage..'
		
		IF EXISTS (SELECT * FROM @tempdbspace s WHERE s.version_store_mb >= 0.30*CONVERT(numeric(20,2),data_used_mb))
			select running_query, t.*
			from @tempdbusage t
			full outer join (values ('@tempdbusage') )dummy(running_query) on 1 = 1
			order by is_snapshot DESC, transaction_begin_time ASC;
		ELSE
			select running_query, t.* --top (@first_x_rows) 
			from @tempdbusage t
			full outer join (values ('@tempdbusage') )dummy(running_query) on 1 = 1
			order by size_bytes desc;
	END

	IF @verbose > 0
		PRINT '('+convert(varchar, getdate(), 21)+') Compare @tempdbspace.[data_used_pct] with @data_used_pct_threshold ('+convert(varchar,@data_used_pct_threshold)+')..'	
	IF ((SELECT data_used_pct FROM @tempdbspace) > @data_used_pct_threshold)
	BEGIN
		IF @verbose > 0
			PRINT '('+convert(varchar, getdate(), 21)+') Found @tempdbspace.[data_used_pct] > '+convert(varchar,@data_used_pct_threshold)+' %'
			
		IF EXISTS (SELECT * FROM @tempdbspace s WHERE s.version_store_mb >= 0.30*CONVERT(numeric(20,2),data_used_mb)) -- If Version Store Issue
		BEGIN
			IF @verbose > 0
			BEGIN
				PRINT '('+convert(varchar, getdate(), 21)+') Version Store Issue.';
				PRINT '('+convert(varchar, getdate(), 21)+') version_store_mb >= 30% of data_used_mb';
				PRINT '('+convert(varchar, getdate(), 21)+') Pick top spid (@sql_kill) order by ''ORDER BY is_snapshot DESC, transaction_begin_time ASC''';
			END
			SELECT TOP 1 @sql_kill = CONVERT(varchar(30), tu.spid)
			FROM	@tempdbusage tu
			WHERE   host_process_id IS NOT NULL
			AND     login_name NOT IN ('sa', 'NT AUTHORITY\SYSTEM')
			ORDER BY is_snapshot DESC, transaction_begin_time ASC;
		END
		ELSE
		BEGIN -- Not Version Store issue.
			IF @verbose > 0
			BEGIN
				PRINT '('+convert(varchar, getdate(), 21)+') Not Version Store Issue.';
				PRINT '('+convert(varchar, getdate(), 21)+') version_store_mb < 30% of data_used_mb';
				PRINT '('+convert(varchar, getdate(), 21)+') Pick top spid (@sql_kill) order by ''(ISNULL(size_bytes,0)+ISNULL(log_bytes,0)+ISNULL(log_rsvd,0)) DESC''';
			END
			SELECT TOP 1 @sql_kill = CONVERT(varchar(30), tu.spid)
			FROM @tempdbusage tu
			WHERE         host_process_id IS NOT NULL
			AND         login_name NOT IN ('sa', 'NT AUTHORITY\SYSTEM')
			AND size_bytes <> 0
			ORDER BY (ISNULL(size_bytes,0)+ISNULL(log_bytes,0)+ISNULL(log_rsvd,0)) DESC;
		END
		

		IF @verbose > 0
			PRINT '('+convert(varchar, getdate(), 21)+') Top tempdb consumer spid (@sql_kill) = '+@sql_kill;
  
		IF (@sql_kill IS NOT NULL)
		BEGIN
			IF (@kill_spids = 1)
			BEGIN
				IF @verbose > 0
					PRINT '('+convert(varchar, getdate(), 21)+') Kill top consumer.';
				UPDATE @tempdbusage SET action_taken = 'Process Terminated' WHERE spid = @sql_kill
				SET @sql = 'kill ' + @sql_kill;
				PRINT (@sql);
				EXEC (@sql);
				IF @verbose > 0
					PRINT '('+convert(varchar, getdate(), 21)+') Update @tempdbusage with action_taken ''Process Terminated''.';
			END
			ELSE
			BEGIN
				UPDATE @tempdbusage SET action_taken = 'No Action' WHERE spid = @sql_kill
				IF @verbose > 0
					PRINT '('+convert(varchar, getdate(), 21)+') Update @tempdbusage with action_taken ''No Action''.';
			END;

		END;

		IF @verbose > 0
			PRINT '('+convert(varchar, getdate(), 21)+') Populate table dbo.tempdb_saver_history with top 10 session details.';
		IF EXISTS (SELECT * FROM @tempdbspace s WHERE s.version_store_mb >= 0.30*CONVERT(numeric(20,2),data_used_mb))
			INSERT INTO dbo.tempdb_saver_history
			SELECT *
			FROM  @tempdbusage 
			order by is_snapshot DESC, transaction_begin_time ASC;
		ELSE
			INSERT INTO dbo.tempdb_saver_history
			SELECT *
			FROM  @tempdbusage 
			order by size_bytes desc;
	END;
	ELSE
	BEGIN
		IF @verbose > 0
			PRINT '('+convert(varchar, getdate(), 21)+') Current tempdb space usage under threshold.'	
	END

	IF @verbose > 0
		PRINT '('+convert(varchar, getdate(), 21)+') Purge dbo.tempdb_saver_history with @retention_days = '+convert(varchar,@retention_days);
	DELETE FROM dbo.tempdb_saver_history WHERE collection_time <= DATEADD(day, -@retention_days, GETDATE());
END
GO