/*	Name:			Ajay Dwivedi
	Created Date:	18-Apr-2018
	Purpose:		Script to provide backup for Database Restore
*/
SET NOCOUNT ON;
DECLARE @dbName VARCHAR(125),
		@backupStartDate datetime,
		@stopAtTime datetime;
DECLARE @SQLString nvarchar(2000);  
DECLARE @ParmDefinition nvarchar(500);  
SET @dbName = 'Cosmo';
--SET @stopAtTime = '2018-04-16 23:40:00'

IF OBJECT_ID('tempdb..#BackupHistory') IS NOT NULL
	DROP TABLE #BackupHistory;
CREATE TABLE #BackupHistory
(
	[BackupFile] [nvarchar](260) NULL,
	[BackupTypeDescription] [varchar](21) NULL,
	[ServerName] [char](100) NULL,
	[UserName] [nvarchar](128) NULL,
	[database_name] [nvarchar](128) NULL,
	[DatabaseCreationDate] [datetime] NULL,
	[BackupSize] [numeric](20, 0) NULL,
	[FirstLSN] [numeric](25, 0) NULL,
	[LastLSN] [numeric](25, 0) NULL,
	[CheckpointLSN] [numeric](25, 0) NULL,
	[DatabaseBackupLSN] [numeric](25, 0) NULL,
	[BackupStartDate] [datetime] NULL,
	[BackupFinishDate] [datetime] NULL,
	[CompatibilityLevel] [tinyint] NULL,
	[Collation] [nvarchar](128) NULL,
	[IsCopyOnly] [bit] NULL,
	[RecoveryModel] [nvarchar](60) NULL
) ;

/* Build the SQL string to get all latest backups for database. */  
SET @SQLString =  
     N'SELECT	BackupFile = bmf.physical_device_name,
		CASE bs.type WHEN ''D'' THEN ''Database'' WHEN ''I'' THEN ''Differential database'' WHEN ''L'' THEN ''Log'' ELSE NULL END as BackupTypeDescription,
		LTRIM(RTRIM(CONVERT(CHAR(100), SERVERPROPERTY(''Servername'')))) as ServerName,
		UserName = bs.user_name,
		bs.database_name,
		DatabaseCreationDate = bs.database_creation_date,
		BackupSize = bs.backup_size,
		FirstLSN = bs.first_lsn, 
		LastLSN = bs.last_lsn, 
		CheckpointLSN = bs.checkpoint_lsn,
		DatabaseBackupLSN = bs.database_backup_lsn,
		BackupStartDate = bs.backup_start_date,
		BackupFinishDate = bs.backup_finish_date,
		CompatibilityLevel = bs.compatibility_level,
		Collation = bs.collation_name,
		IsCopyOnly = bs.is_copy_only,
		RecoveryModel = bs.recovery_model
FROM	msdb.dbo.backupmediafamily AS bmf
INNER JOIN msdb.dbo.backupset AS bs ON bmf.media_set_id = bs.media_set_id
WHERE	database_name = @q_dbName
	AND	bs.backup_start_date >= @q_backupStartDate
AND		(	@q_stopAtDate IS NULL OR bs.backup_start_date < @q_stopAtDate -- All the TLogbackups will be covered
			OR 
			(	bs.backup_start_date = (	SELECT MIN(i.backup_start_date) as LastTLogToRestore 
											FROM msdb..backupset as i WHERE i.database_name = bs.database_name AND i.type = ''L'' 
											AND i.backup_start_date >= @q_stopAtDate 
										)
			)
		)';  

SET @ParmDefinition = N'@q_dbName varchar(125), @q_backupStartDate datetime, @q_stopAtDate datetime'; 
  
DECLARE databases_cursor CURSOR LOCAL FORWARD_ONLY FOR 
		--	Find latest Full backup for each database
		SELECT MAX(bs.backup_start_date) AS Latest_FullBackupDate, database_name
		FROM msdb.dbo.backupmediafamily AS bmf INNER JOIN msdb.dbo.backupset AS bs 
		ON bmf.media_set_id = bs.media_set_id WHERE bs.type='D' and is_copy_only = 0
		AND (@dbName IS NULL OR LTRIM(RTRIM(@dbName)) = '' OR @dbName = database_name)
		AND (@stopAtTime IS NULL OR LTRIM(RTRIM(@stopAtTime)) = '' OR CAST(bs.backup_start_date as datetime)< @stopAtTime)
		GROUP BY database_name;

OPEN databases_cursor
FETCH NEXT FROM databases_cursor INTO @backupStartDate, @dbName;

WHILE @@FETCH_STATUS = 0 
BEGIN
	BEGIN TRY
		--SELECT	[@backupStartDate] = @backupStartDate, [@stopAtTime] = @stopAtTime;
		--PRINT	@SQLString;
		--	Find latest backups
		INSERT #BackupHistory
		EXECUTE sp_executesql @SQLString, @ParmDefinition,  
							  @q_dbName = @dbName,
							  @q_backupStartDate = @backupStartDate,
							  @q_stopAtDate = @stopAtTime; 
	END TRY
	BEGIN CATCH
		PRINT ' -- ---------------------------------------------------------';
		PRINT ERROR_MESSAGE();
		PRINT ' -- ---------------------------------------------------------';
	END CATCH
		
	FETCH NEXT FROM databases_cursor INTO @backupStartDate, @dbName;
END

CLOSE databases_cursor;
DEALLOCATE databases_cursor ;

IF EXISTS (SELECT * FROM #BackupHistory)
BEGIN
	SELECT	[@stopAtTime] = ISNULL(@stopAtTime,GETDATE()), *
	FROM #BackupHistory as bh
	ORDER BY BackupStartDate;
END
ELSE
	SELECT 'Check if wrong database name is provided' as [Error Message]