--	https://ola.hallengren.com/sql-server-backup.html

EXECUTE audit_archive..IndexOptimize
/* Update Stats */
@Databases = 'geneva_warehouse',
@FragmentationLow = NULL,
@FragmentationMedium = NULL,
@FragmentationHigh = NULL,
@UpdateStatistics = 'ALL',
@OnlyModifiedStatistics = 'Y';




DECLARE @_dbNames VARCHAR(MAX);

/* Get Comma Separated List of  Database Names which are not on APPSYNC*/
select @_dbNames = COALESCE(@_dbNames+','+DB_NAME(mf.database_id),DB_NAME(mf.database_id))
                 --,mf.physical_name
from sys.master_files as mf
where mf.file_id = 1
         AND mf.database_id <> DB_ID('tempdb')
         AND mf.physical_name not like 'C:\AppSyncMounts\%'
         AND mf.database_id not in (select d.database_id from sys.databases as d where d.is_in_standby = 1 or d.source_database_id IS NOT NULL);

select @_dbNames;


--	Full Backups
EXEC DBA.dbo.[DatabaseBackup]
		@Databases = @_dbNames,
		@Directory = 'E:\Backup', /* Output like 'E:\Backup\backupfile.bak' */ 
		@DirectoryStructure = NULL, /* Do not create directory structure */
		@BackupType = 'FULL', 
		@Compress = 'Y'
		,@CleanupTime = 168 -- 1 week
		,@CleanupMode = 'AFTER_BACKUP';
GO

DECLARE @_dbNames VARCHAR(MAX);

/* Get Comma Separated List of  Database Names which are not on APPSYNC*/
select @_dbNames = COALESCE(@_dbNames+','+DB_NAME(mf.database_id),DB_NAME(mf.database_id))
                 --,mf.physical_name
from sys.master_files as mf
where mf.file_id = 1
         AND DB_NAME(mf.database_id) NOT IN ('master','tempdb','model','msdb','resourcedb')
         AND mf.physical_name not like 'C:\AppSyncMounts\%'
         AND mf.database_id not in (select d.database_id from sys.databases as d where d.is_in_standby = 1 or d.source_database_id IS NOT NULL);

select @_dbNames;

--	Diff Backups
EXEC DBA.dbo.[DatabaseBackup]
		@Databases = @_dbNames,
		@Directory = 'E:\Backup', /* Output like 'E:\Backup\backupfile.bak' */ 
		@DirectoryStructure = NULL, /* Do not create directory structure */
		@BackupType = 'DIFF', 
		@FileExtensionDiff = 'diff',
		@Compress = 'Y'