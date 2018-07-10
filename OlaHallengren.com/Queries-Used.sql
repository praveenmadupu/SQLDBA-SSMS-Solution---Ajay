--	https://ola.hallengren.com/sql-server-backup.html

--	Full Backups
EXEC DBA.dbo.[DatabaseBackup]
		@Databases = 'ALL_DATABASES',
		@Directory = 'E:\Backup', /* Output like 'E:\Backup\backupfile.bak' */ 
		@DirectoryStructure = NULL, /* Do not create directory structure */
		@BackupType = 'FULL', 
		@Compress = 'Y'
		,@CleanupTime = 168 -- 1 week
		,@CleanupMode = 'AFTER_BACKUP';

--	Diff Backups
EXEC DBA.dbo.[DatabaseBackup]
		@Databases = 'USER_DATABASES',
		@Directory = 'E:\Backup', /* Output like 'E:\Backup\backupfile.bak' */ 
		@DirectoryStructure = NULL, /* Do not create directory structure */
		@BackupType = 'DIFF', 
		@FileExtensionDiff = 'diff',
		@Compress = 'Y'