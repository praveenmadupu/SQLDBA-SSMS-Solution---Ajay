USE DBA
--	OlaHallengren - DatabaseBackup - Configurable @Directory Backup Folder Names
--	Modified By Ajay Dwivedi

-- [backupfile] = ServerName_DatabaseName_BackupType_yyyymmdd_hhmmss.bak

EXEC [dbo].[DatabaseBackup]	
		@Databases = 'USER_DATABASES', 

		--@Directory = 'E:\Backup\**DATABASENAME**', 
			/* Output like 'E:\Backup\DatabaseName\backupfile.bak' */

		--@Directory = 'E:\Backup\**DATABASENAME**\**BACKUPTYPE**', 
			/* Output like 'E:\Backup\DatabaseName\BackupType\backupfile.bak' */

		--@Directory = 'E:\Backup', 
			/* Output like 'E:\Backup\ServerName\DatabaseName\BackupType\backupfile.bak' */

		@Directory = 'E:\Backup\*', 
			/* Output like 'E:\Backup\backupfile.bak' */

		@BackupType = 'FULL', 
		@Compress = 'y'