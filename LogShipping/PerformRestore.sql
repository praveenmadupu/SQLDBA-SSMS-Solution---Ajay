use master
go
--	Get all files of path
exec xp_dirtree '\\DC\Backups\SQL-A',0,1;

/*	Step 01 -> Restore Full & TLog in NORECOVERY */
RESTORE DATABASE [LSTesting]
	FROM DISK = '\\DC\Backups\SQL-A\LSTesting_FullBackup_01Apr2018.bak'
	WITH NORECOVERY, REPLACE
GO

RESTORE LOG [LSTesting]
	FROM DISK = N'\\DC\Backups\SQL-A\LSTesting_TLog_01Apr2018.trn'
	WITH NORECOVERY
GO

/*	Step 01 -> Restore Full & TLog in NORECOVERY */
RESTORE DATABASE [LSTesting]
	WITH STANDBY = 'E:\LS_UndoFiles\LSTesting_undo.tuf'
GO

EXEC master..[usp_DBAApplyTLogs] 'LSTesting', 'LSTesting', '\\DC\Backups\SQL-A\', @p_TUFLocation = 'E:\LS_UndoFiles' ,@p_Verbose = 1 ,@p_DryRun = 1

select d.create_date from sys.databases as d where d.name = 'LSTesting'