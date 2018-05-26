;WITH T_RestoreHistory AS
(
	SELECT	[rs].[destination_database_name], 
			[rs].[restore_date], 
			[bs].[backup_start_date], 
			--[bs].[backup_finish_date], 
			[bs].[database_name] as [source_database_name], 
			[bmf].[physical_device_name] as [backup_file_used_for_restore]
			,bs.backup_size
			,[backup_size(MB)] = convert(decimal(20,2),cast(bs.backup_size as float)/1024/1024)
			,[RestoreWindow] = case when DATEPART(hour,restore_date) between 10 and 19 then '10AM Schedule' else '8PM Schedule' end
	FROM msdb..restorehistory rs 
	INNER JOIN msdb..backupset bs 
	ON [rs].[backup_set_id] = [bs].[backup_set_id] 
	INNER JOIN msdb..backupmediafamily bmf 
	ON [bs].[media_set_id] = [bmf].[media_set_id] 
	WHERE destination_database_name LIKE 'IDS_Turner%'
)
,T_LargeSize AS
(
		SELECT	destination_database_name, restore_date, backup_start_date, source_database_name, backup_file_used_for_restore, backup_size, [backup_size(MB)]
				,cast(restore_date as date) AS JobDate
				,RestoreWindow
				,COUNT(restore_date) OVER (PARTITION BY cast(restore_date as date) ,[RestoreWindow]) AS FilesRestored
				,CAST((SUM(backup_size) OVER (PARTITION BY cast(restore_date as date) ,[RestoreWindow]))/1024/1024 AS DECIMAL(20,2)) AS SumSize_TotalFiles_MB
				,DENSE_RANK()OVER(ORDER BY cast(restore_date as date) asc,[RestoreWindow] ASC)  as ExecutionSequence
				,ROW_NUMBER()OVER(PARTITION BY cast(restore_date as date),[RestoreWindow] ORDER BY backup_size DESC) AS RowID
				,MIN(restore_date) OVER (PARTITION BY cast(restore_date as date) ,[RestoreWindow]) AS FirstFile_RestoreDate
				,MAX(restore_date) OVER (PARTITION BY cast(restore_date as date) ,[RestoreWindow]) AS LastFile_RestoreDate		
		FROM T_RestoreHistory
			WHERE restore_date >= DATEADD(DD,-15,GETDATE())
			--AND [backup_size(MB)] >= 100
			--ORDER BY [restore_date] DESC
)
select *,DATEDIFF(minute,FirstFile_RestoreDate,LastFile_RestoreDate) as [Duration(Min)]
from T_LargeSize
	where RowID = 1
	and RestoreWindow = '8PM Schedule'
	order by restore_date DESC
--GO

--declare @restore_date datetime
--set @restore_date = '2018-05-24 21:18:40.173'
--select @restore_date, case when DATEPART(hour,@restore_date) between 10 and 19 then '10AM Schedule' else '8PM Schedule' end