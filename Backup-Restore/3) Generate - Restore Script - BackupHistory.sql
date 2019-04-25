DECLARE @c_database_name VARCHAR(125);
DECLARE @c_physical_device_name VARCHAR(1000);
declare @sqlRestoreText varchar(max);
declare @p_Target_Data_Path varchar(255) = 'W:\MSSQLData\Data\';
declare @p_Target_Log_Path varchar(255) = 'E:\MSSQLData\Logs\';

DECLARE cur_Backups CURSOR LOCAL FAST_FORWARD READ_ONLY FOR
WITH T_bkpHistory as
(
SELECT --ROW_NUMBER()over(partition by bs.database_name order by bs.backup_finish_date desc) as RowID,
		CONVERT(CHAR(100), SERVERPROPERTY('Servername')) AS SERVER
	,bs.database_name
	,bs.backup_start_date
	,bs.backup_finish_date
	,bs.expiration_date
	,CASE bs.type
		WHEN 'D'
			THEN 'Database'
		WHEN 'L'
			THEN 'Log'
		END AS backup_type
	,convert(decimal(18,3),(bs.backup_size)/1024/1024) as backup_size_MB
	,convert(decimal(18,3),(bs.backup_size)/1024/1024/1024) as backup_size_GB
	,bmf.logical_device_name
	,bmf.physical_device_name
	,bs.NAME AS backupset_name
	,bs.description
	,first_lsn
	,last_lsn
	,checkpoint_lsn
	,database_backup_lsn
	,is_copy_only
FROM msdb.dbo.backupmediafamily AS bmf
INNER JOIN msdb.dbo.backupset AS bs ON bmf.media_set_id = bs.media_set_id
WHERE bs.backup_start_date >= (select max(bsi.backup_start_date) FROM msdb.dbo.backupmediafamily AS bmfi INNER JOIN msdb.dbo.backupset AS bsi ON bmfi.media_set_id = bsi.media_set_id where bsi.database_name = bs.database_name and bsi.type = 'D' --and bmfi.physical_device_name not like '\\Ann1vespdb01\%'
)
and bs.type = 'D'  --and bmf.physical_device_name not like '\\Ann1vespdb01\%'
)
select database_name, physical_device_name
from T_bkpHistory
where database_name in ('AMGExtra','AMGMusic','AMGMusicMore')
--(database_name not like 'RoviMusicShipping_EU_19%' and database_name not like 'RoviMusicShipping_EU_18%' AND database_name <> 'RoviMusicShipping_Archive' 
--		AND database_name not in ('master','model','msdb','tempdb'))
ORDER BY backup_start_date;

OPEN cur_Backups;

FETCH NEXT FROM cur_Backups INTO @c_database_name, @c_physical_device_name;

WHILE @@FETCH_STATUS = 0
BEGIN
	--PRINT '@c_database_name = '+@c_database_name+CHAR(13)+CHAR(10)+'@c_physical_device_name = '+@c_physical_device_name;
	set @sqlRestoreText = '
	RESTORE DATABASE '+QUOTENAME(@c_database_name)+' FROM  DISK = N'''+( case when charindex(':',@c_physical_device_name)<>0 then '\\'+@@SERVERNAME+'\'+REPLACE(@c_physical_device_name,':','$') else @c_physical_device_name end )+'''
		WITH RECOVERY
			 ,STATS = 3
			 ,REPLACE
	';

	select @sqlRestoreText += --name, physical_name,
	'		 ,MOVE N'''+name+''' TO N'''+(case when mf.type_desc = 'ROWS' then @p_Target_Data_Path ELSE @p_Target_Log_Path END )+ RIGHT(mf.physical_name,CHARINDEX('\',REVERSE(mf.physical_name))-1) +'''
	'
	from sys.master_files as mf 
	where mf.database_id = DB_ID(@c_database_name);

	SET @sqlRestoreText += '
	GO'

	PRINT @sqlRestoreText;

	FETCH NEXT FROM cur_Backups INTO @c_database_name, @c_physical_device_name;
END