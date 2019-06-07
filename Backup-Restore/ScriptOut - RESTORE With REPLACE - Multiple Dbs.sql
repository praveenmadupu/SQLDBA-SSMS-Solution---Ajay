/*
	Get RESTORE statement for databases Keeping pre-existing FILE STRUCTURE
	Run on Target (Destination)
*/
SET NOCOUNT ON;

declare @p_dbName varchar(100);
declare @sqlRestoreText varchar(max);
declare @counter int = 1;
declare @total_counts int;

IF OBJECT_ID('tempdb..#Dbs') IS NOT NULL
	DROP TABLE #Dbs;
SELECT ROW_NUMBER()OVER(ORDER BY dbName) as ID, dbName
INTO #Dbs
FROM (VALUES
			('AMG_AVG'),('AMG_Extra'),('AMG_Music'),('AMG_MusicMore'),('Babel'),('DSG_EU'),('Facebook'),
			('Mosaic'),('MuzeUK'),('MuzeUS'),('MuzeVideo'),('Prism'),('RGS'),('RCM_rovicore_20130710_NoMusic1a_en-US'),
			('Sky'),('Staging'),('Staging2'),('Twitter'),('TVGolConfigs'),('UKVideo')
	) Databases(dbName);
set @p_dbName = 'Cosmo';

select @total_counts = count(*) from #Dbs;

while @counter <= @total_counts
BEGIN
	SELECT @p_dbName = dbName FROM #Dbs d WHERE d.ID = @counter;

	set @sqlRestoreText = '
	RESTORE DATABASE '+QUOTENAME(@p_dbName)+' FROM  DISK = N''Your-Backup-File-Path-in-Here''
		WITH RECOVERY
			 ,STATS = 3
			 ,REPLACE
	';

	select @sqlRestoreText += --name, physical_name,
	'		 ,MOVE N'''+name+''' TO N'''+physical_name+'''
	'
	from sys.master_files as mf 
	where DB_NAME(mf.database_id) IN ('AMG_AVG','AMG_Extra','AMG_Music','AMG_MusicMore','Babel','DSG_EU','Facebook','Mosaic',
	'MuzeUK','MuzeUS','MuzeVideo','Prism','RGS','RCM_rovicore_20130710_NoMusic1a_en-US','Sky','Staging','Staging2','Twitter',
	'TVGolConfigs','UKVideo');

	SET @sqlRestoreText += '
	GO'

	PRINT @sqlRestoreText;

	SET @counter += 1;
END
