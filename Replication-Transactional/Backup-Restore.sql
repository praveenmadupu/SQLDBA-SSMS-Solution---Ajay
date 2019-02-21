BACKUP DATABASE [TivoSQLInventory] 
	TO DISK = N'G:\MSSQLData\SQL2016_Backup\TivoSQLInventory\FULL\TivoSQLInventory_FULL_20190206_225446.bak' 
	WITH NO_CHECKSUM, COMPRESSION;


RESTORE DATABASE [TivoSQLInventory_Ajay] FROM  DISK = N'G:\MSSQLData\SQL2016_Backup\TivoSQLInventory\FULL\TivoSQLInventory_FULL_20190206_225446.bak' 
    WITH RECOVERY
         ,STATS = 3
		 ,MOVE N'TivoSQLInventory' TO N'F:\MSSQLData\SQL2016_Data\TivoSQLInventory_Ajay.mdf'
		 ,MOVE N'TivoSQLInventory_log' TO N'F:\MSSQLData\SQL2016_Log\TivoSQLInventory_Ajay_log.ldf'

GO


RESTORE DATABASE [TivoSQLInventory_Dev] FROM  DISK = 'G:\MSSQLData\SQL2016_Backup\TivoSQLInventory_Distributor\FULL\TivoSQLInventory_Distributor_FULL_20190214_034700.bak' 
    WITH RECOVERY
         ,STATS = 3
         ,REPLACE
		 ,MOVE N'TivoSQLInventory' TO N'F:\MSSQLData\SQL2016_Data\TivoSQLInventory_Dev.mdf'
		 ,MOVE N'TivoSQLInventory_log' TO N'F:\MSSQLData\SQL2016_Data\TivoSQLInventory_Dev_log.ldf'
GO


select top 1 cl.DatabaseName, cl.Command, cl.StartTime 
from DBA.dbo.CommandLog as cl 
where cl.CommandType = 'BACKUP_DATABASE' 
order by cl.StartTime desc
