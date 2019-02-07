BACKUP DATABASE [TivoSQLInventory] 
	TO DISK = N'G:\MSSQLData\SQL2016_Backup\TivoSQLInventory\FULL\TivoSQLInventory_FULL_20190206_225446.bak' 
	WITH NO_CHECKSUM, COMPRESSION;


RESTORE DATABASE [TivoSQLInventory_Ajay] FROM  DISK = N'G:\MSSQLData\SQL2016_Backup\TivoSQLInventory\FULL\TivoSQLInventory_FULL_20190206_225446.bak' 
    WITH RECOVERY
         ,STATS = 3
		 ,MOVE N'TivoSQLInventory' TO N'F:\MSSQLData\SQL2016_Data\TivoSQLInventory_Ajay.mdf'
		 ,MOVE N'TivoSQLInventory_log' TO N'F:\MSSQLData\SQL2016_Log\TivoSQLInventory_Ajay_log.ldf'

GO