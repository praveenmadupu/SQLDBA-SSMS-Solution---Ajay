-- Where am I?
SELECT  @@SERVERNAME;
GO

-- Is there already a Distributor here?
EXEC sp_get_distributor;
GO

-- Add the distributor
EXEC sp_adddistributor @distributor = N'TUL1DBAPMTDB1\SQL2016',
    @password = N'Pa$$w0rd'; 
GO

/*

RESTORE DATABASE [TivoSQLInventory_Ajay] FROM  DISK = N'Your-Backup-File-Path-in-Here'
    WITH RECOVERY
         ,STATS = 3
         ,REPLACE
		 ,MOVE N'TivoSQLInventory' TO N'F:\MSSQLData\SQL2016_Data\TivoSQLInventory_Ajay.mdf'
		 ,MOVE N'TivoSQLInventory_log' TO N'F:\MSSQLData\SQL2016_Data\TivoSQLInventory_Ajay_log.ldf'

GO
*/

-- A few observations:
-- Database name is configurable
-- Keep note of the path for the data and log file
-- Default data file is just 5MBs so consider @data_file_size
EXEC sp_adddistributiondb @database = N'TivoSQLInventory_Distributor',
    @data_folder = N'F:\MSSQLData\SQL2016_Data\', @log_folder = N'F:\MSSQLData\SQL2016_Log\',
    @log_file_size = 2, @min_distretention = 0, @max_distretention = 72,
    @history_retention = 48;
GO

-- Configuring a publisher to use the distribution db
USE TivoSQLInventory_Distributor;
GO

EXEC sp_adddistpublisher @publisher = N'TUL1DBAPMTDB1\SQL2016',
    @distribution_db = N'TivoSQLInventory_Distributor', @security_mode = 1,
    @working_directory = N'\\TUL1DBAPMTDB1\Replication\', @thirdparty_flag = 0, -- if SQL and not another product
    @publisher_type = N'MSSQLSERVER';
GO

-- Let's confirm what we created
EXEC sp_get_distributor;

SELECT  is_distributor,
        *
FROM    sys.servers
WHERE   name = 'repl_distributor' AND
        data_source = @@SERVERNAME;
GO

-- Which database is the distributor?
SELECT  name
FROM    sys.databases
WHERE   is_distributor = 1;

-- Specific to the database
EXEC sp_helpdistributiondb;
GO