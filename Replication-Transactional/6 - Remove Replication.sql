/*
	https://docs.microsoft.com/en-us/previous-versions/sql/sql-server-2008-r2/ms152757(v=sql.105)
	https://docs.microsoft.com/en-us/previous-versions/sql/sql-server-2008-r2/ms188734(v=sql.105)
*/

-- Verify Server Name 1st
SELECT @@SERVERNAME as srvName
GO

-- Script out existing replication settings



-- Remove replication objects from the subscription database on MYSUB.
DECLARE @subscriptionDB AS sysname
SET @subscriptionDB = N'TivoSQLInventory_Dev'

-- Remove replication objects from a subscription database (if necessary).
USE master
EXEC sp_removedbreplication @subscriptionDB
GO

-- Remove replication from [distribution] database
use [master]
exec sp_dropdistributor @no_checks = 1
GO