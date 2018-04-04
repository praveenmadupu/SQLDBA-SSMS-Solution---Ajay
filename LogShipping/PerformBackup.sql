DECLARE @fileName VARCHAR(256) -- filename for backup  
DECLARE @fileDate VARCHAR(20) -- used for file name
DECLARE @dbName VARCHAR(125) = 'LSTesting';
DECLARE @backupPath VARCHAR(125) = '\\DC\Backups\SQL-A\';
DECLARE @sqlString NVARCHAR(MAX);

SELECT @fileDate = DATENAME(DAY,GETDATE())+CAST(DATENAME(MONTH,GETDATE()) AS VARCHAR(3))
		+DATENAME(YEAR,GETDATE())+'_'+REPLACE(REPLACE(RIGHT(CONVERT(VARCHAR, GETDATE(), 100),7),':',''), ' ','0');
SELECT @fileName = (SELECT @backupPath+@dbName+'_TLog_'+ @fileDate + '.trn');

SET @sqlString = '
BACKUP LOG '+QUOTENAME(@dbName)+'
	TO DISK = '''+@fileName+'''';

EXEC (@sqlString);