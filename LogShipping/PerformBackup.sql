DECLARE @fileName VARCHAR(256) -- filename for backup  
DECLARE @fileDate VARCHAR(20) -- used for file name
DECLARE @dbName VARCHAR(125) = 'LSTesting';
DECLARE @backupPath VARCHAR(125) = '\\DC\Backups\SQL-A\';
DECLARE @sqlString NVARCHAR(MAX);
DECLARE @backupType VARCHAR(20) = 'Full'
DECLARE @backupTypeContext VARCHAR(20);
DECLARE @backupExtension VARCHAR(20);

SELECT	@backupTypeContext = (CASE WHEN @backupType = 'Full' THEN 'DATABASE' ELSE 'LOG' END)
		,@backupExtension = CASE WHEN @backupType = 'Full' THEN '.bak' ELSE '.trn' END;

SELECT @fileDate = DATENAME(DAY,GETDATE())+CAST(DATENAME(MONTH,GETDATE()) AS VARCHAR(3))
		+DATENAME(YEAR,GETDATE())+'_'+REPLACE(REPLACE(RIGHT(CONVERT(VARCHAR, GETDATE(), 100),7),':',''), ' ','0');
SELECT @fileName = (SELECT @backupPath+@dbName+'_'+@backupTypeContext+'_'+ @fileDate + @backupExtension);

SET @sqlString = '
BACKUP '+@backupTypeContext+' '+QUOTENAME(@dbName)+'
	TO DISK = '''+@fileName+'''';

PRINT	@sqlString;
--EXEC (@sqlString);