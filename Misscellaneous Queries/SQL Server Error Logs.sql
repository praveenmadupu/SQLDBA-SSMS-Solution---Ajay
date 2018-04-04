--	https://www.mssqltips.com/sqlservertip/1476/reading-the-sql-server-log-files-using-tsql/
/*	This procedure takes four parameters:

Value of error log file you want to read: 0 = current, 1 = Archive #1, 2 = Archive #2, etc...
Log file type: 1 or NULL = error log, 2 = SQL Agent log
Search string 1: String one you want to search for
Search string 2: String two you want to search for to further refine the results
If you do not pass any parameters this will return the contents of the current error log.
*/
EXEC master..xp_readerrorlog 0,1
EXEC master..xp_readerrorlog 0,1, N'Server process ID is'
EXEC master..xp_readerrorlog 0,1, N'System Manufacturer:', N'System Model'
EXEC master..xp_readerrorlog 0,1, N'sockets',N'processors'
EXEC master..xp_readerrorlog 0,1, N'File Initialization'
EXEC master..xp_readerrorlog 0,1, N'Server is listening on'
EXEC master..xp_readerrorlog 0,1, N'Dedicated admin connection support'

EXEC master.dbo.xp_readerrorlog 0, 1, EUExtracts, NULL, "2018-04-02", NULL, "desc"