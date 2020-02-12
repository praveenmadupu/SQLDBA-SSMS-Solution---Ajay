/*
PowerShell script to check an application that's locking a file?
https://docs.microsoft.com/en-us/sysinternals/downloads/handle

Get-LockingProcess -Path 'E:\Get-MSSQLLinkPasswords.ps1.txt'

handle 'E:\Get-MSSQLLinkPasswords.ps1.txt' -accepteula

F:\DBAResources\Handle
*/

create table tempdb..output (id int identity(1,1) not null, output varchar(500), collection_time datetime2 default getdate());
--truncate table tempdb..output

declare @timeMax datetime2 = '2020-02-10 06:30:00.000'

WAITFOR TIME '05:50'; 
while (GETDATE() <@timeMax)
begin
	insert tempdb..output (output)
	EXEC master..xp_cmdshell 'handle ''F:\dump\AMG_avg_data.csq'' -accepteula'  

	waitfor delay '00:00:5'
end

select * from tempdb..output;

EXEC msdb.dbo.sp_send_dbmail  
    @recipients = 'ajay.dwivedi@gmail.com',  
    @query = 'select * from tempdb..output',
    @subject = 'Blocking for job [DBA Log Walk - Simple Recovery dbs]',  
    @attach_query_result_as_file = 1 ;  