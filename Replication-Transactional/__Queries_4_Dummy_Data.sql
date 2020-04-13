USE [DbaReplication]
GO


CREATE TABLE [dbo].[ReplTable03]
(
	[ID] BIGINT IDENTITY(1,1) PRIMARY KEY,
	[ColStr01] [char](4000) NULL default REPLICATE(CHAR(ABS(CHECKSUM(NEWID()))%26+65),4000),
	[ColStr02] [varchar](3000) NULL default REPLICATE(CHAR(ABS(CHECKSUM(NEWID()))%26+65),3000),
	created_date datetime default getdate()
)
GO




$dbServer = 'YourDbServerName';
$dbName = 'DbaReplication';
$query = @"
insert dbo.ReplTable03
values (default,default,default)
"@

while($true) {
    Invoke-DbaQuery -SqlInstance $dbServer -Database $dbName -Query $query;
    Start-Sleep -Seconds 10;
}