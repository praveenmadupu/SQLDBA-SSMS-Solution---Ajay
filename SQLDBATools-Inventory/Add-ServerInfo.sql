--ComputerName    VolumeName Capacity(GB) Used Space(GB) Used Space(%) FreeSpace(GB) Label    CollectionTime     
USE SQLDBATools;

select * from Info.PowerShellFunctionCalls

select * from [info].[Server];
select * from dbo.VolumeInfo;
select * from Staging.VolumeInfo;

select * from [info].[Server];
select * from [Staging].[ServerInfo];

SELECT * FROM dbo.Instance;
truncate table Staging.VolumeInfo;

/*	--	PowerShell Command to add new servers
$sqlQuery = @"
SELECT Name as InstanceName FROM dbo.[Instance] WHERE IsDecommissioned = 0;
"@;

$queryResult = Invoke-Sqlcmd2 -ServerInstance $InventoryInstance -Database $InventoryDatabase -Query $sqlQuery;

foreach ($i in $queryResult)
{
    #$i.InstanceName;
    Add-ServerInfo -ComputerName $i.InstanceName -EnvironmentType Prod -CallServerInfoTSQLProcedure No -ErrorAction SilentlyContinue;
}
*/

