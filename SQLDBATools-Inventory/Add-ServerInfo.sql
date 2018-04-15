USE SQLDBATools;
select * from Info.PowerShellFunctionCalls

-- Instance_ID, SNo, Name, IsVM, NodeType, GeneralDescription, IPAddress, EnvironmentType, BusinessUnit, Product, [SupportedApplication ], Domain, Version, Release, ProductKey, OSVersion, BusinessOwner, PrimaryContact, SecondaryContact, IsDecommissioned, IsPowerShellLinked, IsSQLClusterNode, IsAlwaysOnNode
SELECT * FROM dbo.Instance;

/* Info.Application => ApplicationID, ApplicationName, Owner, DeleteOwner, OwnershipDelegationEndDate, PrimaryContact, SecondaryContact, SecondaryContact2, BusinessUnit, Product
*/
--ServerID, ServerName, EnvironmentType, DNSHostName, IPAddress, Domain, OperatingSystem, SPVersion, Model, RAM, CPU, CollectionTime
	--[info].[Server] (Add columns) => ClusterNetworkName,GeneralDescription, ApplicationID, Domain, IsDecommissioned, IsPowerShellLinked, IsVM, IsFailoverClusterNode
select * from [info].[Server];

select * from [info].[Instance]
/* [info].[Instance] => InstanceID, SQLInstance, InstanceName, SQLNetworkName, GeneralDescription, IPAddress, Version, Release, ProductKey, IsDecommissioned, IsPowerShellLinked, IsSQLClusterNode, IsAlwaysOnNode
*/



/*	--	PowerShell Command to add new servers
$sqlQuery = @"
SELECT Name as InstanceName FROM dbo.[Instance] WHERE IsDecommissioned = 0  and Domain = 'Corporate.local';
"@;

$queryResult = Invoke-Sqlcmd2 -ServerInstance $InventoryInstance -Database $InventoryDatabase -Query $sqlQuery;

Clear-Host;

foreach ($i in $queryResult)
{
    try {
        #$i.InstanceName;
        Invoke-Sqlcmd -ServerInstance $i.InstanceName -Database master -InputFile 'C:\temp\SQLFiles\1. who_is_active_v11_30(Modified).sql' -ErrorAction SilentlyContinue;
        Invoke-Sqlcmd -ServerInstance $i.InstanceName -Database master -InputFile 'C:\temp\SQLFiles\2. sp_HealthCheck.sql' -ErrorAction SilentlyContinue;
        Invoke-Sqlcmd -ServerInstance $i.InstanceName -Database master -InputFile 'C:\temp\SQLFiles\3. Certificate Based Authentication.sql' -ErrorAction SilentlyContinue ;
        @" 
Scripts executed on $($i.InstanceName)
"@ | out-host;
    }
    catch {
        $ErrorMessage = $_.Exception.Message;
        $FailedItem = $_.Exception.ItemName;
    @"
Error occurred while running Queries on $($i.InstanceName)
$ErrorMessage
"@ | Out-host;
    }
}
*/

