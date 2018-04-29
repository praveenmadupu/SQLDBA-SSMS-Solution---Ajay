-- 1) Open SQL Server Management Studio and Connect to Server from PowerShell
ssms.exe <scriptfile> -S $serverName -E

-- 2) Add datetime in FileName
Write-Host "fileName_$(Get-Date -Format ddMMMyyyyTHHmm).sql";

-- 3) Unattended script execution
	https://dba.stackexchange.com/questions/197360/how-to-execute-sql-server-query-in-ssms-using-powershell
sqlcmd -Q 'E:\PowerShell_PROD\Screenshot\ServerDetails.sql' -E -S localhost

-- 4) Get file name
"F:\Mssqldata\Data\UserTracking_data.mdf" -match "^(?'PathPhysicalName'.*[\\\/])(?'BasePhysicalName'.+)"
$Matches['BasePhysicalName'] => UserTracking_data.mdf
$Matches['PathPhysicalName'] => F:\Mssqldata\Data\

-- 5) Is Null or Empty
[string]::IsNullOrEmpty($StopAt_Time) -eq $false

-- 6) Create a PS Drive for Demo Purposes
New-PSDrive -Persist -Name "P" -PSProvider "FileSystem" -Root "\\Tul1cipedb3\g$"

-- 7) Add color to Foreground and Background text
write-host "[OK]" -ForegroundColor Cyan

-- 7) File exists or not
[System.IO.File]::Exists($n)

-- 8) Get all files on drive by Size
Get-ChildItem -Path 'F:\' -Recurse -Force -ErrorAction SilentlyContinue | 
    Select-Object Name, @{l='ParentPath';e={$_.DirectoryName}}, @{l='SizeBytes';e={$_.Length}}, @{l='Owner';e={((Get-ACL $_.FullName).Owner)}}, CreationTime, LastAccessTime, LastWriteTime, @{l='IsFolder';e={if($_.PSIsContainer) {1} else {0}}}, @{l='SizeMB';e={$_.Length/1mb}}, @{l='SizeGB';e={$_.Length/1gb}} |
    Sort-Object -Property SizeBytes -Descending | Out-GridView

-- 9) Check if -Verbose switch is used
$PSCmdlet.MyInvocation.BoundParameters["Verbose"].IsPresent

-- 10) Check if Module is installed
if (Get-Module -ListAvailable -Name SqlServer) {
    Write-Host "Module exists"
} else {
    Write-Host "Module does not exist"
}

-- 11) Find path of SQLDBATools Module
(Get-Module -ListAvailable SQLDBATools).Path

-- 12) Log entry into ErrorLogs table
$MessageText = "Get-WmiObject : Access is denied. Failed in execution of Get-ServerInfo";
Write-Host $MessageText -ForegroundColor Red;
Add-CollectionError -ComputerName $ComputerName -Cmdlet 'Add-ServerInfo' -CommandText "Add-ServerInfo -ComputerName '$ComputerName'" -ErrorText $MessageText -Remark $null;
return;

-- 13) Querying using SQLProvider
$computerName = 'TUL1CIPEDB2'

Get-ChildItem SQLSERVER:\SQL\$computerName\DEFAULT
$sqlInstance = Get-Item SQLSERVER:\SQL\$computerName\DEFAULT
$sqlInstance | gm -MemberType Property

$sqlInstance | select ComputerNamePhysicalNetBIOS, Name, Edition, ErrorLogPath, IsCaseSensitive, IsClustered,
                            IsHadrEnabled, IsFullTextInstalled, LoginMode, NetName, PhysicalMemory,
                            Processors, ServiceInstanceId, ServiceName, ServiceStartMode, 
                            VersionString, Version, DatabaseEngineEdition

$sqlInstance.Information | Select-Object * | fl
$sqlInstance.Properties | Select-Object Name, Value | ft -AutoSize
$sqlInstance.Configuration

-- 14) Querying SqlServer using PowerShell
$computerName = 'TUL1CIPEDB2'

<# SMO #> 
$server = New-Object Microsoft.SqlServer.Management.Smo.Server("$computerName")
$server | Select-Object ComputerNamePhysicalNetBIOS, Name, Edition, ErrorLogPath, IsCaseSensitive, IsClustered,
                            IsHadrEnabled, IsFullTextInstalled, LoginMode, NetName, PhysicalMemory,
                            Processors, ServiceInstanceId, ServiceName, ServiceStartMode, 
                            VersionString, Version, DatabaseEngineEdition

$server.Configuration.MaxServerMemory
$server.Configuration.CostThresholdForParallelism
$server.Configuration.MinServerMemory
$server.Configuration.MaxDegreeOfParallelism
$server.Configuration.Properties | ft -AutoSize -Wrap
                            
<# SQL Provider #> 
Get-ChildItem SQLSERVER:\SQL\$computerName\DEFAULT
$sqlInstance = Get-Item SQLSERVER:\SQL\$computerName\DEFAULT
$sqlInstance | gm -MemberType Property

$sqlInstance | select ComputerNamePhysicalNetBIOS, Name, Edition, ErrorLogPath, IsCaseSensitive, IsClustered,
                            IsHadrEnabled, IsFullTextInstalled, LoginMode, NetName, PhysicalMemory,
                            Processors, ServiceInstanceId, ServiceName, ServiceStartMode, 
                            VersionString, Version, DatabaseEngineEdition

$sqlInstance.Information | Select-Object * | fl
$sqlInstance.Properties | Select-Object Name, Value | ft -AutoSize
$sqlInstance.Configuration 