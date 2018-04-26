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