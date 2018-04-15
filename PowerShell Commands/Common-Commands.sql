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
