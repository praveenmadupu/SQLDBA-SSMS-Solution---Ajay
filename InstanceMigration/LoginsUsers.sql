--	01) Script Out User Permissions
Import-Module dbatools;
Export-DbaUser -SqlInstance TUL1CIPXDB18 -Database Logging -User RV_BP_APPUSR

--	01) Script Out User Permissions
Import-Module dbatools;
Export-DbaUser -SqlInstance TUL1CIPXDB18 -Database Logging -User RV_BP_APPUSR

--	02) Script Out User Permissions
$scriptPath = Get-DatabasePermissions -SqlInstance tul1advDdb1old2;
$server = 'servername'
$files = Get-ChildItem $scriptPath;

foreach($file in $files) {
    $dbName = $file.BaseName;
    $fileName = $file.FullName;
    Write-Host "Writing to database '$dbName'" -ForegroundColor Black -BackgroundColor White;
    Invoke-DbaQuery -SqlInstance $server -Database $dbName -File $fileName;
}