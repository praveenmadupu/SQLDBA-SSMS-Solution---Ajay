$dbServer = 'somedbserver'
$tsqlQuery = @"
select db_name(mf.database_id) as dbName, mf.name, mf.type_desc, mf.physical_name from sys.master_files mf where mf.physical_name like 'H:\%';
"@;

$rs = Invoke-DbaQuery -SqlInstance $dbServer -Query $tsqlQuery
$files = Invoke-Command -ComputerName $dbServer -ScriptBlock {Get-ChildItem -Path 'H:\MSSQLData\Data' -Recurse}

$dbFiles = $rs | Select-Object -ExpandProperty physical_name;
$diskFiles = $files | Where-Object {$_.PSIsContainer -eq $false};

foreach($fl in $diskFiles)
{
    $diskFile = $fl.FullName;
    if($diskFile -in $dbFiles) {
        Write-Host "$diskFile is +nt" -ForegroundColor Green;
    }
    else {
        Write-Host "$diskFile is ABSENT" -ForegroundColor Red;
    }
}