Import-Module dbatools;
$collector_root_directory = 'D:\MSSQL15.MSSQLSERVER\SQLWATCH';
$data_collector_template_path = “$collector_root_directory\DBA_PerfMon_Collector-Template.xml”;
$log_file_path = "$collector_root_directory\$($env:COMPUTERNAME)__"
$data_collector_set_name = 'DBA_PerfMon_Collector';
$dsn = 'DBAInventory';
$DBAInventory = 'MSI';
$last_log_file_imported = Invoke-DbaQuery -SqlInstance $DBAInventory -Query 'select top 1 DisplayString from DBA.dbo.DisplayToID order by LogStopTime desc' | Select-Object -ExpandProperty DisplayString;

$current_collector_state = logman -n $data_collector_set_name;
$location_line = $current_collector_state | Where-Object {$_ -like 'Output Location:*'}
$status_line = $current_collector_state | Where-Object {$_ -like 'Status:*'}
$current_log_file = $location_line.Replace("Output Location:",'').trim();
$current_log_file_status = $status_line.Replace("Status:",'').trim();

$perfmonfiles = Get-ChildItem -Path $collector_root_directory  -Filter *.blg |
                    Where-Object {$_.FullName -gt $last_log_file_imported -or $last_log_file_imported -eq $null}

if($current_log_file_status -eq 'Running') {
    logman stop -name “$data_collector_set_name”
    logman start -name “$data_collector_set_name”
}


foreach($perfmonfile in $perfmonfiles)
{
    $sourceBlg = $perfmonfile.FullName;
    $sqlDSNconection = "SQL:$dsn!$sourceBlg"

    $AllArgs = @($sourceBlg, '-f', 'SQL', '-o', $sqlDSNconection)
    $relog_result = relog $AllArgs
}

#Add-OdbcDsn -Name "DBAInventory" -DriverName "SQL Server" -DsnType "System" -SetPropertyValue @("Server=MSI", "Trusted_Connection=Yes", "Database=DBA")
