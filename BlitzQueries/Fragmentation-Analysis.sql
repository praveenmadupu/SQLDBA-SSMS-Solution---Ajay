# Script to Get Fragmentation Stats for All Dbs on Multiple Servers (in Parallel Jobs)
$dbServers = @('TUL1CIPBIAPP1','TUL1CIPBIAPP2','TUL1CIPBIAPP3','TUL1CIPXDB18','TUL1SUB2008');

$dbQuery = @"
select d.name 
from sys.databases as d 
where not(d.source_database_id IS NOT NULL or d.state_desc = 'OFFLINE' or d.database_id <= 4)
"@;

$IndexQuery = @"
select	@@serverName as ServerName,
		db_name(ips.database_id) as DataBaseName,
		object_name(ips.object_id) as ObjectName,
		sch.name as SchemaName,
		ind.name as IndexName,
		ips.index_type_desc,
		avg_fragmentation_in_percent as avg_fragmentation,
		avg_page_space_used_in_percent,
		page_count,
		ps.row_count,
		STATS_DATE(ind.object_id, ind.index_id) AS StatsUpdated
from sys.dm_db_index_physical_stats(DB_ID(),NULL,NULL,NULL,'LIMITED') as ips
  inner join sys.tables as tbl
    on ips.object_id = tbl.object_id
  inner join sys.schemas as sch
    on tbl.schema_id = sch.schema_id  
  inner join sys.indexes as ind
    on ips.index_id = ind.index_id and
       ips.object_id = ind.object_id
  inner join sys.dm_db_partition_stats as ps
    on ps.object_id = ips.object_id and
       ps.index_id = ips.index_id
where page_count >= 1000;
"@;

foreach($srv in $dbServers) {
    Write-Host $srv -ForegroundColor DarkYellow;

    # find databases from server
    $alldbs = Invoke-DbaQuery -SqlInstance $srv -Query $dbQuery | Select-Object -ExpandProperty name;
    
    foreach($db in $alldbs) {
        Write-Host "`tJob started for [$srv].[$db]..." -ForegroundColor Green;
        $ScriptBlock = { 
            param($srv, $db, $IndexQuery)
            Invoke-DbaQuery -SqlInstance $srv -Database $db -Query $IndexQuery -QueryTimeOut 3600
        }
        Start-Job -Name "IndexStats-$srv-$db" -ScriptBlock $ScriptBlock -ArgumentList $srv, $db, $IndexQuery;
    }
} # Server loop
Write-Host "Jobs created/started for each Server/Database pair." -ForegroundColor Green;


$IndexAnalysisResult = @();
do {
    # Find completed jobs, Retrieve Data, and Remove them
    $Jobs_Completed = Get-Job -Name IndexStats* | Where-Object {$_.State -eq 'Completed'};
    $IndexAnalysisResult += $Jobs_Completed | Receive-Job;
    $Jobs_Completed | Remove-Job;

    # Wait for 10 seconds
    Start-Sleep -Seconds 10;
    $Jobs_Yet2Process = Get-Job -Name IndexStats* | 
                        Where-Object {$_.State -in ('NotStarted','Running','Suspending','Stopping')};
}
while($Jobs_Yet2Process -ne $null); # keep looping if jobs are still in progress

# Save to Excel
$IndexAnalysisResult | Export-Excel -Path C:\Temp\IndexAnalysisResult.xlsx -WorksheetName 'IndexAnalysisResult';

# Find Jobs with Failures
$Jobs_Issue = Get-Job -Name IndexStats* | 
              Where-Object {$_.State -notin ('Completed','NotStarted','Running','Suspending','Stopping')};
if($Jobs_Issue -ne $null) {
    Write-Host @"
Some jobs failed. Execute below script
`$Jobs_Yet2Process
"@
}

<#
$excel = Import-Excel 'C:\temp\IndexAnalysisResult.xlsx'
$i = 1
$excel | select -Property ServerName, DatabaseName -Unique | ForEach-Object {
            $_ | Add-Member -NotePropertyName ID -NotePropertyValue $i -PassThru;
            $i += 1;
        } | ogv
#>