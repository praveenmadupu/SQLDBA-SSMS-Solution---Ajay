EXEC sp_WhoIsActive @get_plans=1, @get_full_inner_text=1, @get_transaction_info=1, @get_task_info=2, @get_locks=1, @get_avg_time=1, @get_additional_info=1,@find_block_leaders=1, @get_outer_command =1;

EXEC sp_healthcheck
/*
$instance = 'ANN1VESPDB01';
$excelPath = "C:\Temp\$instance.xlsx";
$sqlQuery = @" 
exec sp_whoIsActive @get_plans=1, @get_full_inner_text=1, 
                    @get_transaction_info=1, @get_task_info=2, 
                    @get_locks=1, @get_avg_time=1, @get_additional_info=1,
                    @find_block_leaders=1
"@;

Invoke-Sqlcmd -ServerInstance $instance -Query $sqlQuery | Export-Excel $excelPath -Show;
*/