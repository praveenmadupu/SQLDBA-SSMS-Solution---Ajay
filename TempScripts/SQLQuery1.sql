select
	  time = pc.snapshot_time
	, 'CPU usage %' = sum(case 
	      when 1=1 and mpc.counter_name = 'Processor Time %' then pc.[cntr_value_calculated] 
	      --when serverproperty('EngineEdition') = 3 and mpc.counter_name = 'CPU usage %' then pc.[cntr_value_calculated] 
	      --when serverproperty('EngineEdition') <> 3 and mpc.counter_name = 'Processor Time %' then pc.[cntr_value_calculated]
	      else null end)
	, 'Batch Requests/sec' = avg(case when mpc.counter_name = 'Batch Requests/sec' then pc.[cntr_value_calculated] else null end)
	, 'Logins/sec' = avg(case when mpc.counter_name = 'Logins/sec' then pc.[cntr_value_calculated] else null end)
	, 'Transactions/sec' = sum(case when mpc.counter_name = 'Transactions/sec' and pc.instance_name not in ('_Total','mssqlsystemresource') then pc.[cntr_value_calculated] else null end)
	, 'User Connections' = avg(case when mpc.counter_name = 'User Connections' then pc.[cntr_value_calculated] else null end)
	, 'SQL Compilations/sec' = avg(case when mpc.counter_name = 'SQL Compilations/sec' then pc.[cntr_value_calculated] else null end)
	, 'Availability Group Bytes' = avg(case when mpc.counter_name in ('Bytes Sent to Replica/sec','Bytes Received from Replica/sec') then pc.[cntr_value_calculated] else null end)
  --
  ,'Connection Memory (KB)' = avg(case when mpc.counter_name = 'Connection Memory (KB)' then pc.[cntr_value_calculated] else null end)
  ,'Optimizer Memory (KB)' = avg(case when mpc.counter_name = 'Optimizer Memory (KB)' then pc.[cntr_value_calculated] else null end)
  ,'SQL Cache Memory (KB)' = avg(case when mpc.counter_name = 'SQL Cache Memory (KB)' then pc.[cntr_value_calculated] else null end)
  ,'Total Server Memory (KB)' = avg(case when mpc.counter_name = 'Total Server Memory (KB)' then pc.[cntr_value_calculated] else null end)
  ,'Stolen Server Memory (KB)' = avg(case when mpc.counter_name = 'Stolen Server Memory (KB)' then pc.[cntr_value_calculated] else null end)
  ,'Target Server Memory (KB)' = avg(case when mpc.counter_name = 'Target Server Memory (KB)' then pc.[cntr_value_calculated] else null end)
from [dbo].[sqlwatch_logger_perf_os_performance_counters] pc
inner join [dbo].[sqlwatch_meta_performance_counter] mpc
	on pc.sql_instance = mpc.sql_instance
	and pc.performance_counter_id = mpc.performance_counter_id
where $__timeFilter(pc.snapshot_time)
and pc.snapshot_time >= dateadd(minute,-15,getutcdate())
and pc.[sql_instance] = '$sql_instance'
and mpc.counter_name in ('CPU usage %'
  ,'Batch Requests/sec'
  ,'Logins/sec'
  ,'Transactions/sec'
  ,'User Connections'
  ,'SQL Compilations/sec'
  ,'Bytes Sent to Replica/sec'
  ,'Bytes Received from Replica/sec'
  ,'Processor Time %'
  ,'% Processor Time'
  ,'Connection Memory (KB)'
  ,'Optimizer Memory (KB)'
  ,'SQL Cache Memory (KB)'
  ,'Total Server Memory (KB)'
  ,'Stolen Server Memory (KB)'
  , 'Target Server Memory (KB)'
  )
group by pc.snapshot_time
order by time asc