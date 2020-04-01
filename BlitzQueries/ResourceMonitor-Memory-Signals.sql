--	https://www.sqlskills.com/blogs/jonathan/identifying-external-memory-pressure-with-dm_os_ring_buffers-and-ring_buffer_resource_monitor/
exec sp_HealthCheck 2

select cast(sm.total_physical_memory_kb * 1.0 / 1024 / 1024 as numeric(20,0)) as total_physical_memory_gb, 
		cast(sm.available_physical_memory_kb * 1.0 / 1024 / 1024 as numeric(20,2)) as available_physical_memory_gb, 
		cast((sm.total_page_file_kb - sm.available_page_file_kb) * 1.0 / 1024 / 1024 as numeric(20,0)) as used_page_file_gb,
		cast(sm.system_cache_kb * 1.0 / 1024 /1024 as numeric(20,2)) as system_cache_gb, 
		cast((sm.available_physical_memory_kb - sm.system_cache_kb) * 1.0 / 1024 as numeric(20,2)) as free_memory_mb,
		sm.system_memory_state_desc,
		cast(((sm.total_physical_memory_kb-sm.available_physical_memory_kb) * 100.0) / sm.total_physical_memory_kb as numeric(20,2)) as memory_usage_percentage
from sys.dm_os_sys_memory as sm

--	System Memory Usage
SELECT	top 3 EventTime,
		record.value('(/Record/ResourceMonitor/Notification)[1]', 'varchar(max)') as [Type],
		record.value('(//Record/MemoryRecord/MemoryUtilization)[1]', 'bigint') AS Memory_utilization_Percentage,
		cast(record.value('(//Record/MemoryRecord/TotalPhysicalMemory)[1]', 'bigint') * 1.0 / 1024/1024 as decimal(20,2)) as [TotalPhysicalMemory_GB],
		cast(record.value('(/Record/MemoryRecord/AvailablePhysicalMemory)[1]', 'bigint') * 1.0 / 1024 as decimal(20,2)) AS [Avail Phys Mem, Mb],
		record.value('(/Record/ResourceMonitor/IndicatorsProcess)[1]', 'int') as [IndicatorsProcess],
		record.value('(/Record/ResourceMonitor/IndicatorsSystem)[1]', 'int') as [IndicatorsSystem],
		cast(record.value('(/Record/MemoryRecord/AvailableVirtualAddressSpace)[1]', 'bigint') * 1.0 / 1024/1024 as decimal(20,2)) AS [Avail VAS, Gb]
FROM (	SELECT	DATEADD (ss, (-1 * ((cpu_ticks / CONVERT (float, ( cpu_ticks / ms_ticks ))) - [timestamp])/1000), GETDATE()) AS EventTime,
				CONVERT (xml, record) AS record
		FROM sys.dm_os_ring_buffers
		CROSS JOIN sys.dm_os_sys_info
		WHERE ring_buffer_type = 'RING_BUFFER_RESOURCE_MONITOR'
	 ) AS tab
ORDER BY EventTime DESC;

SELECT	top 3 record_id, EventTime,  
		CASE WHEN system_cpu_utilization_post_sp2 IS NOT NULL THEN system_cpu_utilization_post_sp2 ELSE system_cpu_utilization_pre_sp2 END AS system_cpu_utilization,  
		CASE WHEN sql_cpu_utilization_post_sp2 IS NOT NULL THEN sql_cpu_utilization_post_sp2 ELSE sql_cpu_utilization_pre_sp2 END AS sql_cpu_utilization 
FROM  (	SELECT	record.value('(Record/@id)[1]', 'int') AS record_id,
				DATEADD (ms, -1 * (ts_now - [timestamp]), GETDATE()) AS EventTime,
				100-record.value('(Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') AS system_cpu_utilization_post_sp2, 
				record.value('(Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') AS sql_cpu_utilization_post_sp2,
				100-record.value('(Record/SchedluerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') AS system_cpu_utilization_pre_sp2,
				record.value('(Record/SchedluerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') AS sql_cpu_utilization_pre_sp2
		FROM (	SELECT	timestamp, CONVERT (xml, record) AS record, cpu_ticks / (cpu_ticks/ms_ticks) as ts_now
				FROM sys.dm_os_ring_buffers cross apply sys.dm_os_sys_info
				WHERE ring_buffer_type = 'RING_BUFFER_SCHEDULER_MONITOR'
				AND record LIKE '%<SystemHealth>%'
			 ) AS t 
	  ) AS t
ORDER BY EventTime desc