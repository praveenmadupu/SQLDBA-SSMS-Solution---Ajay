
-- https://www.sqlshack.com/troubleshooting-sql-server-issues-sys-dm_os_performance_counters/
INSERT dbo.dm_os_performance_counters
SELECT /* -- all performance counters that do not require additional calculation */
		[collection_time] = GETDATE(),
		object_name, counter_name, instance_name, cntr_value ,cntr_type
--into dbo.dm_os_performance_counters
FROM sys.dm_os_performance_counters as pc
WHERE cntr_type in (65792 /* PERF_COUNTER_LARGE_RAWCOUNT */
					)
  and
  (	( [object_name] LIKE N'%Buffer Manager%' AND counter_name = N'Page life expectancy' )
  or
	( [object_name] LIKE N'%Memory Manager%' AND counter_name = N'Memory Grants Pending' )
  or
	( counter_name = 'Total Server Memory (KB)' )
  or
	( counter_name = 'Target Server Memory (KB)' )
  )
--
UNION ALL
--
SELECT /* counter that require Fraction & Base */
		GETDATE() as collection_time, fr.object_name, fr.counter_name, fr.instance_name, cntr_value = case when bs.cntr_value <> 0 then (100*(fr.cntr_value/bs.cntr_value)) else fr.cntr_value end, fr.cntr_type
FROM sys.dm_os_performance_counters as fr
CROSS APPLY
  (	SELECT * FROM sys.dm_os_performance_counters as bs 
	WHERE bs.cntr_type = 1073939712 /* PERF_LARGE_RAW_BASE  */ 
	 AND bs.object_name = fr.object_name AND bs.instance_name = fr.instance_name AND bs.counter_name LIKE (rtrim(fr.counter_name)+' Base%')
  ) as bs
WHERE fr.cntr_type = 537003264 /* PERF_LARGE_RAW_FRACTION */
  and
  ( ( fr.counter_name like 'Buffer cache hit ratio%' )
  );

-- ============================================================================================
-- ============================================================================================
-- https://www.sqlshack.com/troubleshooting-sql-server-issues-sys-dm_os_performance_counters/
IF OBJECT_ID('tempdb..#dm_os_performance_counters_PERF_AVERAGE_BULK_t1') IS NOT NULL
	DROP TABLE #dm_os_performance_counters_PERF_AVERAGE_BULK_t1;
SELECT GETDATE() as collection_time, * 
INTO #dm_os_performance_counters_PERF_AVERAGE_BULK_t1
FROM sys.dm_os_performance_counters as pc
WHERE cntr_type in (1073874176 /* PERF_AVERAGE_BULK */
					,1073939712 /* PERF_LARGE_RAW_BASE */
					) --  
  and
  ( ( counter_name like '%Average Wait Time%' and instance_name = 'database')
  );

-- Another Query
-- https://www.sqlshack.com/troubleshooting-sql-server-issues-sys-dm_os_performance_counters/
IF OBJECT_ID('tempdb..#dm_os_performance_counters_PERF_COUNTER_BULK_COUNT_t1') IS NOT NULL
	DROP TABLE #dm_os_performance_counters_PERF_COUNTER_BULK_COUNT_t1;
SELECT GETDATE() as collection_time, * 
INTO #dm_os_performance_counters_PERF_COUNTER_BULK_COUNT_t1
FROM sys.dm_os_performance_counters as pc
WHERE cntr_type = 272696576 /* PERF_COUNTER_BULK_COUNT */
  and
  ( (object_name like 'SQLServer:SQL Statistics%' and counter_name like '%Batch Requests/sec%' )
    or
	(object_name like 'SQLServer:Buffer Manager%' and counter_name like 'Page lookups/sec%')
	or
	(object_name like 'SQLServer:Buffer Manager%' and counter_name like 'Lazy writes/sec%')
	or
	(object_name like 'SQLServer:Buffer Manager%' and counter_name like 'Page reads/sec%')
	or
	(object_name like 'SQLServer:Buffer Manager%' and counter_name like 'Page writes/sec%')
	or
	(object_name like 'SQLServer:Buffer Manager%' and counter_name like 'Logins/sec%')
	or
	(object_name like 'SQLServer:Locks%' and counter_name like 'Number of Deadlocks/sec%' and instance_name like '_Total%')
	or
	(object_name like 'SQLServer:Access Methods%' and counter_name like 'Full Scans/sec%')
	or
	(object_name like 'SQLServer:Access Methods%' and counter_name like 'Forwarded Records/sec%')
	or
	(object_name like 'SQLServer:Access Methods%' and counter_name like 'Index Searches/sec%')
	or
	(object_name like 'SQLServer:Access Methods%' and counter_name like 'Page Splits/sec%')
  );

--
WAITFOR DELAY '00:00:04'
--

IF OBJECT_ID('tempdb..#dm_os_performance_counters_PERF_AVERAGE_BULK_t2') IS NOT NULL
	DROP TABLE #dm_os_performance_counters_PERF_AVERAGE_BULK_t2;
SELECT GETDATE() as collection_time, * 
INTO #dm_os_performance_counters_PERF_AVERAGE_BULK_t2
FROM sys.dm_os_performance_counters as pc
WHERE cntr_type in (1073874176 /* PERF_AVERAGE_BULK */
					,1073939712 /* PERF_LARGE_RAW_BASE */
					) --  
  and
  ( ( counter_name like '%Average Wait Time%' and instance_name = 'database')
  );
WITH Time_Samples AS (
	SELECT t1.collection_time as time1, t2.collection_time as time2,
			t1.object_name, t1.counter_name, t1.instance_name,
			t1.cntr_type as cntr_type_t1, t1.cntr_value as cntr_value_t1,
			t2.cntr_type as cntr_type_t2, t2.cntr_value as cntr_value_t2
	FROM #dm_os_performance_counters_PERF_AVERAGE_BULK_t1 as t1
	join #dm_os_performance_counters_PERF_AVERAGE_BULK_t2 as t2
	  on t1.collection_time < t2.collection_time and 
	     t2.object_name = t1.object_name and t2.counter_name = t1.counter_name and ISNULL(t2.instance_name,'') = ISNULL(t1.instance_name,'')
)
INSERT dbo.dm_os_performance_counters
SELECT [collection_time] = fr.time2,
		object_name = fr.object_name, 
		counter_name = fr.counter_name, 
		instance_name = fr.instance_name, 
		cntr_value = case when (bs.cntr_value_t2-bs.cntr_value_t1) <> 0 then (fr.cntr_value_t2-fr.cntr_value_t1)/(bs.cntr_value_t2-bs.cntr_value_t1) else (fr.cntr_value_t2-fr.cntr_value_t1) end
		,cntr_type = fr.cntr_type_t2
FROM Time_Samples as fr join Time_Samples as bs on fr.cntr_type_t2 = '1073874176' and bs.cntr_type_t2 = '1073939712';

-- Another Query
IF OBJECT_ID('tempdb..#dm_os_performance_counters_PERF_COUNTER_BULK_COUNT_t2') IS NOT NULL
	DROP TABLE #dm_os_performance_counters_PERF_COUNTER_BULK_COUNT_t2;
SELECT GETDATE() as collection_time, * 
INTO #dm_os_performance_counters_PERF_COUNTER_BULK_COUNT_t2
FROM sys.dm_os_performance_counters as pc
WHERE cntr_type = 272696576 /* PERF_COUNTER_BULK_COUNT */
  and
  ( (object_name like 'SQLServer:SQL Statistics%' and counter_name like '%Batch Requests/sec%' )
    or
	(object_name like 'SQLServer:Buffer Manager%' and counter_name like 'Page lookups/sec%')
	or
	(object_name like 'SQLServer:Buffer Manager%' and counter_name like 'Lazy writes/sec%')
	or
	(object_name like 'SQLServer:Buffer Manager%' and counter_name like 'Page reads/sec%')
	or
	(object_name like 'SQLServer:Buffer Manager%' and counter_name like 'Page writes/sec%')
	or
	(object_name like 'SQLServer:Buffer Manager%' and counter_name like 'Logins/sec%')
	or
	(object_name like 'SQLServer:Locks%' and counter_name like 'Number of Deadlocks/sec%' and instance_name like '_Total%')
	or
	(object_name like 'SQLServer:Access Methods%' and counter_name like 'Full Scans/sec%')
	or
	(object_name like 'SQLServer:Access Methods%' and counter_name like 'Forwarded Records/sec%')
	or
	(object_name like 'SQLServer:Access Methods%' and counter_name like 'Index Searches/sec%')
	or
	(object_name like 'SQLServer:Access Methods%' and counter_name like 'Page Splits/sec%')
  );
WITH Time_Samples AS (
	SELECT t1.collection_time as time1, t2.collection_time as time2,
			t1.object_name, t1.counter_name, t1.instance_name,
			t1.cntr_type as cntr_type_t1, t1.cntr_value as cntr_value_t1,
			t2.cntr_type as cntr_type_t2, t2.cntr_value as cntr_value_t2
	FROM #dm_os_performance_counters_PERF_COUNTER_BULK_COUNT_t1 as t1
	join #dm_os_performance_counters_PERF_COUNTER_BULK_COUNT_t2 as t2
	  on t1.collection_time < t2.collection_time and 
	     t2.object_name = t1.object_name and t2.counter_name = t1.counter_name and ISNULL(t2.instance_name,'') = ISNULL(t1.instance_name,'')
)
INSERT dbo.dm_os_performance_counters
SELECT [collection_time] = time2,
		object_name = object_name, 
		counter_name = counter_name, 
		instance_name = instance_name, 
		cntr_value = (cntr_value_t2-cntr_value_t1)/(DATEDIFF(SECOND,time1,time2))
		,cntr_type = cntr_type_t2
FROM Time_Samples;


select * from dbo.dm_os_performance_counters