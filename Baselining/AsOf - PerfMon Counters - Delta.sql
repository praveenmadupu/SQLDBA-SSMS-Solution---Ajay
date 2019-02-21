-- select distinct CheckDate from dbo.BlitzFirst order by CheckDate DESC
--	https://blogs.msdn.microsoft.com/psssql/2013/09/23/interpreting-the-counter-values-from-sys-dm_os_performance_counters/

DECLARE @p_CheckDate datetimeoffset
SET @p_CheckDate = '2019-02-20 23:30:00.9048392 -05:00';

--select * from [dbo].BlitzFirst_PerfmonStats_Deltas2 where CheckDate = @p_CheckDate;
select * from [dbo].BlitzFirst_PerfmonStats_Actuals2 where CheckDate = @p_CheckDate

