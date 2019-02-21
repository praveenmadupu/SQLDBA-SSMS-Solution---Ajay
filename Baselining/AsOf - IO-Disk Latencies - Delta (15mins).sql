-- select distinct CheckDate from dbo.BlitzFirst order by CheckDate DESC

DECLARE @p_CheckDate datetimeoffset
SET @p_CheckDate = '2019-02-20 23:30:00.9048392 -05:00';

--	How to examine IO subsystem latencies from within SQL Server (Disk Latency)
	--	https://www.sqlskills.com/blogs/paul/how-to-examine-io-subsystem-latencies-from-within-sql-server/
	--	https://sqlperformance.com/2015/03/io-subsystem/monitoring-read-write-latency
	--	https://www.brentozar.com/blitz/slow-storage-reads-writes/
SELECT * FROM [dbo].[BlitzFirst_FileStats_Deltas2] WHERE CheckDate = @p_CheckDate
GO