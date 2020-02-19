use DBA

--	Find past replication latency more than 30 Minutes
select * from DBA..[Repl_TracerToken_History]
where overall_latency >= 230
and publication in ('Mosaic_2014')
order by publisher_commit desc, publication desc;

--	Replication History for Time Range
select * from DBA..[Repl_TracerToken_History] h
where h.publisher_commit >= '2020-02-07 00:00:00.000'
and h.publisher_commit <= '2020-02-07 16:00:00.000'
order by publisher_commit asc, publication asc;

--	Get current Latency
select * from DBA..vw_Repl_Latency;

select * from DBA..[Repl_TracerToken_Header] where is_processed = 0
select top 1000 * from distribution.dbo.MSdistribution_history h

/*
The replication agent has not logged a progress message in 10 minutes. This might indicate an unresponsive agent or high system activity. Verify that records are being replicated to the destination and that connections to the Subscriber, Publisher, and Distributor are still active.
*/

https://stackoverflow.com/a/45965260/4449743


https://repltalk.com/2010/03/11/divide-and-conquer-transactional-replication-using-tracer-tokens/


https://docs.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-repltrans-transact-sql?view=sql-server-ver15
--	All the transactions in the publication database transaction log that are marked for replication but have not been marked as distributed
use Mosaic
exec sp_repltrans  

--	Commands for transactions marked for replication
use Mosaic
exec sp_replshowcmds

--	How to enable replication agents for logging to output files in SQL Server
	--	https://support.microsoft.com/en-us/help/312292/how-to-enable-replication-agents-for-logging-to-output-files-in-sql-se
-Output C:\Temp\Mosaic_2014_OUTPUT.txt -Outputverboselevel 2

--	0 = Error messages only
--	1 = All Progress
--	2 = Error + Progress