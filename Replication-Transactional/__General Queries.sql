use DBA

--	Find past replication latency more than 30 Minutes
select * from DBA..[Repl_TracerToken_History]
where overall_latency >= 30
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
