USE DBA;

SELECT DENSE_RANK()OVER(ORDER BY collection_time ASC) AS CollectionBatchNO, *
  FROM [DBA].[dbo].[WhoIsActive_ResultSets] as r
  WHERE r.blocking_session_id IS NOT NULL OR r.blocked_session_count > 0;