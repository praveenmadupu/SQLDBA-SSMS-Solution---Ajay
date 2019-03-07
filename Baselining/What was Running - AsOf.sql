USE DBA
GO

-- select distinct CheckDate from dbo.BlitzFirst order by CheckDate DESC
DECLARE @p_CheckDate datetimeoffset
             ,@p_Collection_Time datetime;
SET @p_CheckDate = '2019-02-22 01:30:00.9468500 -05:00';
SET @p_Collection_Time = (SELECT MIN(collection_Time) AS collection_Time  FROM [DBA].[dbo].[WhoIsActive_ResultSets] WHERE collection_Time >= CAST(@p_CheckDate AS DATETIME))

SELECT * FROM DBA.dbo.BlitzFirst
       WHERE CheckDate = @p_CheckDate
       --ORDER BY CheckDate DESC

SELECT * FROM [DBA].[dbo].[WhoIsActive_ResultSets] AS r
       WHERE r.collection_Time = @p_Collection_Time

/*
	--	http://whoisactive.com/docs/16_morewaits/
	--	http://whoisactive.com/docs/28_access/
	--	http://whoisactive.com/docs/22_locks/

*/
