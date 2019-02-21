-- select distinct CheckDate from dbo.BlitzFirst order by CheckDate DESC

DECLARE @p_CheckDate datetimeoffset
		,@p_Collection_Time datetime;
SET @p_CheckDate = '2019-02-20 23:30:00.9048392 -05:00';
SET @p_Collection_Time = (SELECT MIN(collection_Time) AS collection_Time  FROM [dbo].[WhoIsActive_ResultSets] WHERE collection_Time >= CAST(@p_CheckDate AS DATETIME))

SELECT * FROM dbo.BlitzFirst
	WHERE CheckDate = @p_CheckDate
	--ORDER BY CheckDate DESC

SELECT * FROM [dbo].[WhoIsActive_ResultSets] AS r
	WHERE r.collection_Time = @p_Collection_Time