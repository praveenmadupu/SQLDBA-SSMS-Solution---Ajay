USE DBA
GO

DROP FUNCTION dbo.fn_GetNextCollectionTime
GO

CREATE FUNCTION dbo.fn_GetNextCollectionTime (@p_Collection_Time datetime = NULL)
RETURNS datetime AS 
BEGIN
	/*	Created By:			Ajay Dwivedi
		Version:			0.0
		Modification:		(May 13, 2019) - Creating for 1st time
	*/
	DECLARE @collection_time datetime;

	SELECT	@collection_time = MIN(r.collection_time)
	FROM	dbo.WhoIsActive_ResultSets as r
	WHERE	r.collection_time >= cast(@p_Collection_Time as datetime);
	
	RETURN (@collection_time);
END
GO

--	SELECT DBA.dbo.fn_GetNextCollectionTime('May 12 2019 11:30AM')