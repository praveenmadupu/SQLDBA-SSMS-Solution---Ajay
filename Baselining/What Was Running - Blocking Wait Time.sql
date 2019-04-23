DECLARE @p_wait_info varchar(2000)
SET @p_wait_info = '(1x: 53956ms)LCK_M_S, (8x: 34449/34449/34451ms)CXPACKET:6, (1x: 661622ms)CXPACKET:2'
SET @p_wait_info = '(8x: 34449/34449/34451ms)CXPACKET:6, (1x: 53956ms)LCK_M_S, (1x: 661622ms)CXPACKET:2'

select	--lock_text
		[WaitTime(Seconds)] =
				CAST(SUBSTRING(lock_text,
					CHARINDEX(':',lock_text)+1,
					CHARINDEX('ms',lock_text)-(CHARINDEX(':',lock_text)+1)
				) AS BIGINT)/1000
from (
	SELECT	[lock_text] = CASE WHEN CHARINDEX(',',LEFT(@p_wait_info,  CHARINDEX(',',@p_wait_info,CHARINDEX('LCK_',@p_wait_info))-1   )) <> 0
								THEN REVERSE(LEFT(	REVERSE(LEFT(@p_wait_info,  CHARINDEX(',',@p_wait_info,CHARINDEX('LCK_',@p_wait_info))-1)),
												CHARINDEX(',',REVERSE(LEFT(@p_wait_info,  CHARINDEX(',',@p_wait_info,CHARINDEX('LCK_',@p_wait_info))-1)))-1
											))
								ELSE LEFT(@p_wait_info,  CHARINDEX(',',@p_wait_info,CHARINDEX('LCK_',@p_wait_info))-1   )
								END
) as wi
