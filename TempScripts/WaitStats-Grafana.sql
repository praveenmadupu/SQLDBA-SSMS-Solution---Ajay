--set nocount on;
DECLARE @server_name varchar(256);
DECLARE @start_time datetime2;
DECLARE @end_time datetime2;
set @server_name = 'MSI';
--set @start_time = master.dbo.utc2local($__timeFrom());
--set @end_time = master.dbo.utc2local($__timeTo());
set @start_time = DATEADD(MINUTE,-60,GETDATE());
set @end_time = GETDATE();

DECLARE @verbose bit = 1;

if OBJECT_ID('tempdb..#wait_stats_range') is not null
	drop table tempdb..#wait_stats_range;

select IDENTITY(int,1,1) as id, point_in_time
into #wait_stats_range
from (
	SELECT si.wait_stats_cleared_time as point_in_time
	FROM [DBA].dbo.dm_os_sys_info AS si
	WHERE 1 = 1
	--and pc.server_name = @server_name
	and collection_time BETWEEN @start_time AND @end_time
	--
	union
	--
	select point_in_time
	from (values (@start_time),(@end_time)) Times (point_in_time)
) as ranges
order by point_in_time asc;

if @verbose = 1
begin
	select * from #wait_stats_range;
	select count(distinct CollectionTime) as WaitStats_Sample_Counts from [DBA].[dbo].[WaitStats];
end

IF OBJECT_ID('tempdb..#Wait_Stats_Delta') IS NOT NULL
	DROP TABLE #Wait_Stats_Delta;
CREATE TABLE #Wait_Stats_Delta
(
	--[id_T1] [int] NULL,
	--[id_T2] [int] NULL,
	--[point_in_time_T1] [datetime2](7) NULL,
	--[point_in_time_T2] [datetime2](7) NULL,
	--[CollectionTime_T1] [datetime2](7) NULL,
	--[CollectionTime_T2] [datetime2](7) NULL,
	[CollectionTime] [datetime2](7) NOT NULL,
	[CollectionTime_Duration_Seconds] [int] NOT NULL,
	[WaitType] [nvarchar](120) NOT NULL,
	[Wait_S] [decimal](15, 2) NULL,
	[Resource_S] [decimal](15, 2) NULL,
	[Signal_S] [decimal](15, 2) NULL,
	[WaitCount] [bigint] NULL,
	[Percentage] [decimal](10,2) NULL,
	[AvgWait_S] [decimal](35, 22) NULL,
	[AvgRes_S] [decimal](35, 22) NULL,
	[AvgSig_S] [decimal](35, 22) NULL
)

declare @l_id int
		,@l_point_in_time datetime2
		,@l_counter int = 1
		,@l_counter_max int;

select @l_counter_max = max(id) from #wait_stats_range;

--if @verbose = 1
--	select [@l_counter] = @l_counter, [@l_counter_max] = @l_counter_max;

while @l_counter < @l_counter_max -- execute for N-1 times
begin
	select @l_point_in_time = point_in_time from #wait_stats_range as tr where tr.id = @l_counter;

	if @verbose = 1
		select [@l_counter] = @l_counter, [@l_counter_max] = @l_counter_max,  [@l_point_in_time] = @l_point_in_time;

	;WITH T1 AS (
		select id = tr.id, point_in_time = tr.point_in_time, 
				ws.CollectionTime, 
				ws.WaitType, 
				ws.Wait_S, ws.Resource_S, ws.Signal_S, ws.WaitCount, ws.Percentage, ws.AvgWait_S, ws.AvgRes_S, ws.AvgSig_S
		from [DBA].[dbo].[WaitStats] AS ws with (nolock) full join #wait_stats_range as tr on tr.id = @l_counter
		where ws.CollectionTime = (	case when @l_counter = 1
										 then (select MIN(wsi.CollectionTime) as CollectionTime from [DBA].[dbo].[WaitStats] as wsi with (nolock) where wsi.CollectionTime >= tr.point_in_time)
										 when @l_counter = @l_counter_max - 1
										 then ISNULL( (select MIN(wsi.CollectionTime) as CollectionTime from [DBA].[dbo].[WaitStats] as wsi with (nolock) where wsi.CollectionTime >= tr.point_in_time),
												(select MAX(wsi.CollectionTime) as CollectionTime from [DBA].[dbo].[WaitStats] as wsi with (nolock) where wsi.CollectionTime <= tr.point_in_time)
											  )
										else (select MIN(wsi.CollectionTime) as CollectionTime from [DBA].[dbo].[WaitStats] as wsi with (nolock) where wsi.CollectionTime > tr.point_in_time)
										end
								  )
	)
	,T2 AS (
		select id = tr.id, point_in_time = tr.point_in_time, 
				ws.CollectionTime, 
				ws.WaitType, 
				ws.Wait_S, ws.Resource_S, ws.Signal_S, ws.WaitCount, ws.Percentage, ws.AvgWait_S, ws.AvgRes_S, ws.AvgSig_S
		from [DBA].[dbo].[WaitStats] AS ws with (nolock) full join #wait_stats_range as tr on tr.id = @l_counter+1
		where ws.CollectionTime = (	case when @l_counter = 1 AND (@l_counter <> (@l_counter_max - 1))
										 then (select MAX(wsi.CollectionTime) as CollectionTime from [DBA].[dbo].[WaitStats] as wsi with (nolock) where wsi.CollectionTime < tr.point_in_time)
										 when @l_counter = 1 AND (@l_counter = (@l_counter_max - 1))
										 then (select MAX(wsi.CollectionTime) as CollectionTime from [DBA].[dbo].[WaitStats] as wsi with (nolock) where wsi.CollectionTime <= tr.point_in_time)
										 when @l_counter = @l_counter_max - 1
										 then (select MAX(wsi.CollectionTime) as CollectionTime from [DBA].[dbo].[WaitStats] as wsi with (nolock) where wsi.CollectionTime <= tr.point_in_time)
										else (select MAX(wsi.CollectionTime) as CollectionTime from [DBA].[dbo].[WaitStats] as wsi with (nolock) where wsi.CollectionTime < tr.point_in_time)
										end
								  )
	)
	,T_Waits_Delta AS (
		SELECT CollectionTime, CollectionTime_Duration_Seconds, WaitType, Wait_S, Resource_S, Signal_S, WaitCount,
				[Percentage],
				AvgWait_S, AvgRes_S, AvgSig_S
		--INTO tempdb..Wait_Stats_Delta
		FROM (
				SELECT --id_T1 = T1.id, id_T2 = T2.id,
						--point_in_time_T1 = T1.point_in_time, point_in_time_T2 = T2.point_in_time,
						--CollectionTime_T1 = T1.CollectionTime, CollectionTime_T2 = T2.CollectionTime,
						CollectionTime = T2.CollectionTime,
						CollectionTime_Duration_Seconds = DATEDIFF(second,T1.CollectionTime,T2.CollectionTime),
						WaitType = COALESCE(T1.WaitType,T2.WaitType),
						Wait_S = ISNULL(T2.Wait_S,0.0) - ISNULL(T1.Wait_S,0.0),
						Resource_S = ISNULL(T2.Resource_S,0.0) - ISNULL(T1.Resource_S,0.0),
						Signal_S = ISNULL(T2.Signal_S,0.0) - ISNULL(T1.Signal_S,0.0),
						WaitCount = ISNULL(T2.WaitCount,0.0) - ISNULL(T1.WaitCount,0.0),
						[Percentage] = NULL, --ISNULL(T2.Wait_S,0.0) - ISNULL(T1.Wait_S,0.0),
						AvgWait_S = CASE WHEN (ISNULL(T2.WaitCount,0.0) - ISNULL(T1.WaitCount,0.0)) = 0 THEN (ISNULL(T2.Wait_S,0.0) - ISNULL(T1.Wait_S,0.0))
										 ELSE (ISNULL(T2.Wait_S,0.0) - ISNULL(T1.Wait_S,0.0)) / (ISNULL(T2.WaitCount,0.0) - ISNULL(T1.WaitCount,0.0))
										 END,
						AvgRes_S = CASE WHEN (ISNULL(T2.WaitCount,0.0) - ISNULL(T1.WaitCount,0.0)) = 0 THEN (ISNULL(T2.Resource_S,0.0) - ISNULL(T1.Resource_S,0.0))
										ELSE (ISNULL(T2.Resource_S,0.0) - ISNULL(T1.Resource_S,0.0)) / (ISNULL(T2.WaitCount,0.0) - ISNULL(T1.WaitCount,0.0))
										END,
						AvgSig_S = CASE WHEN (ISNULL(T2.WaitCount,0.0) - ISNULL(T1.WaitCount,0.0)) = 0 THEN (ISNULL(T2.Signal_S,0.0) - ISNULL(T1.Signal_S,0.0))
										ELSE (ISNULL(T2.Signal_S,0.0) - ISNULL(T1.Signal_S,0.0)) / (ISNULL(T2.WaitCount,0.0) - ISNULL(T1.WaitCount,0.0))
										END
				FROM T1 full outer join T2 on T2.WaitType = T1.WaitType
			) as waits
		WHERE 1 = 1
			AND CollectionTime_Duration_Seconds > 0.0
			--AND Wait_S >= 0.0
	)
	INSERT #Wait_Stats_Delta
	SELECT CollectionTime, CollectionTime_Duration_Seconds, WaitType, Wait_S, Resource_S, Signal_S, WaitCount,
				[Percentage] = (Wait_S*100.0)/Total_Wait_S,
				AvgWait_S, AvgRes_S, AvgSig_S
	FROM T_Waits_Delta as d
	JOIN (select sum(i.Wait_S) as Total_Wait_S from T_Waits_Delta as i) as t ON 1 = 1
	ORDER BY Wait_S DESC;	

	set @l_counter += 1;
end

select CollectionTime as time, CollectionTime_Duration_Seconds as Duration_S, WaitType, Wait_S, Resource_S, Signal_S, WaitCount, [Percentage], AvgWait_S, AvgRes_S, AvgSig_S
from #Wait_Stats_Delta;

