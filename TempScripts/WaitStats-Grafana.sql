set nocount on;
/*	Get @start_time & @end_time from user/Grafana
	Get times when WaitStats was flushed
	Divide wait stats for the intervals
*/
DECLARE @server_name varchar(256);
DECLARE @start_time datetime2;
DECLARE @end_time datetime2;
--set @server_name = '$server';
set @start_time = DATEADD(MINUTE,-180,GETDATE());
set @end_time = GETDATE();
--set @start_time = master.dbo.utc2local($__timeFrom());
--set @end_time = master.dbo.utc2local($__timeTo());

DECLARE @verbose bit = 0;

if OBJECT_ID('tempdb..#wait_stats_lower_bounds') is not null
	drop table tempdb..#wait_stats_lower_bounds;
with T_waits_ranges_lower_bounds as (
	select *
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
)
select /* Get times where WaitStats was Cleared */
	   IDENTITY(int,1,1) as id, 
	   point_in_time
into #wait_stats_lower_bounds
from T_waits_ranges_lower_bounds
order by point_in_time asc;

if OBJECT_ID('tempdb..#wait_stats_time_range') is not null
	drop table tempdb..#wait_stats_time_range;
select l.id as range_id, l.point_in_time as range_lower_time, u.point_in_time as range_upper_time
into #wait_stats_time_range
from #wait_stats_lower_bounds as l
join #wait_stats_lower_bounds as u on u.id = l.id + 1
order by range_id;


if @verbose = 1
begin
	select [RunningQuery] = '#wait_stats_lower_bounds', * from #wait_stats_lower_bounds;
	select [RunningQuery] = '#wait_stats_time_range', * from #wait_stats_time_range;
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
	[CollectionTime_Duration_Seconds] [bigint] NOT NULL,
	[WaitType] [nvarchar](120) NOT NULL,
	[Wait_S] [decimal](15, 2) NULL,
	[Resource_S] [decimal](15, 2) NULL,
	[Signal_S] [decimal](15, 2) NULL,
	[WaitCount] [bigint] NULL,
	[Percentage] [decimal](5,2) NULL,
	[AvgWait_S] [decimal](35, 22) NULL,
	[AvgRes_S] [decimal](35, 22) NULL,
	[AvgSig_S] [decimal](35, 22) NULL
)

declare @l_id int
		,@l_point_in_time datetime2
		,@l_counter int = 1
		,@l_counter_max int;

select @l_counter_max = max(range_id) from #wait_stats_time_range;
/* Loop for N-1 times, where N =  point in times for range calculation */
while @l_counter <= @l_counter_max -- execute for N-1 times
begin

	if OBJECT_ID('tempdb..#wait_stats_range_lower') is not null
		drop table #wait_stats_range_lower;
	select  /*	Get wait stats for start time of the time range.
			*/
			id = r.range_id, r.range_lower_time, r.range_upper_time,
			ws.CollectionTime, 
			ws.WaitType, 
			ws.Wait_S, ws.Resource_S, ws.Signal_S, ws.WaitCount, ws.Percentage, ws.AvgWait_S, ws.AvgRes_S, ws.AvgSig_S
			,[condition] = (case when (@l_counter <> 1) and (@l_counter <>  @l_counter_max) -- Intermediate range
										then 'Intermediate range'
										when (@l_counter = 1) and (@l_counter = @l_counter_max) -- 1st/last, and only range
										then '1st/last, and only range'
										when (@l_counter = 1) and (@l_counter <> @l_counter_max) -- 1st and intermediate range
										then '1st and intermediate range'
										when (@l_counter = @l_counter_max) and (@l_counter - 1 <> 0) -- last and intermediate
										then 'last and intermediate'
									else null
									end)
	into #wait_stats_range_lower
	from #wait_stats_time_range as r, [DBA].[dbo].[WaitStats] AS ws with (nolock)
	where r.range_id = @l_counter /* loop filter */
		and ws.CollectionTime = (	case when (@l_counter <> 1) and (@l_counter <>  @l_counter_max) -- Intermediate range
										then (select MIN(wsi.CollectionTime) as CollectionTime from [DBA].[dbo].[WaitStats] as wsi with (nolock) where wsi.CollectionTime > r.range_lower_time)
										when (@l_counter = 1) and (@l_counter = @l_counter_max) -- 1st/last, and only range
										then (select MIN(wsi.CollectionTime) as CollectionTime from [DBA].[dbo].[WaitStats] as wsi with (nolock) where wsi.CollectionTime >= r.range_lower_time)
										when (@l_counter = 1) and (@l_counter <> @l_counter_max) -- 1st and intermediate range
										then (select MIN(wsi.CollectionTime) as CollectionTime from [DBA].[dbo].[WaitStats] as wsi with (nolock) where wsi.CollectionTime >= r.range_lower_time)
										when (@l_counter = @l_counter_max) and (@l_counter - 1 <> 0) -- last and intermediate
										then (select MIN(wsi.CollectionTime) as CollectionTime from [DBA].[dbo].[WaitStats] as wsi with (nolock) where wsi.CollectionTime > r.range_lower_time)
									else null
									end
								);
	
	if OBJECT_ID('tempdb..#wait_stats_range_upper') is not null
		drop table #wait_stats_range_upper;
	select  /*	Get wait stats for start time of the time range.
			*/
			id = r.range_id, r.range_lower_time, r.range_upper_time,
			ws.CollectionTime, 
			ws.WaitType, 
			ws.Wait_S, ws.Resource_S, ws.Signal_S, ws.WaitCount, ws.Percentage, ws.AvgWait_S, ws.AvgRes_S, ws.AvgSig_S
			,[condition] = (case when (@l_counter <> 1) and (@l_counter <>  @l_counter_max) -- Intermediate range
										then 'Intermediate range'
										when (@l_counter = 1) and (@l_counter = @l_counter_max) -- 1st/last, and only range
										then '1st/last, and only range'
										when (@l_counter = 1) and (@l_counter <> @l_counter_max) -- 1st and intermediate range
										then '1st and intermediate range'
										when (@l_counter = @l_counter_max) and (@l_counter - 1 <> 0) -- last and intermediate
										then 'last and intermediate'
									else null
									end)
	into #wait_stats_range_upper
	from #wait_stats_time_range as r, [DBA].[dbo].[WaitStats] AS ws with (nolock)
	where r.range_id = @l_counter /* loop filter */
		and ws.CollectionTime = (	case when (@l_counter <> 1) and (@l_counter <>  @l_counter_max) -- Intermediate range
										then (select MAX(wsi.CollectionTime) as CollectionTime from [DBA].[dbo].[WaitStats] as wsi with (nolock) where wsi.CollectionTime < r.range_upper_time)
										when (@l_counter = 1) and (@l_counter = @l_counter_max) -- 1st/last, and only range
										then (select MAX(wsi.CollectionTime) as CollectionTime from [DBA].[dbo].[WaitStats] as wsi with (nolock) where wsi.CollectionTime <= r.range_upper_time)
										when (@l_counter = 1) and (@l_counter <> @l_counter_max) -- 1st and intermediate range
										then (select MAX(wsi.CollectionTime) as CollectionTime from [DBA].[dbo].[WaitStats] as wsi with (nolock) where wsi.CollectionTime < r.range_upper_time)
										when (@l_counter = @l_counter_max) and (@l_counter - 1 <> 0) -- last and intermediate
										then (select MAX(wsi.CollectionTime) as CollectionTime from [DBA].[dbo].[WaitStats] as wsi with (nolock) where wsi.CollectionTime <= r.range_upper_time)
									else null
									end
								);

	if @verbose = 1
	begin
		select [@l_counter] = @l_counter, [@l_counter_max] = @l_counter_max;
		select [RunningQuery] = '#wait_stats_range_lower', * from #wait_stats_range_lower;
		select [RunningQuery] = '#wait_stats_range_upper', * from #wait_stats_range_upper;
	end
	
	
	;with T_Waits_Delta AS (
		SELECT CollectionTime, CollectionTime_Duration_Seconds, WaitType, Wait_S, Resource_S, Signal_S, WaitCount,
				[Percentage],
				AvgWait_S, AvgRes_S, AvgSig_S
		--INTO tempdb..Wait_Stats_Delta
		FROM (
				SELECT --id_T1 = T1.id, id_T2 = T2.id,
						--point_in_time_T1 = T1.point_in_time, point_in_time_T2 = T2.point_in_time,
						--CollectionTime_T1 = T1.CollectionTime, CollectionTime_T2 = T2.CollectionTime,
						[RunningQuery] = 'Wait_Stats_Delta',
						CollectionTime = COALESCE(T2.CollectionTime,T1.CollectionTime),
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
				FROM #wait_stats_range_lower as T1 full outer join #wait_stats_range_upper as T2 on T2.WaitType = T1.WaitType
			) as waits
		WHERE 1 = 1
			--AND CollectionTime_Duration_Seconds > 0.0
			--AND Wait_S >= 0.0
	)
	
	INSERT #Wait_Stats_Delta
	SELECT CollectionTime = COALESCE(dur.CollectionTime,d.CollectionTime), 
			CollectionTime_Duration_Seconds = COALESCE(dur.CollectionTime_Duration_Seconds,d.CollectionTime_Duration_Seconds, r.Range_Duration_Seconds), 
			WaitType, Wait_S, Resource_S, Signal_S, WaitCount,
				[Percentage] = (Wait_S*100.0)/Total_Wait_S,
				AvgWait_S, AvgRes_S, AvgSig_S
	FROM T_Waits_Delta as d
	JOIN (select sum(i.Wait_S) as Total_Wait_S from T_Waits_Delta as i) as t ON 1 = 1
	JOIN (select max(CollectionTime_Duration_Seconds) as CollectionTime_Duration_Seconds, max(CollectionTime) as CollectionTime from T_Waits_Delta) as dur ON 1 = 1
	JOIN (select DATEDIFF(second,r.range_lower_time,r.range_upper_time) AS Range_Duration_Seconds from #wait_stats_time_range as r where r.range_id = @l_counter) as r ON 1 = 1
	WHERE 1 = 1
		AND Wait_S >= 0.0
	ORDER BY Wait_S DESC;	
	

	set @l_counter += 1;
end

select	cast(CollectionTime as smalldatetime) as time, 
		--master.dbo.time2duration(CollectionTime_Duration_Seconds,'s') as [Duration], 
		WaitType, [Percentage], Wait_S = master.dbo.time2duration(Wait_S,'s'), 
		Resource_S = master.dbo.time2duration(Resource_S,'s'), 
		Signal_S = master.dbo.time2duration(Signal_S,'s'), 
		WaitCount, AvgWait_S, AvgRes_S, AvgSig_S
from #Wait_Stats_Delta
order by time, [Percentage] desc, CollectionTime_Duration_Seconds desc;
