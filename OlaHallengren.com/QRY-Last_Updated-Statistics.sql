use StackOverflow

set transaction isolation level read uncommitted;
select	db_name() as DbName, schema_name(o.schema_id)+'.'+o.name as ObjectName, sp.stats_id, st.name, sp.last_updated, ps.rows_total,
		sp.rows_sampled, sp.steps, sp.unfiltered_rows, sp.modification_counter
		,convert(numeric(20,0),SQRT(ps.rows_total * 1000)) as SqrtFormula
		,case when sp.modification_counter >= convert(numeric(20,0),SQRT(ps.rows_total * 1000)) then 1 else 0 end as _Ola_IndexOptimize
from sys.stats as st
cross apply sys.dm_db_stats_properties(st.object_id, st.stats_id) as sp
join sys.objects o on o.object_id = st.object_id
outer apply (SELECT SUM(ps.row_count) AS rows_total
						FROM sys.dm_db_partition_stats as ps WHERE ps.object_id = st.object_id AND ps.index_id < 2
						GROUP BY ps.object_id
) as ps
where o.is_ms_shipped = 0
and (schema_name(o.schema_id)+'.'+o.name) IN ('dbo.table1','dbo.table2')
and (case when sp.modification_counter >= convert(numeric(20,0),SQRT(ps.rows_total * 1000)) then 1 else 0 end) = 1
order by  sp.last_updated asc
go
