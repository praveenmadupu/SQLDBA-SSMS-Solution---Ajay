--	01) Find replication jobs
select j.name, c.name, j.description from msdb.dbo.sysjobs_view as j inner join msdb.dbo.syscategories as c on c.category_id = j.category_id
	where j.enabled = 1
	and c.name like '%repl%'
	order by c.name