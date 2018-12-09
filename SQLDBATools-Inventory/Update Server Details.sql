use sqldbatools
go

select * from Info.Server s
	where s.ServerName = 'TUL1MDQ1WDP01'

update Info.Server
set EnvironmentType = 'QA'
where ServerName = 'TUL1MDQ1WDP01'

;WITH T_Backups AS
(
	select * 
			,row_number()over(partition by SqlInstance,Name order by Name) as RowID
	from [Staging].[DatabaseInfo]
)
--insert [Info].[Database]
select * from t_Backups d
where d.RowID = 1
and d.name 

select * from Info.[Database]