Using Stack Overflow Queries to Generate Workloads
	https://www.brentozar.com/archive/2016/08/dell-dba-days-prep-using-stackexchange-queries-generate-workloads/

Scripted Simulation of SQL Server Loads
https://github.com/gavdraper/ChaosLoad

What is the best way to auto-generate INSERT statements for a SQL Server table?
	https://stackoverflow.com/questions/982568/what-is-the-best-way-to-auto-generate-insert-statements-for-a-sql-server-table


SELECT p.*
FROM dbo.Users as u
join dbo.Posts as p
on u.Id = p.OwnerUserId
where DisplayName = @DisplayName
order by ViewCount;

/*
SELECT TOP (1) DisplayName FROM dbo.Users --where Id in (1,4449743,26837,545629,61305,440595,4197,17174) 
ORDER BY NEWID();
*/