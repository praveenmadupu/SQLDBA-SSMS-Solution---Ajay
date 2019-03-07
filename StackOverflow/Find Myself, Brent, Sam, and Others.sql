select top 100 * from dbo.Users as u where Id in (1,4449743,26837,545629,61305,440595,4197)
/*
select top 100 * from dbo.Users as u
	--where Id in (1,4449743,26837,545629)
	--where u.DisplayName IN ('Brent Ozar')
	where u.Reputation >= 100
	and u.DisplayName like '%Erik Darling%'
	--where u.WebsiteUrl like '%ajaydwivedi%'
	order by u.Reputation desc
*/