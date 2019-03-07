--dbcc freeproccache;
--dbcc dropcleanbuffers;
USE StackOverflow
go

if OBJECT_ID('dbo.PostTags') IS NOT NULL
	drop table dbo.PostTags
GO

--create nonclustered index nci_posts_Tags on dbo.Posts(Id,Tags) where  Tags IS NOT NULL 
--go

;with t_posts as
(	
select Id, Tags, TotalTags = len(Tags)-len(replace(Tags,'>',''))
from dbo.Posts as p
	where Tags IS NOT NULL
	--and Id = 6
	--and len(tags) = 3
	--order by Id
)
,t_posttags as
(
	select	Id, Tags, TotalTags
			,TagName = case when TotalTags = 1
							then cast(REPLACE(REPLACE(Tags,'<',''),'>','') as nvarchar(150))
							else cast(SUBSTRING(Tags,2,CHARINDEX('>',Tags)-2) as nvarchar(150))
						end
			,TagsRemaining = case when TotalTags = 1
								then Null
								else SUBSTRING(Tags,CHARINDEX('>',Tags)+1,LEN(Tags))
								end
			--TotalTags = len(Tags)-len(replace(Tags,'>','')), 
			,TagCounter = 1
			,RemainingTagsCounts = case when TotalTags = 1
								then 0
								else len(Tags)-len(replace(Tags,'>',''))-1
								end
	from t_posts
	--
	
	union all
	--
	select	c.Id, c.Tags, c.TotalTags,
			TagName = case when c.TotalTags = TagCounter + 1
							then cast(REPLACE(REPLACE(TagsRemaining,'<',''),'>','') as nvarchar(150))
							else SUBSTRING(TagsRemaining,2,CHARINDEX('>',TagsRemaining)-2)
						end,
			TagsRemaining = case when c.TotalTags = TagCounter + 1
								then null
								else SUBSTRING(TagsRemaining,CHARINDEX('>',TagsRemaining)+1,LEN(TagsRemaining))
							end,
			--TotalTags = len(Tags)-len(replace(Tags,'>','')), 
			TagCounter = p.TagCounter+1, 
			RemainingTagsCounts = case when c.TotalTags = TagCounter + 1
								then 0
								else len(TagsRemaining)-len(replace(TagsRemaining,'>',''))-1
								end
	from t_posts as c
	join t_posttags as p
	on c.Id = p.Id
	and c.TotalTags <> 1
	and  c.TotalTags > TagCounter
	 
)
select --*, 
		cast(p.Id as int) as PostId, TagId = cast(t.Id as int)
into dbo.PostTags
from t_posttags as p
	join dbo.Tags as t
	on t.TagName = p.TagName
order by PostId, TagId

-- drop table dbo.PostTags
--select * from dbo.PostTags

/*
drop index nci_posts_Tags on dbo.Posts
go


ALTER TABLE dbo.PostTags ALTER COLUMN [PostId] INTEGER NOT NULL
go
ALTER TABLE dbo.PostTags ALTER COLUMN [TagId] INTEGER NOT NULL
go

ALTER TABLE dbo.PostTags
	ADD CONSTRAINT PK_PostTags__PostId_TagId PRIMARY KEY CLUSTERED ([PostId],[TagId])
go

*/