use Test
go

create table dbo.TestData
( id int identity(1,1), ColString01 char(4000) default replicate(char(65+(abs(checksum(NEWID()))%26)),4000), 
	ColString02 char(4000) default replicate(char(65+(abs(checksum(NEWID()))%26)),4000)
)

insert Test.dbo.TestData
values (default,default)
go