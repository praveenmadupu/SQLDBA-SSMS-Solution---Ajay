create table #TableSize (
    Name varchar(255),
    [rows] int,
    reserved varchar(255),
    data varchar(255),
    index_size varchar(255),
    unused varchar(255))
create table #ConvertedSizes (
    Name varchar(255),
    [rows] int,
    reservedKb int,
    dataKb int,
    reservedIndexSize int,
    reservedUnused int)

EXEC sp_MSforeachtable @command1="insert into #TableSize
EXEC sp_spaceused '?'"
insert into #ConvertedSizes (Name, [rows], reservedKb, dataKb, reservedIndexSize, reservedUnused)
select name, [rows], 
SUBSTRING(reserved, 0, LEN(reserved)-2), 
SUBSTRING(data, 0, LEN(data)-2), 
SUBSTRING(index_size, 0, LEN(index_size)-2), 
SUBSTRING(unused, 0, LEN(unused)-2)
from #TableSize

select * from #ConvertedSizes
order by reservedKb desc

drop table #TableSize
drop table #ConvertedSizes