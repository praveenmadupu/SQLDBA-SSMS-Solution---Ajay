use audit_archive;

create table #TableSize (
    [name] varchar(255),
    [rows] int,
    reserved varchar(255),
    [data] varchar(255),
    index_size varchar(255),
    unused varchar(255));

create table #ConvertedSizes (
    [name] varchar(255),
    [rows] int,
    reservedKb int,
    dataKb int,
    reservedIndexSize int,
    reservedUnused int,
	reservedMB as cast(reservedKb/1024.0 as numeric(20,2)),
	reservedGB as cast(reservedKb/1024.0/1024.0 as numeric(20,2)),
	dataMB as cast(dataKb/1024.0 as numeric(20,2)),
	)

EXEC sp_MSforeachtable @command1="insert into #TableSize
EXEC sp_spaceused '?'";

insert into #ConvertedSizes ([name], [rows], reservedKb, dataKb, reservedIndexSize, reservedUnused)
select [name], [rows], 
SUBSTRING(reserved, 0, LEN(reserved)-2), 
SUBSTRING(data, 0, LEN(data)-2), 
SUBSTRING(index_size, 0, LEN(index_size)-2), 
SUBSTRING(unused, 0, LEN(unused)-2)
from #TableSize

select [name], [rows], reservedMB, dataMB, reservedIndexSize, reservedUnused
from #ConvertedSizes where reservedMB > 5.0
order by reservedKb desc

drop table #TableSize
drop table #ConvertedSizes
