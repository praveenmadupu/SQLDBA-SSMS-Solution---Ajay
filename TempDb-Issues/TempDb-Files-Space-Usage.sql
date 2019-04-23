use tempdb;
select df.name, df.physical_name, size_gb = (df.size*8.0/1024/1024), CAST(FILEPROPERTY(df.name, 'SpaceUsed') as BIGINT)/128.0/1024 AS SpaceUsed_gb
		,(size/128.0 -CAST(FILEPROPERTY(name,'SpaceUsed') AS INT)/128.0)/1024 AS FreeSpace_GB
from tempdb.sys.database_files as df


