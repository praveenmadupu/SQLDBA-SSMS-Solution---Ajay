Get-ClusterLog -Cluster sqlclusteradmin -TimeSpan 30 -UseLocalTime -Destination 'c:\temp\clusterLogs'

select * from sys.dm_os_cluster_nodes