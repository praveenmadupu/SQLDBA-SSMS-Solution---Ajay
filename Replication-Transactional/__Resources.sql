1) SQL Server Transactional Replication A Deep Dive - Drew Furgiuele
	https://www.youtube.com/watch?v=m28K21Widn0
2) PluralSight Course - "SQL Server - Transactional Replication Fundamentals"
3) YouTube - SQL Server Replication
https://www.youtube.com/playlist?list=PLbkU_gVPZ7OT8gcTJQ0uTi9r4uyZJmUcP
4) YouTube - Tuning and Troubleshooting Transactional Replication - Kendal Van Dyke
https://www.youtube.com/watch?v=UBdAAvMMGwo
5) SQL Server Replication Scripts to get Replication Configuration Information
https://www.mssqltips.com/sqlservertip/1808/sql-server-replication-scripts-to-get-replication-configuration-information/
6) https://www.msqlserver.net/2015/03/the-subscriptions-have-been-marked.html
7) https://docs.microsoft.com/en-us/sql/relational-databases/replication/troubleshoot-tran-repl-errors?view=sql-server-2017

8) Add article to transactional publication without generating new snapshot
https://dba.stackexchange.com/questions/12725/add-article-to-transactional-publication-without-generating-new-snapshot

Rules:-
-----
1) Log Reader agent always resides at Distributor
2) Distribution Agent 
	> resides at Distributer for "Push" subscription
	> resides at Subscriber for "Pull" subscription

Find Replication Jobs using query "01) Get Replication Jobs.sql"
