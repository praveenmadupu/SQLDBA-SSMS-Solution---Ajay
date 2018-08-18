--	https://www.computerhope.com/robocopy.htm


--	Copy single file named 'ACTIONSHEET_DATA_20180321.csq' from source folder to destination folder
robocopy \\TUL1CIPEDB2\I$\Backup \\tul1dbapfs2\f$\TUL1CIPEDB2\Full_Backups COSMO_DATA_20180815.csq
robocopy \\tul1cipedb2\I$\pssdiag_Output_June06_Ajay "E:\Cosmo Issue\Replication_PSSDiag_Output" CosmoServer__0125AM_to_0205AM_CST.zip

robocopy \\TUL1CIPEDB2\C$\DBA\SQLTrace E:\PerformanceAnalysis\Cosmo_Publisher_Baseline\SQLTrace TUL1CIPEDB2_25Jun2018_1030PM.zip
robocopy \\TUL1CIPEDB2\C$\DBA\SQLTrace \\TUL1CIPCNPDB1\G$\DBA\SQLTrace TUL1CIPEDB2_25Jun2018_1030PM.trc
robocopy \\TUL1CIPCNPDB1\G$\DBA\SQLTrace E:\PerformanceAnalysis\Cosmo_Publisher_Baseline\SQLTrace TUL1CIPCNPDB1_26Jun2018_0153AM.zip
