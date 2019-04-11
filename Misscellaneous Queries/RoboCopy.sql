--	https://www.computerhope.com/robocopy.htm
--	https://stackoverflow.com/questions/40744335/how-do-i-force-robocopy-to-overwrite-files
--	https://stackoverflow.com/a/40750265

--	Copy single file named 'ACTIONSHEET_DATA_20180321.csq' from source folder to destination folder
robocopy \\TUL1CIPEDB2\I$\Backup \\tul1dbapfs2\f$\TUL1CIPEDB2\Full_Backups COSMO_DATA_20180815.csq
robocopy \\tul1cipedb2\I$\pssdiag_Output_June06_Ajay "E:\Cosmo Issue\Replication_PSSDiag_Output" CosmoServer__0125AM_to_0205AM_CST.zip

robocopy \\TUL1CIPEDB2\C$\DBA\SQLTrace E:\PerformanceAnalysis\Cosmo_Publisher_Baseline\SQLTrace TUL1CIPEDB2_25Jun2018_1030PM.zip
robocopy \\TUL1CIPEDB2\C$\DBA\SQLTrace \\TUL1CIPCNPDB1\G$\DBA\SQLTrace TUL1CIPEDB2_25Jun2018_1030PM.trc
robocopy \\TUL1CIPCNPDB1\G$\DBA\SQLTrace E:\PerformanceAnalysis\Cosmo_Publisher_Baseline\SQLTrace TUL1CIPCNPDB1_26Jun2018_0153AM.zip

robocopy src dst sample.txt /is      # copy if attributes are equal
robocopy src dst sample.txt /it      # copy if attributes differ
robocopy src dst sample.txt /is /it  # copy irrespective of attributes


Step 01 - Copy Dbs Required by Job [Restore AMGMusicProdcopy DB from production]
robocopy \\Ann1vesPDb02\F$\dump\db01\ v:\dump\DB02_dump\ "AMGMusicAuthority-data.DMP" "AMGMusicMore-data.DMP" "AMGMusic-data.DMP" "EntryAggregation-data.DMP" "VestaMusicProcessing-data.DMP"

Step 02 - Copy All Dbs Except Step 01
robocopy \\Ann1vesPDb02\F$\dump\db01\ v:\dump\DB02_dump\ *-data.DMP /XF "AMGMusicAuthority-data.DMP" "AMGMusicMore-data.DMP" "AMGMusic-data.DMP" "EntryAggregation-data.DMP" "VestaMusicProcessing-data.DMP"