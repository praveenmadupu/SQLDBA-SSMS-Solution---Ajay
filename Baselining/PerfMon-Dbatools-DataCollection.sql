Template Path
\\tul1dbapmtdb1\H$\Performance-Issues\DBA_PerfMon_Collector-Template.xml

LogMan.exe => Manage Performance Monitor & performance logs from the command line.
https://ss64.com/nt/logman.html
https://docs.microsoft.com/en-us/windows-server/administration/windows-commands/logman

logman import -name “DBA_PerfMon_Collector” -xml “E:\GitHub\SQLDBA-SSMS-Solution\Baselining\DBA_PerfMon_Collector-Template.xml”
logman update -name “DBA_PerfMon_Collector” -f bin -v mmddhhmm -o "E:\Downloads" -rf 00:05:00 -max 102400
logman start -name “DBA_PerfMon_Collector”