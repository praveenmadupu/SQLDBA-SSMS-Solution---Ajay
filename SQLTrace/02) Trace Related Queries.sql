--	Find all running SQL Traces on Server
	--	[traceid] = 1 is System Trace (Default)
SELECT getdate() as currentTime, @@servername as srvName, * FROM ::fn_trace_getinfo(0);
select * from sys.traces

--	To Start trace
exec sp_trace_setstatus 2, 1;

--	To Stop running trace
	-- Stop [traceid] = 2 SQL Trace
exec sp_trace_setstatus 2, 0;
	
--	To Remove it entirely 
exec sp_trace_setstatus 2, 2;

--	Command to perform ReadTrace
"C:\Program Files\Microsoft Corporation\RMLUtils\ReadTrace.exe" -IG:\DBA\SQLTrace\SQLTrace\TUL1CIPEDB2_18Jun2018_1124PM.trc -oG:\DBA\SQLTrace -f
"C:\Program Files\Microsoft Corporation\RMLUtils\ReadTrace.exe" -IE:\PerformanceAnalysis\2018, June 18 - Publisher Trace\TUL1CIPCNPDB1_19Jun2018_0230AM\TUL1CIPCNPDB1_19Jun2018_0230AM.trc -oE:\PerformanceAnalysis -f