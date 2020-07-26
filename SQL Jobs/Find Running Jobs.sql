SELECT 'Executing-Jobs' as RunningQuery,
	Concat
    (
        RIGHT('00'+CAST(ISNULL((datediff(second,ja.start_execution_date,GETDATE()) / 3600 / 24), 0) AS VARCHAR(2)),2)
        ,' '
        ,RIGHT('00'+CAST(ISNULL(datediff(second,ja.start_execution_date,GETDATE()) / 3600  % 24, 0) AS VARCHAR(2)),2)
        ,':'
        ,RIGHT('00'+CAST(ISNULL(datediff(second,ja.start_execution_date,GETDATE()) / 60 % 60, 0) AS VARCHAR(2)),2)
        ,':'
        ,RIGHT('00'+CAST(ISNULL(datediff(second,ja.start_execution_date,GETDATE()) % 3600 % 60, 0) AS VARCHAR(2)),2)
    ) as [dd hh:mm:ss],
    --ja.job_id,
    j.name AS job_name,
    ja.start_execution_date,      
    ISNULL(last_executed_step_id,0)+1 AS current_executed_step_id,
    Js.step_name
FROM msdb.dbo.sysjobactivity ja 
LEFT JOIN msdb.dbo.sysjobhistory jh 
    ON ja.job_history_id = jh.instance_id
JOIN msdb.dbo.sysjobs j 
ON ja.job_id = j.job_id
JOIN msdb.dbo.sysjobsteps js
    ON ja.job_id = js.job_id
    AND ISNULL(ja.last_executed_step_id,0)+1 = js.step_id
WHERE ja.session_id = (SELECT TOP 1 session_id FROM msdb.dbo.syssessions ORDER BY agent_start_date DESC)
AND start_execution_date is not null
AND stop_execution_date is null
--AND j.name = '(dba) Resource Consumption Data - Load and Process - Non-DESCO'
ORDER BY ja.start_execution_date

/*
select J.Name as JobName, RP.program_name
from msdb..sysjobs J with (nolock)
inner join master..sysprocesses RP with (nolock)
on RP.program_name like 'SQLAgent - TSQL JobStep (Job ' + master.dbo.fn_varbintohexstr(convert(binary(16),J.job_id )) + '%'
*/

SELECT * FROM DBA.dbo.SqlAgentJobs

use DBA;

select QUOTENAME(c.COLUMN_NAME) AS COLUMN_NAME, c.DATA_TYPE from INFORMATION_SCHEMA.columns c where c.table_name = 'SqlAgentJobs'