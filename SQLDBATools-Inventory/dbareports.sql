--	https://github.com/sqlcollaborative/dbareports
--	https://dbareports.io/getting-started/

--	PowerShell command to setup DbaReports
Install-DbaReports -SqlServer TUL1DBAPMTDB1 -InstallDatabase SQLDBATools -InstallPath 'F:\SQLDBATools' -JobCategory 'SQLDBATools' -JobPrefix "SQLDBATools" -LogFileFolder 'F:\SQLDBATools' -ReportsFolder 'F:\SQLDBATools\Reports'

