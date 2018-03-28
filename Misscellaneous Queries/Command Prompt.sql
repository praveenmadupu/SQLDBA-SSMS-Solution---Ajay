--	1) Get files 
exec xp_cmdshell 'dir \\tul1cipcnpdb1\f$\dump\*Staging_* /od /b '
/*
output
TUL1CIPCNPDB1_Staging_LOG_20180323_000501.csq
TUL1CIPCNPDB1_Staging_LOG_20180323_001500.csq
*/
