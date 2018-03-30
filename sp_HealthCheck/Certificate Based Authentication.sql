USE master
GO

GRANT EXECUTE ON OBJECT::[dbo].[sp_HealthCheck] TO [public]
GO

EXEC sp_ms_marksystemobject 'sp_HealthCheck'
go

CREATE CERTIFICATE [CodeSigningCertificate]
	ENCRYPTION BY PASSWORD = 'Work@Y0urBest'
	WITH EXPIRY_DATE = '2099-01-01'
		,SUBJECT = 'dbo.sp_HealthCheck Code Signing Cert'
GO

CREATE LOGIN [CodeSigningLogin] FROM CERTIFICATE [CodeSigningCertificate];
GO

GRANT VIEW SERVER STATE TO [CodeSigningLogin]
GO

ADD SIGNATURE TO [dbo].[sp_HealthCheck]
	BY CERTIFICATE [CodeSigningCertificate]
	WITH PASSWORD = 'Work@Y0urBest'
GO