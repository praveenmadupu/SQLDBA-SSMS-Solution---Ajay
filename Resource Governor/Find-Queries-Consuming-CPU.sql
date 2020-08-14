USE [master]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/* Create pools for the groups of users you want to track: */
CREATE RESOURCE POOL pool_WebSite;
CREATE RESOURCE POOL pool_Accounting;
CREATE RESOURCE POOL pool_ReportingUsers;
GO

CREATE WORKLOAD GROUP wg_WebSite USING [pool_WebSite];
CREATE WORKLOAD GROUP wg_Accounting USING [pool_Accounting];
CREATE WORKLOAD GROUP wg_ReportingUsers USING [pool_ReportingUsers];
GO

/* For the purposes of my demo, I'm going to create
a few SQL logins that I'm going to classify into
different groups. You won't need to do this, since
your server already has logins. */
CREATE LOGIN [WebSiteApp] WITH PASSWORD=N'Passw0rd!', 
DEFAULT_DATABASE=[StackOverflow], CHECK_EXPIRATION=OFF, CHECK_POLICY=OFF
GO
ALTER SERVER ROLE [sysadmin] ADD MEMBER [WebSiteApp]
GO

CREATE LOGIN [AccountingApp] WITH PASSWORD=N'Passw0rd!', 
DEFAULT_DATABASE=[StackOverflow], CHECK_EXPIRATION=OFF, CHECK_POLICY=OFF
GO
ALTER SERVER ROLE [sysadmin] ADD MEMBER [AccountingApp]
GO

CREATE LOGIN [IPFreely] WITH PASSWORD=N'Passw0rd!', 
DEFAULT_DATABASE=[StackOverflow], CHECK_EXPIRATION=OFF, CHECK_POLICY=OFF
GO
ALTER SERVER ROLE [sysadmin] ADD MEMBER [IPFreely]
GO


/* On login, this function will run and put people
into different groups based on who they are. */

CREATE FUNCTION [dbo].[ResourceGovernorClassifier]() 
RETURNS sysname 
WITH SCHEMABINDING
AS
BEGIN
	-- Define the return sysname variable for the function
	DECLARE @grp_name AS sysname;

	SELECT @grp_name = CASE SUSER_NAME()
		WHEN 'WebSiteApp' THEN 'wg_WebSite'
		WHEN 'AccountingApp' THEN 'wg_Accounting'
		WHEN 'IPFreely' THEN 'wg_ReportingUsers'
		ELSE 'default' END;

	RETURN @grp_name;
END
GO

/* Tell Resource Governor which function to use: */
ALTER RESOURCE GOVERNOR 
WITH ( CLASSIFIER_FUNCTION = dbo.[ResourceGovernorClassifier])
GO

/* Make changes effective
ALTER RESOURCE GOVERNOR RECONFIGURE
GO
*/


SELECT *
FROM sys.dm_resource_governor_resource_pools;
