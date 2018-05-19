use ServiceCatalog
go

select * from [dbo].[SqlServerCategoryMSTR]
select * from [dbo].[SqlServerCategoryDetailMSTR]

insert into [dbo].[SqlServerCategoryDetailMSTR]
(	[CategoryID], [SpecificationName], [SpecificationHelpText], 
	[SpecificationHtmlFieldType], [SpecificationGroup], [SpecificationValues])
-- Populating data for Login Creation
VALUES (1,'Server Name','Provide SQL Server Instance Name here',
		'Text',NULL,NULL
	)

insert into [dbo].[SqlServerCategoryDetailMSTR]
(	[CategoryID], [SpecificationName], [SpecificationHelpText], 
	[SpecificationHtmlFieldType], [SpecificationGroup], [SpecificationValues])
-- Populating data for Login Creation
VALUES (1,'Database Name','Provide SQL Server database name here',
		'Text',NULL,NULL
	)

insert into [dbo].[SqlServerCategoryDetailMSTR]
(	[CategoryID], [SpecificationName], [SpecificationHelpText], 
	[SpecificationHtmlFieldType], [SpecificationGroup], [SpecificationValues])
-- Populating data for Login Creation
VALUES (1,'Login Name','Enter desired name for login creation here',
		'Text',NULL,NULL
	)
GO


-- TODO: Set parameter values here.
DECLARE @RC int;
EXECUTE @RC = [dbo].[usp_AddRequest_SqlServer_DatabaseCreation] 
   @pSqlInstance = 'TESTVM'
  ,@pDatabaseName = 'CosmoUAT2'
  ,@pApplicationName = 'cosmo'
  ,@pRequester = 'corporate\adwivedi'
