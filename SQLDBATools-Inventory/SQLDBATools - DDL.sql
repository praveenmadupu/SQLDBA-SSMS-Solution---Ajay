USE SQLDBATools
GO

CREATE SCHEMA [Staging]
GO

CREATE TABLE [Staging].[DatabaseBackups](
	[ServerName] [sysname] NOT NULL,
	[DatabaseName] [sysname] NOT NULL,
	[DatabaseCreationDate] [datetime2](7) NOT NULL,
	[RecoveryModel] [varchar](15) NOT NULL,
	[LastFullBackupDate] [datetime2](7) NULL,
	[LastDifferentialBackupDate] [datetime2](7) NULL,
	[LastLogBackupDate] [datetime2](7) NULL,
	[CollectionTime] [DATETIME2](7) NOT NULL
) ON [PRIMARY]
GO

--TRUNCATE TABLE [Staging].[DatabaseBackups]

USE [SQLDBATools]
GO

CREATE TABLE [dbo].[Instance](
	[Instance_ID] [BIGINT] IDENTITY(1,1) PRIMARY KEY,
	[SNo] [float] NULL,
	[Name] [varchar](255) NULL,
	[IsVM] [bit] NULL,
	[IsSQLClusterNode] [bit] DEFAULT 0,
	[IsAlwaysOnNode] [bit] DEFAULT 0,
	[NodeType] [varchar](255) NULL,
	[GeneralDescription] [varchar](255) NULL,
	[IPAddress] [varchar](255) NULL,
	[EnvironmentType] [varchar](255) NULL,
	[BusinessUnit] [varchar](255) NULL,
	[Product] [varchar](255) NULL,
	[SupportedApplication ] [varchar](255) NULL,
	[Domain] [varchar](255) NULL,
	[Version] [varchar](255) NULL,
	[Release] [varchar](255) NULL,
	[ProductKey] [varchar](255) NULL,
	[OSVersion] [varchar](255) NULL,
	[BusinessOwner] [varchar](255) NULL,
	[PrimaryContact] [varchar](255) NULL,
	[SecondaryContact] [varchar](255) NULL,
	[IsDecommissioned] [bit] DEFAULT 0,
	[IsPowerShellLinked] [bit] NULL
) ON [PRIMARY]
GO


USE [SQLDBATools]
GO
--DROP TABLE  [dbo].[DatabaseBackup]
CREATE TABLE [dbo].[DatabaseBackup]
(
	[Instance_ID] [bigint] NOT NULL,
	[DatabaseName] [sysname] NOT NULL,
	[DatabaseCreationDate] [smalldatetime] NOT NULL,
	[RecoveryModel] [varchar](15) NOT NULL,
	[LastFullBackupDate] [smalldatetime] NULL,
	[LastDifferentialBackupDate] [smalldatetime] NULL,
	[LastLogBackupDate] [smalldatetime] NULL,
	[CollectionTime] [smalldatetime] NOT NULL,
	[BatchNumber] [bigint] NOT NULL
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[DatabaseBackup]
	ADD CONSTRAINT pk_DatabaseBackup PRIMARY KEY([CollectionTime], [Instance_ID], [DatabaseName])
GO

ALTER TABLE [dbo].[DatabaseBackup]     
ADD CONSTRAINT FK_DatabaseBackup_Instance_ID FOREIGN KEY (Instance_ID)     
    REFERENCES [dbo].[Instance] (Instance_ID)     
    --ON DELETE CASCADE    
    --ON UPDATE CASCADE  
GO

CREATE NONCLUSTERED INDEX NCI_DatabaseBackup_BatchNumber
	ON [dbo].[DatabaseBackup] ([BatchNumber])
GO

IF OBJECT_ID('dbo.usp_ETL_DatabaseBackup') IS NULL
	EXEC ('CREATE PROCEDURE dbo.usp_ETL_DatabaseBackup AS RETURN 1;');
GO
ALTER PROCEDURE dbo.usp_ETL_DatabaseBackup
AS
BEGIN
	SET NOCOUNT ON;
	DECLARE @_BatchNumber BIGINT;

	SET @_BatchNumber = ISNULL((SELECT MAX([BatchNumber]) FROM [dbo].[DatabaseBackup]),0) + 1;

	BEGIN TRAN
		INSERT [dbo].[DatabaseBackup]
		(	[Instance_ID], DatabaseName, DatabaseCreationDate, RecoveryModel, LastFullBackupDate, LastDifferentialBackupDate, LastLogBackupDate, [CollectionTime], [BatchNumber])
		SELECT I.Instance_ID, DatabaseName, DatabaseCreationDate, RecoveryModel, LastFullBackupDate, LastDifferentialBackupDate, LastLogBackupDate, [CollectionTime], @_BatchNumber
		FROM	[Staging].[DatabaseBackups] AS S
		INNER JOIN
				[dbo].[Instance] AS I
			ON	I.Name = S.ServerName;

		TRUNCATE TABLE [Staging].[DatabaseBackups];
	COMMIT TRAN
END
GO

CREATE TABLE dbo.JobHistory
(
	JobName SYSNAME NOT NULL,
	ScriptPath VARCHAR(255) NULL,
	StartTime smalldatetime NOT NULL
)
GO

IF OBJECT_ID('dbo.vw_DatabaseBackups') IS NULL
	EXEC ('CREATE VIEW dbo.vw_DatabaseBackups AS SELECT 1 AS [Message]');
GO
ALTER VIEW dbo.vw_DatabaseBackups
AS
	SELECT	I.Name as ServerInstance
			,B.[DatabaseName]
			,B.[DatabaseCreationDate]
			,B.[RecoveryModel]
			,[IsFullBackupInLast24Hours] = CASE	WHEN	[LastFullBackupDate] IS NULL OR DATEDIFF(HH,[LastFullBackupDate],GETDATE()) >= 24
												THEN	'No'
												ELSE	'Yes'
												END
			,[IsFullBackupInLast7Days] = CASE	WHEN	[LastFullBackupDate] IS NULL OR DATEDIFF(DD,[LastFullBackupDate],GETDATE()) >= 7
												THEN	'No'
												ELSE	'Yes'
												END
			,B.[LastFullBackupDate]
			,B.[LastDifferentialBackupDate]
			,B.[LastLogBackupDate]
			,B.[CollectionTime]
	FROM	[dbo].[DatabaseBackup] AS B	
	INNER JOIN
		(	SELECT MAX([BatchNumber]) AS [BatchNumber_Latest] FROM [dbo].[DatabaseBackup] ) AS L
		ON	L.BatchNumber_Latest = B.BatchNumber
	INNER JOIN
			[dbo].[Instance] AS I
		ON	I.Instance_ID = B.Instance_ID
GO

IF OBJECT_ID('dbo.Instance_Name') IS NULL
	EXEC ('CREATE FUNCTION dbo.Instance_Name RETURNS BIT AS BEGIN RETURN 1 END');
GO
ALTER FUNCTION dbo.Instance_Name
	( @Instance_ID bigint )  
RETURNS SYSNAME  
AS
BEGIN   
    DECLARE @Name SYSNAME;
	SET @Name = (SELECT Name FROM [dbo].[Instance] WHERE [Instance_ID] = @Instance_ID);

    RETURN @Name;  
END  
GO

--SELECT * FROM dbo.vw_DatabaseBackups;

DECLARE @tableHTML  NVARCHAR(MAX) ;
DECLARE @subject VARCHAR(200);

SET @subject = 'Database Backup History - '+CAST(CAST(GETDATE() AS DATE) AS VARCHAR(20));
--SELECT @subject

SET @tableHTML =  N'
<style>
#BackupHistory {
    font-family: "Trebuchet MS", Arial, Helvetica, sans-serif;
    border-collapse: collapse;
    width: 100%;
}

#BackupHistory td, #BackupHistory th {
    border: 1px solid #ddd;
    padding: 8px;
}

#BackupHistory tr:nth-child(even){background-color: #f2f2f2;}

#BackupHistory tr:hover {background-color: #ddd;}

#BackupHistory th {
    padding-top: 12px;
    padding-bottom: 12px;
    text-align: left;
    background-color: #4CAF50;
    color: white;
}
</style>'+
    N'<H1>'+@subject+'</H1>' +  
    N'<table border="1" id="BackupHistory">' +  
    N'<tr>
	<th>ServerInstance</th>' + 
		N'<th>DatabaseName</th>' +  
		N'<th>DatabaseCreationDate</th>'+
		N'<th>RecoveryModel</th>'+
		N'<th>HasFullBackup<br>InLast24Hours</th>' +  
		N'<th>IsFullBackup<br>InLast7Days</th>' + 
		N'<th>LastFullBackupDate</th>'+
		N'<th>LastDifferential<br>BackupDate</th>' +  
		N'<th>LastLogBackupDate</th>'+
		N'<th>CollectionTime</th>
	</tr>' +  
    CAST ( ( SELECT td = ServerInstance, '',  
                    td = DatabaseName, '',  
                    td = DatabaseCreationDate, '',  
                    td = RecoveryModel, '',  
                    td = IsFullBackupInLast24Hours, '',  
					td = IsFullBackupInLast7Days, '', 					
					td = ISNULL(CAST(LastFullBackupDate AS varchar(100)),' '), '', 
					td = ISNULL(CAST(LastDifferentialBackupDate AS varchar(100)),' '), '', 
					td = ISNULL(CAST(LastLogBackupDate AS varchar(100)),' '), '',
                    td = CollectionTime  
              FROM dbo.vw_DatabaseBackups as b
				WHERE b.IsFullBackupInLast24Hours = 'No'  
              FOR XML PATH('tr'), TYPE   
    ) AS NVARCHAR(MAX) ) +  
    N'</table>' ;  

EXEC msdb.dbo.sp_send_dbmail 
	@recipients='ajay.dwivedi@tivo.com',--;Nasir.Malik@tivo.com',  
    @subject = @subject,  
    @body = @tableHTML,  
    @body_format = 'HTML' ; 
GO