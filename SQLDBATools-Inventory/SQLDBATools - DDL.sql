USE SQLDBATools
GO

CREATE SCHEMA [Staging]
GO

CREATE TABLE [Staging].[ServerInfo]
(
	[ServerName] [varchar](125) NULL,
	[EnvironmentType] varchar(125) NOT NULL,
	[DNSHostName] [varchar](125) NULL,
	[IPAddress] [varchar](15) NULL,
	[Domain] [varchar](125) NULL,
	[OperatingSystem] [varchar](125) NULL,
	[SPVersion] [varchar](125) NULL,
	[Model] [varchar](125) NULL,
	[RAM] [int] NULL,
	[CPU] [tinyint] NULL,
	[CollectionTime] [smalldatetime] NULL
) ON [StagingData]
GO

CREATE TABLE [Info].[Server]
(
	[ServerID] INT IDENTITY(1,1) NOT NULL,
	[ServerName] [varchar](125) NULL,
	[EnvironmentType] varchar(125) NOT NULL,
	[DNSHostName] [varchar](125) NULL,
	[IPAddress] [varchar](15) NULL,
	[Domain] [varchar](125) NULL,
	[OperatingSystem] [varchar](125) NULL,
	[SPVersion] [varchar](125) NULL,
	[Model] [varchar](125) NULL,
	[RAM] [int] NULL,
	[CPU] [tinyint] NULL,
	[CollectionTime] [smalldatetime] NULL
) ON [MasterData]
GO

ALTER TABLE [Info].[Server]
	ADD CONSTRAINT pk_Info_Server PRIMARY KEY(ServerID)
GO

ALTER TABLE [Info].[Server] 
	ADD CONSTRAINT UK_Info_Server_ServerName UNIQUE (ServerName) 
GO

CREATE TABLE [dbo].[VolumeInfo]
(
	ID [BIGINT] IDENTITY(1,1) NOT NULL,
	[ServerName] [varchar](125) NOT NULL,
	[VolumeName] [varchar](125) NOT NULL,
	[CapacityGB] [decimal](20, 2) NOT NULL,
	[UsedSpaceGB] [decimal](20, 2) NOT NULL,
	[UsedSpacePercent] [decimal](20, 2) NOT NULL,
	[FreeSpaceGB] [decimal](20, 2) NOT NULL,
	[Label] [varchar](125) NULL,
	[CollectionTime] [datetime2](7) NOT NULL
) ON [CollectedData]
GO

ALTER TABLE [dbo].[VolumeInfo]
	ADD CONSTRAINT pk_dbo_VolumeInfo PRIMARY KEY(ServerName,VolumeName)
GO

ALTER TABLE [dbo].[VolumeInfo]     
	ADD CONSTRAINT FK_VolumeInfo_ServerName FOREIGN KEY (ServerName)     
    REFERENCES [Info].[Server]  (ServerName)     
    --ON DELETE CASCADE    
    --ON UPDATE CASCADE  
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
) ON [StagingData]
GO

CREATE TABLE Staging.VolumeInfo
(
	[ServerName] [varchar](125) NOT NULL,
	[VolumeName] [varchar](125),
	[CapacityGB] DECIMAL(20,2) NOT NULL,
	[UsedSpaceGB] DECIMAL(20,2) NOT NULL,
	[UsedSpacePercent] DECIMAL(20,2) NOT NULL,
	[FreeSpaceGB] DECIMAL(20,2) NOT NULL,
	[Label] [varchar](125) NULL,
	[CollectionTime] [datetime2](7) NOT NULL
) ON [StagingData]
GO

--TRUNCATE TABLE [Staging].[DatabaseBackups]

USE [SQLDBATools]
GO

CREATE TABLE [info].[Instance]
(
	[Instance_ID] int IDENTITY(1,1) PRIMARY KEY,
	[SNo] [float] NULL,
	[Name] [varchar](255) NULL,
	[NetworkName] [varchar](125) NULL,
	[IsVM] [bit] NULL,
	[IsSQLClusterNode] [bit] DEFAULT 0,
	[IsAlwaysOnNode] [bit] DEFAULT 0,
	[NodeType] [varchar](125) NULL,
	[GeneralDescription] [varchar](255) NULL,
	[IPAddress] [varchar](125) NULL,
	[EnvironmentType] [varchar](125) NULL,
	[BusinessUnit] [varchar](125) NULL,
	[Product] [varchar](125) NULL,
	[SupportedApplication ] [varchar](125) NULL,
	[Domain] [varchar](125) NULL,
	[Version] [varchar](125) NULL,
	[Release] [varchar](125) NULL,
	[ProductKey] [varchar](125) NULL,
	[OSVersion] [varchar](125) NULL,
	[BusinessOwner] [varchar](125) NULL,
	[PrimaryContact] [varchar](125) NULL,
	[SecondaryContact] [varchar](125) NULL,
	[IsDecommissioned] [bit] DEFAULT 0,
	[IsPowerShellLinked] [bit] NULL
) ON [MasterData]
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

CREATE table Info.PowerShellFunctionCalls
(
	ID BIGINT IDENTITY(1,1) NOT NULL,
	[CmdLetName] VARCHAR(125) NOT NULL,
	[ParentScript] VARCHAR(125) NULL,
	[ScriptText] VARCHAR(125) NOT NULL,
	[ServerName] VARCHAR(125) NULL,
	[Result] VARCHAR(50) DEFAULT 'Success',
	CollectionTime SMALLDATETIME DEFAULT GETDATE(),
	[ErrorMessage] VARCHAR(2000) NULL
) ON [StagingData]
GO
CREATE NONCLUSTERED INDEX NCI_Info_PowerShellFunctionCalls_CollectionTime ON Info.PowerShellFunctionCalls (CollectionTime)
ON [StagingData]
GO
CREATE NONCLUSTERED INDEX NCI_Info_PowerShellFunctionCalls_ServerName ON Info.PowerShellFunctionCalls (ServerName) WHERE ServerName is not null ON [StagingData]
GO


ALTER PROCEDURE [dbo].[usp_ETL_ServerInfo]
AS
BEGIN
	SET NOCOUNT ON;

	BEGIN TRAN
		;WITH CTE AS (
			SELECT ServerName, [EnvironmentType], DNSHostName, IPAddress, Domain, OperatingSystem, SPVersion, Model, RAM, CPU, CollectionTime
					,ROW_NUMBER()OVER(PARTITION BY ServerName ORDER BY DNSHostName DESC, CollectionTime) AS RowID
			FROM Staging.ServerInfo
			WHERE ServerName IS NOT NULL
		)
		INSERT [Info].[Server]
		(ServerName, [EnvironmentType], DNSHostName, IPAddress, Domain, OperatingSystem, SPVersion, Model, RAM, CPU, CollectionTime)
		SELECT ServerName, [EnvironmentType], DNSHostName, IPAddress, Domain, OperatingSystem, SPVersion, Model, RAM, CPU, CollectionTime
		FROM CTE
		WHERE RowID = 1;

		DELETE [Staging].[ServerInfo]
			WHERE ServerName IN (SELECT i.ServerName FROM [Info].[Server] AS i);
	COMMIT TRAN
END
GO

USE SQLDBATools;
GO
--DROP PROCEDURE [dbo].[usp_ETL_VolumeInfo]
ALTER PROCEDURE [dbo].[usp_ETL_VolumeInfo]
AS
BEGIN
	SET NOCOUNT ON;

	BEGIN TRAN
		-- Truncate table
		TRUNCATE TABLE dbo.VolumeInfo;

		;WITH CTE AS (
			SELECT ServerName, VolumeName, CapacityGB, UsedSpaceGB, UsedSpacePercent, FreeSpaceGB, Label, CollectionTime
					,ROW_NUMBER()OVER(PARTITION BY ServerName, VolumeName ORDER BY ServerName, VolumeName) AS RowID
			FROM Staging.VolumeInfo
			WHERE ServerName IS NOT NULL
		)
		INSERT dbo.VolumeInfo
		(ServerName, VolumeName, CapacityGB, UsedSpaceGB, UsedSpacePercent, FreeSpaceGB, Label, CollectionTime)
		SELECT ServerName, VolumeName, CapacityGB, UsedSpaceGB, UsedSpacePercent, FreeSpaceGB, Label, CollectionTime
		FROM CTE
		WHERE RowID = 1;

		DELETE o
		FROM [Staging].VolumeInfo AS o
		WHERE EXISTS (SELECT i.ServerName FROM dbo.VolumeInfo AS i WHERE i.ServerName = o.ServerName AND i.VolumeName = o.VolumeName);
	COMMIT TRAN
END
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