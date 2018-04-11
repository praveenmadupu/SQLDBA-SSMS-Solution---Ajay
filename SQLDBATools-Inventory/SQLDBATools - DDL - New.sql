USE [master]
GO

/****** Object:  Database [SQLDBATools]    Script Date: 4/10/2018 7:10:02 AM ******/
CREATE DATABASE [SQLDBATools] ON  PRIMARY 
( NAME = N'SQLDBATools', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL10_50.MSSQLSERVER\MSSQL\DATA\SQLDBATools.mdf' , SIZE = 2097152KB , MAXSIZE = UNLIMITED, FILEGROWTH = 1048576KB ), 
 FILEGROUP [CollectedData] 
( NAME = N'SQLDBATools_CollectedData', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL10_50.MSSQLSERVER\MSSQL\DATA\SQLDBATools_CollectedData.ndf' , SIZE = 1048576KB , MAXSIZE = UNLIMITED, FILEGROWTH = 1048576KB ), 
 FILEGROUP [MasterData] 
( NAME = N'SQLDBATools_MasterData', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL10_50.MSSQLSERVER\MSSQL\DATA\SQLDBATools_MasterData.ndf' , SIZE = 1048576KB , MAXSIZE = UNLIMITED, FILEGROWTH = 1048576KB ), 
 FILEGROUP [StagingData] 
( NAME = N'SQLDBATools_StagingData', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL10_50.MSSQLSERVER\MSSQL\DATA\SQLDBATools_StagingData.ndf' , SIZE = 1048576KB , MAXSIZE = UNLIMITED, FILEGROWTH = 1048576KB )
 LOG ON 
( NAME = N'SQLDBATools_log', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL10_50.MSSQLSERVER\MSSQL\DATA\SQLDBATools_log.ldf' , SIZE = 1048576KB , MAXSIZE = 2048GB , FILEGROWTH = 524288KB )
GO

USE [SQLDBATools]
GO

/****** Object:  Schema [info]    Script Date: 4/10/2018 7:12:26 AM ******/
CREATE SCHEMA [info]
GO

/****** Object:  Schema [Staging]    Script Date: 4/10/2018 7:12:26 AM ******/
CREATE SCHEMA [Staging]
GO


CREATE TABLE [dbo].[DatabaseBackup](
	[Instance_ID] [bigint] NOT NULL,
	[DatabaseName] [sysname] NOT NULL,
	[DatabaseCreationDate] [smalldatetime] NOT NULL,
	[RecoveryModel] [varchar](15) NOT NULL,
	[LastFullBackupDate] [smalldatetime] NULL,
	[LastDifferentialBackupDate] [smalldatetime] NULL,
	[LastLogBackupDate] [smalldatetime] NULL,
	[CollectionTime] [smalldatetime] NOT NULL,
	[BatchNumber] [bigint] NOT NULL,
 CONSTRAINT [pk_DatabaseBackup] PRIMARY KEY CLUSTERED 
(
	[CollectionTime] ASC,
	[Instance_ID] ASC,
	[DatabaseName] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [CollectedData]
GO

CREATE TABLE [dbo].[DateDimension](
	[DateKey] [int] NOT NULL,
	[Date] [date] NOT NULL,
	[Day] [tinyint] NOT NULL,
	[DaySuffix] [char](2) NOT NULL,
	[Weekday] [tinyint] NOT NULL,
	[WeekDayName] [varchar](10) NOT NULL,
	[IsWeekend] [bit] NOT NULL,
	[IsHoliday] [bit] NOT NULL,
	[HolidayText] [varchar](64) SPARSE  NULL,
	[DOWInMonth] [tinyint] NOT NULL,
	[DayOfYear] [smallint] NOT NULL,
	[WeekOfMonth] [tinyint] NOT NULL,
	[WeekOfYear] [tinyint] NOT NULL,
	[ISOWeekOfYear] [tinyint] NOT NULL,
	[Month] [tinyint] NOT NULL,
	[MonthName] [varchar](10) NOT NULL,
	[Quarter] [tinyint] NOT NULL,
	[QuarterName] [varchar](6) NOT NULL,
	[Year] [int] NOT NULL,
	[MMYYYY] [char](6) NOT NULL,
	[MonthYear] [char](7) NOT NULL,
	[FirstDayOfMonth] [date] NOT NULL,
	[LastDayOfMonth] [date] NOT NULL,
	[FirstDayOfQuarter] [date] NOT NULL,
	[LastDayOfQuarter] [date] NOT NULL,
	[FirstDayOfYear] [date] NOT NULL,
	[LastDayOfYear] [date] NOT NULL,
	[FirstDayOfNextMonth] [date] NOT NULL,
	[FirstDayOfNextYear] [date] NOT NULL,
PRIMARY KEY CLUSTERED 
(
	[DateKey] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [MasterData]
GO

CREATE TABLE [info].[Instance](
	[Instance_ID] [int] IDENTITY(1,1) NOT NULL,
	[SNo] [float] NULL,
	[Name] [varchar](125) NULL,
	[NetworkName] [varchar](125) NULL,
	[IsVM] [bit] NULL,
	[IsSQLClusterNode] [bit] NULL,
	[IsAlwaysOnNode] [bit] NULL,
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
	[IsDecommissioned] [bit] NULL,
	[IsPowerShellLinked] [bit] NULL,
PRIMARY KEY CLUSTERED 
(
	[Instance_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [MasterData]
) ON [MasterData]
GO

CREATE TABLE [info].[ServerInfo](
	[ServerID] [int] IDENTITY(1,1) NOT NULL,
	[DateChecked] [datetime] NULL,
	[ServerName] [nvarchar](50) NULL,
	[DNSHostName] [nvarchar](50) NULL,
	[Domain] [nvarchar](30) NULL,
	[OperatingSystem] [nvarchar](100) NULL,
	[NoProcessors] [tinyint] NULL,
	[IPAddress] [nvarchar](15) NULL,
	[RAM] [int] NULL,
 CONSTRAINT [PK__ServerOS__50A5926BC7005F29] PRIMARY KEY CLUSTERED 
(
	[ServerID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [MasterData]
GO

CREATE TABLE [Staging].[DatabaseBackups](
	[ServerName] [sysname] NOT NULL,
	[DatabaseName] [sysname] NOT NULL,
	[DatabaseCreationDate] [datetime2](7) NOT NULL,
	[RecoveryModel] [varchar](15) NOT NULL,
	[LastFullBackupDate] [datetime2](7) NULL,
	[LastDifferentialBackupDate] [datetime2](7) NULL,
	[LastLogBackupDate] [datetime2](7) NULL,
	[CollectionTime] [datetime2](7) NOT NULL
) ON [StagingData]
GO

CREATE TABLE [Staging].[VolumeInfo](
	[ServerName] [varchar](125) NOT NULL,
	[VolumeName] [varchar](125) NULL,
	[CapacityGB] [decimal](20, 2) NOT NULL,
	[UsedSpaceGB] [decimal](20, 2) NOT NULL,
	[UsedSpacePercent] [decimal](20, 2) NOT NULL,
	[FreeSpaceGB] [decimal](20, 2) NOT NULL,
	[Label] [varchar](125) NULL,
	[CollectionTime] [datetime2](7) NOT NULL
) ON [StagingData]
GO

ALTER TABLE [info].[Instance] ADD  DEFAULT ((0)) FOR [IsSQLClusterNode]
GO

ALTER TABLE [info].[Instance] ADD  DEFAULT ((0)) FOR [IsAlwaysOnNode]
GO

ALTER TABLE [info].[Instance] ADD  DEFAULT ((0)) FOR [IsDecommissioned]
GO

ALTER TABLE [dbo].[DatabaseBackup]  WITH CHECK ADD  CONSTRAINT [FK_DatabaseBackup_Instance_ID] FOREIGN KEY([Instance_ID])
REFERENCES [dbo].[Instance] ([Instance_ID])
GO

ALTER TABLE [dbo].[DatabaseBackup] CHECK CONSTRAINT [FK_DatabaseBackup_Instance_ID]
GO

CREATE FUNCTION [dbo].[Instance_Name]
	( @Instance_ID bigint )  
RETURNS SYSNAME  
AS
BEGIN   
    DECLARE @Name SYSNAME;
	SET @Name = (SELECT Name FROM [dbo].[Instance] WHERE [Instance_ID] = @Instance_ID);

    RETURN @Name;  
END  
GO

CREATE VIEW [info].[vw_DatabaseBackups]
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


CREATE PROCEDURE [staging].[usp_ETL_DatabaseBackup]
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

