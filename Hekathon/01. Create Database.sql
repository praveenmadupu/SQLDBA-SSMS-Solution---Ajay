use [ServiceCatalog]
go

--	01
create table dbo.Application
(	
	ApplicationID int identity(1,1) not null,
	ApplicationName varchar(150) not null,
	CostCenter varchar(50) not null,
	[Owner] varchar(50) not null,
	PrimaryContact varchar(50) null,
	[Description] varchar(200) null
)
go

-- 02
create procedure dbo.usp_AddApplication
	@ApplicationName varchar(150), 
	@CostCenter varchar(50), 
	@Owner varchar(50), 
	@PrimaryContact varchar(50) = NULL, 
	@Description varchar(200)
AS
BEGIN
	DECLARE @_message VARCHAR(20);
	DECLARE @_isFailure tinyint;
	SET @_isFailure = 0;

	/* Validate data first */
	BEGIN TRY
		IF NOT EXISTS (SELECT * FROM dbo.Application as a where a.ApplicationName like '')
		BEGIN
			INSERT INTO dbo.Application
			(ApplicationName, CostCenter, Owner, PrimaryContact, Description)
			VALUES (@ApplicationName, @CostCenter, @Owner, @PrimaryContact, @Description);
		END
		ELSE
			SET @_message = 'Another application with same name already exists';
			SET @_isFailure = 1;
	END TRY
	BEGIN CATCH
		SET @_message = 'Some error occurred.';
		SET @_isFailure = 1;
	END CATCH

	RETURN @_isFailure;	
END
GO

-- 03) 
create table dbo.RequestHeader
(	
	RequestID int identity(1,1) not null,
	DbPlatform varchar(50) not null,
	Requestor varchar(50) not null,
	RequestTime smalldatetime not null,
	ApplicationName varchar(150) null
)
go


-- 04) 
create table dbo.RequestHeaderDetails
(
	RequestID int identity(1,1) not null,
	NotifyUsers varchar(200) null,
	IsCompleted bit not null,
	Status varchar(50) not null
)
go

-- 05)
create table dbo.DatabasePlatforms
(	PlatformType varchar(50) not null,
	Name varchar(50) not null,
	GroupMailID varchar(50),
	EscalationOwner varchar(50),
	PrimaryContact varchar(50)
)

-- 06)
create table dbo.SqlServerCategoryMSTR
(	CategoryID int identity(1,1),
	CategoryName varchar(50),
	SubCategoryName varchar(50),
	IsAutomatic bit default 1,
	IsManual bit default 0,
)
go

-- 07)
create table dbo.SqlServerCategoryDetailMSTR
(	CategoryID int not null,
	SpecificationName varchar(50),
	SpecificationHelpText varchar(200),
	SpecificationHtmlFieldType varchar(100),
	SpecificationGroup varchar(100),
	SpecificationValues varchar(100) null
)
go

-- 08) 
create table dbo.RequestAssignment
(	RequestID int not null,
	AssignedGroup varchar(50) not null,
	AssignedTo varchar(50) null,
	GroupAssignmentDate smalldatetime not null,
	OwnerAssignmentDate smalldatetime null,
	CurrentStatus varchar(50)
)
go

-- 09)
create table dbo.RequestAssignmentHistory
(	RequestHistoryID bigint identity(1,1),
	RequestID int,
	AssignedGroup varchar(50),
	AssignedTo varchar(50),
	AssignedOn smalldatetime,
	[Status] varchar(50),
	ModifiedDate smalldatetime,
	Comments varchar(200)
)
go

create table dbo.SqlServerRequests_DatabaseCreation
(
	RequestID int not null,
	SqlInstance varchar(50) not null, 
	DatabaseName varchar(50) not null, 
	ApplicationName varchar(50) not null, 
	Requester varchar(50) not null, 
	CreatedDate smalldatetime not null,
	IsPending int default 1
)
go

ALTER PROCEDURE usp_AddRequest_SqlServer_DatabaseCreation
	@pSqlInstance varchar(50), @pDatabaseName varchar(50), @pApplicationName varchar(50), @pRequester varchar(50)
AS
BEGIN
	SET NOCOUNT ON;
	DECLARE @_message VARCHAR(200);
	DECLARE @_isFailure tinyint;
	SET @_isFailure = 0;

	DECLARE @_RequestID INT;

	/* Validate data first */
	BEGIN TRY
		-- Populate [dbo].[RequestHeader]
		INSERT INTO [dbo].[RequestHeader]
		(	[DbPlatform], [Requestor], [RequestTime], [ApplicationName]	)
		VALUES ('SqlServer', @pRequester, GETDATE(), @pApplicationName);

		SELECT @_RequestID = SCOPE_IDENTITY();

		--select * from [TUL1DBAPMTDB1].SQLDBATools.Info.Instance
		INSERT INTO [dbo].[RequestAssignment]
		(	[RequestID], AssignedGroup, AssignedTo, GroupAssignmentDate, OwnerAssignmentDate, CurrentStatus	)
		VALUES (@_RequestID, 'SqlServer', NULL, GETDATE(), NULL, 'Open');
		/* Status could be Open, Assigned, WorkInProgress, Pending, Completed, Closed */		

		INSERT INTO [dbo].[RequestAssignmentHistory]
		(	[RequestID], [AssignedGroup], [AssignedTo], [AssignedOn], [Status], [ModifiedDate], [Comments])
		VALUES (@_RequestID, 'SqlServer', NULL, NULL, 'Open', GETDATE(), 'Request opened by User');		

		--SELECT * FROM dbo.SqlServerRequests_DatabaseCreation
		INSERT INTO dbo.SqlServerRequests_DatabaseCreation
		(	[RequestID], SqlInstance, DatabaseName, ApplicationName, Requester, CreatedDate	)
		VALUES (@_RequestID, @pSqlInstance, @pDatabaseName, @pApplicationName, @pRequester, GETDATE() );
	END TRY
	BEGIN CATCH
		SET @_isFailure = 1;
	END CATCH

	IF @_isFailure = 1
		SET @_message = 'Some error occurred while submitting request.';
	ELSE
		SET @_message = 'Database creation request successfully based with Request id '+CAST(@_RequestID AS VARCHAR(10));	

	SELECT @_message AS [Message];

	RETURN @_isFailure;
	
END
GO

/*
select * from [dbo].[DatabasePlatforms]
select * from [dbo].[RequestAssignment]
select * from [dbo].[RequestHeader]
select * from [dbo].[RequestAssignmentHistory]

select * from [TUL1DBAPMTDB1].SQLDBATools.Info.Instance
*/

