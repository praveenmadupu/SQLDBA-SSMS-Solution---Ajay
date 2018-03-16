/*
	Created By:		Ajay Dwivedi
	Created Date:	09-Feb-2018
	Updated Date:	09-Feb-2018
	Version:		0.0
	Purpose:		Create/Update Mail Profile in TiVo SQL Server Environments
*/ 
SET NOCOUNT ON;

--	Declare variables and other objects
DECLARE @DisplayName SYSNAME;
DECLARE @ProfilesAccounts TABLE 
(	profile_id INT, 
	profile_name SYSNAME, 
	account_id INT, 
	account_name SYSNAME, 
	sequence_number INT
);
DECLARE @profile_name SYSNAME, @account_name SYSNAME, @sequence_number INT;


-- Set Display name with Instance/Server Name
SET @DisplayName = 'SQL Alerts - '+@@SERVERNAME;

-- Find out all Mail Profiles with Account Details
INSERT @ProfilesAccounts
EXECUTE msdb.dbo.sysmail_help_profileaccount_sp @profile_name = @@SERVERNAME ;  

-- Create Mail Account and Attach it with Profile if NOT EXISTS
	-- Also, change the Sequence Number for other accounts
IF NOT EXISTS (SELECT * FROM @ProfilesAccounts as a WHERE a.profile_name = @@SERVERNAME AND a.account_name = 'SQLAlerts')
	OR EXISTS (SELECT * FROM @ProfilesAccounts as a WHERE a.profile_name = @@SERVERNAME AND a.account_name <> 'SQLAlerts' AND sequence_number = 1)
BEGIN
	-- Create Database Mail account for SQLAlerts if NOT EXISTS
	IF NOT EXISTS (SELECT * FROM [msdb]..[sysmail_account] as a WHERE a.name = 'SQLAlerts')
	BEGIN
		EXECUTE msdb.dbo.sysmail_add_account_sp  
			@account_name = 'SQLAlerts',  
			@description = 'Mail account for alerts',  
			@email_address = 'SQLAlerts@tivo.com',--'SQLAlerts@RoviCorp.com',  
			@replyto_address = 'IT-Ops-DBA@tivo.com',  
			@display_name = @DisplayName,  
			@mailserver_name = 'relay.corporate.local';
	END

	-- Create a Database Mail profile if NOT EXISTS 
	IF NOT EXISTS ( SELECT * FROM msdb..sysmail_profile as p WHERE p.name = @@SERVERNAME )
	BEGIN
		EXECUTE msdb.dbo.sysmail_add_profile_sp  
			@profile_name = @@SERVERNAME,  
			@description = 'Local default mail profile' ;
	END

	-- Add the account to the profile
	IF NOT EXISTS (SELECT * FROM @ProfilesAccounts as a WHERE a.profile_name = @@SERVERNAME AND a.account_name = 'SQLAlerts')
	EXECUTE msdb.dbo.sysmail_add_profileaccount_sp  
		@profile_name = @@SERVERNAME,  
		@account_name = 'SQLAlerts',  
		@sequence_number = 1 ;  

	-- Update Existing Account Sequence Number greater than 1
	IF EXISTS (SELECT * FROM @ProfilesAccounts as a WHERE a.profile_name = @@SERVERNAME AND a.account_name <> 'SQLAlerts' AND sequence_number = 1)
	BEGIN
		DECLARE C CURSOR LOCAL FAST_FORWARD FOR
			SELECT a.profile_name, a.account_name, a.sequence_number 
			FROM @ProfilesAccounts as a 
			WHERE a.profile_name = @@SERVERNAME AND a.account_name <> 'SQLAlerts';

		OPEN C; 
		FETCH C INTO @profile_name, @account_name, @sequence_number ;

		WHILE (@@FETCH_STATUS = 0)
		BEGIN
			SET @sequence_number = @sequence_number + 1;
			-- Modify Sequence Number
			EXECUTE msdb..sysmail_update_profileaccount_sp
					@profile_name = @profile_name
					,@account_name = @account_name
					,@sequence_number = @sequence_number;

			FETCH C INTO @profile_name, @account_name, @sequence_number;
		END
	END

	-- Grant access to the profile to all users in the msdb database if NOT EXISTS
	IF EXISTS (SELECT * FROM msdb..sysmail_profile as p 
							inner join msdb..sysmail_principalprofile AS pp
						ON pp.profile_id = p.profile_id AND p.name = @@SERVERNAME
	)
	BEGIN
		EXECUTE msdb.dbo.sysmail_add_principalprofile_sp  
			@profile_name = @@SERVERNAME,  
			@principal_name = 'public',
			@is_default = 1 ;
	END
END;


-- Test Mail Profile by Sending Dummy Mail
	EXEC msdb.dbo.sp_send_dbmail  
		@profile_name = @@SERVERNAME,  
		@recipients = 'ajay.dwivedi@tivo.com;nasir.malik@tivo.com',  
		--@copy_recipients = 'IT-Ops-DBA@tivo.com',
		@body = 'This is Test Mail. Kindly verify the EMail Account and Display Name.',  
		@subject = 'Test Mail for New SQLAlerts Account' ;

/*

(1 row affected)
Msg 2627, Level 14, State 1, Procedure sysmail_add_principalprofile_sp, Line 35 [Batch Start Line 0]
Violation of PRIMARY KEY constraint 'SYSMAIL_PRINCIPALPROFILE_ProfilePrincipalMustBeUnique'. Cannot insert duplicate key in object 'dbo.sysmail_principalprofile'. The duplicate key value is (1, 0x00).
The statement has been terminated.
Mail queued.


(2 rows affected)
Msg 2627, Level 14, State 1, Procedure sysmail_add_principalprofile_sp, Line 35 [Batch Start Line 7]
Violation of PRIMARY KEY constraint 'SYSMAIL_PRINCIPALPROFILE_ProfilePrincipalMustBeUnique'. Cannot insert duplicate key in object 'dbo.sysmail_principalprofile'. The duplicate key value is (1, 0x00).
The statement has been terminated.

*/
		
/*
EXEC sp_configure 'show advanced options', 1;  
GO  
RECONFIGURE;  
GO  
EXEC sp_configure 'Database Mail XPs', 1;  
GO  
RECONFIGURE  
GO 
*/