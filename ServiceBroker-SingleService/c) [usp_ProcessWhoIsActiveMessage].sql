USE DBA;
GO

IF OBJECT_ID('DBA..usp_ProcessWhoIsActiveMessage') IS NULL
	EXEC ('CREATE PROCEDURE dbo.usp_ProcessWhoIsActiveMessage AS SELECT 1 as Dummy;');
GO

ALTER PROCEDURE dbo.usp_ProcessWhoIsActiveMessage (@p_verbose bit = 0)
AS
BEGIN -- Procedure body
	/*
		Created By:		Ajay Dwivedi
		Updated Date:	Apr 26, 2019
		Modification:	(26-Apr-2019) Creating Proc for 1st time
	*/
	SET NOCOUNT ON;
	
	-- Receive the request and send a reply
	DECLARE @conversation_handle UNIQUEIDENTIFIER;
	DECLARE @message_body XML;
	DECLARE @message_type_name sysname;
	DECLARE @isExecutedOnce bit = 0;
	DECLARE @jobName varchar(255);
	DECLARE @_ErrorMessage varchar(max);
	DECLARE @l_counter INT = 1;
	DECLARE @l_counter_max INT;

	IF EXISTS (SELECT * FROM sys.service_queues WHERE name = 'WhoIsActiveQueue' AND (is_receive_enabled = 0 OR is_enqueue_enabled = 0))
		ALTER QUEUE WhoIsActiveQueue WITH STATUS = ON;

	SELECT @l_counter_max = COUNT(*) FROM WhoIsActiveQueue;

	WHILE @l_counter <= @l_counter_max
	BEGIN -- Loop Body
		BEGIN TRANSACTION;
		--WAITFOR ( 
			RECEIVE TOP(1)
			@conversation_handle = conversation_handle,
			@message_body = message_body,
			@message_type_name = message_type_name
		  FROM WhoIsActiveQueue
		--), TIMEOUT 1000;

		IF (@message_type_name = N'WhoIsActiveMessage')
		BEGIN
			SET @jobName = CAST(@message_body AS XML).value('(/WhoIsActiveMessage)[1]', 'varchar(125)' );

			INSERT DBA..WhoIsActiveCallerDetails (JobName)
			SELECT @jobName AS JobName;

			IF @isExecutedOnce = 0 OR DBA.dbo.fn_IsJobRunning(@jobName) = 1
			BEGIN
				
				IF DBA.dbo.fn_IsJobRunning('DBA - Log_With_sp_WhoIsActive') = 0
					EXEC msdb..sp_start_job @job_name = 'DBA - Log_With_sp_WhoIsActive';
				ELSE
					PRINT 'Job ''DBA - Log_With_sp_WhoIsActive'' is already running.';
				--BEGIN TRY
					--DECLARE	@destination_table VARCHAR(4000);
					--SET @destination_table = 'DBA.dbo.WhoIsActive_ResultSets';

					--EXEC DBA..sp_WhoIsActive @get_full_inner_text=0, @get_transaction_info=1, @get_task_info=2, @get_locks=1, 
					--					@get_avg_time=1, @get_additional_info=1,@find_block_leaders=1, @get_outer_command =1,
					--					@get_plans=2,
					--			@destination_table = @destination_table ;
					SET @isExecutedOnce = 1;
				--END TRY
				--BEGIN CATCH
					
					--SELECT @_ErrorMessage = 'Error No: '+cast(ERROR_NUMBER() as varchar(20))+char(13)+char(10)
					--					+	'Error Severity: '+cast(ERROR_SEVERITY() as varchar(20))+char(13)+char(10)
					--					+	'Error State: '+cast(ERROR_STATE() as varchar(50))+char(13)+char(10)
					--					+	'Error Procedure: '+(ERROR_PROCEDURE())+char(13)+char(10)
					--					+	'Error Line: '+cast(ERROR_LINE() as varchar(50))+char(13)+char(10)
					--					+	'Error Message: '+(ERROR_MESSAGE())+char(13)+char(10);

					--SET @_ErrorMessage = 'DBA..usp_ProcessWhoIsActiveMessage => '+char(13)+char(10)+@_ErrorMessage;

					--PRINT @_ErrorMessage;
				--END CATCH
			END

			END CONVERSATION @conversation_handle;
		END

		-- Remember to cleanup dialogs by handling EndDialog messages 
		ELSE IF (@message_type_name = N'http://schemas.microsoft.com/SQL/ServiceBroker/EndDialog')
		BEGIN
			 END CONVERSATION @conversation_handle;
		END

		COMMIT TRANSACTION;

		WAITFOR DELAY '00:00:05';
		SET @l_counter = @l_counter + 1;
	END -- Loop Body
END -- Procedure body
GO