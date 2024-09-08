BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'1APS_NJ39LocalCopyFiles', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'No description available.', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'XSUNT\prateek.singh', 
		@notify_email_operator_name=N'Job Failure Notification', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'to copy local files from 17 server', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'use master
go
Declare @sql varchar(8000)
Declare @str1 varchar(1000)
,@logfile  varchar(1000) 
,@html nvarchar(MAX), @body1 Nvarchar(max) 

set @str1 = cast(FORMAT(GETDATE() , ''yyyyMMdd_HHmmss'') as varchar)
set @logfile = ''H:\Copy_Script_FromPA101\Production\Logs\backupnj39LocalDatasbesforOfflineV2_''+@str1+''.txt''


	set @Sql = ''SQLCMD -S  OneLook-DB-DR-1 -i "H:\Copy_Script_FromPA101\Production\backupnj39LocalDatasbesforOfflineV2.sql" -o "''+@logfile+''"''
	EXEC master.sys.xp_cmdshell @Sql

	
EXEC [master].[dbo].[spQueryToHtmlTable]  @html = @html OUTPUT,  @query = N''Select DIRECTORY [DatabaseName], SUBDIRECTORY [FILENAME] , CREATE_DATE DbcreatedonDR
       ,case when ISUPDATED =''''Y''''  THEN ''''Y'''' else ''''Not Restored'''' end Success  from LogDr..[DatabaseBackupLogonNJ39_COPY] WHERE ISUPDATED =''''Y'''' '' ,@orderBy = N''ORDER BY 1'';

set @body1 = '' Hi , <br> Status of Local DB Restore for   Weekly on NJ 39 as below : <br><br>''+

case when @html is null then ''No Databases in the local   Weekly list to be restored on NJ 39 DR -PROD <br> Regards </br>  DR Team'' else @html + ''<br> Regards </br>   DR Team'' end 
 




EXEC [Master].Dbo.sys_sp_send_dbmail  
    @profile_name = ''XSUNT ONELOOK-DB-DR-1 SQL NOTIFICATION'',
    @recipients = ''prateek.singh@xsunt.com'',
    @subject = ''nj 39  Local Weekly Database Copied  on NJ 39 '',
    @body = @body1 ,
    --@from_address = ''prateek.singh@xsunt.com'',
    @body_format = ''HTML'',
    @query_no_truncate = 1,
	@file_attachments =@logfile;
', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'run every sunday evening 7 pm', 
		@enabled=1, 
		@freq_type=8, 
		@freq_interval=2, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20240906, 
		@active_end_date=99991231, 
		@active_start_time=60000, 
		@active_end_time=235959, 
		@schedule_uid=N'dbd4352f-3a84-4b1f-9631-afd451781ebb'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:

GO

