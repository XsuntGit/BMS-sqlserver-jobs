/*
	Created by XSUNT\svc-jenkins using dbatools Export-DbaScript for objects on 172.25.99.102 at 08/06/2023 21:22:51
	See https://dbatools.io/Export-DbaScript for more information
*/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'1APS_Heme_867_copy', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'No description available.', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'XSUNT\prateek.singh', 
		@notify_email_operator_name=N'Job Failure', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'copy heme 867 to pa102', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'

Declare @sql varchar(8000)
Declare @str1 varchar(1000)
declare @dbname varchar(100)


Select @dbname=DBName from BMSHEME_Config_Test..tblDBConfig WHERE DBType=''WeeklyDB''
print @dbname

set @str1 = cast(FORMAT(GETDATE() , ''yyyyMMdd_HHmmss'') as varchar)

	set @Sql = ''SQLCMD -S ONELOOK-DB-2 -d ''+@dbname+'' -i "W:\Work\Scripts\HEME3.0\867_Heme\Copy_867_Processing_Heme-Staging.sql" -o "W:\Work\Scripts\HEME3.0\867_Heme\Logs\Copy_867_Processing_Heme_''+@dbname+''_test_''+@str1+''.txt"''
	EXEC master.sys.xp_cmdshell @Sql

Select @dbname=DBName from BMSHEME_Config..tblDBConfig WHERE DBType=''WeeklyDB''
print @dbname

set @str1 = cast(FORMAT(GETDATE() , ''yyyyMMdd_HHmmss'') as varchar)

	set @Sql = ''SQLCMD -S ONELOOK-DB-2 -d ''+@dbname+'' -i "W:\Work\Scripts\HEME3.0\867_Heme\Copy_867_Processing_Heme.sql" -o "W:\Work\Scripts\HEME3.0\867_Heme\Logs\Copy_867_Processing_Heme_''+@dbname+''_prod_''+@str1+''.txt"''
	EXEC master.sys.xp_cmdshell @Sql', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'copy 867 heme', 
		@enabled=1, 
		@freq_type=8, 
		@freq_interval=62, 
		@freq_subday_type=4, 
		@freq_subday_interval=5, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20220411, 
		@active_end_date=99991231, 
		@active_start_time=100000, 
		@active_end_time=235900, 
		@schedule_uid=N'91ec69d6-e970-4103-8b7b-713e20e49407'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:

GO

