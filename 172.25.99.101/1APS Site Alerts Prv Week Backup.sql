/*
	Created by XSUNT\svc-jenkins using dbatools Export-DbaScript for objects on 172.25.99.101 at 08/06/2023 21:22:17
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
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'1APS_Site Alerts Prv Week Backup', 
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
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'step for prv site alerts', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'use bmsheme3
go
declare @TgtTblName varchar(100)
declare @SQL_Tgt1 varchar (500)
declare @SQL_Tgt2 varchar (500)

set @TgtTblName=''tblSiteAlertsProc_''+convert(varchar(8), getDate(), 112)+''_''+ replace(convert(varchar, getdate(),108),'':'','''')
set @SQL_Tgt1=''if Object_ID(N''''''+@TgtTblName+'''''', N''''U'''') is not null drop table '' + @TgtTblName
set @SQL_Tgt2=''select * into '' +@TgtTblName+'' from tblSiteAlertsProc'' 

exec(@SQL_Tgt1)
exec(@SQL_Tgt2)
go

truncate table tblSiteAlertsProc_History
insert into   tblSiteAlertsProc_History 
Select *
from tblSiteAlertsProc

---- wednesday run separately

    EXEC msdb.dbo.sp_send_dbmail
    @profile_name = ''XSUNT ONELOOK-DB-1 SQL Notification'',
    @recipients = ''prateek.singh@xsunt.com;nguillot@xsunt.com'',
    @subject = ''Site Alerts History refreshed'',
    @body = ''Site Alerts History refreshed for the next week'',
    @body_format = ''HTML'',
    @query_no_truncate = 1,
    @attach_query_result_as_file = 0', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'schedule for site alert', 
		@enabled=1, 
		@freq_type=8, 
		@freq_interval=8, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20220415, 
		@active_end_date=99991231, 
		@active_start_time=100000, 
		@active_end_time=235959, 
		@schedule_uid=N'32b3ef37-4c60-49fe-924c-ad9c93d32d07'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:

GO

