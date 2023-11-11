BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'1APS_Non Weekly DB  Restore fromPA101', 
		@enabled=0, 
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
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'copy the fles from pa 101', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=3, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'
use [master]
go

if object_id (N''LogDR..tblNonweeklyTablePA101toDR'') IS NOT NULL
DROP TABLE LogDR..tblNonweeklyTablePA101toDR 
go

Select * into  LogDR..tblNonweeklyTablePA101toDR 
from  [ONELOOK-DB-1].[Log].DBO.tblNonweeklyTablePA101toDR
                              
  --update t1 
		--	  set t1.DbcreatedonDR =getdate()
		--	  ,t1.Success =null
		--	  from [ONELOOK-DB-1].[Log].DBO.tblNonweeklyTablePA101toDR t1
		--	  where dbname=''BMSCV2EarlyLook20201225_V1''

   --update t1 
			--  set t1.DbcreatedonDR =getdate()
			--  ,t1.Success =null
			--  from  [LogDR].DBO.tblNonweeklyTablePA101toDR t1
		--	  where dbname=''BMSCV2EarlyLook20201225_V1''

Declare @sql varchar(8000)
Declare @str1 varchar(1000)
,@logfile  varchar(1000) 
,@html nvarchar(MAX), @body1 Nvarchar(max) 

set @str1 = cast(FORMAT(GETDATE() , ''yyyyMMdd_HHmmss'') as varchar)
set @logfile = ''H:\Copy_Script_FromPA101\Production\Logs\RestoreDBNonWeeklyPA101_''+@str1+''.txt''

if (Select  count(*) from LogDR..tblNonweeklyTablePA101toDR
where Success is null
 
)>0 
  begin 
	set @Sql = ''SQLCMD -S OneLook-DB-DR-1 -i "H:\Copy_Script_FromPA101\Production\RestoreDBNonWeeklyPA101.sql" -o "''+@logfile+''"''
	EXEC master.sys.xp_cmdshell @Sql

	
EXEC [master].[dbo].[spQueryToHtmlTable]  @html = @html OUTPUT,  @query = N''Select distinct DbName,DbcreatedonDR
       ,case when Success =''''Y'''' THEN success else ''''Not Restored'''' end Success  from LogDr..tblNonweeklyTablePA101toDR'' ,@orderBy = N''ORDER BY 1'';

set @body1 = '' Hi , <br> Status of DB Restore for Non Weekly on NJ 39 as below : <br><br>''+

case when @html is null then ''No Databases in the Non Weekly list to be restored on NJ 39 DR -PROD <br> Regards </br>  DR Team'' else @html + ''<br> Regards </br>   DR Team'' end 
 


 EXEC msdb.dbo.sp_send_dbmail
    @profile_name = ''XSUNT ONELOOK-DB-DR-1 SQL NOTIFICATION'',
    @recipients = ''prateek.singh@xsunt.com'',
    @subject = ''DB-DR-PRODUCTION on  Non Weekly Database Restored(Copied) on NJ 39 from PA 101'',
    @body = @body1 ,
    --@from_address = ''prateek.singh@xsunt.com'',
    @body_format = ''HTML'',
    @query_no_truncate = 1,
	@file_attachments =@logfile;
   
 END
Else 
Begin 
  print '' All non weekly Databases are already restored''

End 


', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'copy files form pa 101', 
		@enabled=1, 
		@freq_type=8, 
		@freq_interval=1, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20230615, 
		@active_end_date=99991231, 
		@active_start_time=125900, 
		@active_end_time=235959, 
		@schedule_uid=N'3668920b-4051-42d5-810c-acf009501f05'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:

GO

