/*
	Created by XSUNT\svc-jenkins using dbatools Export-DbaScript for objects on 172.25.99.101 at 08/06/2023 21:24:40
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
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'1APS_Vyvanse DB Restore on PA 101', 
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
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'run Vyvanse Script', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'--from Log..tblweeklyTableNJ42toPA
use [master]
go

if object_id (N''Log..tblweeklyTableNJ42toPA'') IS NOT NULL
DROP TABLE Log..tblweeklyTableNJ42toPA 
go

Select * into  Log..tblweeklyTableNJ42toPA 
from  [COLO-SQL-42].[Log].DBO.tblweeklyTableNJ42toPA
                              
  --update t1 
		--	  set t1.DbcreatedonDR =getdate()
		--	  ,t1.Success =null
		--	  from [ONELOOK-SQL].[Log].DBO.tblweeklyTablePA102toDR t1
		--	  where dbname=''BMSCV2EarlyLook20201225_V1''

   --update t1 
			--  set t1.DbcreatedonDR =getdate()
			--  ,t1.Success =null
			--  from  [LogDR].DBO.tblweeklyTablePA102toDR t1
		--	  where dbname=''BMSCV2EarlyLook20201225_V1''

Declare @sql varchar(8000)
Declare @str1 varchar(1000)
,@logfile  varchar(1000) 
,@html nvarchar(MAX), @body1 Nvarchar(max) 

set @str1 = cast(FORMAT(GETDATE() , ''yyyyMMdd_HHmmss'') as varchar)
set @logfile = ''W:\work\scripts\Vyvanse\Logs\RestoreDBWeekly_''+@str1+''.txt''

if (Select  count(*) from Log..tblweeklyTableNJ42toPA
where Success is null
 
)>0 
  begin 
	set @Sql = ''SQLCMD -S ONELOOK-DB-1 -i "W:\work\scripts\Vyvanse\RestoreDBWeekly.sql" -o "''+@logfile+''"''
	EXEC master.sys.xp_cmdshell @Sql

	
EXEC [Test_Prateek].[dbo].[spQueryToHtmlTable]  @html = @html OUTPUT,  @query = N''Select DbName,DbcreatedonDR
       ,case when Success =''''Y'''' THEN success else ''''Not Restored'''' end Success  from Log..tblweeklyTableNJ42toPA
	   
	   '' ,@orderBy = N''ORDER BY 1'';

set @body1 = '' Hi , <br> Status of DB Restore on PA 101 as below : <br><br>''+

case when @html is null then ''No Databases in the list to be restored on PA 101 DR -PROD <br> Regards </br>  DR Team'' else @html + ''<br> Regards </br>   DR Team'' end 
 


EXEC master.dbo.sys_sp_send_dbmail
    @profile_name = ''XSUNT ONELOOK-DB-1 SQL NOTIFICATION'',
    @recipients = ''prateek.singh@xsunt.com;akorolev@xsunt.com'',
    @subject = ''Vyvanse Weekly Database Restored on PA 101 from NJ 42'',
    @body = @body1 ,
    --@from_address =  ''prateek.singh@xsunt.com;akorolev@xsunt.com'',
    @body_format = ''HTML'',
    @query_no_truncate = 1,
	@file_attachments =@logfile;
   


 End 

Else 
Begin 
  print '' All weekly Databases are already restored''

End 


', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'run the script for Vyvanse Project', 
		@enabled=1, 
		@freq_type=8, 
		@freq_interval=2, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20210221, 
		@active_end_date=99991231, 
		@active_start_time=120000, 
		@active_end_time=235959, 
		@schedule_uid=N'225106e0-a9cc-43e6-b9d8-72ec66f4e895'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:

GO

