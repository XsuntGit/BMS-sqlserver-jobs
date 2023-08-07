/*
	Created by XSUNT\svc-jenkins using dbatools Export-DbaScript for objects on 172.25.99.101 at 08/06/2023 21:22:19
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
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'1APS_Update Date Top200', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'No description available.', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'XSUNT\srv-task', 
		@notify_email_operator_name=N'Job Failure Notification', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Run Create Weekly Integrated demand weeks identification', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=3, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'
Declare @sql varchar(8000)
Declare @str1 varchar(1000)
set @str1 = cast(FORMAT(GETDATE() , ''yyyyMMdd_HHmmss'') as varchar)

	set @Sql = ''SQLCMD -S ONELOOK-DB-1 -i "W:\work\scripts\BMS\OneLook\HEME3.0\Production\Def_Tables\1_Create_tblTimePeriodMaster for Heme_CSCAN.sql" -o "W:\work\scripts\BMS\OneLook\HEME3.0\Production\Automation\WeeklyRun\Logs\1_Create_tblTimePeriodMaster for Heme_CSCAN_''+@str1+''.txt"''
	EXEC master.sys.xp_cmdshell @Sql
', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Update the TOP 200 Dates on Friday', 
		@step_id=2, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'Declare @dt  varchar(8)

select @dt  = convert(varchar(8),CAST(DATEADD(D,7,WK) AS DATE),112)  from bmsonc3..tblwkrollup 
where 1=1
and  tp = ''W1''
 
 Update t1 
 set datevalue = @dt
--SELECT   [DateVar]
--      ,[DateValue]
  FROM [BMSONC3].[dbo].[tblDef_Dates] t1
  WHERE DATEVAR =''Top200MelRCCWeekDate''

  ;

  
   DECLARE @html nvarchar(MAX), @body1 Nvarchar(max) , @int int, @dt1 nvarchar(25),@sub nvarchar(100)
EXEC Test_Prateek.[dbo].[spQueryToHtmlTable]  @html = @html OUTPUT,  @query = N''select distinct *
from [BMSONC3].[dbo].[tblDef_Dates] WHERE DATEVAR =''''Top200MelRCCWeekDate'''''' ,@orderBy = N''ORDER BY 1'';

set @body1 = case when @html is null then ''Mel RCC TOP Date Not Updated <br> Regards </br>  PRATEEK'' else @html + ''<br> Regards </br>  PRATEEK'' end 
 
  select @int = case when @html is not null then 1 else 0 end 
 select @dt1 =  Convert(nvarchar(25),getdate())
  set @sub = ''Top200 MEL RCC Week Date Updated  Run as of : '' +@dt1
  set @body1 = @sub + ''<br> <br> <br>''+ @body1
  print @body1 
  print @int
  print @dt1
  print @sub


EXEC msdb.dbo.sp_send_dbmail
    @profile_name = ''XSUNT ONELOOK-DB-1 SQL Notification'',
    @recipients = ''prateek.singh@xsunt.com'',
       --	@from_address = ''prateek.singh@xsunt.com'',
    @subject = @sub,
    @body = @body1 ,
    @body_format = ''HTML'';
', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 2
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Update the Table for Top 200 Account YER', 
		@enabled=1, 
		@freq_type=8, 
		@freq_interval=32, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20190614, 
		@active_end_date=99991231, 
		@active_start_time=100100, 
		@active_end_time=235959, 
		@schedule_uid=N'ad034e39-3937-4bec-b558-5bf5b455a2b8'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:

GO

