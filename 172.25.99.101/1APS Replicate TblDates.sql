/*
	Created by XSUNT\svc-jenkins using dbatools Export-DbaScript for objects on 172.25.99.101 at 08/06/2023 21:24:35
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
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'1APS_Replicate TblDates', 
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
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Create tbl dates NJ 41', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'Declare @sql varchar(max) , @sql2 varchar(max), @db varchar(100) ,@dbprev varchar(100),  @dt varchar(8) = convert(varchar(8),getdate(),112) , @dt_14 varchar(8) = convert(varchar(8),getdate() -14 ,112)
Declare @link1 varchar(100) = ''[COLO-SQL-41]''  , @link2 varchar(100) = ''[ONELOOK-SQL]''

 

select @dbprev = name from ( 
select  distinct NAME ,rank() over ( order by name desc) rk    from [ONELOOK-SQL].MSDB.SYS.DATABASES WHERE NAME LIKE ''%BMSONCOLOGY%V%''  ) xx where rk =1

PRINT @DBPREV
 
set @sql = ''

if object_id(N''''test_prateek..tbldates'''') IS NOT NULL DROP TABLE test_prateek..tbldates


select * into test_prateek..tbldates from ''+@link2+''.''+@dbprev+''.dbo.tbldates

select * from test_prateek..tbldates where Datevar=''''WeekDate''''

''
exec(@sql)
 

DECLARE @html nvarchar(MAX), @body1 Nvarchar(max) , @int int, @dt1 nvarchar(25),@sub nvarchar(100)
EXEC Test_Prateek.[dbo].[spQueryToHtmlTable]  @html = @html OUTPUT,  @query = N''select distinct  Datevar + '''' = '''' +DateValue  Weeklydateupdate
from test_prateek..tbldates where Datevar=''''WeekDate'''''' ,@orderBy = N''ORDER BY 1'';

set @body1 = case when @html is null then ''TBLDates not created in Test_Prateek <br> Regards </br>  PRATEEK'' else ''TBLDates created in Test_Prateek for  WeekDate :   ''+@dbprev+'' <br> <br>''+@html + ''<br> Regards </br>  PRATEEK'' end 
 
  select @int = case when @html is not null then 1 else 0 end 
 select @dt1 =  Convert(nvarchar(25),getdate())
  set @sub = ''TBLDates created in Test_Prateek for  WeekDate ''+@dbprev+''  Run as of : '' +@dt1
  set @body1 =  ''Hi Prateek, <br> <br>''+ @body1
  print @body1 
  print @int
  print @dt1
  print @sub


EXEC msdb.dbo.sp_send_dbmail
    @profile_name = ''XSUNT ONELOOK-DB-1 SQL Notification'',
    @recipients = ''prateek.singh@xsunt.com'',
	 
    @subject = @sub,
    @body = @body1 ,
    @body_format = ''HTML'';
', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'create tbl dates', 
		@enabled=1, 
		@freq_type=8, 
		@freq_interval=32, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20190719, 
		@active_end_date=99991231, 
		@active_start_time=100001, 
		@active_end_time=235959, 
		@schedule_uid=N'0187aae8-78c1-48f1-8af0-b125989e014d'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:

GO

