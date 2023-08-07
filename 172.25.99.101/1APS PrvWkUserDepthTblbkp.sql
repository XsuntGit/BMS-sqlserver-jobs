BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'1APS_PrvWkUserDepthTblbkp', 
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
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Backup step for prv wk for user depth table', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=3, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'Use BMSONC3
GO


declare @CurrDate date
	declare @Sql varchar(8000)
	declare @FileLocation varchar(255)
	,@txt1 as varchar(4000) ,@txt2 as varchar(max) , @rowcount as bigint, @maxdate as varchar(20)
	,@sub nvarchar(100)
 DECLARE @html nvarchar(MAX), @body1 Nvarchar(max) , @int int, @dt1 nvarchar(25)
   select @int = case when @html is not null then 1 else 0 end 
 select @dt1 =  Convert(nvarchar(25),getdate())

Begin try
If object_id(N''tblOYUsers_PrvWK_ONC30'', N''U'') is not null
 drop table tblOYUsers_PrvWK_ONC30

select * into tblOYUsers_PrvWK_ONC30
from tblOYInitiators_Proc_ONC30 
where [Target] = ''Y'' and SalesTgt = ''Y''

      
	  set @sub= ''Success -  Prv Week User Depth Report Table Backed up as of : ''
	  Select @rowcount=count(*) from tblOYUsers_PrvWK_ONC30
 	  set @txt2 = ''No. of Records in  <Strong> tblOYUsers_PrvWK_ONC30  </Strong> --->   <Strong>'' +  cast(format(@rowcount  , ''N0'') as varchar) +''</Strong>''
	  
    set @body1 =   ''Hi Team,<br> <br> Prv Week User Depth Report Table Backed  as of : <Strong>''+@dt1+''</Strong> <br><br>''
    +@txt2 +'' <br><br><br><br>  Thanks , <br> OneLookOncology Team <br>'' 

End try

Begin catch


  set @body1 =  ''Hi Team,  <br><br>''
  +''There is an error in  Prv Week User Depth Report Table  backup processing as below.Please look into the same. <br><br>''
 +''<b> ERROR_NUMBER: ''+cast( ERROR_NUMBER() as varchar)+''| ''+
 ''ERROR_SEVERITY: ''+cast( ERROR_SEVERITY() as varchar)+''| ''+
 ''ERROR_STATE: ''+cast( ERROR_STATE() as varchar)+''| ''+
 ''ERROR_LINE: ''+cast( ERROR_LINE() as varchar)+''| ''+
 ''ERROR_MESSAGE: ''+convert( varchar(8000), ERROR_MESSAGE() )
 +''</b>  <br><br>
 Thanks , <br>
OneLookOncology Team <br>'' 
 
   set @sub = ''Error -  Prv Week User Depth Report Table Backed up as of : '' +@dt1
  set @body1 =  @body1



End catch


 

 
   set @sub = @sub +@dt1
  set @body1 =  @body1
  EXEC msdb.dbo.sp_send_dbmail
    @profile_name = ''XSUNT ONELOOK-DB-1 SQL NOTIFICATION'',
	----@recipients = ''prateek.singh@xsunt.com'',
    @recipients = ''prateek.singh@xsunt.com;nguillot@xsunt.com;rushil.patel@xsunt.com'',
 
    @subject = @sub,
    @body = @body1 ,
    @body_format=''HTML''', 
		@database_name=N'BMSONC3', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Backup Heme version', 
		@step_id=2, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'
Use BMSHEME3
GO


declare @CurrDate date
	declare @Sql varchar(8000)
	declare @FileLocation varchar(255)
	,@txt1 as varchar(4000) ,@txt2 as varchar(max) , @rowcount as bigint, @maxdate as varchar(20)
	,@sub nvarchar(100)
 DECLARE @html nvarchar(MAX), @body1 Nvarchar(max) , @int int, @dt1 nvarchar(25)
   select @int = case when @html is not null then 1 else 0 end 
 select @dt1 =  Convert(nvarchar(25),getdate())

Begin try
If object_id(N''tblOYUsers_PrvWK_HEME'', N''U'') is not null
 drop table tblOYUsers_PrvWK_HEME

select * into tblOYUsers_PrvWK_HEME
from tblOYInitiators_Proc_HEME 
--where [Target] = ''Y'' and SalesTgt = ''Y''

      
	  set @sub= ''Success -  Prv Week User Depth Report Table Backed up as of : ''
	  Select @rowcount=count(*) from tblOYUsers_PrvWK_HEME
 	  set @txt2 = ''No. of Records in  <Strong> tblOYUsers_PrvWK_HEME  </Strong> --->   <Strong>'' +  cast(format(@rowcount  , ''N0'') as varchar) +''</Strong>''
	  
    set @body1 =   ''Hi Team,<br> <br> Prv Week User Depth Report Table Backed  as of : <Strong>''+@dt1+''</Strong> <br><br>''
    +@txt2 +'' <br><br><br><br>  Thanks , <br> OneLookOncology Team <br>'' 

End try

Begin catch


  set @body1 =  ''Hi Team,  <br><br>''
  +''There is an error in  Prv Week User Depth Report Table  backup processing as below.Please look into the same. <br><br>''
 +''<b> ERROR_NUMBER: ''+cast( ERROR_NUMBER() as varchar)+''| ''+
 ''ERROR_SEVERITY: ''+cast( ERROR_SEVERITY() as varchar)+''| ''+
 ''ERROR_STATE: ''+cast( ERROR_STATE() as varchar)+''| ''+
 ''ERROR_LINE: ''+cast( ERROR_LINE() as varchar)+''| ''+
 ''ERROR_MESSAGE: ''+convert( varchar(8000), ERROR_MESSAGE() )
 +''</b>  <br><br>
 Thanks , <br>
OneLookOncology Team <br>'' 
 
   set @sub = ''Error -  Prv Week User Depth Report Table Backed up as of : '' +@dt1
  set @body1 =  @body1



End catch


 
 
   set @sub = @sub +@dt1
  set @body1 =  @body1
  EXEC msdb.dbo.sp_send_dbmail
    @profile_name = ''XSUNT ONELOOK-DB-1 SQL NOTIFICATION'',
	----@recipients = ''prateek.singh@xsunt.com'',
    @recipients = ''prateek.singh@xsunt.com;nguillot@xsunt.com'',
 
    @subject = @sub,
    @body = @body1 ,
    @body_format=''HTML''', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'run every wed for prv wk bkp', 
		@enabled=1, 
		@freq_type=8, 
		@freq_interval=2, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20210331, 
		@active_end_date=99991231, 
		@active_start_time=220001, 
		@active_end_time=235959, 
		@schedule_uid=N'9329833d-7c70-4292-b753-49e24ee6cb76'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:

GO

