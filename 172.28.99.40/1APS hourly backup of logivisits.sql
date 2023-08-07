BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'1APS_hourly backup of logivisits', 
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
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'run hourly step', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'use  BMSOnelook_DRSite
 go
 declare @maxdate datetime, @username varchar(20) =''DRSiteQC''

 Select @maxdate = [DateEntered] from  [tblLogVisits] 
 

 insert into  [tblLogVisits] (  [BU] ,[UserName] ,[UserGeo] ,[MP] ,[Market] ,[AccessType] ,[Geo] ,[Page]  ,[DataPeriod]
 ,[DataType]  ,[TimePeriod] ,[Action] ,[Information] ,[Browser] ,[BrowserType] ,[IP] ,[DateEntered] 
  )

Select [BU]
      ,[UserName]
      ,[UserGeo]
      ,[MP]
      ,[Market]
      ,[AccessType]
      ,[Geo]
      ,[Page]
      ,[DataPeriod]
      ,[DataType]
      ,[TimePeriod]
      ,[Action]
      ,  [Information]
      ,[Browser]
      ,[BrowserType]
      ,[IP]
      ,[DateEntered] from [BMSZep_Config].[dbo].[tblLogVisits] 
where  [DateEntered] >@maxdate and  username=@username



 insert into  [tblLogVisits] (  [BU] ,[UserName] ,[UserGeo] ,[MP] ,[Market] ,[AccessType] ,[Geo] ,[Page]  ,[DataPeriod]
 ,[DataType]  ,[TimePeriod] ,[Action] ,[Information] ,[Browser] ,[BrowserType] ,[IP] ,[DateEntered] 
  )
Select [BU]
      ,[UserName]
      ,[UserGeo]
      ,[MP]
      ,[Market]
      ,[AccessType]
      ,[Geo]
      ,[Page]
      ,[DataPeriod]
      ,[DataType]
      ,[TimePeriod]
      ,[Action]
        , [Information]
      ,[Browser]
      ,[BrowserType]
      ,[IP]
      ,[DateEntered] from BMSCV3_Config.[dbo].[tblLogVisits] 
where [DateEntered] >@maxdate and  username=@username
 


 insert into  [tblLogVisits] (  [BU] ,[UserName] ,[UserGeo] ,[MP] ,[Market] ,[AccessType] ,[Geo] ,[Page]  ,[DataPeriod]
 ,[DataType]  ,[TimePeriod] ,[Action] ,[Information] ,[Browser] ,[BrowserType] ,[IP] ,[DateEntered] 
  )
Select [BU]
      ,[UserName]
      ,[UserGeo]
      ,[MP]
      ,[Market]
      ,[AccessType]
      ,[Geo]
      ,[Page]
      ,[DataPeriod]
      ,[DataType]
      ,[TimePeriod]
      ,[Action]
        , [Information]
      ,[Browser]
      ,[BrowserType]
      ,[IP]
      ,[DateEntered] from Orencia3_Config.[dbo].[tblLogVisits] 
where  [DateEntered] >@maxdate and  username=@username


 insert into  [tblLogVisits] (  [BU] ,[UserName] ,[UserGeo] ,[MP] ,[Market] ,[AccessType] ,[Geo] ,[Page]  ,[DataPeriod]
 ,[DataType]  ,[TimePeriod] ,[Action] ,[Information] ,[Browser] ,[BrowserType] ,[IP] ,[DateEntered] 
  ) 

Select [BU]
      ,[UserName]
      ,[UserGeo]
      ,[MP]
      ,[Market]
      ,[AccessType]
      ,[Geo]
      ,[Page]
      ,[DataPeriod]
      ,[DataType]
      ,[TimePeriod]
      ,[Action]
        , [Information]
      ,[Browser]
      ,[BrowserType]
      ,[IP]
      ,[DateEntered] from BMSOneLook.[dbo].[tblLogVisits] 
where  [DateEntered] >@maxdate and  username=@username
 
 insert into  [tblLogVisits] (  [BU] ,[UserName] ,[UserGeo] ,[MP] ,[Market] ,[AccessType] ,[Geo] ,[Page]  ,[DataPeriod]
 ,[DataType]  ,[TimePeriod] ,[Action] ,[Information] ,[Browser] ,[BrowserType] ,[IP] ,[DateEntered] 
  )
Select [BU]
      ,[UserName]
      ,[UserGeo]
      ,[MP]
      ,[Market]
      ,[AccessType]
      ,[Geo]
      ,[Page]
      ,[DataPeriod]
      ,[DataType]
      ,[TimePeriod]
      ,[Action]
        , [Information]
      ,[Browser]
      ,[BrowserType]
      ,[IP]
      ,[DateEntered] from BMSOncology3_Config.[dbo].[tblLogVisits] 
where  [DateEntered] >@maxdate and  username=@username
  
 insert into  [tblLogVisits] (  [BU] ,[UserName] ,[UserGeo] ,[MP] ,[Market] ,[AccessType] ,[Geo] ,[Page]  ,[DataPeriod]
 ,[DataType]  ,[TimePeriod] ,[Action] ,[Information] ,[Browser] ,[BrowserType] ,[IP] ,[DateEntered] 
  )
Select [BU]
      ,[UserName]
      ,[UserGeo]
      ,[MP]
      ,[Market]
      ,[AccessType]
      ,[Geo]
      ,[Page]
      ,[DataPeriod]
      ,[DataType]
      ,[TimePeriod]
      ,[Action]
        , [Information]
      ,[Browser]
      ,[BrowserType]
      ,[IP]
      ,[DateEntered] from BMSVAP3_Config.[dbo].[tblLogVisits] 
where  [DateEntered] >@maxdate  and  username=@username



 insert into  [tblLogVisits] (  [BU] ,[UserName] ,[UserGeo] ,[MP] ,[Market] ,[AccessType] ,[Geo] ,[Page]  ,[DataPeriod]
 ,[DataType]  ,[TimePeriod] ,[Action] ,[Information] ,[Browser] ,[BrowserType] ,[IP] ,[DateEntered] 
  )
Select [BU]
      ,[UserName]
      ,[UserGeo]
      ,[MP]
      ,[Market]
      ,[AccessType]
      ,[Geo]
      ,[Page]
      ,[DataPeriod]
      ,[DataType]
      ,[TimePeriod]
      ,[Action]
        , [Information]
      ,[Browser]
      ,[BrowserType]
      ,[IP]
      ,[DateEntered] from BMSOneLookPR.[dbo].[tblLogVisits]
where  [DateEntered] >@maxdate  and  username=@username


 insert into  [tblLogVisits] (  [BU] ,[UserName] ,[UserGeo] ,[MP] ,[Market] ,[AccessType] ,[Geo] ,[Page]  ,[DataPeriod]
 ,[DataType]  ,[TimePeriod] ,[Action] ,[Information] ,[Browser] ,[BrowserType] ,[IP] ,[DateEntered] 
  )
Select ''ZeposiaUC''[BU]
      ,[UserName]
      ,[UserGeo]
      ,[MP]
      ,[Market]
      ,[AccessType]
      ,[Geo]
      ,[Page]
      ,[DataPeriod]
      ,[DataType]
      ,[TimePeriod]
      ,[Action]
        , [Information]
      ,[Browser]
      ,[BrowserType]
      ,[IP]
      ,[DateEntered] from BMSZepUC_Config.[dbo].[tblLogVisits]
where  [DateEntered] >@maxdate  and  username=@username
 
  EXEC [Master].Dbo.sys_sp_send_dbmail  
    @profile_name = ''XSUNT DR-ONELOOK-DB NOTIFICATION'',
    @recipients = ''prateek.singh@xsunt.com'',
    @subject = ''Hourly backup of LogVisits table on DR Site'',
    @body = ''Hourly backup of LogVisits table DoneT <br> Thanks <br> OneLook DR Team'' ,
    @body_format = ''HTML'',
    @query_no_truncate = 1,
    @attach_query_result_as_file = 0;', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'run hourly to back tables', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=8, 
		@freq_subday_interval=1, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20210408, 
		@active_end_date=99991231, 
		@active_start_time=0, 
		@active_end_time=235959, 
		@schedule_uid=N'83425577-9b22-4170-8652-7d4d40be27af'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:

GO

