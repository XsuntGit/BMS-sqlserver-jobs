BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'1APS_ZipTerrAlignmentChange', 
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
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'ZipTerrAlignmentChange', 
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
 Use BMSRData_CSCAN
 go
 
if ((Select cast(LastLoadDate as date) 
from BMSRData_CSCAN..cnfgTableLoadList  where TableName =''tblZipAlignment'' )  = cast(getdate() as date) )
and (  (Select  cast(ZipTerrdate as date)  from  tblEmailZipTerrAlignChange)  < cast(getdate() as date)    ) 
Begin
  DECLARE @html nvarchar(MAX), @body1 Nvarchar(max) , @body2 Nvarchar(max) , @html2 nvarchar(MAX) , @body3 Nvarchar(max)
  , @date1 varchar(100) , @sub1 varchar(8000)
EXEC Test_Prateek.[dbo].[spQueryToHtmlTable]  @html = @html OUTPUT,  @query = N''select terr  [Added Territory]
from (
select distinct terr from BMSRData_CSCAN..tblZIPAlignment with(nolock)
except
select distinct terr from BMSRData_CSCAN_Backup.dbo.tblZIPAlignment_Prv with(nolock)
) a
'' ,@orderBy = N''order by [Added Territory]'';

EXEC Test_Prateek.[dbo].[spQueryToHtmlTable]  @html = @html2 OUTPUT,  @query = N''select terr  [Removed Territory]
from (
select distinct terr from BMSRData_CSCAN_Backup.dbo.tblZIPAlignment_Prv with(nolock)
except
select distinct terr from BMSRData_CSCAN..tblZIPAlignment with(nolock)
) a
'' ,@orderBy = N''order by[Removed Territory]'';

set @body1 =''Zip to terr alignment Added:  <br> <br> ''+ case when @html is null then ''<Strong> No Territories  </Strong> Added <br> <br> '' 
 else '' <br> <br> '' + @html  end
 
set @body2 = ''Zip to terr alignment Removed: <br> <br> ''   + case when @html2 is null then '',<Strong> No Territories  </Strong> Removed <br> <br>'' 
 else  '' <br> <br> '' + @html2 end

 Set @body3 =  @body1 + @body2 + ''<br> <br> Regards </br>  Data Loading Team''

 
 delete from tblEmailZipTerrAlignChange
 insert into  tblEmailZipTerrAlignChange  values (getdate())
 

Set  @date1 =  convert(varchar(30),  getdate() ) 
set @sub1 =  ''Zip / Territory Added or Removed List  as of :'' +@date1
EXEC master.dbo.sys_sp_send_dbmail
    @profile_name = ''XSUNT ONELOOK-DB-1 SQL Notification'',
	@recipients = ''prateek.singh@xsunt.com;yancheng.zhou@xsunt.com;lsun@xsunt.com'',
    ----@recipients = ''prateek.singh@xsunt.com;xiao.li@xsunt.com;nguillot@xsunt.com;rushil.patel@xsunt.com'',
   ----@from_address = ''prateek.singh@xsunt.com'',
    @subject = @sub1 ,
    @body = @body3 ,
    @body_format = ''HTML'',
    @query_no_truncate = 1,
    @attach_query_result_as_file = 0;
 
 End

 Else 
 Begin
 Print '' Error ''
 
  End

', 
		@database_name=N'BMSRData_CSCAN', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Zip ter alignment', 
		@enabled=1, 
		@freq_type=8, 
		@freq_interval=32, 
		@freq_subday_type=4, 
		@freq_subday_interval=30, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20211122, 
		@active_end_date=99991231, 
		@active_start_time=120000, 
		@active_end_time=145959, 
		@schedule_uid=N'1f49b072-adec-4fdb-9f6c-bd7dd7304364'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:

GO

