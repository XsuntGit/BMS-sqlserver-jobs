/*
	Created by XSUNT\svc-jenkins using dbatools Export-DbaScript for objects on 172.25.99.102 at 08/06/2023 21:22:54
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
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'1APS_Single View Production Refresh on PA102_ONC', 
		@enabled=0, 
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
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Refresh Main single View on pa102 - Production', 
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
,@filelog varchar(1000)
,@filelog2 Varchar(1000)
,@combinedfile varchar(2000)
, @messagebody varchar(8000)
set @str1 = cast(FORMAT(GETDATE() , ''yyyyMMdd_HHmmss'') as varchar)
Set @filelog = ''"H:\Work\Scripts\ONC\Production\Run_From_102\Single View\Logs\Step_2_Refresh_Singleview_Production_Logs_''+@str1+''.txt"''
Set @filelog2 = ''"H:\Work\Scripts\ONC\Production\Run_From_102\Single View\Logs\Step_2_Refresh_Singleview_Production_Metadata_Logs_''+@str1+''.txt"''
print @filelog
print @filelog2

DECLARE @html nvarchar(MAX), @body1 Nvarchar(max) , @dt1 varchar(20), @sub varchar(1000)

Set @dt1 = convert( varchar(10), getdate(),101)
set @sub = ''Single View run status on BMSONCSV : ''+ @dt1

If (
Select SV_Production from BMSONCSV_Staging..tblSingleViewRefresh 
 ) =''Y''
Begin 

	set @Sql = ''SQLCMD -S ONELOOK-SQL -i "H:\Work\Scripts\ONC\Production\Run_From_102\Single View\Step_2_Refresh_Singleview.sql" -o ''+@filelog+''''
	EXEC master.sys.xp_cmdshell @Sql

    set @Sql = ''SQLCMD -S ONELOOK-SQL -i "H:\Work\Scripts\ONC\Production\Run_From_102\Single View\Step_2_Refresh_Singleview_Production_Metadata.sql" -o ''+@filelog2+''''
	EXEC master.sys.xp_cmdshell @Sql

  ----If OBJECT_ID(N''BMSONCSV_Production..SingleviewEmailLog'') is not null
  ----drop table BMSONCSV_Production..SingleviewEmailLog


------ Update the Log Table TimeStamp
--insert into   BMSONCSV_Production..SingleviewEmailLog
--  Select *  from 
--[ONELOOK-DB-1].[Log].[dbo].[Log_Backup_Restore_ONELOOKDB1_To_ONELOOKDB2]
--where SingleviewUPdTime is null


------ Display table details
--Select * from 
--[ONELOOK-DB-1].[Log].[dbo].[Log_Backup_Restore_ONELOOKDB1_To_ONELOOKDB2]
--where SingleviewUPdTime is null and [Message] is null

-------Update the table
-- update
--[ONELOOK-DB-1].[Log].[dbo].[Log_Backup_Restore_ONELOOKDB1_To_ONELOOKDB2]
--set  SingleviewUPdTime = getdate()
--where SingleviewUPdTime is null and [Message] is null

--Set  @messagebody  =''Updated Single View Tables ON BMSONCSV_Production''

--update t1
--set t1.singleviewupdtime=t2.Singleviewupdtime
--, t1.[Message]= @messagebody
--from BMSONCSV_Production..SingleviewEmailLog t1
--join [ONELOOK-DB-1].[Log].[dbo].[Log_Backup_Restore_ONELOOKDB1_To_ONELOOKDB2] t2 on 
--t1.dbname=t2.dbname
--and t1.starttime=t2.starttime
--and t1.endtime=t2.endtime
--where t1.Singleviewupdtime is null

------

-- insert into BMSONCSV_Production..tblSingleViewRefresh 
-- (
-- DbName ,
--SV_Production_Refresh ,
--SV_Production_Refresh_Timestamp
-- )
--  Select top 1  DbName,''Y'',SingleViewUpdTime from BMSONCSV_Production..SingleviewEmailLog 
-- order by 1 desc
---update singleview refresh
update
BMSONCSV_Staging..tblSingleViewRefresh 
set SV_Production =''U''
,SV_Production_Timestamp=getdate()


 set @filelog =replace(@filelog,''"'','''')
  set @filelog2 =replace(@filelog2,''"'','''')
  set @combinedfile=@filelog+'';''+@filelog2
 set @messagebody = ''Single View on Production is updated ! ''


 
EXEC [master].[dbo].[spQueryToHtmlTable]  @html = @html OUTPUT,  @query = N''Select *
from BMSONCSV_Staging..tblSingleViewRefresh 
'' ,@orderBy = N''ORDER BY 1 desc'';
print @html
 set @body1 = case when @html is null then ''Single View in Production is not Updated <br> Regards </br>  OneLookOncology Team''
 else ''Hi Team ,<br> <br> ''+@messagebody+''<br> <br> ''+@html + ''<br> Regards </br>  OneLookOncology Team'' end 


EXEC master.dbo.sys_sp_send_dbmail
    @profile_name = ''XSUNT ONELOOK-SQL NOTIFICATION'',
    @recipients = ''prateek.singh@xsunt.com;nguillot@xsunt.com;devin.cannon@xsunt.com;xiao.li@xsunt.com;justin.frey@xsunt.com;zoe.zhuang@xsunt.com;lzubarev@xsunt.com'',
    -----@from_address = ''prateek.singh@xsunt.com'',
    @subject = @sub,
    @body = @body1 ,
    @body_format = ''HTML'',
    @query_no_truncate = 1,
    @file_attachments =@combinedfile


End 

Else 
if(
Select SV_Production from BMSONCSV_Staging..tblSingleViewRefresh ) =''U''

Begin 
  Select @messagebody= ''Single view at Production is already updated on  ''+ max( cast(SV_Production_Timestamp as varchar))  from BMSONCSV_Staging..tblSingleViewRefresh  
  Print @messagebody
  set @filelog =''''
   --set @body1 =   ''Hi Team , <br> ''+@messagebody+''<br> <br><br> Regards </br>  OneLookOncology Team'' 
End

Else 
 

Begin 
  Select @messagebody= ''Please check the SV_Production column should be "Y"  '' 
  Print @messagebody
  set @filelog =''''
  --set @body1 =   ''Hi Team , <br> ''+@messagebody+''<br> <br>  <br> Regards </br>  OneLookOncology Team'' 
End


', 
		@database_name=N'master', 
		@output_file_name=N'H:\Work\Scripts\ONC\Production\Run_From_102\Single View\Logs\Step_2_SingleView_Production_LogfromJob.txt', 
		@flags=2
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'refresh sngle view-Production', 
		@enabled=1, 
		@freq_type=8, 
		@freq_interval=6, 
		@freq_subday_type=4, 
		@freq_subday_interval=5, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20200910, 
		@active_end_date=99991231, 
		@active_start_time=90001, 
		@active_end_time=225959, 
		@schedule_uid=N'8432745c-e342-4d89-a91d-441bfc9815a2'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:

GO

