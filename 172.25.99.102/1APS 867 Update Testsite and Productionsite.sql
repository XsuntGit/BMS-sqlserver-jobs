BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'1APS_867 Update Testsite and Productionsite', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'867 getting updated on test and production', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'XSUNT\prateek.singh', 
		@notify_email_operator_name=N'Job Failure', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'run Store proc', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'exec [sp_867_updateTestandProduction]', 
		@database_name=N'BMSONC_867', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'update onc 3.0 staging', 
		@step_id=2, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'

use BMSOncology3Daily_Staging
go




----------if ((select Testsite from [ONELOOK-DB-1].[Bmsonc_867].dbo.run_867)=''1'' or (select Notificationpost from [ONELOOK-DB-1].[Bmsonc_867].dbo.run_867)=''1'')
if((Select cast(run867_onc30 as date)  from bmsonc_867..tbl867_runONC30 where stat_stg is null)= cast(getdate() as date)
 
)
Begin
Print ''inside to update ONC 3.0 ''


Declare @sql varchar(8000)
Declare @str1 varchar(1000)
,@filelog varchar(1000)
,@filelog2 Varchar(1000)
,@combinedfile varchar(2000)
, @messagebody varchar(8000)
set @str1 = cast(FORMAT(GETDATE() , ''yyyyMMdd_HHmmss'') as varchar)
--Set @filelog = ''"H:\Work\Scripts\ONC\Production\Run_From_102\867 \Logs\Step_2_Refresh_Singleview_Staging_Logs_''+@str1+''.txt"''
--Set @filelog2 = ''"H:\Work\Scripts\ONC\Production\Run_From_102\867 \Logs\Step_2_Refresh_Singleview_Staging_Metadata_Logs_''+@str1+''.txt"''
print @filelog
print @filelog2

 
	set @Sql = ''SQLCMD -S ONELOOK-SQL -i "H:\Work\Scripts\ONC\Production\Run_867_ONC30\Staging\Update_Staging.sql" -o "H:\Work\Scripts\ONC\Production\Run_867_ONC30\Staging\Logs\Update_Staging_''+@str1+''.txt" ''
	EXEC master.sys.xp_cmdshell @Sql

		set @Sql = ''SQLCMD -S ONELOOK-SQL -i "H:\Work\Scripts\ONC\Production\Run_867_ONC30\Staging\tbl867MasterTables.sql" -o "H:\Work\Scripts\ONC\Production\Run_867_ONC30\Staging\Logs\tbl867MasterTables_''+@str1+''.txt" ''
	EXEC master.sys.xp_cmdshell @Sql

		set @Sql = ''SQLCMD -S ONELOOK-SQL -i "H:\Work\Scripts\ONC\Production\Run_867_ONC30\Staging\tbl867_GeoSales.sql" -o "H:\Work\Scripts\ONC\Production\Run_867_ONC30\Staging\Logs\tbl867_GeoSales_''+@str1+''.txt" ''
	EXEC master.sys.xp_cmdshell @Sql

		set @Sql = ''SQLCMD -S ONELOOK-SQL -i "H:\Work\Scripts\ONC\Production\Run_867_ONC30\Staging\tbl867AcctS.sql" -o "H:\Work\Scripts\ONC\Production\Run_867_ONC30\Staging\Logs\tbl867AcctS_''+@str1+''.txt" ''
	EXEC master.sys.xp_cmdshell @Sql

		set @Sql = ''SQLCMD -S ONELOOK-SQL -i "H:\Work\Scripts\ONC\Production\Run_867_ONC30\Staging\tbl867AcctSChild.sql" -o "H:\Work\Scripts\ONC\Production\Run_867_ONC30\Staging\Logs\tbl867AcctSChild_''+@str1+''.txt" ''
	EXEC master.sys.xp_cmdshell @Sql

		set @Sql = ''SQLCMD -S ONELOOK-SQL -i "H:\Work\Scripts\ONC\Production\Run_867_ONC30\Staging\tbl867AcctSRating.sql" -o "H:\Work\Scripts\ONC\Production\Run_867_ONC30\Staging\Logs\tbl867AcctSRating_''+@str1+''.txt" ''
	EXEC master.sys.xp_cmdshell @Sql

		set @Sql = ''SQLCMD -S ONELOOK-SQL -i "H:\Work\Scripts\ONC\Production\Run_867_ONC30\Staging\tbl867AcctSChildRating.sql" -o "H:\Work\Scripts\ONC\Production\Run_867_ONC30\Staging\Logs\tbl867AcctSChildRating_''+@str1+''.txt" ''
	EXEC master.sys.xp_cmdshell @Sql

 
 		set @Sql = ''SQLCMD -S ONELOOK-SQL -i "H:\Work\Scripts\ONC\Production\Run_867_ONC30\Staging\tbl867OrderDet.sql" -o "H:\Work\Scripts\ONC\Production\Run_867_ONC30\Staging\Logs\tbl867OrderDet_''+@str1+''.txt" ''
	EXEC master.sys.xp_cmdshell @Sql

 		set @Sql = ''SQLCMD -S ONELOOK-SQL -i "H:\Work\Scripts\ONC\Production\Run_867_ONC30\Staging\tbl867GS ACCT Linear Trend.sql" -o "H:\Work\Scripts\ONC\Production\Run_867_ONC30\Staging\Logs\tbl867GS ACCT Linear Trend_''+@str1+''.txt" ''
	EXEC master.sys.xp_cmdshell @Sql

 
 ------
 update t1 set T1.stat_stg=''Y'' from bmsonc_867..tbl867_runONC30 t1


DECLARE @html nvarchar(MAX), @body1 Nvarchar(max) , @dt1 varchar(20), @sub varchar(1000)

Set @dt1 = convert( varchar(10), getdate(),101)
set @sub = ''867 run in 3.0 Staging   : ''+ @dt1
----EXEC [master].[dbo].[spQueryToHtmlTable]  @html = @html OUTPUT,  @query = N''Select SV_BMSONC3,SV_BMSONC3_Timestamp, SV_Staging,SV_Staging_Timestamp 
----from 3.0 Staging..tblSingleViewRefresh 
----'' ,@orderBy = N''ORDER BY 1 desc'';
----print @html

----set @body1 = case when @html is null then ''867  in Staging is not Updated <br> Regards </br>  OneLookOncology Team''
---- else ''Hi Team , <br> 867  on 3.0 Staging is updated ! <br> <br> ''+@html + ''<br> Regards </br>  OneLookOncology Team'' end 
 
 Set @body1 =''Hi Team , <br> 867  on 3.0 Staging is updated ! <br> <br>  <br> Regards </br>  OneLookOncology Team''
 ----set @filelog =replace(@filelog,''"'','''')
 ---- set @filelog2 =replace(@filelog2,''"'','''')
 ---- set @combinedfile=@filelog+'';''+@filelog2
EXEC master.dbo.sys_sp_send_dbmail
    @profile_name = ''XSUNT ONELOOK-SQL NOTIFICATION'',
	@recipients = ''prateek.singh@xsunt.com'',
    ---@recipients = ''prateek.singh@xsunt.com;nguillot@xsunt.com;devin.cannon@xsunt.com;xiao.li@xsunt.com;rushil.patel@xsunt.com;zoe.zhuang@xsunt.com;lzubarev@xsunt.com'',
    -----@from_address = ''prateek.singh@xsunt.com'',
    @subject = @sub,
    @body = @body1 ,
    @body_format = ''HTML'',
    @query_no_truncate = 1
  -----  @file_attachments =@combinedfile

End
Else
Print ''Already Updated ONC 3.0 Stg ''

', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'update prod', 
		@step_id=3, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'

use BMSOncology3Daily_Production
go




----------if ((select Testsite from [ONELOOK-DB-1].[Bmsonc_867].dbo.run_867)=''1'' or (select Notificationpost from [ONELOOK-DB-1].[Bmsonc_867].dbo.run_867)=''1'')
if((Select cast(run867_onc30 as date)  from bmsonc_867..tbl867_runONC30 where stat_prod is null)= cast(getdate() as date)
 
)
Begin
Print ''inside to update ONC 3.0 prod ''


Declare @sql varchar(8000)
Declare @str1 varchar(1000)
,@filelog varchar(1000)
,@filelog2 Varchar(1000)
,@combinedfile varchar(2000)
, @messagebody varchar(8000)
set @str1 = cast(FORMAT(GETDATE() , ''yyyyMMdd_HHmmss'') as varchar)
--Set @filelog = ''"H:\Work\Scripts\ONC\Production\Run_From_102\867 \Logs\Step_2_Refresh_Singleview_Production_Logs_''+@str1+''.txt"''
--Set @filelog2 = ''"H:\Work\Scripts\ONC\Production\Run_From_102\867 \Logs\Step_2_Refresh_Singleview_Production_Metadata_Logs_''+@str1+''.txt"''
print @filelog
print @filelog2

 
	set @Sql = ''SQLCMD -S ONELOOK-SQL -i "H:\Work\Scripts\ONC\Production\Run_867_ONC30\Staging\Update_Production.sql" -o "H:\Work\Scripts\ONC\Production\Run_867_ONC30\Staging\Logs\Update_Production_''+@str1+''.txt" ''
	EXEC master.sys.xp_cmdshell @Sql

	-----update HEME Team B 867 20230104

	set @Sql = ''SQLCMD -S ONELOOK-SQL -i "H:\Work\Scripts\ONC\Production\Run_867_ONC30\Staging\TeamB_867\Run867_forTeamB_Processing.sql" -o "H:\Work\Scripts\ONC\Production\Run_867_ONC30\Staging\TeamB_867\Logs\Procesing_Update_Production_''+@str1+''.txt" ''
	EXEC master.sys.xp_cmdshell @Sql
 	-----update HEME Team SUBQ   867 20230104

	set @Sql = ''SQLCMD -S ONELOOK-SQL -i "H:\Work\Scripts\ONC\Production\Run_867_ONC30\Staging\TeamSUBQ_867\Run867_forTeamSUBQ_Processing.sql" -o "H:\Work\Scripts\ONC\Production\Run_867_ONC30\Staging\TeamSUBQ_867\Logs\Procesing_Update_Production_''+@str1+''.txt" ''
	EXEC master.sys.xp_cmdshell @Sql
 
 ------
 update t1 set T1.stat_prod=''Y'' from bmsonc_867..tbl867_runONC30 t1


DECLARE @html nvarchar(MAX), @body1 Nvarchar(max) , @dt1 varchar(20), @sub varchar(1000)

Set @dt1 = convert( varchar(10), getdate(),101)
set @sub = ''867 run in 3.0 Production   : ''+ @dt1
----EXEC [master].[dbo].[spQueryToHtmlTable]  @html = @html OUTPUT,  @query = N''Select SV_BMSONC3,SV_BMSONC3_Timestamp, SV_Production,SV_Production_Timestamp 
----from 3.0 Production..tblSingleViewRefresh 
----'' ,@orderBy = N''ORDER BY 1 desc'';
----print @html

----set @body1 = case when @html is null then ''867  in Production is not Updated <br> Regards </br>  OneLookOncology Team''
---- else ''Hi Team , <br> 867  on 3.0 Production is updated ! <br> <br> ''+@html + ''<br> Regards </br>  OneLookOncology Team'' end 
 
 Set @body1 =''Hi Team , <br> 867  on 3.0 Production is updated ! <br> <br>  <br> Regards </br>  OneLookOncology Team''
 ----set @filelog =replace(@filelog,''"'','''')
 ---- set @filelog2 =replace(@filelog2,''"'','''')
 ---- set @combinedfile=@filelog+'';''+@filelog2
EXEC master.dbo.sys_sp_send_dbmail
    @profile_name = ''XSUNT ONELOOK-SQL NOTIFICATION'',
	@recipients = ''prateek.singh@xsunt.com;nguillot@xsunt.com;justin.frey@xsunt.com;shen.lin@xsunt.com'',
    ---@recipients = ''prateek.singh@xsunt.com;nguillot@xsunt.com;devin.cannon@xsunt.com;xiao.li@xsunt.com;rushil.patel@xsunt.com;zoe.zhuang@xsunt.com;lzubarev@xsunt.com'',
    -----@from_address = ''prateek.singh@xsunt.com'',
    @subject = @sub,
    @body = @body1 ,
    @body_format = ''HTML'',
    @query_no_truncate = 1
  -----  @file_attachments =@combinedfile

End
Else
Print ''Already Updated ONC 3.0  Prod ''

', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Daily 6 PM', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=4, 
		@freq_subday_interval=30, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20190321, 
		@active_end_date=99991231, 
		@active_start_time=64500, 
		@active_end_time=235500, 
		@schedule_uid=N'd5e1bdcc-09c2-4e10-9735-648c149e9595'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:

GO

