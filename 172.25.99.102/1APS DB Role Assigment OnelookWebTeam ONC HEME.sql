/*
	Created by XSUNT\svc-jenkins using dbatools Export-DbaScript for objects on 172.25.99.102 at 08/06/2023 21:22:47
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
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'1APS_DB Role Assigment_OnelookWebTeam_ONC_HEME', 
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
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Assign OneLookWebteam to Dbs ONC', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'Use [Master]
 go

Declare @V_Sql nvarchar (4000) , @dbname nvarchar(500) 

DECLARE Db_cursor CURSOR FOR
SELECT
       distinct Name 
FROM
            [Master].sys.databases
 where create_date >= cast(getdate()-2 as date)
 and name not in (
''master''
,''model''
,''msdb''
,''tempdb''
)
and  (name like ''%BMSONC%''
or name like ''%onelook%''
or name like ''%HIT%''
or name like ''%account%''
or name like ''%BMSPR%''
or name like ''%BMSHEME%''
)
and name not  like ''%BMS%CHIN%''
OPEN Db_cursor
FETCH NEXT
FROM
      Db_cursor
INTO
      @dbname
 
WHILE @@FETCH_STATUS = 0
Begin
print @dbname
		set @V_SQL =N''	use '' + @dbName + ''
		                     
							--  ALTER AUTHORIZATION ON SCHEMA::db_owner TO dbo;
							 ALTER AUTHORIZATION ON SCHEMA::db_ddladmin TO dbo;
							 ALTER AUTHORIZATION ON SCHEMA::db_datawriter TO dbo;
							 ALTER AUTHORIZATION ON SCHEMA::db_datareader TO dbo;

							IF  EXISTS (SELECT * FROM sys.database_principals WHERE name = N''''OneLookWebTeam'''')
								DROP USER OneLookWebTeam
 

							CREATE USER [OneLookWebTeam] FOR LOGIN [OneLookWebTeam]

							ALTER USER [OneLookWebTeam] WITH DEFAULT_SCHEMA=[dbo]

							ALTER ROLE [db_datareader] ADD MEMBER [OneLookWebTeam]
							ALTER ROLE [db_datawriter] ADD MEMBER [OneLookWebTeam]
							ALTER ROLE [db_ddladmin] ADD MEMBER [OneLookWebTeam]
							GRANT EXECUTE TO  [OneLookWebTeam]--added by tiger
					
 
			''
		--	print @V_SQL 
			EXEC  master.dbo.sp_executesql @V_SQL
 FETCH NEXT
FROM
      Db_cursor
INTO
      @dbname

End

CLOSE Db_cursor
DEALLOCATE Db_cursor

DECLARE @html nvarchar(MAX), @body1 Nvarchar(max) 
EXEC [master].[dbo].[spQueryToHtmlTable]  @html = @html OUTPUT,  @query = N''SELECT
       distinct Name,cast(create_date as date) as Create_Date
FROM
            [Master].sys.databases
 where create_date >= cast(getdate()-2 as date)
 and name not in (
''''master''''
,''''model''''
,''''msdb''''
,''''tempdb'''')
and name not like ''''%BMS%CHINA%''''

and  (name like ''''%BMSONC%''''
or name like ''''%onelook%''''
or name like ''''%BMSHEME%''''
or name like ''''%HIT%''''
or name like ''''%account%''''
or name like ''''%BMSPR%''''
)
'' ,@orderBy = N''ORDER BY 1'';
print @html

set @body1 = case when @html is null then ''NO Databases asssigned OneLookWebteam DB Roles for ONC <br> Regards </br>  OneLookOncology Team''
 else ''List of Databases OneLookWebTeam DB Role Assigned on PA102 for ONC  <br> <br> ''+@html + ''<br> Regards </br>  OneLookOncology Team'' end 
 


EXEC master.dbo.sys_sp_send_dbmail
    @profile_name = ''XSUNT ONELOOK-SQL NOTIFICATION'',
    @recipients = ''prateek.singh@xsunt.com;devin.cannon@xsunt.com;yancheng.zhou@xsunt.com'',
    ----@from_address = ''prateek.singh@xsunt.com'',
    @subject = ''List of Databases OneLookWebTeam DB Role Assigned on PA102 for ONC'',
    @body = @body1 ,
    @body_format = ''HTML'',
    @query_no_truncate = 1,
    @attach_query_result_as_file = 0;
	 
', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'run to assign2', 
		@enabled=1, 
		@freq_type=8, 
		@freq_interval=3, 
		@freq_subday_type=8, 
		@freq_subday_interval=2, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20230521, 
		@active_end_date=99991231, 
		@active_start_time=0, 
		@active_end_time=235959, 
		@schedule_uid=N'843d1c5c-3a46-44cc-a193-c127f70f51d8'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Runt to assign dbroles', 
		@enabled=1, 
		@freq_type=8, 
		@freq_interval=60, 
		@freq_subday_type=8, 
		@freq_subday_interval=4, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20200820, 
		@active_end_date=99991231, 
		@active_start_time=100001, 
		@active_end_time=235959, 
		@schedule_uid=N'22a6e6cc-f4f6-4d6a-ab65-2a7c53d573a9'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:

GO

