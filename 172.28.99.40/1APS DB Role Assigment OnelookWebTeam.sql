﻿BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'1APS_DB Role Assigment_OnelookWebTeam', 
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
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'ASSIGN ONELOOKTEAM ROLE', 
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
and not  (name like ''%BMSONC%''
or name like ''%onelook%''
or name like ''%HIT%''
or name like ''%account%''
)
and name not  like ''%BMS%CHIN%''
and name not like ''%XsuntAdmin%''
and name not like ''%BMSHEME%''
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

					IF  EXISTS (SELECT * FROM sys.database_principals WHERE name = N''''XSUNT\OneLook_SQL_RW'''')
					DROP USER [XSUNT\OneLook_SQL_RW]
				CREATE USER [XSUNT\OneLook_SQL_RW]FOR LOGIN [XSUNT\OneLook_SQL_RW]
				ALTER USER [XSUNT\OneLook_SQL_RW] WITH DEFAULT_SCHEMA=[dbo]
				ALTER ROLE [db_datareader] ADD MEMBER [XSUNT\OneLook_SQL_RW]
				ALTER ROLE [db_datawriter] ADD MEMBER [XSUNT\OneLook_SQL_RW]
				ALTER ROLE [db_ddladmin] ADD MEMBER [XSUNT\OneLook_SQL_RW]
				GRANT EXECUTE TO  [XSUNT\OneLook_SQL_RW]        


				IF  EXISTS (SELECT * FROM sys.database_principals WHERE name = N''''XSUNT\OneLook_SQL_Admins'''')
					DROP USER [XSUNT\OneLook_SQL_Admins]
				CREATE USER [XSUNT\OneLook_SQL_Admins]FOR LOGIN [XSUNT\OneLook_SQL_Admins]
				ALTER USER [XSUNT\OneLook_SQL_Admins] WITH DEFAULT_SCHEMA=[dbo]
				ALTER ROLE [db_datareader] ADD MEMBER [XSUNT\OneLook_SQL_Admins]
				ALTER ROLE [db_datawriter] ADD MEMBER [XSUNT\OneLook_SQL_Admins]
				ALTER ROLE [db_ddladmin] ADD MEMBER [XSUNT\OneLook_SQL_Admins]
				GRANT EXECUTE TO  [XSUNT\OneLook_SQL_Admins]

				IF  EXISTS (SELECT * FROM sys.database_principals WHERE name = N''''XSUNT\OneLook_SQL_RO'''')
					DROP USER [XSUNT\OneLook_SQL_RO]
				CREATE USER [XSUNT\OneLook_SQL_RO]FOR LOGIN [XSUNT\OneLook_SQL_RO]
				ALTER USER [XSUNT\OneLook_SQL_RO] WITH DEFAULT_SCHEMA=[dbo]
				ALTER ROLE [db_datareader] ADD MEMBER [XSUNT\OneLook_SQL_RO]
 
 
			''
		 	print @V_SQL 
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
and name not like ''''%XsuntAdmin%''''
and name not like ''''%BMSHEME%''''
and not  (name like ''''%BMSONC%''''
or name like ''''%onelook%''''
or name like ''''%HIT%''''
or name like ''''%account%''''
)
'' ,@orderBy = N''ORDER BY 1'';
print @html

set @body1 = case when @html is null then ''NO Databases asssigned OneLookWebTeam DB Role Assigned on NJ40  <br> Regards </br>  OneLookNEURO Team''
 else ''List of Databases OneLookWebTeam DB Role Assigned on NJ40   <br> <br> ''+@html + ''<br> Regards </br>  OneLookNEURO Team'' end 
 


EXEC master.dbo.sys_sp_send_dbmail
    @profile_name = ''XSUNT DR-OneLook-DB NOTIFICATION'',
    @recipients = ''prateek.singh@xsunt.com;alen.zhang@xsunt.com;akorolev@xsunt.com;nathan.siviy@xsunt.com;tianyuan.gu@xsunt.com'',
    ----@from_address = ''prateek.singh@xsunt.com'',
    @subject = ''List of Databases OneLookWebTeam DB Role Assigned on NJ40 '',
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
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'ASSIGN ROLES EVERY 2 HOURS', 
		@enabled=1, 
		@freq_type=8, 
		@freq_interval=62, 
		@freq_subday_type=8, 
		@freq_subday_interval=5, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20240627, 
		@active_end_date=99991231, 
		@active_start_time=0, 
		@active_end_time=235959, 
		@schedule_uid=N'87c58ea0-d5e0-440e-8ae3-b13b8c7f3ddd'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:

GO

