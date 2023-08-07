BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'1APS_DailyRestoreWeeklyDBfromPA102', 
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
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'restore weekl db', 
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
use [master]
go

if object_id (N''LogDR..tblweeklyTablePA102toDR'') IS NOT NULL
DROP TABLE LogDR..tblweeklyTablePA102toDR 
go

Select * into  LogDR..tblweeklyTablePA102toDR 
from  [ONELOOK-SQL].[Log].DBO.tblweeklyTablePA102toDR
                              
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
set @logfile = ''H:\work\scripts\BMS\Production\Logs\RestoreDBWeekly_''+@str1+''.txt''

if (Select  count(*) from LogDR..tblweeklyTablePA102toDR
where Success is null
--and DbName= ''BMSCV2EarlyLook20201225_V1'' 
)>0 
  begin 
	set @Sql = ''SQLCMD -S DR-ONELOOK-DB -i "H:\work\scripts\BMS\Production\RestoreDBWeekly.sql" -o "''+@logfile+''"''
	EXEC master.sys.xp_cmdshell @Sql

	
EXEC [master].[dbo].[spQueryToHtmlTable]  @html = @html OUTPUT,  @query = N''Select DbName,DbcreatedonDR
       ,case when Success =''''Y'''' THEN success else ''''Not Restored'''' end Success  from LogDr..tblweeklyTablePA102toDR'' ,@orderBy = N''ORDER BY 1'';

set @body1 = '' Hi , <br> Status of DB Restore on NJ 40 as below : <br><br>''+

case when @html is null then ''No Databases in the list to be restored on NJ 40 DR -PROD <br> Regards </br>  DR Team'' else @html + ''<br> Regards </br>   DR Team'' end 
 
---------------------Onelook webteam addition
Declare @V_Sql nvarchar (4000) , @dbname nvarchar(500) 

DECLARE Db_cursor CURSOR FOR
SELECT
       distinct Name 
FROM
            [Master].sys.databases
 where 1=1--create_date >= cast(getdate()-2 as date)
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
and name like ''BMSCV3%''
union 
Select ''BMSOneLook''
 
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


SELECT
       distinct Name 
FROM
            [Master].sys.databases
 where 1=1 --create_date >= cast(getdate()-2 as date)
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
and name like ''BMSCV3%''
union 
Select ''BMSOneLook''
 
EXEC  [Master].Dbo.sys_sp_send_dbmail  
    @profile_name = ''XSUNT DR-ONELOOK-DB NOTIFICATION'',
    @recipients = ''prateek.singh@xsunt.com'',
    @subject = ''DR-PRODUCTION Weekly Database Restored on NJ 40 from PA 102'',
    @body = @body1 ,
    --@from_address = ''prateek.singh@xsunt.com'',
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
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'restore weekl dbs on nj 40', 
		@enabled=1, 
		@freq_type=8, 
		@freq_interval=62, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20210118, 
		@active_end_date=99991231, 
		@active_start_time=214500, 
		@active_end_time=235959, 
		@schedule_uid=N'a72a57f9-1153-4399-98a5-c03a3f524d2a'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:

GO

