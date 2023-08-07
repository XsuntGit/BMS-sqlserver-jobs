BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'1APS_Vyvanse DB Cleanup', 
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
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Remove Extra Db', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'use master
go

Declare @copysql varchar(8000)
, @V_SQL nvarchar (4000)
, @ExecSqlResult nvarchar(4000)
, @delsql varchar(8000)
, @killcmd nvarchar(4000)
,@file varchar(2000) 
, @dropsql nvarchar(4000)
,@dbname varchar(500) 
,@restoresql nvarchar(4000) 
,@DbGroup varchar(500)
,@prevdblist varchar(max)


     Select '' Start Drop Dbs :''+ convert(varchar(30) , getdate() ,127)
					  DECLARE drop_cursor CURSOR FOR
			Select Dropsql from (
				Select  ''if exists( select 1 from [Master].sys.databases where Name='''''' +[name]+'''''')
						  DROP DATABASE [''+[name] +'']''   as Dropsql ,
						   rank() over( order by [name] desc ) rk
						  from [Master].sys.databases
					 
						where 1=1 --[Name] like''%''+@dbgroup+''%'' and [Name] not like ''%training%''
						and [Name]  like ''%RedemptionWeekly%''
						) t1   where rk > 2
					OPEN drop_cursor
					FETCH NEXT FROM drop_cursor into @dropsql
					WHILE @@FETCH_STATUS = 0
					BEGIN
 
						print @dropsql
						     --			   if @DbGroup like ''%RedemptionWeekly%''
										 --  Begin 
										 --   Set @V_SQL=@dropsql
											-- EXEC @ExecSqlResult=  master.dbo.sp_executesql @V_SQL
											--  if @ExecSqlResult <> 0
											--	  Begin
											--	  raiserror(@ExecSqlResult,11,1)
											--	  end
											------EXEC [ONELOOK-SQL].master.dbo.sp_executesql  @dropsql
										 --  End 
										 --  ELSE
										 --  Begin
											EXEC (@dropsql)
										 --  End
					     

				      FETCH NEXT FROM drop_cursor into @dropsql
			        END

					CLOSE drop_cursor
					DEALLOCATE drop_cursor', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Tuesday run to clean up vyvanse db', 
		@enabled=1, 
		@freq_type=8, 
		@freq_interval=4, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20211108, 
		@active_end_date=99991231, 
		@active_start_time=100000, 
		@active_end_time=235959, 
		@schedule_uid=N'32d040a5-132d-460a-90eb-be1196f2e891'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:

GO

