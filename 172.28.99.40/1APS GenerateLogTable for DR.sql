BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'1APS_GenerateLogTable for DR', 
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
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'generate Log Table for DR Step1', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'use LogDR
go

if OBJECT_ID(N''tblDRLogVisits'') is not null
drop table tblDRLogVisits
go

; With 

C as
(
Select Distinct top 1 BU 
from [BMSZep_Config].[dbo].[tblLogVisits] 
union
Select Distinct top 1 BU 
from BMSCV3_Config.[dbo].[tblLogVisits]
union
Select Distinct top 1 BU 
from Orencia3_Config.[dbo].[tblLogVisits] 
union
Select Distinct  BU 
from  BMSOneLook.[dbo].[tblLogVisits] 
where cast(dateentered as date) >= cast(getdaTE() -7  as date) and BU not in (''IMM2'',''CV2'',''CV 2.0'' )
union
Select Distinct top 1 BU 
from BMSOncology3_Config.[dbo].[tblLogVisits]
union
Select Distinct top 1 BU 
from BMSVAP3_Config.[dbo].[tblLogVisits]
union
Select Distinct top 1 BU 
from BMSOneLookPR.[dbo].[tblLogVisits]
union 
Select Distinct top 1 ''ZeposiaUC''[BU]
from BMSZepUC_Config.[dbo].[tblLogVisits]


)
, C2  as
(

Select Min (Dt) as Dt from DimDate 
where Dt between ( Select dateadd (d,-5,SatweekDt ) from Dimdate
where dt  =cast(getdate() as date)) and  
(
Select  SatweekDt  from Dimdate
where dt  =cast(getdate() as date) )
 
)

 
,
c1 as (

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
      ,cast([Information] as nvarchar) [Information]
      ,[Browser]
      ,[BrowserType]
      ,[IP]
      ,[DateEntered] from BMSOnelook_DRSite.[dbo].[tblLogVisits] 
where cast(dateentered as date) >= ( Select Dt from C2)
--order by DateEntered  desc
--union

--Select [BU]
--      ,[UserName]
--      ,[UserGeo]
--      ,[MP]
--      ,[Market]
--      ,[AccessType]
--      ,[Geo]
--      ,[Page]
--      ,[DataPeriod]
--      ,[DataType]
--      ,[TimePeriod]
--      ,[Action]
--        ,cast([Information] as nvarchar) [Information]
--      ,[Browser]
--      ,[BrowserType]
--      ,[IP]
--      ,[DateEntered] from BMSCV3_Config.[dbo].[tblLogVisits] 
--where cast(dateentered as date) >= ( Select Dt from C2)
----order by DateEntered  desc

--union

--Select [BU]
--      ,[UserName]
--      ,[UserGeo]
--      ,[MP]
--      ,[Market]
--      ,[AccessType]
--      ,[Geo]
--      ,[Page]
--      ,[DataPeriod]
--      ,[DataType]
--      ,[TimePeriod]
--      ,[Action]
--        ,cast([Information] as nvarchar) [Information]
--      ,[Browser]
--      ,[BrowserType]
--      ,[IP]
--      ,[DateEntered] from Orencia3_Config.[dbo].[tblLogVisits] 
--where cast(dateentered as date) >= ( Select Dt from C2)
--union 

--Select [BU]
--      ,[UserName]
--      ,[UserGeo]
--      ,[MP]
--      ,[Market]
--      ,[AccessType]
--      ,[Geo]
--      ,[Page]
--      ,[DataPeriod]
--      ,[DataType]
--      ,[TimePeriod]
--      ,[Action]
--        ,cast([Information] as nvarchar) [Information]
--      ,[Browser]
--      ,[BrowserType]
--      ,[IP]
--      ,[DateEntered] from BMSOneLook.[dbo].[tblLogVisits] 
--where cast(dateentered as date) >= ( Select Dt from C2)
--union 

--Select [BU]
--      ,[UserName]
--      ,[UserGeo]
--      ,[MP]
--      ,[Market]
--      ,[AccessType]
--      ,[Geo]
--      ,[Page]
--      ,[DataPeriod]
--      ,[DataType]
--      ,[TimePeriod]
--      ,[Action]
--        ,cast([Information] as nvarchar) [Information]
--      ,[Browser]
--      ,[BrowserType]
--      ,[IP]
--      ,[DateEntered] from BMSOncology3_Config.[dbo].[tblLogVisits] 
--where cast(dateentered as date) >= ( Select Dt from C2)
--union 
--Select [BU]
--      ,[UserName]
--      ,[UserGeo]
--      ,[MP]
--      ,[Market]
--      ,[AccessType]
--      ,[Geo]
--      ,[Page]
--      ,[DataPeriod]
--      ,[DataType]
--      ,[TimePeriod]
--      ,[Action]
--        ,cast([Information] as nvarchar) [Information]
--      ,[Browser]
--      ,[BrowserType]
--      ,[IP]
--      ,[DateEntered] from BMSVAP3_Config.[dbo].[tblLogVisits] 
--where cast(dateentered as date) >= ( Select Dt from C2)
--union 
--Select [BU]
--      ,[UserName]
--      ,[UserGeo]
--      ,[MP]
--      ,[Market]
--      ,[AccessType]
--      ,[Geo]
--      ,[Page]
--      ,[DataPeriod]
--      ,[DataType]
--      ,[TimePeriod]
--      ,[Action]
--        ,cast([Information] as nvarchar) [Information]
--      ,[Browser]
--      ,[BrowserType]
--      ,[IP]
--      ,[DateEntered] from BMSOneLookPR.[dbo].[tblLogVisits]
--where cast(dateentered as date) >= ( Select Dt from C2)
 
)

Select BU , NumofClicks,   cast(FORMAT (StartTime, ''yyyy-MM-ddTHH:mm:ss'') as varchar)  StartTime
,cast( format (EndTime, ''yyyy-MM-ddTHH:mm:ss'') as varchar)  EndTime
into tblDRLogVisits
from (
 Select C.BU,Isnull(NumofClicks,0) NumofClicks ,StartTime,EndTime 
 
 from C Left join (
Select BU , Count(*) NumofClicks,  Min(DateEntered)   as   StartTime
,  max(DateEntered)   as EndTime   from C1 group by BU)  C1   On c.BU=C1.BU

 ) t1

 DECLARE @html nvarchar(MAX), @body1 Nvarchar(max) 
EXEC [master].[dbo].[spQueryToHtmlTable]  @html = @html OUTPUT,  @query = N''select distinct *
from LogDR..tblDRLogVisits '' ,@orderBy = N''ORDER BY 1'';

set @body1 = case when @html is null then ''NO file to Log table information found for DR Site  <br> Thanks, <br> OneLookTeam''
 else ''Hi Team ,<br>  Please find the Log Table details on DR Site as below : <br> <br>'' + @html+'' <br> Thanks, <br> OneLookTeam'' end 
 

 EXEC [Master].Dbo.sys_sp_send_dbmail  
    @profile_name = ''XSUNT DR-ONELOOK-DB NOTIFICATION'',
    @recipients = ''prateek.singh@xsunt.com;nguillot@xsunt.com;msun@xsunt.com;yi.zhu@xsunt.com;lsun@xsunt.com;tracy.wu@xsunt.com'',
    @subject = ''Log Information for DR site '',
    @body = @body1,
    @body_format = ''HTML'',
    @query_no_truncate = 1,
    @attach_query_result_as_file = 0;', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'every thursday generate Log Table for DR', 
		@enabled=1, 
		@freq_type=8, 
		@freq_interval=16, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20210310, 
		@active_end_date=99991231, 
		@active_start_time=83000, 
		@active_end_time=235959, 
		@schedule_uid=N'8611d8ff-6f6c-471d-bcb7-9fe7780014ba'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:

GO

