BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'1APS_Database Space PA101', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'No description available.', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'sa', 
		@notify_email_operator_name=N'Job Failure Notification', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'run the script to calc the db size', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'If object_id(N''Tempdb..#Dbspacechkemails'',N''U'') is not null
drop table Tempdb..#Dbspacechkemails
go

SELECT DISTINCT Rank() over (order by dovs.volume_mount_point) RK,
dovs.logical_volume_name AS DriveName,
dovs.volume_mount_point AS Drive,
 replace(cast(round(dovs.available_bytes/(1048576.0*1027),2) as varchar),''00'','''')    AS FreeSpaceInGB,
 replace(cast(round(dovs.total_bytes/(1048576.0*1027),2) as varchar),''00'','''') AS TotalSpaceInGB,
 replace( replace(cast( round(round(dovs.available_bytes/(1048576.0*1027),2) 
 / round(dovs.total_bytes/(1048576.0*1027),2)*100,0) as varchar),''00'',''''),''.'','''') +'' %''FreePCT
 , case when round(dovs.available_bytes/(1048576.0*1027),2) 
 / round(dovs.total_bytes/(1048576.0*1027),2)  < 0.10 then 0 else 1 end as FreePCtColor
  into Tempdb..#Dbspacechkemails
FROM sys.master_files mf
CROSS APPLY sys.dm_os_volume_stats(mf.database_id, mf.FILE_ID) dovs
where dovs.volume_mount_point not like ''C%''
order by drive 


Select distinct Drivename, Drive , FreeSpaceInGB, TotalSpaceInGB ,FreePCT from Tempdb..#Dbspacechkemails

-------- Top 5 Files from each Drive

If object_id(N''Tempdb..#Dbdetailschkemail'',N''U'') is not null
drop table Tempdb..#Dbdetailschkemail
go

Select * into Tempdb..#Dbdetailschkemail from (

select Dense_Rank() over (partition by Drive Order by PhysicalfilesizeinGB desc,PhysicalFileLocation ) Rk
,LogicalName  DriveName	,Upper(Drive) Drive	--,type_desc
,	FreeSpaceInGB	,TotalSpaceInGB ,FreePCT,PhysicalFileLocation	,PhysicalfilesizeinGB	,DBName	
 from
(SELECT DISTINCT DB_NAME(dovs.database_id) DBName,
mf.physical_name PhysicalFileLocation,
CAST(((mF.SIZE) * 8 / 1024.00 / 1024.00) AS NUMERIC(18,2)) 
  PhysicalfilesizeinGB,
dovs.logical_volume_name AS LogicalName,
dovs.volume_mount_point AS Drive,
mf.type_desc,
 replace(cast(round(dovs.available_bytes/(1048576.0*1027),2) as varchar),''00'','''')    AS FreeSpaceInGB,
 replace(cast(round(dovs.total_bytes/(1048576.0*1027),2) as varchar),''00'','''') AS TotalSpaceInGB,
rtrim( replace( replace(cast( round(round(dovs.available_bytes/(1048576.0*1027),2) 
/ round(dovs.total_bytes/(1048576.0*1027),2)*100,0) as varchar),''00'',''''),''.'','''') )+''%''FreePCT
 
FROM sys.master_files mf
CROSS APPLY sys.dm_os_volume_stats(mf.database_id, mf.FILE_ID) dovs
) t1
where t1.Drive not like ''C%'' and t1.Drive not like ''I%'' and t1.Drive  like ''E%''

) t2 where rk < 9
union all
select * from 
(

select Dense_Rank() over (partition by Drive Order by PhysicalfilesizeinGB desc,PhysicalFileLocation ) Rk
,LogicalName  DriveName	,Upper(Drive) Drive	--,type_desc
,	FreeSpaceInGB	,TotalSpaceInGB ,FreePCT,PhysicalFileLocation	,PhysicalfilesizeinGB	,DBName	
 from
(SELECT DISTINCT DB_NAME(dovs.database_id) DBName,
mf.physical_name PhysicalFileLocation,
CAST(((mF.SIZE) * 8 / 1024.00 / 1024.00) AS NUMERIC(18,2)) 
  PhysicalfilesizeinGB,
dovs.logical_volume_name AS LogicalName,
dovs.volume_mount_point AS Drive,
mf.type_desc,
 replace(cast(round(dovs.available_bytes/(1048576.0*1027),2) as varchar),''00'','''')    AS FreeSpaceInGB,
 replace(cast(round(dovs.total_bytes/(1048576.0*1027),2) as varchar),''00'','''') AS TotalSpaceInGB,
rtrim( replace( replace(cast( round(round(dovs.available_bytes/(1048576.0*1027),2) 
/ round(dovs.total_bytes/(1048576.0*1027),2)*100,0) as varchar),''00'',''''),''.'','''') )+''%''FreePCT
 
FROM sys.master_files mf
CROSS APPLY sys.dm_os_volume_stats(mf.database_id, mf.FILE_ID) dovs
) t1
where t1.Drive not like ''C%'' and t1.Drive not like ''I%'' and t1.Drive not like ''E%''

) t2 where rk < 6
go

select Rk , DriveName, Drive, FreeSpaceinGB,TotalSpaceinGb,FreePCT
,PhysicalFileLocation,PhysicalfilesizeinGB
,DBName
from Tempdb..#Dbdetailschkemail
go


DECLARE @MAIL_BODY VARCHAR(8000)
 
/* HEADER */
SET @MAIL_BODY = ''<table border="1" align="left" cellpadding="10" cellspacing="0" style="color:black;font-family:consolas;text-align:center;">'' +
    ''<tr>
    <th> Drivename</th>
    <th>Drive</th>
    <th>FreeSpaceInGB</th>
    <th>TotalSpaceInGB</th>
    <th>FreePCT</th>
    </tr>''
 
/* ROWS */
SELECT 
    @MAIL_BODY = @MAIL_BODY +
        ''<tr>'' + 
        ''<td>'' +  Drivename  + ''</td>'' +
        ''<td>'' + Drive + ''</td>'' +
        ''<td>'' + FreeSpaceInGB + ''</td>'' +
        ''<td>'' + TotalSpaceInGB+ ''</td>'' +
        ''<td'' + CASE WHEN FreePCTcolor =1 THEN '' style="color:green;">'' ELSE '' style="color:red;">'' END + FreePCT + ''</td>'' +
        ''</tr>''
FROM
  ( SELECT  distinct *
  FROM Tempdb..#Dbspacechkemails ) t1 order by Drive
 
SELECT @MAIL_BODY = @MAIL_BODY + ''</table>''

--print @mail_body

 
  DECLARE @html nvarchar(MAX), @body1 Nvarchar(max) , @int int, @dt1 nvarchar(25),@sub nvarchar(100)

EXEC Test_Prateek.[dbo].[spQueryToHtmlTable]  @html = @html OUTPUT,  @query = N''select distinct Rk , DriveName , Drive, FreeSpaceinGB,TotalSpaceinGb,FreePCT
,PhysicalFileLocation,PhysicalfilesizeinGB
,DBName
from Tempdb..#Dbdetailschkemail'' ,@orderBy = N''ORDER BY 3,1'';
   select @int = case when @html is not null then 1 else 0 end 
 select @dt1 =  Convert(nvarchar(25),getdate())
  set @body1 = case when @MAIL_BODY is null then ''the job did not run successfully <br> Regards </br>  Xsunt Team'' 
  else  ''Hi Team,<br> Below is the status of Space availability on PA101 as of : ''+@dt1+''<br><br>''+
 @MAIL_BODY+ ''<br><br><br><br><br><br><br><br> <br><br> Below is the Top 8/5 highest size files in each drive: <br>'' 
    end 
	set @body1 = @body1+  
 +''<br>''
 + @html  +''<br>Thanks , <br>
   Xsunt Team <br>''
   set @sub = ''Space avaibility on PA 101 as of : '' +@dt1
  set @body1 =  @body1
  EXEC msdb.dbo.sp_send_dbmail
    @profile_name = ''XSUNT ONELOOK-DB-1 SQL NOTIFICATION'',
	@recipients = ''prateek.singh@xsunt.com;nguillot@xsunt.com;larisa.labas@xsunt.com;azubarev@xsunt.com;lsun@xsunt.com'',
    --@recipients = ''prateek.singh@xsunt.com'' ,
    --@from_address = ''OnelookOncology@xsunt.com'',
    @subject = @sub,
    @body = @body1 ,
    @body_format=''HTML''', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'run thos job thursday Friday', 
		@enabled=1, 
		@freq_type=8, 
		@freq_interval=48, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20200723, 
		@active_end_date=99991231, 
		@active_start_time=100300, 
		@active_end_time=235959, 
		@schedule_uid=N'126ba7de-a214-4a23-8d36-c2d9a7af30d3'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:

GO

