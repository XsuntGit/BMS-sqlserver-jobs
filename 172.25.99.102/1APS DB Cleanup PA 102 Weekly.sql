/*
	Created by XSUNT\svc-jenkins using dbatools Export-DbaScript for objects on 172.25.99.102 at 08/06/2023 21:22:45
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
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'1APS_DB Cleanup PA 102 Weekly', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'No description available.', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'XSUNT\srv-task', 
		@notify_email_operator_name=N'Job Failure', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'db cleanup jon', 
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

If object_id(N''Tempdb..#Dbdetailschkemail'',N''U'') is not null
drop table Tempdb..#Dbdetailschkemail
go

;With C1 as (
Select distinct name
, create_date
,isnull(t2.CDT,t3.YYYYMM) as TypeDt
,case when t2.CDT is not null then ''Weekly'' else ''Monthly'' end as DbType
,len( isnull(t2.CDT,t3.YYYYMM)) as LenTypeDt
,SUBSTRING(name , 1, charindex(isnull(t2.CDT,t3.YYYYMM),name)-1 ) as Grp
,SUBSTRING(name , 1, charindex(isnull(t2.CDT,t3.YYYYMM),name)-1 )+isnull(t2.CDT,t3.YYYYMM) as GrpName
--, count( SUBSTRING(name , 1, charindex(isnull(t2.CDT,t3.YYYYMM),name)-1 )+isnull(t2.CDT,t3.YYYYMM))
--over (partition by SUBSTRING(name , 1, charindex(isnull(t2.CDT,t3.YYYYMM),name)-1 )
--  ) ct
from 
sys.databases t1
left join (
Select CDT,YYYYMM from BMSONC_867..dimdate) t2 on t1.name like ''%''+t2.cdt+''%''
left join (
Select CDT,YYYYMM from BMSONC_867..dimdate) t3 on t1.name like ''%''+t3.YYYYMM+''%''
where (substring(REVERSE(name),1,charindex(''_'',REVERSE(name)))) like ''%v_''
and name not like ''BMSOncology%''  
and name not like ''BMSChina%''
) 

Select case when right(t1.Grp,1)=''_'' then replace(t1.Grp,''_'','''') 
else t1.Grp end as DBGrp,t1.DbType, DbName,cast(Dbcount as varchar) Dbcount
, isStandard
, convert(varchar(10), create_date,112) create_date
,  isCurrentYear

into Tempdb..#Dbdetailschkemail
from (

Select t1.Grp,t1.DbType,name as DbName,Dbcount, 
case when c1.DbType=''weekly'' and DbCount >3 then ''No''
when c1.DbType=''monthly'' and DbCount >2 then ''No'' else ''Yes'' end as isStandard
, cast(create_date as date) create_date
, case when year(cast(create_date as date)) < year(getdate()) then ''No'' else ''Yes'' end as isCurrentYear
from (
select Grp ,DbType ,count( distinct name) as DbCount
from C1
group by grp ,DbType
) t1
left join C1 on c1.Grp=t1.Grp
where t1.grp is not null
) t1

DECLARE @MAIL_BODY VARCHAR(max)
 
/* HEADER */
SET @MAIL_BODY = ''<table border="1" align="left" cellpadding="10" cellspacing="0" style="color:black;font-family:consolas;text-align:center;">'' +
    ''<tr>
    <th> DBGrp</th>
    <th>DbType</th>
    <th>DbName</th>
    <th>Dbcount</th>
    <th>isStandard</th>
	<th>create_date</th>
	<th>isCurrentYear</th>
    </tr>''
 
/* ROWS */
SELECT 
    @MAIL_BODY = @MAIL_BODY +
        ''<tr>'' + 
        ''<td>'' +  DBGrp  + ''</td>'' +
        ''<td>'' + DbType + ''</td>'' +
        ''<td>'' + DbName + ''</td>'' +
        ''<td>'' + Dbcount+ ''</td>'' +
        ''<td'' + CASE  WHEN isStandard =''Yes''  THEN '' style="color:green;">'' ELSE '' style="color:red;">'' END + isStandard + ''</td>'' +
         ''<td>'' + create_date+ ''</td>'' +
        ''<td'' + CASE  WHEN isCurrentYear =''Yes''  THEN '' style="color:green;">'' ELSE '' style="color:red;">'' END + isCurrentYear + ''</td>'' +
        
		''</tr>''
FROM
  ( SELECT  *
  FROM Tempdb..#Dbdetailschkemail ) t1 order by DbName
 
SELECT @MAIL_BODY = @MAIL_BODY + ''</table>''
print @mail_body

 DECLARE @html nvarchar(MAX), @body1 Nvarchar(max) , @int int, @dt1 nvarchar(25),@sub nvarchar(100)

--EXEC BMSONC_867.[dbo].[spQueryToHtmlTable]  @html = @html OUTPUT,  @query = N''select*
--from Tempdb..#Dbdetailschkemail'' ,@orderBy = N''ORDER BY 2'';
    select @dt1 =  Convert(nvarchar(25),getdate())
   set @sub = ''Database Clean up on PA 102 Weekly''
  set @body1 = ''Hi Team,<br> Please find the List of DBs on PA 102 as Per Weekly/Monthly standard. Do reply to this email if you have cleaned 
  up Old/Unused DBs. <br>
  As per Standard agreed : <br><br> <Strong> Monthly 2 DBs per BU/Project </Strong> <br>
   <Strong> Weekly 3 DBs per BU/Project </Strong> <br>  Exception allowed for under Dev Projects or Production re-run only <br> <br>
  ''+''<br> Thanks <br> DB Admin Team''+''<br><br>'' + @MAIL_BODY 
  print @body1
  EXEC msdb.dbo.sp_send_dbmail
    @profile_name = ''xsunt onelook-sql notification'',
	@recipients = ''alen.zhang@xsunt.com;gloria.gu@xsunt.com;tracy.wu@xsunt.com;elvira.liu@xsunt.com;victor.xu@xsunt.com;rushil.patel@xsunt.com;akorolev@xsunt.com;nathan.siviy@xsunt.com;tiffany.wang@xsunt.com;young.ko@xsunt.com;yancheng.zhou@xsunt.com;feng.lu@xsunt.com;hannah.zheng@xsunt.com;'',
    @copy_recipients=''prateek.singh@xsunt.com;azubarev@xsunt.com;nguillot@xsunt.com;lsun@xsunt.com;'',--@recipients = '''' ,
    @subject = @sub,
    @body = @body1 ,
    @body_format=''HTML''



', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'db cleanup job', 
		@enabled=1, 
		@freq_type=8, 
		@freq_interval=48, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20220410, 
		@active_end_date=99991231, 
		@active_start_time=100000, 
		@active_end_time=235959, 
		@schedule_uid=N'048a934a-b0b5-448d-8cf6-6e1ef5fed43f'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:

GO

