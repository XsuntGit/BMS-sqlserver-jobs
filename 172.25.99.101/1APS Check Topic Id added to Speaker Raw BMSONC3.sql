BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'1APS_Check Topic Id added to Speaker Raw-BMSONC3', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'No description available.', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'XSUNT\prateek.singh', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Add Speaker Topic ID', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'use BMSONC3
go
--2019-05-03 16:45:43.787

--update  BMSONC3..[tblHSEventTopicIndication_New]
--set mod_date =''2019-05-03 16:45:43.787''


--update   [tblHSEventTopicIndication_New]
--set mod_date =''2019-05-03 16:45:43.787''


insert into [tblHSEventTopicIndication_Stg] ( Speaker_topic_id  , Market , Product,SPEAKER_PRODUCT_NAME ,ProgramID , ProgramDate  )
 
 select  distinct  Speaker_Topic_ID ,Market , Product, SPEAKER_PRODUCT_NAME ,xx.ProgramID , ProgramDate
  
from (

select distinct Speaker_Topic_ID , SPEAKER_PRODUCT_NAME, 
case
       when SPEAKER_PRODUCT_NAME = ''NON-BRANDED ACTIVITY'' then ''UNBRAND'' 
       when SPEAKER_PRODUCT_NAME = ''OPDIVO HCC'' then ''HCC'' 
	   WHEN SPEAKER_PRODUCT_NAME  =''AUGTYRO LUNG'' THEN  ''AUGTYRO''
       when SPEAKER_PRODUCT_NAME = ''SPRYCEL'' then ''SPR''
       when SPEAKER_PRODUCT_NAME = ''OPDIVO ADJ MEL'' then ''ADJ''
       when SPEAKER_PRODUCT_NAME = ''OPDIVO+YERVOY RCC'' then ''O+Y RCC''
       when SPEAKER_PRODUCT_NAME = ''OPDIVO + YERVOY MELANOMA'' then ''YER''
       when SPEAKER_PRODUCT_NAME = ''EMPLICITI'' then ''ELO''
       when SPEAKER_PRODUCT_NAME = ''OPDIVO H&N'' then ''HN''
       when SPEAKER_PRODUCT_NAME = ''OPDIVO LUNG'' then ''NSC''
	   when SPEAKER_PRODUCT_NAME = ''REGIMEN (OPDIVO + YERVOY)'' then ''YER'' 
	   when SPEAKER_PRODUCT_NAME = ''YERVOY ADJUVANT'' then ''YER''
	   when SPEAKER_PRODUCT_NAME = ''YERVOY/OPDIVO MELANOMA'' then ''YER''
       when SPEAKER_PRODUCT_NAME = ''OPDIVO RCC'' then ''RCC''
	   when SPEAKER_PRODUCT_NAME = ''OPDIVO+YERVOY HCC'' then ''O+Y HCC''
	   when SPEAKER_PRODUCT_NAME = ''OPDIVO+YERVOY LUNG'' then ''O+Y NSC''
	   when SPEAKER_PRODUCT_NAME = ''OPDIVO MET GASTROESOPHAGEAL'' then ''UGI Met''
	   when SPEAKER_PRODUCT_NAME = ''OPDIVO ADJ GASTROESOPHAGEAL'' then ''UGI Adj''
	   when SPEAKER_PRODUCT_NAME = ''OPDIVO BLADDER'' then ''BLA''
       when SPEAKER_PRODUCT_NAME = ''OPDIVO'' then case 
	   
	   when t3.SRC_PRD_Desc = ''NON-BRANDED ACTIVITY'' then ''UNBRAND'' 
       when t3.SRC_PRD_Desc = ''OPDIVO HCC'' then ''HCC'' 
       when t3.SRC_PRD_Desc = ''SPRYCEL'' then ''SPR''
       when t3.SRC_PRD_Desc = ''OPDIVO ADJ MEL'' then ''ADJ''
       when t3.SRC_PRD_Desc = ''OPDIVO+YERVOY RCC'' then ''O+Y RCC''
       when t3.SRC_PRD_Desc = ''OPDIVO + YERVOY MELANOMA'' then ''YER''
       when t3.SRC_PRD_Desc = ''EMPLICITI'' then ''ELO''
       when t3.SRC_PRD_Desc = ''OPDIVO H&N'' then ''HN''
       when t3.SRC_PRD_Desc = ''OPDIVO LUNG'' then ''NSC''
	   when t3.SRC_PRD_Desc = ''REGIMEN (OPDIVO + YERVOY)'' then ''YER'' 
	   when t3.SRC_PRD_Desc = ''YERVOY ADJUVANT'' then ''YER''
	   when t3.SRC_PRD_Desc = ''YERVOY/OPDIVO MELANOMA'' then ''YER''
	   when t3.SRC_PRD_Desc = ''OPDIVO+YERVOY HCC'' then ''O+Y HCC''
	   when t3.SRC_PRD_Desc = ''OPDIVO+YERVOY LUNG'' then ''O+Y NSC''
	   when t3.SRC_PRD_Desc = ''OPDIVO MET GASTROESOPHAGEAL'' then ''UGI Met''
	   when t3.SRC_PRD_Desc = ''OPDIVO ADJ GASTROESOPHAGEAL'' then ''UGI Adj''
	   when t3.SRC_PRD_Desc = ''OPDIVO BLADDER'' then ''BLA''
       when t3.SRC_PRD_Desc = ''OPDIVO RCC'' then ''RCC''
	   else 
	   ''BRAND'' end
end as Product, 
case
       when SPEAKER_PRODUCT_NAME = ''NON-BRANDED ACTIVITY'' then ''UNBRAND'' 
       when SPEAKER_PRODUCT_NAME = ''OPDIVO HCC'' then ''HCC''
       when SPEAKER_PRODUCT_NAME = ''SPRYCEL'' then ''SPR''
	   WHEN SPEAKER_PRODUCT_NAME  =''AUGTYRO LUNG'' THEN  ''ROS''
       when SPEAKER_PRODUCT_NAME = ''OPDIVO ADJ MEL'' then ''YER''
       when SPEAKER_PRODUCT_NAME = ''OPDIVO+YERVOY RCC'' then ''RCC''
       when SPEAKER_PRODUCT_NAME = ''OPDIVO + YERVOY MELANOMA'' then ''YER''
       when SPEAKER_PRODUCT_NAME = ''EMPLICITI'' then ''ELO''
       when SPEAKER_PRODUCT_NAME = ''OPDIVO H&N'' then ''HN''
       when SPEAKER_PRODUCT_NAME = ''OPDIVO LUNG'' then ''NSC''
	   when SPEAKER_PRODUCT_NAME = ''REGIMEN (OPDIVO + YERVOY)'' then ''YER'' 
	   when SPEAKER_PRODUCT_NAME = ''YERVOY ADJUVANT'' then ''YER''
	   when SPEAKER_PRODUCT_NAME = ''YERVOY/OPDIVO MELANOMA'' then ''YER''
	   when SPEAKER_PRODUCT_NAME = ''OPDIVO+YERVOY HCC'' then ''HCC''
	   when SPEAKER_PRODUCT_NAME = ''OPDIVO+YERVOY LUNG'' then ''NSC''
	   when SPEAKER_PRODUCT_NAME = ''OPDIVO RCC'' then ''RCC''
	   when SPEAKER_PRODUCT_NAME = ''OPDIVO MET GASTROESOPHAGEAL'' then ''UGI''
	   when SPEAKER_PRODUCT_NAME = ''OPDIVO ADJ GASTROESOPHAGEAL'' then ''UGA''
	   when SPEAKER_PRODUCT_NAME = ''OPDIVO BLADDER''  then ''BLA''
       when SPEAKER_PRODUCT_NAME = ''OPDIVO'' then case 
	   
	   when t3.SRC_PRD_Desc = ''NON-BRANDED ACTIVITY'' then ''UNBRAND'' 
       when t3.SRC_PRD_Desc = ''OPDIVO HCC'' then ''HCC'' 
       when t3.SRC_PRD_Desc = ''SPRYCEL'' then ''SPR''
       when t3.SRC_PRD_Desc = ''OPDIVO ADJ MEL'' then ''YER''
       when t3.SRC_PRD_Desc = ''OPDIVO+YERVOY RCC'' then ''RCC''
       when t3.SRC_PRD_Desc = ''OPDIVO + YERVOY MELANOMA'' then ''YER''
       when t3.SRC_PRD_Desc = ''EMPLICITI'' then ''ELO''
       when t3.SRC_PRD_Desc = ''OPDIVO H&N'' then ''HN''
       when t3.SRC_PRD_Desc = ''OPDIVO LUNG'' then ''NSC''
	   when t3.SRC_PRD_Desc = ''REGIMEN (OPDIVO + YERVOY)'' then ''YER'' 
	   when t3.SRC_PRD_Desc = ''YERVOY ADJUVANT'' then ''YER''
	   when t3.SRC_PRD_Desc = ''YERVOY/OPDIVO MELANOMA'' then ''YER''
       when t3.SRC_PRD_Desc = ''OPDIVO RCC'' then ''RCC''
	   when t3.SRC_PRD_Desc = ''OPDIVO MET GASTROESOPHAGEAL'' then ''UGI''
	   when t3.SRC_PRD_Desc = ''OPDIVO ADJ GASTROESOPHAGEAL'' then ''UGA''
	   when t3.SRC_PRD_Desc = ''OPDIVO BLADDER'' then ''BLA''
	   when t3.SRC_PRD_Desc = ''OPDIVO+YERVOY HCC'' then ''HCC''
	   when t3.SRC_PRD_Desc = ''OPDIVO+YERVOY LUNG'' then ''NSC''
	   else 
	   ''BRAND'' end
end as market
, EVENT_DT programdate
,t1.speaker_product_id  
,SRC_PRD_DESC
--,RTRIM(LTRIM(replace(t3.SRC_PRD_DESC,''OPDIVO'',''''))) SRC_PRD_DESC
,t3.SRC_EVENT_ID ProgramID
,t3.prd_id
,t1.SRC_EVENT_ID 
--select distinct speaker_product_name
from [BMSRData].[dbo].[tblHSEventSpeakerRaw]  t1
join 
[BMSRData].[dbo].[tblHSEventRaw]t3
on t1.SRC_EVENT_ID = t3.SRC_EVENT_ID
 
where Terr_Num_CD like ''C%''
 
--and t1.speaker_topic_id is not null
and t1.Speaker_product_name not like ''%ELIQUIS%''

)xx --where product is null 
where 1=1
--and market is not null
--and SPEAKER_PRODUCT_NAME  <> ''opdivo''
--and market  <>   ''brand'' 
--and market is null
--and programdate >= ''20190101''

 except 

 select    distinct   [Speaker_Topic_id]
      ,[Market]
      ,[Product]
      ,[SPEAKER_PRODUCT_NAME]  
	  ,ProgramID
	  ,ProgramDate
	  from [dbo].[tblHSEventTopicIndication_Stg]
  
 order by 1 desc

 

 insert into BMSONC3.[dbo].[tblHSEventTopicIndication_New] ( Speaker_topic_id  , Market , Product,SPEAKER_PRODUCT_NAME,programdate,programid  )
 
 select distinct Speaker_topic_id  , Market , Product,SPEAKER_PRODUCT_NAME  ,programdate,programid  from BMSONC3.[dbo].[tblHSEventTopicIndication_Stg]
 except
 select  distinct Speaker_topic_id  , Market , Product,SPEAKER_PRODUCT_NAME ,programdate,programid   from BMSONC3.[dbo].[tblHSEventTopicIndication_New]
 
 ;
 



 if object_id(N''[BMSONC3].[dbo].[tblHSEventTopicTextMap_stg2]'') is not null drop table [BMSONC3].[dbo].[tblHSEventTopicTextMap_stg2]
 
 
 
 
 select distinct   Speaker_topic_ID ,Speaker_product_name,Mkt ,Product 
 
 into  [BMSONC3].[dbo].[tblHSEventTopicTextMap_stg2]

 from (
 
 select * from (
 
 select distinct Speaker_Topic_id,Speaker_product_name,product,market mkt  from BMSONC3.[dbo].[tblHSEventTopicIndication_New] a


  where cast(a.Mod_Date as date) = cast(getdate() as date) ) a

 left join 

 (

  select distinct Speaker_Topic_id speaker_topic_id2 from BMSONC3.[dbo].[tblHSEventTopicIndication_New] a

 where cast(a.Mod_Date as date) < cast(getdate() as date)


 ) b on a.speaker_topic_id = b.speaker_topic_id2  

 ) xx where speaker_topic_id2 is null


 
  DECLARE @html nvarchar(MAX), @body1 Nvarchar(max) 
EXEC Test_Prateek.[dbo].[spQueryToHtmlTable]  @html = @html OUTPUT,  @query = N''select distinct *
from [BMSONC3].[dbo].[tblHSEventTopicTextMap_stg2]'' ,@orderBy = N''ORDER BY 1'';

set @body1 = case when @html is null then ''BMSONC3--No New Topic Id  is added in Speaker Raw File this Week <br> Regards </br>  PRATEEK'' else @html + ''<br> Regards </br>  PRATEEK'' end 
 

EXEC master.dbo.sys_sp_send_dbmail
    @profile_name = ''XSUNT ONELOOK-DB-1 SQL Notification'',
	@recipients = ''prateek.singh@xsunt.com'',
    ----@recipients = ''prateek.singh@xsunt.com;xiao.li@xsunt.com;nguillot@xsunt.com;rushil.patel@xsunt.com'',
   ----@from_address = ''prateek.singh@xsunt.com'',
    @subject = ''BMSONC3--Topic Id in Speaker Raw File'',
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
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Add Product Id into the new table from the file', 
		@enabled=1, 
		@freq_type=8, 
		@freq_interval=32, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20190923, 
		@active_end_date=99991231, 
		@active_start_time=170112, 
		@active_end_time=235959, 
		@schedule_uid=N'9a177dfd-971f-4a5d-a437-60cc7ac7012a'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:

GO

