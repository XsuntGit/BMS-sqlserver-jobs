/*
	Created by XSUNT\svc-jenkins using dbatools Export-DbaScript for objects on 172.25.99.101 at 08/06/2023 21:21:57
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
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'1APS_Check for New Text for Product Name-BMSONC3', 
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
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Product addition in Speaker File', 
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
if object_id(N''[BMSONC3].[dbo].[tblHSEventTopicTextMap_stg]'') is not null drop table [BMSONC3].[dbo].[tblHSEventTopicTextMap_stg]


select
	     case when t1.[SPEAKER_PRODUCT_NAME] is  null then t2.Speaker_product_name end  	 as NewProductText
	 



into [BMSONC3].[dbo].[tblHSEventTopicTextMap_stg]
   FROM [BMSONC3].[dbo].[tblHSEventTopicTextMap] t1
   full outer join
   (
    select  distinct  
   SPEAKER_PRODUCT_NAME  
 from (
select distinct Speaker_Topic_ID , SPEAKER_PRODUCT_NAME, 
case
       when SPEAKER_PRODUCT_NAME = ''NON-BRANDED ACTIVITY'' then ''UNBRAND'' 
       when SPEAKER_PRODUCT_NAME = ''OPDIVO HCC'' then ''HCC'' 
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
	   when SPEAKER_PRODUCT_NAME  = ''OPDIVO MET GASTROESOPHAGEAL'' then ''UGI Met''
	   when SPEAKER_PRODUCT_NAME  = ''OPDIVO ADJ GASTROESOPHAGEAL'' then ''UGI Adj''
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
	   when t3.SRC_PRD_Desc  = ''OPDIVO MET GASTROESOPHAGEAL'' then ''UGI Met''
	   when t3.SRC_PRD_Desc  = ''OPDIVO ADJ GASTROESOPHAGEAL'' then ''UGI Adj''
       when t3.SRC_PRD_Desc = ''OPDIVO RCC'' then ''RCC''
	   else 
	   ''BRAND'' end
end as Product, 
case
       when SPEAKER_PRODUCT_NAME = ''NON-BRANDED ACTIVITY'' then ''UNBRAND'' 
       when SPEAKER_PRODUCT_NAME = ''OPDIVO HCC'' then ''HCC''
       when SPEAKER_PRODUCT_NAME = ''SPRYCEL'' then ''SPR''
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
	   when t3.SRC_PRD_Desc = ''OPDIVO+YERVOY HCC'' then ''HCC''
	   when t3.SRC_PRD_Desc = ''OPDIVO+YERVOY LUNG'' then ''NSC''
	   else 
	   ''BRAND'' end
end as market
 
from [BMSRData].[dbo].[tblHSEventSpeakerRaw]  t1
join 
[BMSRData].[dbo].[tblHSEventRaw]t3
on t1.SRC_EVENT_ID = t3.SRC_EVENT_ID
where Terr_Num_CD like ''C%''
and t1.Speaker_product_name not like ''%ELIQUIS%''
)xx
--union all select ''Test'' union all select ''Test2''
)t2 on t1. speaker_product_name = t2.speaker_product_name

where 1=1 
and t1.SPEAKER_PRODUCT_NAME  is null ; 


  DECLARE @html nvarchar(MAX), @body1 Nvarchar(max) 
EXEC Test_Prateek.[dbo].[spQueryToHtmlTable]  @html = @html OUTPUT,  @query = N''select distinct NewProductText 
from [BMSONC3].[dbo].[tblHSEventTopicTextMap_stg]'' ,@orderBy = N''ORDER BY 1'';

set @body1 = case when @html is null then ''BMSONC3---No New text for Product Name is added in Speaker Raw File this Week <br> Regards </br>  PRATEEK'' else @html + ''<br> Regards </br>  PRATEEK'' end 
 


EXEC master.dbo.sys_sp_send_dbmail
    @profile_name = ''XSUNT ONELOOK-DB-1 SQL NOTIFICATION'',
	@recipients = ''prateek.singh@xsunt.com'',
    --@recipients = ''prateek.singh@xsunt.com;xiao.li@xsunt.com;nguillot@xsunt.com;rushil.patel@xsunt.com'',
    @subject = ''BMSONC3 database - Product Name Text in Speaker Raw File'',
    @body = @body1 ,
    --@from_address = ''prateek.singh@xsunt.com'',
    @body_format = ''HTML'',
    @query_no_truncate = 1,
    @attach_query_result_as_file = 0;
 
', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'run the product text file', 
		@enabled=1, 
		@freq_type=8, 
		@freq_interval=32, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20190923, 
		@active_end_date=99991231, 
		@active_start_time=170015, 
		@active_end_time=235959, 
		@schedule_uid=N'7863bc89-d206-47bc-82e0-d6c9939e5f09'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:

GO

