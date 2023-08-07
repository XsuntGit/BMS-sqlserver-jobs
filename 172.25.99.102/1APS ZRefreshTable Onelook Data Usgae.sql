BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'1APS_ZRefreshTable Onelook Data Usgae', 
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
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Refresh the table for onelook data usgae', 
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
use OneLook_UsageDB
go

Declare @maxdate datetime
select @maxdate = max(dateentered ) from dashONC30_LogVisits
print @maxdate
----------if OBJECT_ID ( N''dashONC30_LogVisits'') is not null
----------Drop Table dashONC30_LogVisits
----------Select * into  dashONC30_LogVisits from  bmsonc_867.dbo.dashONC30_LogVisits
 
insert into dashONC30_LogVisits 
Select distinct  [BU]
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
      ,cast([Information] as varchar) [Information] 
      ,[Browser]
      ,[BrowserType]
      ,[IP]
      ,[DateEntered],FORMAT(DateEntered ,''dddd'') as Day1, case 
when FORMAT(DateEntered ,''dddd'') =''Sunday'' then 1
when FORMAT(DateEntered ,''dddd'') =''Monday'' then 2
when FORMAT(DateEntered ,''dddd'') =''Tuesday'' then 3
when FORMAT(DateEntered ,''dddd'') =''Wednesday'' then 4
when FORMAT(DateEntered ,''dddd'') =''Thursday'' then 5
when FORMAT(DateEntered ,''dddd'') =''Friday'' then 6
when FORMAT(DateEntered ,''dddd'') =''Saturday'' then 7 end as WkNum,Wk_Str   from  bmsonc_867.dbo.dashONC30_LogVisits t1 with(nolock)
 left join  tblweekdate t3 with(nolock) on cast(t3.Wdate as date)  = cast(dateentered as date)
where dateentered > @maxdate

--SELECT * FROM OPENQUERY([onelook-sql], ''Select *   from  bmsonc_867.dbo.tblLogVisits with(nolock)
--where dateentered > @maxdate'') 
 
------delete t1 from dashONC30_LogVisits t1
------  left join bmsonc3..tblweekdate t3 on cast(t3.Wdate as date)  = cast(dateentered as date)
------  where cast(Wk as date) >= cast(getdate() as date)

if OBJECT_ID ( N''tblreproster'') is not null
Drop Table tblreproster
Select * into  tblreproster from  Bmsonelook.dbo.tblreproster with(nolock)

if OBJECT_ID ( N''OutputGeo'') is not null
Drop Table OutputGeo
Select * into  OutputGeo from  [onelook-db-1].bmsonc301.dbo.OutputGeo with(nolock)


if OBJECT_ID ( N''tblweekdate'') is not null
Drop Table tblweekdate
Select * into  tblweekdate from  [onelook-db-1].bmsonc3.dbo.tblweekdate with(nolock)

if OBJECT_ID ( N''tbldates'') is not null
Drop Table tbldates
go 
 Select ''Weekly'' DateVar, DateValue into tbldates from (
select distinct top 1 Wk as ''DateValue'' from (
Select distinct top 2  wk from dashONC30_LogVisits
order by wk desc
) t1 order by 1
) t1
go


if OBJECT_ID ( N''dashONC30_LogDBMTBM_proc'') is not null
Drop Table dashONC30_LogDBMTBM_proc

Select * into dashONC30_LogDBMTBM_proc from (
 
 Select *, case 
when Information =''Login success! ''and Page=''Login'' then ''Login''
 when Information =''Top Accounts''and Page=''Home'' then ''Top Accounts''
when Information =''867''and Page=''Home'' then ''867''
when Information =''Claims''and Page=''Home'' then ''Claims''
when Information =''Top HCPs''and Page=''Home'' then ''Top HCPs''
when Information =''Execution''and Page=''Home'' then ''Execution''
when Information =''Activity''and Page=''Home'' then ''Activity''
when Information =''Trialists''and Page=''Home'' then ''Trialists''
when Information =''TrialistsByVS''and Page=''Home'' then ''Trialists''
when Information =''Users''and Page=''Home'' then ''Users''
when Information =''Geo Metrics''and Page=''Geo Metrics'' then ''Sales (Default View)''
when Information =''M0003-2''and Page=''Geo Metrics'' then ''Claims''
when Information =''M0003-3''and Page=''Geo Metrics'' then ''Execution''
when Information =''M0003-4''and Page=''Geo Metrics'' then ''All''
when Information =''AccountGrps''and Page=''Grouped Accounts'' then ''Sales (Default View)''
when Information =''M00022-2''and Page=''Grouped Accounts'' then ''Claims''
when Information =''M00022-3''and Page=''Grouped Accounts'' then ''Execution''
when Information =''M00022-4''and Page=''Grouped Accounts'' then ''All''
when Information =''Accounts''and Page=''UnGrouped Accounts'' then ''Ungrouped Accounts''
when Information =''VAAccounts''and Page=''VA Accounts'' then ''VAAccounts''
when Information =''M0001-1''and Page=''HCPs'' then ''Sales''
when Information =''HCPs''and Page=''HCPs'' then ''Claims (Default View)''
when Information =''M0001-3''and Page=''HCPs'' then ''Execution''
when Information =''M0001-4''and Page=''HCPs'' then ''All''
when Information =''Search''and Page=''SingleView Dashboard'' then ''Singleview Dashboard''
when Information =''GoBeyondContest''and Page=''Go Beyond (District) Contest'' then ''Go Beyond Contest''
when Information =''Ranking''and Page=''Ranking Report'' then ''Ranking Report''
when Information =''V867''and Page=''867 Daily Sales'' then ''867 Daily Sales''
when Information =''M0009-1''and Page=''867 Daily Sales'' then ''867 Daily Sales''
when Information =''M0009-2''and Page=''867 Daily Sales'' then ''867 Daily Sales''
when Information =''M0009-22''and Page=''867 Daily Sales'' then ''867 Daily Sales''
when Information =''M0009-23''and Page=''867 Daily Sales'' then ''867 Daily Sales''
when Information =''M0009-22''and Page=''867 Daily Sales'' then ''867 Daily Sales''
when Information =''OrderSet''and Page=''Order Set'' then ''Order Set''
when Information =''TargetList''and Page=''Target List'' then ''Target List''
when Information =''OSL Target List''and Page=''OSLTargetList'' then ''OSL Target List''
when Information =''PromoPrograms''and Page=''Promo Programs'' then ''Promo Programs''
when Information =''ExecutionSummary''and Page=''Execution Summary'' then ''Execution Summary''
when Information =''BusinessPlan''and Page=''Business Planning'' then ''Business Planning''
when Information =''NonUsersNewPatients''and Page=''Prescribers with 1L NSCLC New Patients (LTD)'' then ''Prescribers with 1L NSCLC New Patients (LTD)''
when Information =''UGILaunchPlanning''and Page=''UGI Pre-Launch Planning Report'' then ''UGI Pre-Launch Planning Report''
when Information =''Covid''and Page=''US Field Recovery'' then ''Covid''
when Information =''RSFAQ''and Page=''FAQs'' then ''FAQs''
when Information =''File_Top10.pdf''and Page=''FileDownload'' then ''Top 10 Questions''
when Information =''File_NavIconAcronymKey.pdf''and Page=''FileDownload'' then ''Navigation/Icons/Acroynms''
when Information =''Training''and Page=''Training'' then ''Onelook 3.0 Navigation Training''
when Information =''File_867vsDDD.pdf''and Page=''FileDownload'' then ''867 vs DDD Sales Data''
when Information =''File_Claims.pdf''and Page=''FileDownload'' then ''Claims Data Training''
when Information =''RSVideo''and Page=''Teaser Video'' then ''Teaser Video''
when Information =''QueryTool''and Page=''QueryTool'' then ''QueryTool''
end Info
, case
when Information =''Login success! ''and Page=''Login'' then ''Login''
when Information =''Top Accounts''and Page=''Home'' then ''Dashboard''
when Information =''867''and Page=''Home'' then ''Dashboard''
when Information =''Claims''and Page=''Home'' then ''Dashboard''
when Information =''Top HCPs''and Page=''Home'' then ''Dashboard''
when Information =''Execution''and Page=''Home'' then ''Dashboard''
when Information =''Activity''and Page=''Home'' then ''Dashboard''
when Information =''Trialists''and Page=''Home'' then ''Dashboard''
when Information =''TrialistsByVS''and Page=''Home'' then ''Dashboard''
when Information =''Users''and Page=''Home'' then ''Dashboard''
when Information =''Geo Metrics''and Page=''Geo Metrics'' then ''Geo Summary''
when Information =''M0003-2''and Page=''Geo Metrics'' then ''Geo Summary''
when Information =''M0003-3''and Page=''Geo Metrics'' then ''Geo Summary''
when Information =''M0003-4''and Page=''Geo Metrics'' then ''Geo Summary''
when Information =''AccountGrps''and Page=''Grouped Accounts'' then ''Grouped Accounts''
when Information =''M00022-2''and Page=''Grouped Accounts'' then ''Grouped Accounts''
when Information =''M00022-3''and Page=''Grouped Accounts'' then ''Grouped Accounts''
when Information =''M00022-4''and Page=''Grouped Accounts'' then ''Grouped Accounts''
when Information =''Accounts''and Page=''UnGrouped Accounts'' then ''Ungrouped Accounts''
when Information =''VAAccounts''and Page=''VA Accounts'' then ''VA Accounts''
when Information =''M0001-1''and Page=''HCPs'' then ''HCPS''
when Information =''HCPs''and Page=''HCPs'' then ''HCPS''
when Information =''M0001-3''and Page=''HCPs'' then ''HCPS''
when Information =''M0001-4''and Page=''HCPs'' then ''HCPS''
when Information =''Search''and Page=''SingleView Dashboard'' then ''Singleview Dashboard''
when Information =''GoBeyondContest''and Page=''Go Beyond (District) Contest'' then ''Reports''
when Information =''Ranking''and Page=''Ranking Report'' then ''Reports''
when Information =''V867''and Page=''867 Daily Sales'' then ''Reports''
when Information =''M0009-1''and Page=''867 Daily Sales'' then ''Reports''
when Information =''M0009-2''and Page=''867 Daily Sales'' then ''Reports''
when Information =''M0009-22''and Page=''867 Daily Sales'' then ''Reports''
when Information =''M0009-23''and Page=''867 Daily Sales'' then ''Reports''
when Information =''M0009-22''and Page=''867 Daily Sales'' then ''Reports''
when Information =''OrderSet''and Page=''Order Set'' then ''Reports''
when Information =''TargetList''and Page=''Target List'' then ''Reports''
when Information =''OSL Target List''and Page=''OSLTargetList'' then ''Reports''
when Information =''PromoPrograms''and Page=''Promo Programs'' then ''Reports''
when Information =''ExecutionSummary''and Page=''Execution Summary'' then ''Reports''
when Information =''BusinessPlan''and Page=''Business Planning'' then ''Reports''
when Information =''NonUsersNewPatients''and Page=''Prescribers with 1L NSCLC New Patients (LTD)'' then ''Reports''
when Information =''UGILaunchPlanning''and Page=''UGI Pre-Launch Planning Report'' then ''Reports''
when Information =''Covid''and Page=''US Field Recovery'' then ''Covid Map''
when Information =''RSFAQ''and Page=''FAQs'' then ''Resources''
when Information =''File_Top10.pdf''and Page=''FileDownload'' then ''Resources''
when Information =''File_NavIconAcronymKey.pdf''and Page=''FileDownload'' then ''Resources''
when Information =''Training''and Page=''Training'' then ''Resources''
when Information =''File_867vsDDD.pdf''and Page=''FileDownload'' then ''Resources''
when Information =''File_Claims.pdf''and Page=''FileDownload'' then ''Resources''
when Information =''RSVideo''and Page=''Teaser Video'' then ''Resources''
when Information =''QueryTool''and Page=''QueryTool'' then ''QueryTool''
end as InfoGrp
 
 from (

SELECT  distinct ''DBM'' DataType, OncOriginal, Page, Action, cast(Information as varchar(max)) as Information 

,cast(a.Wk as date) Wk
  FROM dashONC30_LogVisits a 
  inner join   tblRepRoster b on a.username = b.emplogin
  --left join bmsonc3..tblweekdate t3 on cast(t3.Wdate as date)  = cast(dateentered as date)
  where empemail like ''%@bms.com''   
  and SFA in (
select ''184'' union select ''368'') 
 
  and OncOriginal is not null  
 
 ) t1
 ) t2 Where info is not null
 union
 Select * from (
 
 Select *, case 
when Information =''Login success! ''and Page=''Login'' then ''Login''
 when Information =''Top Accounts''and Page=''Home'' then ''Top Accounts''
when Information =''867''and Page=''Home'' then ''867''
when Information =''Claims''and Page=''Home'' then ''Claims''
when Information =''Top HCPs''and Page=''Home'' then ''Top HCPs''
when Information =''Execution''and Page=''Home'' then ''Execution''
when Information =''Activity''and Page=''Home'' then ''Activity''
when Information =''Trialists''and Page=''Home'' then ''Trialists''
when Information =''TrialistsByVS''and Page=''Home'' then ''Trialists''
when Information =''Users''and Page=''Home'' then ''Users''
when Information =''Geo Metrics''and Page=''Geo Metrics'' then ''Sales (Default View)''
when Information =''M0003-2''and Page=''Geo Metrics'' then ''Claims''
when Information =''M0003-3''and Page=''Geo Metrics'' then ''Execution''
when Information =''M0003-4''and Page=''Geo Metrics'' then ''All''
when Information =''AccountGrps''and Page=''Grouped Accounts'' then ''Sales (Default View)''
when Information =''M00022-2''and Page=''Grouped Accounts'' then ''Claims''
when Information =''M00022-3''and Page=''Grouped Accounts'' then ''Execution''
when Information =''M00022-4''and Page=''Grouped Accounts'' then ''All''
when Information =''Accounts''and Page=''UnGrouped Accounts'' then ''Ungrouped Accounts''
when Information =''VAAccounts''and Page=''VA Accounts'' then ''VAAccounts''
when Information =''M0001-1''and Page=''HCPs'' then ''Sales''
when Information =''HCPs''and Page=''HCPs'' then ''Claims (Default View)''
when Information =''M0001-3''and Page=''HCPs'' then ''Execution''
when Information =''M0001-4''and Page=''HCPs'' then ''All''
when Information =''Search''and Page=''SingleView Dashboard'' then ''Singleview Dashboard''
when Information =''GoBeyondContest''and Page=''Go Beyond (District) Contest'' then ''Go Beyond Contest''
when Information =''Ranking''and Page=''Ranking Report'' then ''Ranking Report''
when Information =''V867''and Page=''867 Daily Sales'' then ''867 Daily Sales''
when Information =''M0009-1''and Page=''867 Daily Sales'' then ''867 Daily Sales''
when Information =''M0009-2''and Page=''867 Daily Sales'' then ''867 Daily Sales''
when Information =''M0009-22''and Page=''867 Daily Sales'' then ''867 Daily Sales''
when Information =''M0009-23''and Page=''867 Daily Sales'' then ''867 Daily Sales''
when Information =''M0009-22''and Page=''867 Daily Sales'' then ''867 Daily Sales''
when Information =''OrderSet''and Page=''Order Set'' then ''Order Set''
when Information =''TargetList''and Page=''Target List'' then ''Target List''
when Information =''OSL Target List''and Page=''OSLTargetList'' then ''OSL Target List''
when Information =''PromoPrograms''and Page=''Promo Programs'' then ''Promo Programs''
when Information =''ExecutionSummary''and Page=''Execution Summary'' then ''Execution Summary''
when Information =''BusinessPlan''and Page=''Business Planning'' then ''Business Planning''
when Information =''NonUsersNewPatients''and Page=''Prescribers with 1L NSCLC New Patients (LTD)'' then ''Prescribers with 1L NSCLC New Patients (LTD)''
when Information =''UGILaunchPlanning''and Page=''UGI Pre-Launch Planning Report'' then ''UGI Pre-Launch Planning Report''
when Information =''Covid''and Page=''US Field Recovery'' then ''Covid''
when Information =''RSFAQ''and Page=''FAQs'' then ''FAQs''
when Information =''File_Top10.pdf''and Page=''FileDownload'' then ''Top 10 Questions''
when Information =''File_NavIconAcronymKey.pdf''and Page=''FileDownload'' then ''Navigation/Icons/Acroynms''
when Information =''Training''and Page=''Training'' then ''Onelook 3.0 Navigation Training''
when Information =''File_867vsDDD.pdf''and Page=''FileDownload'' then ''867 vs DDD Sales Data''
when Information =''File_Claims.pdf''and Page=''FileDownload'' then ''Claims Data Training''
when Information =''RSVideo''and Page=''Teaser Video'' then ''Teaser Video''
when Information =''QueryTool''and Page=''QueryTool'' then ''QueryTool''
end Info
, case
when Information =''Login success! ''and Page=''Login'' then ''Login''
when Information =''Top Accounts''and Page=''Home'' then ''Dashboard''
when Information =''867''and Page=''Home'' then ''Dashboard''
when Information =''Claims''and Page=''Home'' then ''Dashboard''
when Information =''Top HCPs''and Page=''Home'' then ''Dashboard''
when Information =''Execution''and Page=''Home'' then ''Dashboard''
when Information =''Activity''and Page=''Home'' then ''Dashboard''
when Information =''Trialists''and Page=''Home'' then ''Dashboard''
when Information =''TrialistsByVS''and Page=''Home'' then ''Dashboard''
when Information =''Users''and Page=''Home'' then ''Dashboard''
when Information =''Geo Metrics''and Page=''Geo Metrics'' then ''Geo Summary''
when Information =''M0003-2''and Page=''Geo Metrics'' then ''Geo Summary''
when Information =''M0003-3''and Page=''Geo Metrics'' then ''Geo Summary''
when Information =''M0003-4''and Page=''Geo Metrics'' then ''Geo Summary''
when Information =''AccountGrps''and Page=''Grouped Accounts'' then ''Grouped Accounts''
when Information =''M00022-2''and Page=''Grouped Accounts'' then ''Grouped Accounts''
when Information =''M00022-3''and Page=''Grouped Accounts'' then ''Grouped Accounts''
when Information =''M00022-4''and Page=''Grouped Accounts'' then ''Grouped Accounts''
when Information =''Accounts''and Page=''UnGrouped Accounts'' then ''Ungrouped Accounts''
when Information =''VAAccounts''and Page=''VA Accounts'' then ''VA Accounts''
when Information =''M0001-1''and Page=''HCPs'' then ''HCPS''
when Information =''HCPs''and Page=''HCPs'' then ''HCPS''
when Information =''M0001-3''and Page=''HCPs'' then ''HCPS''
when Information =''M0001-4''and Page=''HCPs'' then ''HCPS''
when Information =''Search''and Page=''SingleView Dashboard'' then ''Singleview Dashboard''
when Information =''GoBeyondContest''and Page=''Go Beyond (District) Contest'' then ''Reports''
when Information =''Ranking''and Page=''Ranking Report'' then ''Reports''
when Information =''V867''and Page=''867 Daily Sales'' then ''Reports''
when Information =''M0009-1''and Page=''867 Daily Sales'' then ''Reports''
when Information =''M0009-2''and Page=''867 Daily Sales'' then ''Reports''
when Information =''M0009-22''and Page=''867 Daily Sales'' then ''Reports''
when Information =''M0009-23''and Page=''867 Daily Sales'' then ''Reports''
when Information =''M0009-22''and Page=''867 Daily Sales'' then ''Reports''
when Information =''OrderSet''and Page=''Order Set'' then ''Reports''
when Information =''TargetList''and Page=''Target List'' then ''Reports''
when Information =''OSL Target List''and Page=''OSLTargetList'' then ''Reports''
when Information =''PromoPrograms''and Page=''Promo Programs'' then ''Reports''
when Information =''ExecutionSummary''and Page=''Execution Summary'' then ''Reports''
when Information =''BusinessPlan''and Page=''Business Planning'' then ''Reports''
when Information =''NonUsersNewPatients''and Page=''Prescribers with 1L NSCLC New Patients (LTD)'' then ''Reports''
when Information =''UGILaunchPlanning''and Page=''UGI Pre-Launch Planning Report'' then ''Reports''
when Information =''Covid''and Page=''US Field Recovery'' then ''Covid Map''
when Information =''RSFAQ''and Page=''FAQs'' then ''Resources''
when Information =''File_Top10.pdf''and Page=''FileDownload'' then ''Resources''
when Information =''File_NavIconAcronymKey.pdf''and Page=''FileDownload'' then ''Resources''
when Information =''Training''and Page=''Training'' then ''Resources''
when Information =''File_867vsDDD.pdf''and Page=''FileDownload'' then ''Resources''
when Information =''File_Claims.pdf''and Page=''FileDownload'' then ''Resources''
when Information =''RSVideo''and Page=''Teaser Video'' then ''Resources''
when Information =''QueryTool''and Page=''QueryTool'' then ''QueryTool''
end as InfoGrp
 
 from (

SELECT  distinct ''TBM'' DataType, OncOriginal, Page, Action, cast(Information as varchar(max)) as Information 

,cast(a.Wk as date) Wk
  FROM  dashONC30_LogVisits a 
  inner join tblRepRoster b on a.username = b.emplogin
  --left join bmsonc3..tblweekdate t3 on cast(t3.Wdate as date)  = cast(dateentered as date)
  where empemail like ''%@bms.com''   
  and SFA in (
 Select distinct sfcd from  outputgeo) 
 
  and OncOriginal is not null  
 
 ) t1
 ) t2 Where info is not null

 ---Select * from Test_Prateek..dashONC30_LogDBMTBM_proc



If Object_id(N''dashONC30_LogDBMTBM'') is not null
drop table dashONC30_LogDBMTBM

 DECLARE @cols AS NVARCHAR(MAX)
    ,@cols1 as   nvarchar(max)
    ,@cols3 as   nvarchar(max)
    , @query AS  NVARCHAR(MAX)


Select
    @cols = STRING_AGG( QUOTENAME(c.WK),'','' )
from
    (
        Select distinct top 100000
            Wk
        from
            dashONC30_LogDBMTBM_proc order by 1 
    )
    c

select
    @cols1 = STRING_AGG( ''ISNULL('' + QUOTENAME(c.Wk) + '', 0.000 ) '' + QUOTENAME(c.Wk) ,'','')
from
    (
         Select distinct top 100000
            Wk
        from
           dashONC30_LogDBMTBM_proc order by 1 
    )
    c
Print @cols 
print @cols1

Set @query =''
 Select
 distinct
DataType,InfoGrp,Info,''+@cols1+'' 

into dashONC30_LogDBMTBM
from(
Select * from (
 Select DataType,InfoGrp,Info,Wk,Count(OncOriginal)Ct   from  dashONC30_LogDBMTBM_proc
 group by DataType,InfoGrp,Info,Wk
 ) t1
 ) t1
 pivot
 (
 Sum(Ct) for Wk in (''+@cols+'')
 ) pv
 ''
Print @query
Exec(@query)


--- share in a table 
If Object_id(N''dashONC30_LogDBMTBM_ShareRegion'') is not null
drop table dashONC30_LogDBMTBM_ShareRegion


  Select distinct t1.*,t3.Unique_Users usr, case when t3.Unique_Users=0 then ''0.00'' else t1.Unique_Users/cast(t3.unique_users as decimal(5,2)) end as calc
  into dashONC30_LogDBMTBM_ShareRegion
  from (
  Select count(distinct EmpLogin) Unique_Users,Wk,UserGeo,GeoName, cast(substring(Usergeo,2,1) as int) as GeoIDx
 from (
SELECT   distinct EmpLogin,ONCOriginal,EmpEmail,Action,Browser,left(cast(UserGeo as varchar),2)+''000000''UserGeo,GeoName
, Wk  
,cast(Information as varchar) Info
  FROM dashONC30_LogVisits a 
  inner join   tblRepRoster b on a.username = b.emplogin
  ---left join bmsonc3..tblweekdate t3 on cast(t3.Wdate as date)  = cast(dateentered as date)
  left join  OutputGeo t4 on t4.geo =left(cast(UserGeo as varchar),2)+''000000''
  where empemail like ''%@bms.com'' 
  and cast(Information as varchar) like ''%login success%'' --AND UserGeo<>''C0000000''
 )t1 group by UserGeo,Wk,GeoName
 ) t1
left join (



 Select Sum(Unique_Users) Unique_Users,
 Wk,''C0000000'' UserGeo,''NATION'' GeoName, 0 GeoIDx
 from (
   Select count(distinct EmpLogin) Unique_Users,Wk,UserGeo,GeoName, cast(substring(Usergeo,2,1) as int) as GeoIDx
 from (
SELECT   distinct EmpLogin,ONCOriginal,EmpEmail,Action,Browser,left(cast(UserGeo as varchar),2)+''000000''UserGeo,GeoName
,  Wk  
,cast(Information as varchar) Info
  FROM dashONC30_LogVisits a 
  inner join   tblRepRoster b on a.username = b.emplogin
---  left join bmsonc3..tblweekdate t3 on cast(t3.Wdate as date)  = cast(dateentered as date)
  left join  OutputGeo t4 on t4.geo =left(cast(UserGeo as varchar),2)+''000000''
  where empemail like ''%@bms.com'' 
  and cast(Information as varchar) like ''%login success%'' --AND UserGeo<>''C0000000''
 )t1 group by UserGeo,Wk,GeoName
 ) t2 group by Wk
 ) t3 on t1.Wk=t3.wk
 

 -----
 If Object_id(N''dashONC30_LogDBMTBM_Alltimeclick'') is not null
drop table dashONC30_LogDBMTBM_Alltimeclick

 Select cast(t1.Total_Clicks as varchar) as tod,cast(t2.Total_Clicks as varchar) yest , cast(t3.Total_Clicks as varchar) allt
 into dashONC30_LogDBMTBM_Alltimeclick
 from(
Select Count (*) Total_Clicks ,''A'' T
from (
SELECT   a.*
---,convert(varchar(8),cast(Wk as date),112) Wk 
  FROM dashONC30_LogVisits a 
  inner join   tblRepRoster b on a.username = b.emplogin
 --- left join bmsonc3..tblweekdate t3 on cast(t3.Wdate as date)  = cast(dateentered as date)
  where empemail like ''%@bms.com'' and cast(dateentered as date)=cast(getdate() as date)
   )t1 ) t1
	left join 
	(
	Select Count (*) Total_Clicks ,''A'' Y
from (
SELECT   a.*
---,convert(varchar(8),cast(Wk as date),112) Wk 
  FROM dashONC30_LogVisits a 
  inner join   tblRepRoster b on a.username = b.emplogin
---  left join bmsonc3..tblweekdate t3 on cast(t3.Wdate as date)  = cast(dateentered as date)
  where empemail like ''%@bms.com'' and cast(dateentered as date)=cast(getdate() -1 as date)
    ) t1 ) t2 on t=y

		left join 
	(
	Select Count (*) Total_Clicks ,''A'' a
from (
SELECT   a.*
----,convert(varchar(8),cast(Wk as date),112) Wk 
  FROM dashONC30_LogVisits a 
  inner join   tblRepRoster b on a.username = b.emplogin
  ----left join bmsonc3..tblweekdate t3 on cast(t3.Wdate as date)  = cast(dateentered as date)
  where empemail like ''%@bms.com'' --and cast(dateentered as date)=cast(getdate() -1 as date)
    ) t1 ) t3 on t=a

--- share in a table fro total clicks across regions
If Object_id(N''dashONC30_LogTotal_ShareRegion'') is not null
drop table dashONC30_LogTotal_ShareRegion


  Select distinct t1.*,t3.Total_Clicks usr,round( case when t3.Total_Clicks=0 then ''0.00'' else t1.Total_Clicks/cast(t3.Total_Clicks as float) end ,3) as calc
  into dashONC30_LogTotal_ShareRegion
  from (
  Select count(distinct DateEntered) Total_Clicks,Wk,UserGeo,GeoName, cast(substring(Usergeo,2,1) as int) as GeoIDx
 from (
SELECT   distinct EmpLogin,ONCOriginal,EmpEmail,Action,Browser,left(cast(UserGeo as varchar),2)+''000000''UserGeo,GeoName
,  Wk  
,cast(Information as varchar) Info
,DateEntered
  FROM dashONC30_LogVisits a 
  inner join   tblRepRoster b on a.username = b.emplogin
  ----left join bmsonc3..tblweekdate t3 on cast(t3.Wdate as date)  = cast(dateentered as date)
  left join  OutputGeo t4 on t4.geo =left(cast(UserGeo as varchar),2)+''000000''
  where empemail like ''%@bms.com'' 
  --and cast(Information as varchar) like ''%login success%'' --AND UserGeo<>''C0000000''
    group by

   EmpLogin,ONCOriginal,EmpEmail,Action,Browser,left(cast(UserGeo as varchar),2)+''000000'' ,GeoName
,  Wk    
,cast(Information as varchar)  
, DateEntered

 )t1 group by UserGeo,Wk,GeoName
 ) t1
left join (



 Select Sum(Total_Clicks) Total_Clicks,
 Wk,''C0000000'' UserGeo,''NATION'' GeoName, 0 GeoIDx
 from (
   Select count(distinct DateEntered) Total_Clicks,Wk,UserGeo,GeoName, cast(substring(Usergeo,2,1) as int) as GeoIDx
 from (
SELECT    EmpLogin,ONCOriginal,EmpEmail,Action,Browser,left(cast(UserGeo as varchar),2)+''000000''UserGeo,GeoName
,  Wk  
,cast(Information as varchar) Info
, DateEntered
  FROM dashONC30_LogVisits a 
  inner join   tblRepRoster b on a.username = b.emplogin
---  left join bmsonc3..tblweekdate t3 on cast(t3.Wdate as date)  = cast(dateentered as date)
  left join  OutputGeo t4 on t4.geo =left(cast(UserGeo as varchar),2)+''000000''
  where empemail like ''%@bms.com'' 
  --and cast(Information as varchar) like ''%login success%'' --AND UserGeo<>''C0000000''

  group by

   EmpLogin,ONCOriginal,EmpEmail,Action,Browser,left(cast(UserGeo as varchar),2)+''000000'' ,GeoName
,  Wk    
,cast(Information as varchar)  
, DateEntered


 )t1 group by UserGeo,Wk,GeoName
 ) t2 group by Wk
 ) t3 on t1.Wk=t3.wk
 

  
 

----Select InfoGrp,Wk,Count(OncOriginal) Ct from Test_Prateek..dashONC30_LogDBMTBM_proc
----group by InfoGrp,Wk
----order by infogrp,wk 


----Select InfoGrp,convert(varchar(10),Wk,112) Wk,Count(OncOriginal) Clicks from Test_Prateek..dashONC30_LogDBMTBM_proc
----where DataType=''tbm''
----group by InfoGrp,Wk
----order by infogrp,wk 


       EXEC msdb.dbo.sp_send_dbmail
           @profile_name   = ''XSUNT ONELOOK-SQL NOTIFICATION''
		   ,@recipients   = ''prateek.singh@xsunt.com''
 
           , @subject      = ''Log Table Refreshed on PA102 :) ''
           , @body         = ''Log Table Refreshed on PA102''
           , @body_format  = ''HTML''
	', 
		@database_name=N'OneLook_UsageDB', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'every day refresh onelook data usgae tables', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=1, 
		@freq_subday_interval=2, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20210715, 
		@active_end_date=99991231, 
		@active_start_time=190000, 
		@active_end_time=235959, 
		@schedule_uid=N'962f0c53-85f3-4b16-a69f-e7b761da9fb5'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:

GO

