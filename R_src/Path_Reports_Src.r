
setwd('I:\\A-Lydia-Work-Plan\\Projects')


library(readbulk)
library(dplyr)      #for data manipulate
library(data.table)  #library(dtplyr) #library(stringr)
library(lubridate)  #time manipulation
#library(ggplot2)
library(RODBC) 
library(chron) # convert to time 
#library(formattable) #percent
library(stringr)
library(tidyr)
library(rJava)
library(xlsx)
library(xlsxjars)
#install.packages(c('', '', '', ''))
channelXXXX<- odbcDriverConnect('driver={SQL Server};server=DCPWDBSXXXX;
                               database=clarity_rpt_crystal; trusted_connection=true')

#load last report file first for comparison--------------------------------------------------------
last_report <- read.csv('Path_report_workfile0226P2_2018.csv', header=T, stringsAsFactors = F, na.strings='NULL')
#--------------------------------------------------------------------------------------------------

#read individual csv files
raw_data <- read_bulk(directory="DailyExport", subdirectories = FALSE, extension = ".csv",
                      fun=read.csv,header=T, stringsAsFactor=FALSE, na.strings='NULL')

data_file <- raw_data

data_file$Consent.Withdrawn.At[is.na(data_file$Consent.Withdrawn.At)]<- ""

head(unique(data_file$Consent.Withdrawn.At))


#keep useful variables for reports--------------------------------------------------------------
data_file2 <- data_file%>%select(Patient.ID, First.Name, Last.Name, 
                                 Blood.Draw.Completed.At, 
                                 Consent.Completed.At,Consent.Withdrawn.At,Mammogram.Appointment.Location
)

#keep the oringinal data data_file2
Enroll <- data_file2%>%filter(Consent.Completed.At!=""&Consent.Withdrawn.At=="")


# #https://stackoverflow.com/questions/37117960/r-create-temporary-table-in-sql-server-from-r-data-frame
queryId <- "IF ( OBJECT_ID('TEMPDB..#Project_patid') IS NOT NULL) DROP TABLE #Project_patid;
CREATE TABLE #Project_patid (PAT_ID VARCHAR (30));"

sqlQuery(channelXXXX, gsub("\\s|\\t", " ", queryId))

#need to put insert into for each single value
updates<- 
    paste0(paste0("insert into #Project_patid (PAT_ID) values ('", Enroll$Patient.ID, "')"), collapse = ' ')

update_query <- sqlQuery(channelXXXX, updates)


# wait a couple of seconds for the temporary to table to complete!!!!!!!!!!!!!!!!!!!!!!
# to test if that matches Enroll number!!!!
n_distinct(Enroll$Patient.ID)
sqlQuery(channelXXXX, "select count(distinct PAT_ID) from #Project_patid") 



update_id <-  sqlQuery(channelXXXX, "
                       select distinct  case when b.pat_id is null then A.pat_id
                       else b.pat_id end as pat_id, A.pat_id as old_id
                       from #Project_patid as A 
                       left join DCPWDBSXXX.Clarity_Rpt_Crystal.dbo.PAT_MERGE_HISTORY as b ON A.PAT_ID=b.PATIENT_MRG_HIST")


#Patient.ID becomes the old id, pat_id is the new updated ID**************************************
Enroll_ids <- merge(x=Enroll, y=update_id, by.x='Patient.ID', by.y='old_id', all.x=T)


#backlog reports are done so change the path report date from 1/2/2018 --2/5/2018
#enrolled_path <- sqlQuery(channelXXXX, gsub("\\s|\\t", " ", queryPath))

enrolled_path <- sqlQuery(channelXXXX, "
            select distinct ff.*  --use table ORDER_NARRATIVE
            from (
                          select distinct ord.order_proc_id
                          , ord.pat_id
                          , id.identity_id as MRN 
                          , v.accession_num  			 
                          , convert(varchar(10), ord.order_time, 101) as order_date
                          , class.name as order_class
                          , eap.proc_name
                          , convert(varchar(10), ord2.specimn_taken_date, 101) as specimen_taken_date
                          , convert(varchar(8), ord2.specimn_taken_time, 108) as specimen_taken_time
                          , convert(varchar(10), n.contact_date, 101) as path_report_date
                          , n.line
                          , n.narrative 
                          from DCPWDBSXXXX.CLARITY_RPT_CRYSTAL.DBO.order_proc as ord
                          left join dcpwdbsXXXX.clarity_rpt_crystal.dbo.ORDER_PROC_2   as ord2 on ord2.order_proc_id = ord.order_proc_id  
                          left join DCPWDBSXXXX.CLARITY_RPT_CRYSTAL.DBO.IDENTITY_ID as id on id.pat_id = ord.pat_id
                          left join dcpwdbsXXXX.clarity_rpt_crystal.dbo.zc_order_class as class on class.order_class_c = ord.order_class_c
                          left join dcpwdbsXXXX.clarity_rpt_crystal.dbo.v_img_study    as v    on v.ORDER_ID = ord.ORDER_PROC_ID
                          left JOIN DCPWDBSXXXX.CLARITY_RPT_CRYSTAL.DBO.CLARITY_EAP    AS EAP  ON EAP.PROC_ID = ORD.PROC_ID
                          left join dcpwdbsXXXX.clarity_rpt_crystal.dbo.order_narrative  as n on n.ORDER_PROC_ID = ord.ORDER_PROC_ID
                          inner join(
                          select distinct pat_id from (
                          select distinct  case when b.pat_id is null then A.pat_id
                          else b.pat_id end as pat_id, A.pat_id as old_id
                          from #Project_patid as A 
                          left join DCPWDBSXXXX.Clarity_Rpt_Crystal.dbo.PAT_MERGE_HISTORY as b ON A.PAT_ID=b.PATIENT_MRG_HIST
                          ) as ttt ) as pt on pt.pat_id = ord.pat_id
                          where --cast(n.contact_date as date) >= '02/28/2017' --use the report generated date   
                          cast(ord2.specimn_taken_date as date)>= '2017-02-28' 
                          and id.IDENTITY_TYPE_ID = '30102836'   
                          and ord.ORDER_STATUS_C in (3, 5) 
                          and (eap.proc_name in (
                          'SURGICAL PATHOLOGY',    	
                          'PATHOLOGY SF',
                          'SURGICAL PATHOLOGY SF',                    
                          'ABSTRACT-EXTERNAL PATHOLOGY',
                          'ABSTRACT-PATHOLOGY',
                          'PATHOLOGY',
                          'PATHOLOGY LETTER',
                          'PATHOLOGY PA',
                          'PATHOLOGY REPORT',
                          'PR ABSTRACT PATHOLOGY',
                          'PR LAB PATHOLOGY CONSULTATION COMPREHENSIVE',
                          'PR SURGICAL PATHOLOGY PROCEDURE',
                          'SURGICAL PATHOLOGY PA'
                          ) or ord.description in (
                          'SURGICAL PATHOLOGY',    	
                          'PATHOLOGY SF',
                          'SURGICAL PATHOLOGY SF',                    
                          'ABSTRACT-EXTERNAL PATHOLOGY',
                          'ABSTRACT-PATHOLOGY',
                          'PATHOLOGY',
                          'PATHOLOGY LETTER',
                          'PATHOLOGY PA',
                          'PATHOLOGY REPORT',
                          'PR ABSTRACT PATHOLOGY',
                          'PR LAB PATHOLOGY CONSULTATION COMPREHENSIVE',
                          'PR SURGICAL PATHOLOGY PROCEDURE',
                          'SURGICAL PATHOLOGY PA')) )ff
                         
                          
                          union
                          
                          
                          select distinct ff.*   --use table ORDER_RES_COMMENT 
                          from (
                          select distinct ord.order_proc_id
                          , ord.pat_id
                          , id.identity_id as MRN 
                          , v.accession_num  			 
                          , convert(varchar(10), ord.order_time, 101) as order_date
                          , class.name as order_class
                          , eap.proc_name
                          , convert(varchar(10), ord2.specimn_taken_date, 101) as specimen_taken_date
                          , convert(varchar(8), ord2.specimn_taken_time, 108) as specimen_taken_time
                          , convert(varchar(10), n.contact_date, 101) as path_report_date
                          , n.line_comment as line
                          , n.results_cmt as narrative 
                          from DCPWDBSXXXX.CLARITY_RPT_CRYSTAL.DBO.order_proc as ord
                          left join dcpwdbsXXXX.clarity_rpt_crystal.dbo.ORDER_PROC_2   as ord2 on ord2.order_proc_id = ord.order_proc_id  
                          left join DCPWDBSXXXX.CLARITY_RPT_CRYSTAL.DBO.IDENTITY_ID as id on id.pat_id = ord.pat_id
                          left join dcpwdbsXXXX.clarity_rpt_crystal.dbo.zc_order_class as class on class.order_class_c = ord.order_class_c
                          left join dcpwdbsXXXX.clarity_rpt_crystal.dbo.v_img_study    as v    on v.ORDER_ID = ord.ORDER_PROC_ID
                          left JOIN DCPWDBSXXXX.CLARITY_RPT_CRYSTAL.DBO.CLARITY_EAP    AS EAP  ON EAP.PROC_ID = ORD.PROC_ID
                          left join dcpwdbsXXXX.clarity_rpt_crystal.dbo.order_res_comment as n on n.ORDER_ID = ord.ORDER_PROC_ID
                          inner join(
                          select distinct pat_id from (
                          select distinct  case when b.pat_id is null then A.pat_id
                          else b.pat_id end as pat_id, A.pat_id as old_id
                          from #Project_patid as A 
                          left join DCPWDBSXXXX.Clarity_Rpt_Crystal.dbo.PAT_MERGE_HISTORY as b ON A.PAT_ID=b.PATIENT_MRG_HIST
                          ) as ttt ) as pt on pt.pat_id = ord.pat_id
                          where 
                          cast(ord2.specimn_taken_date as date)>= '2017-02-28' 
                          and id.IDENTITY_TYPE_ID = '30102836'   
                          and ord.ORDER_STATUS_C in (3, 5) 
                          and (eap.proc_name in (
                          'SURGICAL PATHOLOGY',    	
                          'SURGICAL PATHOLOGY SF',                    
                          'ABSTRACT-EXTERNAL PATHOLOGY',
                          'ABSTRACT-PATHOLOGY',
                          'PATHOLOGY',
                          'PATHOLOGY LETTER',
                          'PATHOLOGY PA',
                          'PATHOLOGY REPORT',
                          'PATHOLOGY SF',
                          'PR ABSTRACT PATHOLOGY',
                          'PR LAB PATHOLOGY CONSULTATION COMPREHENSIVE',
                          'PR SURGICAL PATHOLOGY PROCEDURE',
                          'SURGICAL PATHOLOGY PA'
                          )  or ord.description in (
                          'SURGICAL PATHOLOGY',    	
                          'SURGICAL PATHOLOGY SF',                    
                          'ABSTRACT-EXTERNAL PATHOLOGY',
                          'ABSTRACT-PATHOLOGY',
                          'PATHOLOGY',
                          'PATHOLOGY LETTER',
                          'PATHOLOGY PA',
                          'PATHOLOGY REPORT',
                          'PATHOLOGY SF',
                          'PR ABSTRACT PATHOLOGY',
                          'PR LAB PATHOLOGY CONSULTATION COMPREHENSIVE',
                          'PR SURGICAL PATHOLOGY PROCEDURE',
                          'SURGICAL PATHOLOGY PA')))ff
                     
                   union 

                select distinct ff.*   --use table ORDER_RES_COMP_CMT
                from (
                     select distinct ord.order_proc_id
                          , ord.pat_id
                          , id.identity_id as MRN 
                          , v.accession_num  			 
                          , convert(varchar(10), ord.order_time, 101) as order_date
                          , class.name as order_class
                          , eap.proc_name
                          , convert(varchar(10), ord2.specimn_taken_date, 101) as specimen_taken_date
                          , convert(varchar(8), ord2.specimn_taken_time, 108) as specimen_taken_time
                          , convert(varchar(10), n.contact_date, 101) as path_report_date
                          , n.line_comment as line
                          , n.results_comp_cmt as narrative 
                      from DCPWDBSXXXX.CLARITY_RPT_CRYSTAL.DBO.order_proc as ord
                          left join dcpwdbsXXXX.clarity_rpt_crystal.dbo.ORDER_PROC_2   as ord2 on ord2.order_proc_id = ord.order_proc_id  
                          left join DCPWDBSXXXX.CLARITY_RPT_CRYSTAL.DBO.IDENTITY_ID as id on id.pat_id = ord.pat_id
                          left join dcpwdbsXXXX.clarity_rpt_crystal.dbo.zc_order_class as class on class.order_class_c = ord.order_class_c
                          left join dcpwdbsXXXX.clarity_rpt_crystal.dbo.v_img_study    as v    on v.ORDER_ID = ord.ORDER_PROC_ID
                          left JOIN DCPWDBSXXXX.CLARITY_RPT_CRYSTAL.DBO.CLARITY_EAP    AS EAP  ON EAP.PROC_ID = ORD.PROC_ID
                          left join dcpwdbsXXXX.clarity_rpt_crystal.dbo.order_res_comp_cmt as n on n.ORDER_ID = ord.ORDER_PROC_ID
                          inner join(
                          select distinct pat_id from (
                          select distinct  case when b.pat_id is null then A.pat_id
                          else b.pat_id end as pat_id, A.pat_id as old_id
                          from #Project_patid as A 
                          left join DCPWDBSXXXX.Clarity_Rpt_Crystal.dbo.PAT_MERGE_HISTORY as b ON A.PAT_ID=b.PATIENT_MRG_HIST
                          ) as ttt ) as pt on pt.pat_id = ord.pat_id
                          where 
                          cast(ord2.specimn_taken_date as date)>= '2017-02-28'  
                          and id.IDENTITY_TYPE_ID = '30102836'   
                          and ord.ORDER_STATUS_C in (3, 5) 
                          and (eap.proc_name in (
                          'SURGICAL PATHOLOGY',    	
                          'SURGICAL PATHOLOGY SF',                    
                          'ABSTRACT-EXTERNAL PATHOLOGY',
                          'ABSTRACT-PATHOLOGY',
                          'PATHOLOGY',
                          'PATHOLOGY LETTER',
                          'PATHOLOGY PA',
                          'PATHOLOGY REPORT',
                          'PATHOLOGY SF',
                          'PR ABSTRACT PATHOLOGY',
                          'PR LAB PATHOLOGY CONSULTATION COMPREHENSIVE',
                          'PR SURGICAL PATHOLOGY PROCEDURE',
                          'SURGICAL PATHOLOGY PA'
                          ) or ord.description in (
                          'SURGICAL PATHOLOGY',    	
                          'SURGICAL PATHOLOGY SF',                    
                          'ABSTRACT-EXTERNAL PATHOLOGY',
                          'ABSTRACT-PATHOLOGY',
                          'PATHOLOGY',
                          'PATHOLOGY LETTER',
                          'PATHOLOGY PA',
                          'PATHOLOGY REPORT',
                          'PATHOLOGY SF',
                          'PR ABSTRACT PATHOLOGY',
                          'PR LAB PATHOLOGY CONSULTATION COMPREHENSIVE',
                          'PR SURGICAL PATHOLOGY PROCEDURE',
                          'SURGICAL PATHOLOGY PA')))ff")


#de-identification
#http://stat545.com/block022_regular-expression.html
from <- c('PATIENT:.*',
          'Patient Name:.*',
          'MRN:.*',
          'Patient DOB.*',
          'DOB:.*',
          'Date of Birth:.*',
          'CASE:.*',
          'Accession #:.*',
          'ACCESSION #:.*',
          'Accession / CaseNo:.*',
          'ACCESSION NUMBER.*',
          'ORIGINAL ACCESSION NUMBER.*',
          'ORIGINAL ACCESSION #.*',
          '[A-Z]{3}[0-9]{11}',  #accession number
          '[A-Z]{3}-[0-9]{2}-[0-9]{5}',  #accession/case number
          '[A-z]{3}-[0-9]{2}-[0-9]{5}'
          '[A-Z]{2}-[0-9]{2}-[0-9]{5}',
          '[A-Z]{3}.-[0-9]{2}-[0-9]{4}', #pep smear number
          '[A-Z]{3}-[0-9]{2}-[0-9]{4}',
	      '[A-z]{3}[0-9]{2}-[0-9]{3}',
		  '[0-9]{10}',
		  '[0-9]{9}',
		  '[0-9]{8}:[S]{2}',
		  '[0-9]{8}',
          '[0-9]{7}',     
          '[0-9]{2}-[0-9]{4}-[A-z]{2}',
	 	  '[A-Z]{3}[0-9]{2}-[0-9]{7}',
          '[A-Z]{3}[0-9]{2}-[0-9]{6}',
          '[A-Z]{3}[0-9]{2}-[0-9]{5}',          
          '[A-Z]{3}-[0-9]{2}-[0-9]{3}', 
          '[0-9]{2}[A-Z]{2}[0-9]{5}', 
          '[A-z]{2}[0-9]{2}-[0-9]{6}', 
          '[A-z]{3}[0-9]{11}'  #accession number
          #,  #accession/case number   		  '[0-9]{2}/[0-9]{2}/[0-9]{2}' 		
)

to <- c('PATIENT: @@@@@@@@@@@@@',
        'Patient Name:@@@@@@@@@@@@',
        'MRN: @@@@@@@@@@@',
        'Patient DOB/Sex@@@@@@@@@@',
        'DOB:@@@@@@@@@@@@@',
        'Date of Birth:@@@@@@@@@@@@@',
        'CASE:@@@@@@@@@@',
        'Accession #:@@@@@@@@@@',
        'ACCESSION #:@@@@@@@@@@',
        'Accession / CaseNo:@@@@@@@@@@',
        'ACCESSION NUMBER(S):@@@@@@@@@@',
        'ORIGINAL ACCESSION NUMBER @@@@@@@@@@',
        'ORIGINAL ACCESSION #@@@@@@@@@@@@@',
        '@@@@@@@@@@@@@',  #cpt accession number
        '@@@@@@@@@@@@@', #pep smear
        '@@@@@@@@@@@@@',
        '@@@@@@@@@@@@@',
        '@@@@@@@@@@@@@',
        '@@@@@@@@@@@@@',
        '@@@@@@@@@@@@@',
	    '@@@@@@@@@@@@@',
        '@@@@@@@@@@@@@',
        '@@@@@@@@@@@@@',
        '@@@@@@@@@@@@@',
        '@@@@@@@@@@@@@',
        '@@@@@@@@@@@@@',
        '@@@@@@@@@@@@@',
	    '@@@@@@@@@@@@@',
        '@@@@@@@@@@@@@',
        '@@@@@@@@@@@@@',
        '@@@@@@@@@@@@@',
        '@@@@@@@@@@@@@',
	    '@@@@@@@@@@@@@'#,         '@@@@@@@@@@@@@'
)

#processing function which will take the 'from' string and change those to 'to' string
gsub2 <- function(pattern, replacement, x, ...) {
    for(i in 1:length(pattern))
        x <- gsub(pattern[i], replacement[i], x, ...)
    x  #no return will perform faster
}


#call the function and apply strings, narrative is the column with PHI information, that's the column we want to de-identify
enrolled_path$narrative <- gsub2(from, to, enrolled_path$narrative)

#to deal with NA strings
enrolled_path$narrative[is.na(enrolled_path$narrative)] <- " "


#https://stackoverflow.com/questions/18839096/rearrange-a-data-frame-by-sorting-a-column-within-groups
#sort file for organized report
enrolled_path <- enrolled_path[order(enrolled_path$pat_id, 
                                     enrolled_path$order_proc_id, 
                                     enrolled_path$path_report_date,
                                     enrolled_path$line),]

#this will group all the lines together, order_proc_id, represents number of unique reports
#https://stackoverflow.com/questions/8491754/converting-different-rows-of-a-data-frame-to-one-single-row-in-r
com <- enrolled_path%>%group_by(order_proc_id)%>%summarize(Text=paste(narrative, collapse=" "))


report <- merge(x=enrolled_path, y=com, by='order_proc_id')%>%
    select(-line, -narrative)%>%distinct(order_proc_id, .keep_all=TRUE)

#report <- report%>%filter(report$Text!='NA') #filter out empty reports
report <- report%>%filter(report$Text!=" ") 

report <- report%>%filter(report$Text!='This is a historical imaging order for reference images.')

#https://stackoverflow.com/questions/7245453/subset-function-with-different-than
#!!!!!!!#report <- report%>%filter(report$order_class!='Historical') not use
report <-  subset(report, !(order_class %in% 'Historical'))

report <- report%>%select(-order_class, -accession_num)

###--------------------Process specimen taken datetime issue--------------------------------####
# !diagnostics off
report$specimen_taken_time <- as.character(report$specimen_taken_time)
report$specimen_taken_time[is.na(report$specimen_taken_time)] <- "00:00:00"
report$specimen_taken_time <- chron(times=report$specimen_taken_time)

report <- report%>%unite(specimen_taken, specimen_taken_date, specimen_taken_time, sep=" ", remove=T)

n_distinct(report$pat_id)
n_distinct(report$order_proc_id)

#report <- report%>%filter(proc_name =='SURGICAL PATHOLOGY')
# or use ##http://dplyr.tidyverse.org/reference/bind.html
#final <- bind_rows(masterFile, today)

report <- merge(x=report, y=Enroll_ids, by.x='pat_id', by.y='pat_id', all.x=T) 

#Patient.ID, keep old pat_id for comparison
report <-report %>%select(-Consent.Withdrawn.At) 

report$path_report_date <- as.character(report$path_report_date)
#report$path_report_date <- as.Date(report$path_report_date, format='%Y-%m-%d')
#report$path_report_date <- as.Date(report$path_report_date, format='%m/%d/%Y')

report$Consent.Completed.At <- as.Date(report$Consent.Completed.At, format='%Y-%m-%d')
report$Blood.Draw.Completed.At <- as.Date(report$Blood.Draw.Completed.At, format='%Y-%m-%d')
#-----time format different**********************************************************************
# report$Consent.Completed.At <- as.Date(report$Consent.Completed.At, format='%m/%d/%Y')
# report$Blood.Draw.Completed.At <- as.Date(report$Blood.Draw.Completed.At, format='%m/%d/%Y')

#change this after screening blood draw, how many days have passed so far!!!
report <- report %>% mutate(Flag = as.numeric(difftime(Sys.Date(),
                                                       Blood.Draw.Completed.At, #update needed
                                                       units='days'))) 

#REPORT NEED TO BE AFTER PATIENTS' CONSENT DATE******************************************************

report$specimen_date <- report$specimen_taken
report$specimen_date <- as.Date(report$specimen_date, format='%m/%d/%Y')
report$month60 <- as.Date(report$Consent.Completed.At) %m+% months(60)
report <- report%>%filter(specimen_date>=Consent.Completed.At & specimen_date <= month60) #newly added 1/8/2018

report <- report%>%select(-specimen_date, -month60)
#report <- report%>%filter(path_report_date>=Consent.Completed.At) 

#----------------------------------------------------------------------------------------------
# #to bind to master list
# report$order_date <- as.character(report$order_date)
# report$order_date <- as.Date(report$order_date, format='%m/%d/%Y')

report <- report[order(report$Project.Participant.ID, report$path_report_date),]

#-------------------------Add Impression & Recommendation---------------------------------##
order_id <- paste0(paste0("'", report$order_proc_id,"'"),collapse=',')

query2 <- sprintf("select distinct order_proc_id 
                  , line
                  , IMPRESSION
                  from dcpwdbsXXXX.clarity_rpt_crystal.dbo.order_impression	  
                  where order_proc_id in (%s)", order_id)

query2 <- str_replace_all(str_replace_all(query2,"\n",""),"\\s+"," ")

impression_report <-  sqlQuery(channelXXXX, query2)

odbcClose(channelXXXX)

#to deal with NA strings 
impression_report$IMPRESSION[is.na(impression_report$IMPRESSION)] <- " "

impression_report <- impression_report[order(impression_report$order_proc_id, 
                                             impression_report$line),]

impression_report <- impression_report%>%group_by(order_proc_id)%>%
    summarize(imp=paste(IMPRESSION, collapse=" "))

report <- merge(x=report, y=impression_report, by='order_proc_id', all.x=T)

report <- report[, c('order_proc_id', 'pat_id', 'Patient.ID', 'Project.Participant.ID', 'order_date', 
                     'proc_name', 'specimen_taken', 'path_report_date', 
                     'Text', 'imp',  'MRN', 'First.Name', 'Last.Name', 'Consent.Completed.At', 
                     'Blood.Draw.Completed.At','Flag', 'Mammogram.Appointment.Location')]

##-----------------Combine two parts of report together------------------------##
#https://stackoverflow.com/questions/21003311/how-to-combine-multiple-character-columns-into-a-single-column-in-an-r-data-fram
#https://stackoverflow.com/questions/39041115/fixing-a-multiple-warning-unknown-column

report$imp[is.na(report$imp)] <- " "

report <- report%>% unite(text, Text, imp, sep = " ", remove = T)

#---------------------------------------------------------------------------------------------
# colnames(report)[which(names(report) == "Project.Participant.ID")] <- "Project Participant ID"
# colnames(report)[which(names(report) == "Personalized.Enrollment.Code")] <- "Personalized Enrollment Code"

#filter out duplicate reports
report <- report%>%filter(!report$order_proc_id %in% last_report$order_proc_id)

n_distinct(report$MRN)
n_distinct(report$pat_id)
#Add empty columns
#https://stackoverflow.com/questions/18214395/r-add-empty-columns-to-a-dataframe-with-specified-names-from-a-vector
newNames <- c('breast_y_n', 'cancer_y_n', 'cancer_type','invasive_y_n', 'Comment', 'Contain PHI?')
report[, newNames] <- ''

report$pat_id <- as.character(report$pat_id)
report$Patient.ID <- as.character(report$Patient.ID)

report <- rename(report, Project_participant_id = Project.Participant.ID)

#dailyReport <- report%>%filter(Flag>=80)

# or use 
##http://dplyr.tidyverse.org/reference/bind.html
#final <- bind_rows(masterFile, today)
#add new report to previous report to accumulate order_proc_id!!!!!!!!!!!!!!!!!!!!!!!!!!
#https://stackoverflow.com/questions/1299871/how-to-join-merge-data-frames-inner-outer-left-right
# last_report3 <- merge(x=last_report, y=report, by='order_proc_id', all=TRUE)%>%
#     select(order_proc_id, Project.Participant.ID, pat_id, Patient.ID, path_report_date)

reportMiddle <- report%>%select(order_proc_id, Project_participant_id, pat_id, Patient.ID)
#reportMiddle <- dailyReport%>%select(order_proc_id, Project.Participant.ID, pat_id, Patient.ID)

last_report3 <- bind_rows(last_report, reportMiddle)


#this is an accumulative report, keep adding more proc_ids
write.csv(last_report3, 'Path_report_workfile0226_2018.csv', row.names=FALSE)

#-------------------------------------------------------------------------------------------------------------
#-------------------------------------------------------------------------------------------------------------
# check if Patients' first name contained in the text
# Just in case to check patients' name exist in the text or not!
backup <- report
backup$First.Name <- paste0(substr(backup$First.Name, 1, 1),
                            tolower(substr(backup$First.Name, 2, nchar(backup$First.Name))))

backup <- separate(data = backup, col = First.Name, into = c("F1", "F2", "F3"), sep = " ")
backup$firstName_in <- mapply(grepl, pattern=backup$F1, x=backup$text)
unique(backup$firstName_in)

# report$Last.Name <- paste0(substr(report$Last.Name, 1, 1),
# tolower(substr(report$Last.Name, 2, nchar(report$Last.Name))))

# report<- report%>%unite(Name1, First.Name, Last.Name, sep=" ", remove=F)
# report<- report%>%unite(Name2, Last.Name, First.Name, sep=", ", remove=F)

#df$exists_in_description <- mapply(grepl, pattern=df$keyword, x=df$description)


# report$name1_in <- mapply(grepl, pattern=report$Name1, x=report$Text)
# unique(report$name1_in)

# report$name2_in <- mapply(grepl, pattern=report$Name2, x=report$Text)
# unique(report$name2_in)
#-------------------------------------------------------------------------------------------------------------

#write.csv(dailyReport, '80-day-Window.csv', row.names = FALSE)

library(rJava)
library(xlsxjars)
library(xlsx)

#to be used
report <- report%>%mutate(dummy = sample(1:10000000, length(order_proc_id),replace=F))
report <- unite(report, sutter_order_id, 'Project_participant_id', 'dummy', sep='-', remove=F)
report<- report%>%select(-dummy)

write.csv(report, 'Batch14_withPHI0226_2018_withPHI.csv', row.names = F)

write.xlsx(dailyReport, '80-day-window11202017.xlsx', row.names=FALSE)

write.xlsx(report, 'Batch14_Path-Report0226_2018.xlsx', row.names=FALSE)

#report <- backup

#import reviewed, no PHI path reports 
noPHI <- read.csv('Batch8part1_14.csv', stringsAsFactors = F, header=T)
report <- merge(x=noPHI, y=report, by.x='order_proc_id', all.x = T)

report <- report%>%mutate(dummy = sample(1:10000000, length(order_proc_id),replace=F))
report <- unite(report, sutter_order_id, 'Project_participant_id', 'dummy', sep='-', remove=F)
report<- report%>%select(-dummy)

report <- unique(report)
#report with PHI--------------------------------------------------------
write.csv(report, '02052018_B8P2B9withPHI.csv', row.names = F)

#report without PHI for sending----------------------------------------
report <- report[, c('sutter_order_id', 'Project_participant_id', 'order_date', 'proc_name', 'specimen_taken',
                     'path_report_date', 'text', 'breast_y_n', 'cancer_type','invasive_y_n', 
		     'hematologic_malignancy', 'date_of_dx','order_proc_id', 'pat_id', 'Patient.ID', 
               'MRN', 'First.Name', 'Last.Name', 'Consent.Completed.At', 'Blood.Draw.Completed.At', 'Flag',
               'Mammogram.Appointment.Location', 'cancer_y_n',  'dcis_y_n', 
               'leukemia_lymphoma_multiple_myeloma','Comment', 'Contain PHI?' )]

report <- report[, c('sutter_order_id', 'Project_participant_id', 'order_date', 'proc_name', 'specimen_taken',
                     'path_report_date', 'text', 'breast_y_n', 
				 'cancer_type','invasive_y_n', 'hematologic_malignancy', 'date_of_dx')]
#report$path_report_date <- format(as.Date(report$path_report_date), "%m/%d/%Y")

write.csv(report, '02022018_B8P1_noPHI.csv', row.names=FALSE)



