setwd('')

#--------------------------------------------------------------------------------------------------
#Clean up data get it ready for SQL to process 
#--------------------------------------------------------------------------------------------------
#fread will read file faster

data<- fread('G05162017.csv', header=T, stringsAsFactors=F, na.strings = 'NULL')
#read.csv()

#data <- data%>%filter(Patient.ID != 2017042101)%>%
#                      filter(Patient.ID != 2017040501)

#remove space in column names
colnames(data) <- make.names(colnames(data), unique = T)


#consent, blood draw and Qx statistics|only run when necessary---------------------
# Total_consent <- sum(data$Consent.Completed.At!=""&data$Consent.Withdrawn.At=="")
# Total_consent_blood <- sum(data$Consent.Completed.At!=""&data$Blood.Draw.Completed.At!="")
# Total_consent_Qx <- sum(data$Consent.Completed.At!=""&data$Qx.Completed.At!="")
#----------------------------------------------------------------------------------

#select email sent out cohort
#email_sent <- data%>%filter(is.na(Email.Sent.At)==F) #this is for having NAs
email_sent <- data%>%filter(Email.Sent.At!="")


#-------------------------This is for future data manipulation---------------------------------#
#since email will be sent couple of times, find the earliest email sent out time********************
#http://stackoverflow.com/questions/15347282/split-delimited-strings-in-a-column-and-insert-as-new-rows
# 
# library(tidyr)
# email_sent <- email_sent%>%mutate(Email.Sent.At=strsplit(as.character(Email.Sent.At), ','))%>%
#     unnest(Email.Sent.At)
# 
# email_sent <- email_sent%>%mutate(Email.Subject=strsplit(as.character(Email.Subject), ','))%>%
#     unnest(Email.Subject)  
# #sadly, this can be used for one varaibles other it will duplicate rows, increase rows
# #https://cran.r-project.org/web/packages/tidyr/tidyr.pdf*****this increase COLUMNS to match email*** 
# #sent time and subject line
# email_sent <- separate(email_sent, Email.Sent.At, c('Email1', 'Email2'), 
#                                     sep=',', fill='right')
# 
# email_sent <- separate(email_sent, Email.Subject, c('Subject1', 'Subject2'), 
#                        sep=',', fill='right')
# #filter out the biggest value in the comma separated values
# email_sent <- email_sent%>%group_by(POM.ID)%>%filter(Email.Sent.At==min(Email.Sent.At))
#-----------------------------------------------------------------------------------------------#


#the following codes solve problems of have different datetime formats in one column************
#http://stackoverflow.com/questions/13764514/how-to-change-multiple-date-formats-in-same-column
a <- as.Date(email_sent$Email.Sent.At, format = '%m/%d/%Y')
b <- as.Date(email_sent$Email.Sent.At, format = "%Y-%m-%d")
a[is.na(a)] <- b[!is.na(b)]
email_sent$Email.Sent.At <- a
#to see if the format is consistent unique(email_sent$Email.Sent.At)

# email_sent <- email_sent%>%mutate(Email.Sent.At = ifelse(grepl("2017-03-31", Email.Sent.At), "3/31/2017", Email.Sent.At) )  # this will not need any more since different formats problem has been solved


email_sent <- email_sent%>%select(Patient.ID, CSN, Mammogram.Appontment.Date, 
                                  Mammogram.Appointment.Time,
                     Mammogram.Appointment.Location, Date.Of.Birth, Address.Line.1, City, State, 
                   Zip, Email.Sent.At, Email.Subject, Consent.Completed.At, Consent.Withdrawn.At)

#from characters to date format
email_sent$Mammogram.Appontment.Date <- as.Date(email_sent$Mammogram.Appontment.Date, 
                                                format='%m/%d/%Y')
email_sent$Consent.Completed.At <- as.Date(email_sent$Consent.Completed.At, format = '%m/%d/%Y')

email_sent$Date.Of.Birth <- as.Date(email_sent$Date.Of.Birth, format = '%m/%d/%Y')


email_sent <- mutate(email_sent, AGE =(as.numeric(difftime(Email.Sent.At, Date.Of.Birth,  
                                                           unit="weeks"))/52.2))

# email_sent <- email_sent%>%mutate(AGE2=ifelse(AGE<=40, 1, 
#                                              ifelse(AGE>40&AGE<=65, 2, 
#                                                     ifelse(AGE>65, 3, 0))))

#set up oucome, 04/21 Per Sherry, pt withdraw count as Consent outcome 0
email_sent <- email_sent%>%mutate(outcome = ifelse(is.na(Consent.Completed.At)==F&
                                                       Consent.Withdrawn.At=="", 1, 0))


#organize email subject lines
email_sent <- email_sent%>%mutate(
    subjectLine = ifelse(grepl("You're invited to the STRIVE Research Study", Email.Subject), 'D',
                         ifelse(grepl("Will you help with a research study to detect cancer early?",Email.Subject), 'C', 
                                ifelse(grepl("New Research Study at Sutter Health: Help Detect Cancer Early", Email.Subject), 'B',
                                       ifelse(grepl("Help Detect Cancer Early: New Study at Sutter Health", Email.Subject), 'A', 'D')))))

#assume consent the same with mammo completed is completed on site with CRC
email_sent <- email_sent%>%mutate(onsite = ifelse(Mammogram.Appontment.Date ==Consent.Completed.At&
                                                      Consent.Withdrawn.At=="", 1, 0))

email_sent <- email_sent%>%mutate(emailMonth = month(Email.Sent.At))


email_sent<- email_sent%>%filter(year(Mammogram.Appontment.Date)!='2016')%>%select(-Date.Of.Birth,
                                                 -Consent.Withdrawn.At, -Email.Subject)


#write.csv(email_sent, file='email_cohort0421.csv')
#write.csv(email_sent, file='email_cohort0516.csv', row.names=FALSE)
#library(lubridate) guess_formats
#compare hour
#http://stackoverflow.com/questions/23029812/compare-time-in-r
#ifelse(strptime(A, "%H:%M") < strptime(B, "%H:%M"), "Dead", "NONE")

#-How to select patients before certain time --#
# email_sent <- email_sent%>%mutate(time = format(Mammogram.Appointment.Time, format="%H:%M:%S"))
# library(chron)
# email_sent$time <- chron(times=email_sent$time)
# test <- email_sent%>%filter(time>='09:00:00'&time<='17:00:00')

#--------------------------------------------------------------------------------------------------
#Survival analysis  04/27/2017
#--------------------------------------------------------------------------------------------------


#1404 patients with email sent, then import to SQL to get all the required data
#5/19/2017 due to different servers, get enc and race, ethnicity information from 715 seperately

crms <- read.csv('email_cohort0516.csv', header=T, stringsAsFactors = F, na.strings ='NULL')
enc <- read.csv('email_cohort2_enc0519.csv', header=T, stringsAsFactors = F, na.strings ='NULL')
race_eth <- read.csv('email_cohortV2raceandEth0519.csv', header=T, stringsAsFactors = F, 
                     na.strings ='NULL')

data_file <- merge(x=crms, y=enc, by.x='Patient.ID', by.y='pat_id', all.x=T)

data_file <- merge(x=data_file, y=race_eth, by.x='Patient.ID', by.y='pat_id', all.x=T)

#data_file <- fread('Grail04262017.csv', header=T, stringsAsFactors=F, na.strings = 'NULL', 
#                   colClasses = list(character = 25))


#Per Sherry, no consent date will use the data pull date 5/16 
data_file$Consent.Completed.At <- as.Date(data_file$Consent.Completed.At, '%Y-%m-%d')
data_file$Consent.Completed.At[is.na(data_file$Consent.Completed.At)] <- '2017-05-16'

data_file$Email.Sent.At <- as.Date(data_file$Email.Sent.At, '%Y-%m-%d')

data_file <- data_file %>% mutate(time = as.numeric(difftime(Consent.Completed.At, Email.Sent.At,
                                                             units='days')))

#data_file <- data_file %>%select (-patient_race_c, -ethnic_group_c)

#Per Sherry, NA insurance will be coded as 2
data_file$Medicare[is.na(data_file$Medicare)] <-2
data_file$medicaid[is.na(data_file$medicaid)] <-2
data_file$HMO[is.na(data_file$HMO)] <-2
data_file$Commercial[is.na(data_file$Commercial)] <-2
data_file$other[is.na(data_file$other)] <-2  #Other is an insurance category in Clarity


#Unknown means we couldn't find this patients' information from Clarity 
data_file$pcp_serv_area[is.na(data_file$pcp_serv_area)] <- 'Unknown'
data_file$ETHNIC_GROUP[is.na(data_file$ETHNIC_GROUP)] <- 'Unknown'
data_file$RACE_CATEGORY[is.na(data_file$RACE_CATEGORY)] <- 'Unknown'

#onsite 1 means consent the same day mammo completed, 0 means not the same day, 2 means no consent
data_file$onsite <- as.numeric(data_file$onsite)
data_file$onsite[is.na(data_file$onsite)] <-2


data_file[is.na(data_file)] <- 0


#Per Sherry change to this new criteria, 5/2/2017
data_file <- data_file%>%mutate(age=ifelse(AGE<50, 1, 
                                           ifelse(AGE>=50&AGE<60, 2, 
                                                  ifelse(AGE>=60&AGE<70, 3, 
                                                         ifelse(AGE>=70, 4, 0)))))

data_file <- data_file%>%mutate(RACE_CATEGORY = ifelse(grepl('MULTI-RACE', RACE_CATEGORY)|
                         grepl('BLACK OR AFRICAN AMERICAN', RACE_CATEGORY)|
             grepl('NATIVE HAWAIIAN/PACIFIC ISLANDER', RACE_CATEGORY), 'OTHER', RACE_CATEGORY))

data_file <- data_file%>%mutate(ETHNIC_GROUP = ifelse(grepl('OTHER', ETHNIC_GROUP), 
                                                      'NON_HISPANIC', ETHNIC_GROUP))

#data_file$time <- as.numeric(data_file$time)
#pcp visit, 1 means yes, 0 means no pcp visit
data_file <- data_file%>%mutate(count_pcp_visit= ifelse(count_pcp_visit>=1, 1, 0) )


data<- data_file

data <- data%>%mutate(Mammogram.Appontment.Date = as.Date(Mammogram.Appontment.Date, '%Y-%m-%d'))
data <- data%>%mutate(email_vs_mammodate = as.numeric(difftime(Mammogram.Appontment.Date, 
                                                               Email.Sent.At, units='days')))
#data<- data%>%filter(year(mammo_date)!='2016')


#5/10/2017 try to see which days resonable for sending emails----------------------- 

data<- data%>%mutate(email_vs_mammodate=ifelse(email_vs_mammodate>=0&email_vs_mammodate<7, 1, 
                                               ifelse(email_vs_mammodate>=7&email_vs_mammodate<14, 2,
                                                      ifelse(email_vs_mammodate>=14, 3, 0))))
for (j in 0:1){
    for (i in 1:4){
        a = c('A', 'B', 'C', 'D')
        total <- data%>%filter(grepl(a[i], subjectLine))%>%summarize(ct=n())
        consent <- data%>%filter(grepl(a[i], subjectLine))%>%filter(outcome==j)%>%summarize(ct=n())
        age <- data%>%filter(grepl(a[i], subjectLine))%>%filter(outcome==j)%>%group_by(age)%>%summarize(ct=n())
        race <- data%>%filter(grepl(a[i], subjectLine))%>%filter(outcome==j)%>%group_by(RACE_CATEGORY)%>%summarize(ct=n())
        ethnic <- data%>%filter(grepl(a[i], subjectLine))%>%filter(outcome==j)%>%group_by(ETHNIC_GROUP)%>%summarize(ct=n())
        pcp <- data%>%filter(grepl(a[i], subjectLine))%>%filter(outcome==j)%>%group_by(count_pcp_visit)%>%summarize(ct=n())
        days <- data%>%filter(grepl(a[i], subjectLine))%>%filter(outcome==j)%>%group_by(email_vs_mammodate)%>%summarize(ct=n())
        site <- data%>%filter(grepl(a[i], subjectLine))%>%filter(outcome==j)%>%group_by(onsite)%>%summarize(ct=n())
        cat(j)
        cat(a[i])
        cat("\n")
        cat(paste("Total:", total, '\n'))
        cat(paste("Consent:", consent, '\n'))
        cat(paste("Age:", age, '\n'))
        cat(paste("Race:", race, '\n'))
        cat(paste("Ethnic:", ethnic, '\n'))
        cat(paste("PCP:", pcp, '\n'))
        cat(paste("DAYS:", days, '\n'))
        cat(paste("Onsite:", site, '\n'))
    }
}


#-----------------------------------------------------------------------------------------
###Kaplan-Meier method
library(survival)
library(survminer)
#install.packages("survminer")
#install.packages("psych")
fit1 <- survfit(Surv(data$time, data$outcome)~data$age)
# N <- length(unique(data$age))
# plot(fit1, xlab="Time by Days", ylab="Survival Probability", mark.time=F, col=1:N)
# labels <- gsub("data$age:", " ", c('age1<=40', '40<age2<=65', 'age3>65'))#names(fit1$strata))# 
# legend("top", legend =labels, col=1:N, horiz=FALSE, bty="n" )
# summary(fit1)
# png(file='Grail_age.png')
# dev.off()

#ggsurvplot(fit1, data=data, risk.table=T)
ggsurvplot(fit1, linetype = 'strata', legend='right', legend.title='Age', 
           legend.labs=c('age1<50', '50=<age2<60', '60=<age3<70', 'age4>=70'))
#install.packages("rms"), install.packages("survminer")
# library(rms)
# fit0 <- survfit(data$time*data$outcome~data$age, conf.type="log-log")
# survplot(fit0)

fit2 <- survfit(Surv(data$time, data$outcome)~data$subjectLine)
#ggsurvplot(fit2, data=data, risk.table=T)
ggsurvplot(fit2, linetype = 'strata', legend='right', legend.title='Subject', 
           legend.labs=c('Option A: Help Detect Cancer Early: New Study at Sutter Health', 
                         'Option B: New Research Study at Sutter Health: Help Detect Cancer Early', 
                         'Option C: Will you help with a research study to detect cancer early?', 
                         'Option D: You are invited to the STRIVE Research Study'))

fit3 <- survfit(Surv(data$time, data$outcome)~data$count_pcp_visit)
ggsurvplot(fit3, linetype = 'strata', legend='right', legend.title='Subject', 
           legend.labs=c('1:have pcp visit', '0:no pcp visit'))


fit4 <- survfit(Surv(data$time, data$outcome)~data$RACE_CATEGORY)
ggsurvplot(fit4, linetype = 'strata', legend='right', legend.title='Race')


fit5 <- survfit(Surv(data$time, data$outcome)~data$ETHNIC_GROUP)
ggsurvplot(fit5, linetype = 'strata', legend='right', legend.title='Ethnicity')

fit6 <- survfit(Surv(data$time, data$outcome)~data$email_vs_mammodate)
ggsurvplot(fit6, linetype='strata', legend = 'right', 
           legend.title = 'first email vs scheduled mammo',
           legend.labs = c('0:<0', '1: 0~6', '2:7~13', '3:>=14'))


fit7 <- survfit(Surv(data$time, data$outcome)~data$emailMonth)
ggsurvplot(fit7, linetype='strata', legend = 'right', 
           legend.title = 'Month of sending emails', 
           legend.labs = c('3: send in March', '4: sent in April', '5:sent in May'))

fit8 <- survfit(Surv(data$time, data$outcome)~data$onsite)
ggsurvplot(fit8, linetype='strata', legend = 'right', 
           legend.title = 'Mammo the same with consent', 
legend.labs = c('0: Not consent the same day', '1: consent the same day', '2: no consent'))
#----------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------
data$age <- as.factor(data$age)                   #    1
data$count_pcp_visit <- as.factor(data$count_pcp_visit)  # 0
data$subjectline <- as.factor(data$subjectLine)  # A 
data$RACE_CATEGORY <- as.factor(data$RACE_CATEGORY)# WHITE
data$ETHNIC_GROUP <- as.factor(data$ETHNIC_GROUP)  #NON_HISPANIC
data$email_vs_mammodate <- as.factor(data$email_vs_mammodate)
data$onsite <- as.factor(data$onsite)
# levels(data$age)
# levels(data$RACE_CATEGORY)
# levels(data$ETHNIC_GROUP)

data <- within(data, age <- relevel(age, ref = "1"))  #set up reference level
data <- within(data, subjectline <- relevel(subjectLine, ref = 'A'))
data <- within(data, count_pcp_visit <- relevel(count_pcp_visit, ref = '0'))
data <- within(data, RACE_CATEGORY <- relevel(RACE_CATEGORY, ref = 'WHITE'))
data <- within(data, ETHNIC_GROUP <- relevel(ETHNIC_GROUP, ref = 'NON_HISPANIC'))
data <- within(data, email_vs_mammodate <- relevel(email_vs_mammodate, ref = '1'))
data <- within(data, onsite <- relevel(onsite, ref = '1'))

#Cox regression model

fit_cox <- coxph(Surv(time, outcome) ~ (age + subjectLine + count_pcp_visit + 
                            RACE_CATEGORY + ETHNIC_GROUP + email_vs_mammodate + onsite), data=data)
summary(fit_cox)

# fit <- coxph(Surv(t,y) ~ x)
# summary(fit)  #output provides HR CIs
# confint(fit)  #coefficient CIs
# exp(confint(fit))  #Also HR CIs
confint(fit_cox)
exp(confint(fit_cox))









#-------------------------------------------------------------------------------------------------
#http://stackoverflow.com/questions/14434660/how-do-i-convert-a-text-string-of-null-to-a-number
# If you're reading this in with read.csv, set the option na.strings='NULL'. This will import file$x as numeric, instead of factor, with numeric NAs objects in place of the NULL strings. Then to replace NAs with 0s:
# 
# file$x[is.na(file$x)] <- 0




#shrink data size
#data2 <- data%>%filter(data$Consent.Completed.At != "")

#data <- read.csv('GrailCensus04062017.csv', header=T, stringsAsFactors = F)

# par(mfrow=c(1, 2))
# hist(data$pat_ct, col='gray', xlab='Pt Ct', ylab='Frequency of Pt. Ct', main='Frequency')
# hist(data$pat_ct, col='gray', freq=F, xlab='Pt Ct', ylab='Density', main='Density')

#length(data[is.na(data$Email.Opened.At)])

#If you want to count the strings that are not identical to the empty string ("")
# sum(data2$Consent.Completed.At != "")  
# 
# sum(data2$Email.Sent.At !="")
# sum(data2$Email.Opened.At !="")
# sum(data2$Email.Login.At !="")
# sum(data2$Login.At !="")

# dat3 <- aggregate(interests~id, dat2, paste, collapse=",")
# merge(dat1, dat3, "id")
#######################################################################################
#split cells
#http://stackoverflow.com/questions/15347282/split-delimited-strings-in-a-column-and-insert-as-new-rows

# #summarize total--------------------------------------------------------
# data <- data0419%>%group_by(subjectLine)%>%
#     summarize(sent = sum(data0419$'Email Sent At' !=""),
#               open = sum(data0419$'Email Opened At' !=""),
#               login = sum(data0419$'Email Login At' !=""),
#               consent = sum(data0419$'Consent Completed At'!="")
#     )




#http://stackoverflow.com/questions/13764514/how-to-change-multiple-date-formats-in-same-column
# a <- as.Date(data$initialDiagnose,format="%m/%d/%Y") # Produces NA when format is not "%m/%d/%Y"
# b <- as.Date(data$initialDiagnose,format="%d.%m.%Y") # Produces NA when format is not "%d.%m.%Y"
# a[is.na(a)] <- b[!is.na(b)] # Combine both while keeping their ranks
# data$initialDiagnose <- a # Put it back in your dataframe
# data$initialDiagnose
# [1] "2009-01-14" "2005-09-22" "2010-04-21" "2010-01-28" "2009-01-09" "2005-03-28" "2005-01-04" "2005-01-04" "2010-09-17" "2010-01-03"
# Additionnaly here's the preceding method adapted to a situation where you have three (or more) different formats:
# 
# data$initialDiagnose
# [1] 14.01.2009 9/22/2005  12 Mar 97  4/21/2010  28.01.2010 09.01.2009 3/28/2005 
# Levels: 09.01.2009 12 Mar 97 14.01.2009 28.01.2010 3/28/2005 4/21/2010 9/22/2005
# 
# multidate <- function(data, formats){
#     a<-list()
#     for(i in 1:length(formats)){
#         a[[i]]<- as.Date(data,format=formats[i])
#         a[[1]][!is.na(a[[i]])]<-a[[i]][!is.na(a[[i]])]
#         }
#     a[[1]]
#     }
# 
# data$initialDiagnose <- multidate(data$initialDiagnose, 
#                                   c("%m/%d/%Y","%d.%m.%Y","%d %b %y"))
# data$initialDiagnose