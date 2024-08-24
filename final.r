#################
#라이브러리
library(noncompliance)
library(data.table)
library(tidyverse)
library(RcppRoll)
library(AER) 
library(hts)
library(lightgbm)
library(knitr)
library(rmarkdown)
igc <- function() {
  invisible(gc()); invisible(gc())
}
igc()

###################################################
#데이터전처리
###################################################
#데이터 불러오기 및 통합
jeju <- fread("C:/Users/hanshin18/Desktop/공모전 통합/dacon/jeju_data_ver1/201901-202003.csv", encoding = 'UTF-8')
jeju04 <- fread("C:/Users/hanshin18/Desktop/공모전 통합/dacon/jeju_data_ver1/202004.csv", encoding = 'UTF-8')
#4월데이터와 기존 데이터 통합 
jeju <- rbind(jeju,jeju04)
#AMT,CNT,CSTRMR_CNT의 총합
jeju <- jeju[,lapply(.SD,sum),by=.(REG_YYMM,CARD_SIDO_NM,STD_CLSS_NM),.SDcols=c("AMT","CNT","CSTMR_CNT")] %>% arrange(REG_YYMM,CARD_SIDO_NM,STD_CLSS_NM)
#################
#데이터 전처리
#모든 케이스 찾아주기
REG_YYMM1 <- unique(jeju$REG_YYMM)
CARD_SIDO_NM1 <-sort(unique(jeju$CARD_SIDO_NM))
STD_CLSS_NM1 <- sort(unique(jeju$STD_CLSS_NM))
R_C_T <- expand.grid.DT(REG_YYMM1,CARD_SIDO_NM1,STD_CLSS_NM1,col.names = c("REG_YYMM","CARD_SIDO_NM","STD_CLSS_NM"))
#결측값 0으로 채워주기
jeju <- data.table::merge.data.table(R_C_T,jeju,all.x=T) %>% arrange(REG_YYMM,CARD_SIDO_NM,STD_CLSS_NM)
setnafill(jeju,fill=0,cols=c("AMT","CNT","CSTMR_CNT"))
#2020년 5월,6월,7월 데이터 추가해주기 
pre_day <- c(202005,202006,202007)
pre <- expand.grid.DT(pre_day, CARD_SIDO_NM1, STD_CLSS_NM1, col.names = c("REG_YYMM","CARD_SIDO_NM","STD_CLSS_NM"))
pre[,`:=`(AMT=NA,CNT=NA,CSTMR_CNT=NA)]
jeju <-  rbindlist(list(jeju,pre),fill=T) %>% arrange(REG_YYMM,CARD_SIDO_NM,STD_CLSS_NM)
#AMT 수치형으로 변환
jeju$AMT <- as.numeric(jeju$AMT)
###################################################
#파생변수
###################################################
#휴일수 (각 월별 휴일 수)

eight <- c(201901,201904,201907,202006,202007)
nine <- c(201905,201911,202002,202003)
ten <- c(201908,201910,201912,202004)


jeju <- jeju %>% mutate(holi=ifelse(REG_YYMM%in%eight,8,
                                    ifelse(REG_YYMM%in%nine,9,
                                           ifelse(REG_YYMM%in%ten,10,11))))
#################
#계절 (봄,여름,가을,겨울 변수)

spring <- c("03","04","05")
summer <- c("06","07","08")
fall <- c("09","10","11")

jeju <- jeju %>% mutate(season=ifelse(str_sub(REG_YYMM,start = 5)%in%spring,1,
                                      ifelse(str_sub(REG_YYMM,start = 5)%in%summer,2,
                                             ifelse(str_sub(REG_YYMM,start = 5)%in%fall,3,4))))

#메모리 삭제
rm(list=c("REG_YYMM1","pre","pre_day","eight","nine","ten","spring","summer","fall","R_C_T"))
igc()
###################################################
#데이터 버전1
###################################################
#AMT.lag3 
jeju1 <- data.table(jeju)
#이동평균 
jeju1[, AMT.lag3 := shift(AMT, 3), by=c("CARD_SIDO_NM","STD_CLSS_NM")]
#roll_mean2은 lag 3시점 전까지에서 2개씩 평균내는것 
#roll_mean3은 lag 3시점 전까지에서 3개씩 평균내는것 
#roll_max3은 lag 3시점 에서 큰수 끼리 이동평균 내는것
#roll_min3는 lag 3시점에서 작은 수 끼리 이동평균 내는것

jeju1[, `:=`(roll_mean2 = roll_meanr(AMT.lag3, 2),
             roll_mean3 = roll_meanr(AMT.lag3, 3),
             roll_max3 = roll_maxr(AMT.lag3, 3),
             roll_min3 = roll_minr(AMT.lag3, 3)),by=c("CARD_SIDO_NM","STD_CLSS_NM")]

# 가중값 주기 

jeju1[, `:=`(roll_AMT_comb=(1/2)*roll_mean2+(1/3)*roll_mean3+(1/4)*AMT.lag3)]

#################
#CNT.lag3 
#roll_mean2은 lag 3시점 전까지에서 2개씩 평균내는것 
#roll_mean3은 lag 3시점 전까지에서 3개씩 평균내는것 
#roll_max3은 lag 3시점 에서 큰수 끼리 이동평균 내는것
#roll_min3는 lag 3시점에서 작은 수 끼리 이동평균 내는것

jeju1[, CNT.lag3 := shift(CNT, 3), by=c("CARD_SIDO_NM","STD_CLSS_NM")]
jeju1[, `:=`(CNT_roll_mean2 = roll_meanr(CNT.lag3, 2),
             CNT_roll_mean3 = roll_meanr(CNT.lag3, 3),
             CNT_roll_max3 = roll_maxr(CNT.lag3, 3),
             CNT_roll_min3 = roll_minr(CNT.lag3, 3)),by=c("CARD_SIDO_NM","STD_CLSS_NM")]
#가중 평균
jeju1[, `:=`(roll_CNT_comb=(1/2)*CNT_roll_mean2+(1/3)*CNT_roll_mean3+(1/4)*CNT.lag3)]

#################
#CNT 대비 AMT 2019년6월부터 데이터가 존재함 
jeju1 <- jeju1[REG_YYMM>201905]
jeju1 <- jeju1 %>% mutate(a_c=AMT.lag3/CNT.lag3) %>% mutate(a_c=ifelse(is.nan(a_c),0,a_c))
#################
#정규화 및 변수 전처리
#holi,season 변수 
jeju1 <- jeju1 %>% select(!c("holi","season"),everything(),c("holi","season"))
#log화 
jeju1 <- jeju1 %>% mutate_at(4:18,function(x){log(x+1)})
#factor화 시키기 
jeju1 <- jeju1 %>% mutate(REG_YYMM=as.integer(as.factor(REG_YYMM)),season=as.character(season))

jeju1 <- jeju1 %>% dplyr::select(!c(CNT,CSTMR_CNT))
###################################################
#데이터 버전2 이동평균 시계열 데이터 손실을 덜 하게하기위해 
###################################################
jeju2 <- data.table(jeju)

#################
#AMT.lag3
jeju2[, AMT.lag3 := shift(AMT, 3), by=c("CARD_SIDO_NM","STD_CLSS_NM")]

jeju2[, `:=`(roll_mean2 = roll_meanr(AMT.lag3,2,fill=0,na.rm=T),
             roll_max2 = roll_maxr(AMT.lag3, 2,fill=0,na.rm=T),
             roll_min2 = roll_minr(AMT.lag3, 2,fill=0,na.rm=T))
      ,by=c("CARD_SIDO_NM","STD_CLSS_NM")]
#################
#CNT.lag3
jeju2[, CNT.lag3 := shift(CNT, 3), by=c("CARD_SIDO_NM","STD_CLSS_NM")]

jeju2[, `:=`(roll_CNT_mean2 = roll_meanr(CNT.lag3, 2,fill=0,na.rm=T),
             roll_max2 = roll_maxr(CNT.lag3, 2,fill=0,na.rm=T),
             roll_min2 = roll_minr(CNT.lag3, 2,fill=0,na.rm=T))
      ,by=c("CARD_SIDO_NM","STD_CLSS_NM")]
#################
#비율 2019년 4월 데이터부터
jeju2 <- jeju2[REG_YYMM>201903]
jeju2 <- jeju2 %>% mutate(a_c=AMT.lag3/CNT.lag3) %>% mutate(a_c=ifelse(is.nan(a_c),0,a_c))

#################
#정규화 및 전처리
jeju2 <- jeju2 %>% select(!c("holi","season"),everything(),c("holi","season"))
jeju2 <- jeju2 %>% mutate_at(4:13,function(x){log(x+1)})
jeju2 <- jeju2 %>% mutate(REG_YYMM=as.integer(as.factor(REG_YYMM)),season=as.character(season)) %>%  dplyr::select(!c(CNT,CSTMR_CNT))

###################################################
#데이터 버전 3 AMT 로그화 
###################################################
jeju3 <- jeju %>% mutate(CARD_SIDO_NM=factor(CARD_SIDO_NM,labels=letters[1:17]),STD_CLSS_NM=factor(STD_CLSS_NM,labels=10:50))%>% mutate(AMT=log(AMT+1))

jeju3 <- pivot_wider(jeju3,id_cols = c("REG_YYMM","CARD_SIDO_NM","STD_CLSS_NM"),
                     names_from = c("CARD_SIDO_NM","STD_CLSS_NM"),values_from=AMT)%>%
  filter(REG_YYMM<202005)%>%
  dplyr::select(-REG_YYMM) 
#ts matrix 만들기 
jeju3 <- ts(as.matrix(jeju3,ncol=697,nrow=16),start=c(2019,01),freq=12)


###################################################
#데이터 버전 1 회귀적합
###################################################
pred.data1 <- jeju1[REG_YYMM>=12]

#################
#regression
#회귀계수중 유의하지 않은것 삭제 
lm.fit1 <-  lm(AMT~.-roll_mean2-roll_mean3
               ,data=jeju1[REG_YYMM<12])
#AMT에서 0에서 9까지의 값은 0으로 본다.
lm.pred1 <- predict(lm.fit1,newdata=pred.data1)
lm.pred1 <- data.table(AMT=ifelse(exp(lm.pred1)-10>0,exp(lm.pred1)-1,0),sido=rep(CARD_SIDO_NM1,each=41,times=3),STD=rep(STD_CLSS_NM1,times=51),reg=rep(202005:202007,each=697)) %>%pivot_wider(everything(),names_from=reg,values_from=AMT)


#################
#tobit regression
#회귀계수중 유의하지 않은것 삭제 
to.fit1 <- tobit(AMT~.-roll_min3-roll_mean2
                 ,data=jeju1[REG_YYMM<12])
#AMT에서 0에서 9까지의 값은 0으로 본다. 
to.pred1 <- predict(to.fit1,newdata=pred.data1)
to.pred1 <- data.table(AMT=ifelse(exp(to.pred1)-10>0,exp(to.pred1)-1,0),sido=rep(CARD_SIDO_NM1,each=41,times=3),STD=rep(STD_CLSS_NM1,times=51),reg=rep(202005:202007,each=697)) %>%pivot_wider(everything(),names_from=reg,values_from=AMT)

#################
#예측값 평균 (선형회귀와 tobit 회귀의 평균)
reg.pred1 <- (lm.pred1$`202007`+to.pred1$`202007`)/2

###################################################
#데이터 버전 2 회귀적합
###################################################

pred.data2 <- jeju2[REG_YYMM>=14]
#################
#regression
#유의하지 않은 변수 삭제 
lm.fit2 <- lm(AMT~.-roll_max2-roll_CNT_mean2,data=jeju2[REG_YYMM<14])
#작은 값 0으로 처리 (0~9) 후 예측
lm.pred2 <- predict(lm.fit2,newdata=pred.data2)
lm.pred2 <- data.table(AMT=ifelse(exp(lm.pred2)-10>0,exp(lm.pred2)-1,0),sido=rep(CARD_SIDO_NM1,each=41,times=3),STD=rep(STD_CLSS_NM1,times=51),reg=rep(202005:202007,each=697)) %>%pivot_wider(everything(),names_from=reg,values_from=AMT)


#################
#tobit regression
#유의하지 않은 변수 삭제 
to.fit2 <- tobit(AMT~.-roll_max2-roll_CNT_mean2,data=jeju2[REG_YYMM<14])
#작은 값 0으로 처리 후 예측
to.pred2 <- predict(to.fit2,newdata=pred.data2)
to.pred2 <- data.table(AMT=ifelse(exp(to.pred2)-10>0,exp(to.pred2)-1,0),sido=rep(CARD_SIDO_NM1,each=41,times=3),STD=rep(STD_CLSS_NM1,times=51),reg=rep(202005:202007,each=697)) %>%pivot_wider(everything(),names_from=reg,values_from=AMT)

#################
#예측값 평균 (데이터 버전2 선형회귀와 tobit회귀 평균)
reg.pred2 <- (lm.pred2$`202007`+to.pred2$`202007`)/2

###################################################
#arima 및 ets 적합
#계층적 시계열 
###################################################
hts.1 <- hts(jeju3,characters=c(2,2))

#################
#ets모델 적합
#계층적 시계열에서 햐향식 접근 방식 사용 
ets.1 <- forecast(hts.1,method="tdfp",fmethod = "ets",h=3)
ets.1 <- aggts(ets.1,levels = 2)

ets.1 <- as.data.table(ets.1) %>% pivot_longer(cols=everything(),
                                               names_to=c("CARD_SIDO_NM","STD_CLSS_NM"),
                                               names_sep="_",values_to="AMT")
ets.1 <- ets.1%>% mutate(CARD_SIDO_NM=factor(CARD_SIDO_NM,labels=unique(jeju$CARD_SIDO_NM)),STD_CLSS_NM=factor(STD_CLSS_NM,labels=unique(jeju$STD_CLSS_NM)))
#2020년 5월,6월,7월 AMT 예측
ets.1 <- mutate(ets.1,REG_YYMM=rep(c(202005,202006,202007),each=697),AMT=ifelse((exp(AMT)-10)>0,exp(AMT)-1,0)) 

ets.1 <- pivot_wider(ets.1,everything(),names_from = REG_YYMM,values_from = AMT)
ets.1 <- ets.1$'202007'
#################
#arima모델 적합
#계층적 시계열 중 하향식 접근 방식 사용 
#arima 적합
arima.1 <- forecast(hts.1,method="tdfp",fmethod = "arima",h=3)
arima.1 <- aggts(arima.1,levels=2)
#arima 적합 후 기존의 데이터 형태로 변환(long data 형태)
arima.1 <- as.data.table(arima.1) %>% 
  pivot_longer(cols=everything(),
               names_to=c("CARD_SIDO_NM","STD_CLSS_NM"),
              names_sep="_",values_to="AMT")
arima.1 <- arima.1%>% mutate(CARD_SIDO_NM=factor(CARD_SIDO_NM,labels=unique(jeju$CARD_SIDO_NM)),STD_CLSS_NM=factor(STD_CLSS_NM,labels=unique(jeju$STD_CLSS_NM)))
#AMT 가시적인 효과를 위한 전처리 
arima.1 <- mutate(arima.1,REG_YYMM=rep(c(202005,202006,202007),each=697),AMT=ifelse((exp(AMT)-10)>0,exp(AMT)-1,0)) 

arima.1 <- pivot_wider(arima.1,everything(),names_from = REG_YYMM,values_from = AMT)
arima.1 <- arima.1$'202007'
###################################################
#lightgbm
###################################################
#train
#2020년 4월 전까지의 데이터를 train 로 적합
jeju4 <- jeju1[REG_YYMM<11]

cf <- c("season","CARD_SIDO_NM","STD_CLSS_NM")
label <- jeju4[,"AMT"]

train <- jeju4 %>% mutate_at(c("season","CARD_SIDO_NM","STD_CLSS_NM"),function(x){as.numeric(as.factor(x))}) %>% select(-AMT)
#lgb를 위한 matrix 생성
train_matrix <-  lgb.Dataset(data = data.matrix(as.matrix(train)), label =data.matrix(as.matrix(label)),categorical_feature = cf)

#################
#vallidation
#2020년 4월을 validation으로 사용 
val <- jeju1[REG_YYMM==11]
val_label <- val[,"AMT"]

val_data <- val %>% mutate_at(c("season","CARD_SIDO_NM","STD_CLSS_NM"),function(x){as.numeric(as.factor(x))}) %>% select(-AMT)

val_data <- lgb.Dataset.create.valid(train_matrix, data.matrix(as.matrix(val_data)), label=data.matrix(as.matrix(val_label)))

val_data <- list(test=val_data)

#################
#prediction
#2020년 5월.6월,7월 예측

test <- jeju1[REG_YYMM>11]

test <- test %>% mutate_at(c("season","CARD_SIDO_NM","STD_CLSS_NM"),function(x){as.numeric(as.factor(x))}) %>% select(-AMT)



###################################################
#lightgbm 버전 1
###################################################
params1 <-  list(
  learning_rate = 0.03,
  objective = "regression",
  metric = "rmse",
  num_iteration=1000,
  seed=2020)


lgb1 <-  lgb.train(params = params1,data= train_matrix,valids = val_data)
#imp <- lgb.importance(lgb1)
#lgb.plot.importance(imp)

#################
#예측
p1 <-  predict(lgb1,data.matrix(as.matrix(test)),predict_disable_shape_check=T)


p1 <- exp(p1)-1
p1 <- ifelse(p1>10,p1,0)


li1 <- data.table(AMT=p1,sido=rep(CARD_SIDO_NM1,each=41,times=3),STD=rep(STD_CLSS_NM1,times=51),reg=rep(202005:202007,each=697)) %>%pivot_wider(everything(),names_from=reg,values_from=AMT)

li1 <- li1$`202007`
###################################################
#lightgbm 버전2
###################################################
#과적합을 줄이기위한 parameter 조정 
params2 = list(
  learning_rate = 0.5,
  objective = "regression",
  metric = "rmse",
  num_iteration=5000,
  seed=2020,
  num_leaves=30,
  min_data_in_leaf=20
)

lgb2 <-  lgb.train(params = params2,data= train_matrix,valids = val_data)
#imp <- lgb.importance(lgb1)
#lgb.plot.importance(imp)

#################
#예측
p2 <-  predict(lgb2,data.matrix(as.matrix(test)),predict_disable_shape_check=T)

#AMT가 작은 데이터 0~9사이 0처리 
p2 <- (exp(p2))-1
p2 <- ifelse(p2>10,p2,0)


li2 <- data.table(AMT=p2,sido=rep(CARD_SIDO_NM1,each=41,times=3),STD=rep(STD_CLSS_NM1,times=51),reg=rep(202005:202007,each=697)) %>%pivot_wider(everything(),names_from=reg,values_from=AMT)

li2 <- li2$`202007`


###################################################
#제출파일 만들기
###################################################
#전체 회귀, ets. arima,lgb 혼합 모델 
AMT_1 <- (reg.pred1+reg.pred2+ets.1+arima.1+li2+li1)/6
#제출파일 
submi <- fread("C:/Users/hanshin18/Desktop/공모전 통합/dacon/jeju_data_ver1/submission.csv",encoding = 'UTF-8')
submi <-  mutate(submi,AMT=rep(AMT_1,times=2))

fwrite(submi,"C:/Users/hanshin18/Desktop/공모전 통합/dacon/jeju_data_ver1/last_submission.csv",bom=T)
