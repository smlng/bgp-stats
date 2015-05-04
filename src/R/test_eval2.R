library(ggplot2)
library(scales)
library(reshape)
library(plyr)

### functions

calc_slopes <- function(df) {
  ts0 <- min(df$timestamp)
  ts1 <- max(df$timestamp)
  num_ips0 <- df[df$timestamp == ts0,]$num_pfx_ips
  num_ips1 <- df[df$timestamp == ts1,]$num_pfx_ips
  slope_num_ips <- (1 - num_ips0/num_ips1)/(1 - ts0/ts1)
  num_pfx0 <- df[df$timestamp == ts0,]$num_pfx
  num_pfx1 <- df[df$timestamp == ts1,]$num_pfx
  slope_num_pfx <- (1 - num_pfx0/num_pfx1)/(1 - ts0/ts1)
  num_asn0 <- df[df$timestamp == ts0,]$num_asn
  num_asn1 <- df[df$timestamp == ts1,]$num_asn
  slope_num_asn <- (1 - num_asn0/num_asn1)/(1 - ts0/ts1)
  ret <- data.frame(sni=slope_num_ips,snp=slope_num_pfx,sna=slope_num_asn)
  return(ret)
}

calc_avgnum <- function(df) {
  avg_num_ips <- mean(df$rel_num_pfx_ips)
  avg_num_pfx <- mean(df$rel_num_pfx)
  avg_num_asn <- mean(df$rel_num_asn)
  ret <- data.frame(ani=avg_num_ips,anp=avg_num_pfx,ana=avg_num_asn)
  return(ret)
}

calc_avgpfxlen <- function(df) {
  vars <- c ("pl01","pl02","pl03","pl04","pl05","pl06","pl07","pl08",
             "pl09","pl10","pl11","pl12","pl13","pl14","pl15","pl16",
             "pl17","pl18","pl19","pl20","pl21","pl22","pl23","pl24",
             "pl25","pl26","pl27","pl28","pl29","pl30","pl31","pl32")
  cs <- colSums(df[,vars])/nrow(df)
  for (i in 1:length(cs)) {
    cs[i] <- cs[i] * i
  }
  return(data.frame(cs))
}

index.calc_slopes <- function(df) {
  ts0 <- min(df$timestamp)
  ts1 <- max(df$timestamp)
  final0 <- df[df$timestamp == ts0,]$final
  final1 <- df[df$timestamp == ts1,]$final
  slope_final <- (1 - final0/final1)/(1 - ts0/ts1)
  start0 <- df[df$timestamp == ts0,]$start
  start1 <- df[df$timestamp == ts1,]$start
  slope_start <- (1 - start0/start1)/(1 - ts0/ts1)
  high0 <- df[df$timestamp == ts0,]$high
  high1 <- df[df$timestamp == ts1,]$high
  slope_high <- (1 - high0/high1)/(1 - ts0/ts1)
  low0 <- df[df$timestamp == ts0,]$low
  low1 <- df[df$timestamp == ts1,]$low
  slope_low <- (1 - low0/low1)/(1 - ts0/ts1)
  ret <- data.frame (sf=slope_final, ss=slope_start, sh=slope_high, sl=slope_low)
  return(ret)
}
### processing

bgp.stats.all <- read.csv("../../raw/mobi1.rv_eqix.stats",sep=';',header=T,stringsAsFactors=F)
#bgp.stats.all <- read.csv("../../raw/mobi1.rv_eqix.stats",sep=';', header=T)
cn <- colnames(bgp.stats.all)
cn[1] <- "timestamp"
colnames(bgp.stats.all) <- cn
#bgp.stats <- bgp.stats.all[bgp.stats.all$num_pfx_ips > 0 & bgp.stats.all$num_pfx_ips < 3000000000,]
bgp.stats <- bgp.stats.all[rowSums(bgp.stats.all[4:10]) == 0,]
bgp.stats$num_pfx <- rowSums(bgp.stats[4:35])
bgp.stats$year <- as.numeric(strftime(as.POSIXct(bgp.stats$timestamp, origin = "1970-01-01", tz = "UTC"),format="%Y"))
bgp.stats$month <- as.numeric(strftime(as.POSIXct(bgp.stats$timestamp, origin = "1970-01-01", tz = "UTC"),format="%m"))
bgp.stats$week<- as.numeric(strftime(as.POSIXct(bgp.stats$timestamp, origin = "1970-01-01", tz = "UTC"),format="%W"))
bgp.stats$day <- as.numeric(strftime(as.POSIXct(bgp.stats$timestamp, origin = "1970-01-01", tz = "UTC"),format="%d"))
bgp.stats$rel_num_pfx_ips <- bgp.stats$num_pfx_ips / max(bgp.stats$num_pfx_ips)
bgp.stats$rel_num_pfx <- bgp.stats$num_pfx / max(bgp.stats$num_pfx)
bgp.stats$rel_num_asn <- bgp.stats$num_asn / max(bgp.stats$num_asn)

# read stock data
dax.data <- read.csv("../../raw/dax.dat",sep=";",header=T,stringsAsFactors=F)
dax.data$final <- sub(".","",dax.data$final,fixed=TRUE)
dax.data$final <- sub(",",".",dax.data$final,fixed=TRUE)
dax.data$final <- as.numeric(dax.data$final)
dax.data$start <- sub(".","",dax.data$start,fixed=TRUE)
dax.data$start <- sub(",",".",dax.data$start,fixed=TRUE)
dax.data$start <- as.numeric(dax.data$start)
dax.data$high <- sub(".","",dax.data$high,fixed=TRUE)
dax.data$high <- sub(",",".",dax.data$high,fixed=TRUE)
dax.data$high <- as.numeric(dax.data$high)
dax.data$low <- sub(".","",dax.data$low,fixed=TRUE)
dax.data$low <- sub(",",".",dax.data$low,fixed=TRUE)
dax.data$low <- as.numeric(dax.data$low)
dax.data$timestamp <- as.numeric(as.POSIXct(dax.data$date, format="%d.%m.%Y"))
dax.data$date  <- as.Date(dax.data$date,"%d.%m.%Y")
dax.data$year  <- as.numeric(strftime(as.Date(dax.data$date,"%d.%m.%Y"),format="%Y"))
dax.data$month <- as.numeric(strftime(as.Date(dax.data$date,"%d.%m.%Y"),format="%m"))
dax.data$week  <- as.numeric(strftime(as.Date(dax.data$date,"%d.%m.%Y"),format="%W"))
dax.data$day   <- as.numeric(strftime(as.Date(dax.data$date,"%d.%m.%Y"),format="%d"))

# rearrange bgp data
bgp.stats.melt <- melt(bgp.stats, id.vars=c("timestamp","maptype","subtype","year","month","day"))
pv <- c("num_pfx_ips","num_asn","num_pfx")
df <- bgp.stats.melt[bgp.stats.melt$variable %in% pv,]

from_date <- strptime("2008/01/01 0:00:01","%Y/%m/%d %H:%M:%S")
until_date <- strptime("2011/12/31 23:59:59","%Y/%m/%d %H:%M:%S")
ggplot(df, aes(x=as.POSIXct(timestamp, origin = "1970-01-01", tz = "GMT"),y=value,color=variable)) + 
  geom_point() +
  scale_x_datetime(limits=c(as.POSIXct(from_date), as.POSIXct(until_date)),labels = date_format("%Y/%m")) +
  scale_y_log10() +
  facet_wrap(~variable,ncol=1)

## linear regression analysis
lm_num_pfx <- lm(num_pfx~timestamp,data=bgp.stats)
lm_num_pfx_ips <- lm(num_pfx_ips~timestamp,data=bgp.stats)
lm_num_asn <- lm(num_asn~timestamp,data=bgp.stats)

## slope of increase in number of IPs, prefixes, and ASN
bgp.slopes.day <- ddply(bgp.stats,.(year,month,week,day),calc_slopes)
bgp.slopes.day.melt <- melt(bgp.slopes.day, id.vars=c("year","month","week","day"))
bgp.slopes.day.plot <- ggplot(bgp.slopes.day.melt,
                              aes(x=as.Date(paste(bgp.slopes.day.melt$year,
                                                  bgp.slopes.day.melt$month,
                                                  bgp.slopes.day.melt$day,
                                                  sep="/"),"%Y/%m/%d"),
                                  y=value,color=variable)) + 
  geom_point() +
  geom_line() +
  theme_bw() +
  facet_wrap(~variable,ncol=1)

bgp.slopes.week <- ddply(bgp.stats,.(year,week),calc_slopes)
bgp.slopes.week.melt <- melt(bgp.slopes.week, id.vars=c("year","week"))
bgp.slopes.week.plot <- ggplot(bgp.slopes.week.melt,
                               aes(x=as.POSIXct(as.Date(paste(bgp.slopes.week.melt$year,
                                                              bgp.slopes.week.melt$week,
                                                              1,sep="/"),"%Y/%W/%w")),
                                   y=value,color=variable)) + 
  geom_point() +
  geom_line() +
  #scale_x_datetime(limits=c(as.POSIXct(from_date), as.POSIXct(until_date)),labels = date_format("%Y/%m")) +
  scale_y_continuous(limits=c(-50,50)) +
  theme_bw() +
  facet_wrap(~variable,ncol=1)

bgp.slopes.month <- ddply(bgp.stats,.(year,month),calc_slopes)
bgp.slopes.month.melt <- melt(bgp.slopes.month, id.vars=c("year","month"))
bgp.slopes.month.plot <- ggplot(bgp.slopes.month.melt,
                                aes(x=as.POSIXct(as.Date(paste(bgp.slopes.month.melt$year,
                                                    bgp.slopes.month.melt$month,
                                                    01,sep="/"),"%Y/%m/%d")),
                                    y=value,color=variable)) + 
  geom_point() +
  scale_y_continuous(limits=c(-20,20)) +
  scale_x_datetime(limits=c(as.POSIXct(from_date), as.POSIXct(until_date)),labels = date_format("%Y/%m")) +
  geom_line() +
  theme_bw() +
  facet_wrap(~variable,ncol=1)

bgp.slopes.year <- ddply(bgp.stats,.(year),calc_slopes)
bgp.slopes.year.melt <- melt(bgp.slopes.year, id.vars=c("year"))
bgp.slopes.year.plot <- ggplot(bgp.slopes.year.melt,
                               aes(x=as.POSIXct(as.Date(paste(bgp.slopes.year.melt$year,
                                                   01,01,sep="/"),"%Y/%m/%d")),
                                   y=value,color=variable)) + 
  geom_point() +
  geom_line() +
  theme_bw() +
  facet_wrap(~variable,ncol=1)

## avg number of ips, pfx, and asn
bgp.avgnum.day <- ddply(bgp.stats,.(year,month,week,day),calc_avgnum)
bgp.avgnum.day.melt <- melt(bgp.avgnum.day, id.vars=c("year","month","week","day"))
bgp.avgnum.day.plot <- ggplot(bgp.avgnum.day.melt,
                              aes(x=as.POSIXct(as.Date(paste(bgp.avgnum.day.melt$year,
                                                  bgp.avgnum.day.melt$month,
                                                  bgp.avgnum.day.melt$day,
                                                  sep="/"),"%Y/%m/%d")),
                                  y=value,color=variable)) + 
  geom_point() +
  geom_line() +
  theme_bw() +
  facet_wrap(~variable,ncol=1)

bgp.avgnum.week <- ddply(bgp.stats,.(year,week),calc_avgnum)
bgp.avgnum.week.melt <- melt(bgp.avgnum.week, id.vars=c("year","week"))
bgp.avgnum.week.plot <- ggplot(bgp.avgnum.week.melt,
                               aes(x=as.POSIXct(as.Date(paste(bgp.avgnum.week.melt$year,
                                                   bgp.avgnum.week.melt$week,
                                                   1,sep="/"),"%Y/%W/%w")),
                                   y=value,color=variable)) + 
  geom_point() +
  geom_line() +
  #scale_x_datetime(limits=c(as.POSIXct(from_date), as.POSIXct(until_date)),labels = date_format("%Y/%m")) +
  #scale_y_continuous(limits=c(-30,30)) +
  theme_bw() +
  facet_wrap(~variable,ncol=1)

bgp.avgnum.month <- ddply(bgp.stats,.(year,month),calc_avgnum)
bgp.avgnum.month.melt <- melt(bgp.avgnum.month, id.vars=c("year","month"))
bgp.avgnum.month.plot <- ggplot(bgp.avgnum.month.melt,
                                aes(x=as.Date(paste(bgp.avgnum.month.melt$year,
                                                    bgp.avgnum.month.melt$month,
                                                    01,sep="/"),"%Y/%m/%d"),
                                    y=value,color=variable)) + 
  geom_point() +
  geom_line() +
  theme_bw() +
  facet_wrap(~variable,ncol=1)

bgp.avgnum.year <- ddply(bgp.stats,.(year),calc_avgnum)
bgp.avgnum.year.melt <- melt(bgp.avgnum.year, id.vars=c("year"))
bgp.avgnum.year.plot <- ggplot(bgp.avgnum.year.melt,
                               aes(x=as.Date(paste(bgp.avgnum.year.melt$year,
                                                   01,01,sep="/"),"%Y/%m/%d"),
                                   y=value,color=variable)) + 
  geom_point() +
  geom_line() +
  theme_bw() +
  facet_wrap(~variable,ncol=1)

## correlation analysis
bgp.slopes.day.temp <- na.omit(bgp.slopes.day[bgp.slopes.day$sni > -2000 & bgp.slopes.day$sni < 2000, ])
bgp.slopes.day.cor <- cor(bgp.slopes.day.temp[,c("sni","snp","sna")])
bgp.slopes.week.temp <- bgp.slopes.week[bgp.slopes.week$sni > -2000 & bgp.slopes.week$sni < 2000, ]
bgp.slopes.week.cor <- cor(bgp.slopes.week.temp[,c("sni","snp","sna")])
bgp.slopes.month.cor <- cor(bgp.slopes.month[,c("sni","snp","sna")])
bgp.slopes.year.cor <- cor(bgp.slopes.year[,c("sni","snp","sna")])

bgp.avgnum.day.cor <- cor(bgp.avgnum.day[,c("ani","anp","ana")])
bgp.avgnum.week.cor <- cor(bgp.avgnum.week[,c("ani","anp","ana")])
bgp.avgnum.month.cor <- cor(bgp.avgnum.month[,c("ani","anp","ana")])
bgp.avgnum.year.cor <- cor(bgp.avgnum.year[,c("ani","anp","ana")])

## index analysis
dax.slopes.week <- ddply(dax.data,.(year,week),index.calc_slopes)
dax.slopes.week.melt <- melt(dax.slopes.week, id.vars=c("year","week"))
dax.slopes.week.plot <- ggplot(dax.slopes.week.melt,
                               aes(x=as.POSIXct(as.Date(paste(dax.slopes.week.melt$year,
                                                              dax.slopes.week.melt$week,
                                                              1,sep="/"),"%Y/%W/%w")),
                                   y=value,color=variable)) + 
  geom_point() +
  geom_line() +
  #scale_x_datetime(limits=c(as.POSIXct(from_date), as.POSIXct(until_date)),labels = date_format("%Y/%m")) +
  #scale_y_continuous(limits=c(-50,50)) +
  theme_bw() +
  facet_wrap(~variable,ncol=1)

dax.slopes.month <- ddply(dax.data,.(year,month),index.calc_slopes)
dax.slopes.month.melt <- melt(dax.slopes.month, id.vars=c("year","month"))
dax.slopes.month.plot <- ggplot(dax.slopes.month.melt,
                                aes(x=as.POSIXct(as.Date(paste(dax.slopes.month.melt$year,
                                                    dax.slopes.month.melt$month,
                                                    01,sep="/"),"%Y/%m/%d")),
                                    y=value,color=variable)) + 
  geom_point() +
  scale_x_datetime(limits=c(as.POSIXct(from_date), as.POSIXct(until_date)),labels = date_format("%Y/%m")) +
  #scale_y_continuous(limits=c(-20,20)) +
  geom_line() +
  theme_bw() +
  facet_wrap(~variable,ncol=1)

dax.slopes.year <- ddply(dax.data,.(year),index.calc_slopes)
dax.slopes.year.melt <- melt(dax.slopes.year, id.vars=c("year"))
dax.slopes.year.plot <- ggplot(dax.slopes.year.melt,
                               aes(x=as.Date(paste(dax.slopes.year.melt$year,
                                                   01,01,sep="/"),"%Y/%m/%d"),
                                   y=value,color=variable)) + 
  geom_point() +
  geom_line() +
  theme_bw() +
  facet_wrap(~variable,ncol=1)