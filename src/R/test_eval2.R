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

### processing

#bgp.stats.all <- read.csv("../../raw/mobi1.rv_eqix.stats",sep=';', header=T)
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
bgp.stats.melt <- melt(bgp.stats, id.vars=c("timestamp","maptype","subtype","year","month","day"))

pv <- c("num_pfx_ips","num_asn","num_pfx")
df <- bgp.stats.melt[bgp.stats.melt$variable %in% pv,]

from_date <- strptime("2005/01/01 0:00:01","%Y/%m/%d %H:%M:%S")
until_date <- strptime("2007/01/01 23:59:59","%Y/%m/%d %H:%M:%S")
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
slopes.day <- ddply(bgp.stats,.(year,month,week,day),calc_slopes)
slopes.day.melt <- melt(slopes.day, id.vars=c("year","month","week","day"))
slopes.day.plot <- ggplot(slopes.day.melt,aes(x=as.Date(paste(slopes.day.melt$year,
                                                              slopes.day.melt$month,
                                                              slopes.day.melt$day,sep="/"),"%Y/%m/%d"),y=value,color=variable)) + 
  geom_point() +
  geom_line() +
  theme_bw() +
  facet_wrap(~variable,ncol=1)

slopes.week <- ddply(bgp.stats,.(year,week),calc_slopes)
slopes.week.melt <- melt(slopes.week, id.vars=c("year","week"))
slopes.week.plot <- ggplot(slopes.week.melt,aes(x=as.POSIXct(as.Date(paste(slopes.week.melt$year,slopes.week.melt$week,1,sep="/"),"%Y/%W/%w")),y=value,color=variable)) + 
  geom_point() +
  geom_line() +
  #scale_x_datetime(limits=c(as.POSIXct(from_date), as.POSIXct(until_date)),labels = date_format("%Y/%m")) +
  scale_y_continuous(limits=c(-30,30)) +
  theme_bw() +
  facet_wrap(~variable,ncol=1)

slopes.month <- ddply(bgp.stats,.(year,month),calc_slopes)
slopes.month.melt <- melt(slopes.month, id.vars=c("year","month"))
slopes.month.plot <- ggplot(slopes.month.melt,aes(x=as.Date(paste(slopes.month.melt$year,slopes.month.melt$month,01,sep="/"),"%Y/%m/%d"),y=value,color=variable)) + 
  geom_point() +
  geom_line() +
  theme_bw() +
  facet_wrap(~variable,ncol=1)

slopes.year <- ddply(bgp.stats,.(year),calc_slopes)
slopes.year.melt <- melt(slopes.year, id.vars=c("year"))
slopes.year.plot <- ggplot(slopes.year.melt,aes(x=as.Date(paste(slopes.year.melt$year,01,01,sep="/"),"%Y/%m/%d"),y=value,color=variable)) + 
  geom_point() +
  geom_line() +
  theme_bw() +
  facet_wrap(~variable,ncol=1)

## avg number of ips, pfx, and asn
avgnum.day <- ddply(bgp.stats,.(year,month,week,day),calc_avgnum)
avgnum.day.melt <- melt(avgnum.day, id.vars=c("year","month","week","day"))
avgnum.day.plot <- ggplot(avgnum.day.melt,aes(x=as.Date(paste(avgnum.day.melt$year,
                                                              avgnum.day.melt$month,
                                                              avgnum.day.melt$day,sep="/"),"%Y/%m/%d"),y=value,color=variable)) + 
  geom_point() +
  geom_line() +
  theme_bw() +
  facet_wrap(~variable,ncol=1)

avgnum.week <- ddply(bgp.stats,.(year,week),calc_avgnum)
avgnum.week.melt <- melt(avgnum.week, id.vars=c("year","week"))
avgnum.week.plot <- ggplot(avgnum.week.melt,aes(x=as.POSIXct(as.Date(paste(avgnum.week.melt$year,avgnum.week.melt$week,1,sep="/"),"%Y/%W/%w")),y=value,color=variable)) + 
  geom_point() +
  geom_line() +
  #scale_x_datetime(limits=c(as.POSIXct(from_date), as.POSIXct(until_date)),labels = date_format("%Y/%m")) +
  #scale_y_continuous(limits=c(-30,30)) +
  theme_bw() +
  facet_wrap(~variable,ncol=1)

avgnum.month <- ddply(bgp.stats,.(year,month),calc_avgnum)
avgnum.month.melt <- melt(avgnum.month, id.vars=c("year","month"))
avgnum.month.plot <- ggplot(avgnum.month.melt,aes(x=as.Date(paste(avgnum.month.melt$year,avgnum.month.melt$month,01,sep="/"),"%Y/%m/%d"),y=value,color=variable)) + 
  geom_point() +
  geom_line() +
  theme_bw() +
  facet_wrap(~variable,ncol=1)

avgnum.year <- ddply(bgp.stats,.(year),calc_avgnum)
avgnum.year.melt <- melt(avgnum.year, id.vars=c("year"))
avgnum.year.plot <- ggplot(avgnum.year.melt,aes(x=as.Date(paste(avgnum.year.melt$year,01,01,sep="/"),"%Y/%m/%d"),y=value,color=variable)) + 
  geom_point() +
  geom_line() +
  theme_bw() +
  facet_wrap(~variable,ncol=1)

## correlation analysis
slopes.day.temp <- na.omit(slopes.day[slopes.day$sni > -2000 & slopes.day$sni < 2000, ])
slopes.day.cor <- cor(slopes.day.temp[,c("sni","snp","sna")])
slopes.week.temp <- slopes.week[slopes.week$sni > -2000 & slopes.week$sni < 2000, ]
slopes.week.cor <- cor(slopes.week.temp[,c("sni","snp","sna")])
slopes.month.cor <- cor(slopes.month[,c("sni","snp","sna")])
slopes.year.cor <- cor(slopes.year[,c("sni","snp","sna")])

avgnum.day.cor <- cor(avgnum.day[,c("ani","anp","ana")])
avgnum.week.cor <- cor(avgnum.week[,c("ani","anp","ana")])
avgnum.month.cor <- cor(avgnum.month[,c("ani","anp","ana")])
avgnum.year.cor <- cor(avgnum.year[,c("ani","anp","ana")])