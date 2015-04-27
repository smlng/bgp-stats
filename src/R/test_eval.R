library(ggplot2)
library(scales)
library(plyr)
library(rescale2)
df.stats <- read.csv("../../raw/rvl.2005/stats.csv",sep=";",header=F,
                     col.names=c('type','ts','maptype','subtype','pl01','pl02','pl03','pl04','pl05','pl06','pl07','pl08',
                                 'pl09','pl10','pl11','pl12','pl13','pl14','pl15','pl16','pl17','pl18','pl19','pl20',
                                 'pl21','pl22','pl23','pl24','pl25','pl26','pl27','pl28','pl29','pl30','pl31','pl32',
                                 'pfxips','pfxbogus','pfxmoas'))

df.diffs <- read.csv("../../raw/rvl.2005/diffs.csv",sep=";",header=F,
                     col.names=c('type','ts0','ts1','maptype','subtype','pfxips0','pfxips1',
                                 'newips','delips','modips','aggips','deaggips'))

df.stats$date  <- as.POSIXct(df.stats$ts,  origin="1970-01-01", tz="GMT")
df.diffs$date0 <- as.POSIXct(df.diffs$ts0, origin="1970-01-01", tz="GMT")
df.diffs$date1 <- as.POSIXct(df.diffs$ts1, origin="1970-01-01", tz="GMT")

ggplot(df.stats, aes(x=date,y=pfxips)) + 
  geom_line() + 
  scale_x_datetime(limits=c(as.POSIXct('2005/01/01'), as.POSIXct('2005/12/31')),labels = date_format("%Y/%m")) +
  scale_y_log10() +
  theme_bw() +
  theme(legend.position="none",
        strip.text.x = element_text(size = 9),
        axis.title.y=element_text(vjust=0.3),
        axis.title.x=element_text(vjust=0),
        axis.text.x = element_text(angle = 50, hjust = 1),
        text=element_text(size=18)) +
  labs(x="Date",y="#IPs")

ggplot(df.stats, aes(x=date,y=pfxbogus)) + 
  geom_line() + 
  scale_x_datetime(limits=c(as.POSIXct('2005/01/01'), as.POSIXct('2005/12/31')),labels = date_format("%Y/%m")) +
  scale_y_log10() +
  theme_bw() +
  theme(legend.position="none",
        strip.text.x = element_text(size = 9),
        axis.title.y=element_text(vjust=0.3),
        axis.title.x=element_text(vjust=0),
        axis.text.x = element_text(angle = 50, hjust = 1),
        text=element_text(size=18)) +
  labs(x="Date",y="#IPs bogus")

ggplot(df.stats, aes(x=date,y=pfxmoas)) + 
  geom_line() + 
  scale_x_datetime(limits=c(as.POSIXct('2005/01/01'), as.POSIXct('2005/12/31')),labels = date_format("%Y/%m")) +
  theme_bw() +
  theme(legend.position="none",
        strip.text.x = element_text(size = 9),
        axis.title.y=element_text(vjust=0.3),
        axis.title.x=element_text(vjust=0),
        axis.text.x = element_text(angle = 50, hjust = 1),
        text=element_text(size=18)) +
  labs(x="Date",y="#IPs bogus")

ggplot(df.diffs, aes(x=date0,y=newips)) + 
  geom_line() + 
  scale_x_datetime(limits=c(as.POSIXct('2005/01/01'), as.POSIXct('2005/12/31')),labels = date_format("%Y/%m")) +
  scale_y_log10() +
  theme_bw() +
  theme(legend.position="none",
        strip.text.x = element_text(size = 9),
        axis.title.y=element_text(vjust=0.3),
        axis.title.x=element_text(vjust=0),
        axis.text.x = element_text(angle = 50, hjust = 1),
        text=element_text(size=18)) +
  labs(x="Date",y="#IPs new")

ggplot(df.diffs, aes(x=date0,y=delips)) + 
  geom_line() + 
  scale_x_datetime(limits=c(as.POSIXct('2005/01/01'), as.POSIXct('2005/12/31')),labels = date_format("%Y/%m")) +
  scale_y_log10() +
  theme_bw() +
  theme(legend.position="none",
        strip.text.x = element_text(size = 9),
        axis.title.y=element_text(vjust=0.3),
        axis.title.x=element_text(vjust=0),
        axis.text.x = element_text(angle = 50, hjust = 1),
        text=element_text(size=18)) +
  labs(x="Date",y="#IPs del")

ggplot(df.diffs, aes(x=date0,y=modips)) + 
  geom_line() + 
  scale_x_datetime(limits=c(as.POSIXct('2005/01/01'), as.POSIXct('2005/12/31')),labels = date_format("%Y/%m")) +
  #scale_y_continuous(limits=c(0,10000000)) +
  scale_y_log10() +
  theme_bw() +
  theme(legend.position="none",
        strip.text.x = element_text(size = 9),
        axis.title.y=element_text(vjust=0.3),
        axis.title.x=element_text(vjust=0),
        axis.text.x = element_text(angle = 50, hjust = 1),
        text=element_text(size=18)) +
  labs(x="Date",y="#IPs mod")

df.stats.plen <- melt(df.stats[,5:36])
ggplot(df.stats.plen, aes(x=value)) + geom_histogram(aes(y=..density..)) + facet_wrap(~variable)
df.mean.plen <- ddply(df.stats.plen,.(variable), summarize, mean = mean(value))
ggplot(df.mean.plen, aes(x=variable,y=mean)) + geom_point() + scale_y_log10()