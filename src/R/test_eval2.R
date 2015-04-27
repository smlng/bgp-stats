library(scales)
bgp.stats <- read.csv("../../raw/mobi1.rv_eqix.stats",
                      sep=';', header=T)
cn <- colnames(bgp.stats)
cn[1] <- "timestamp"
colnames(bgp.stats) <- cn
bgp.stats$num_pfx <- rowSums(bgp.stats[4:35])
bgp.stats.melt <- melt(bgp.stats, id.vars=c("timestamp","maptype","subtype"))

pv <- c("num_pfx_ips","num_asn","num_pfx")
df <- bgp.stats.melt[bgp.stats.melt$variable %in% pv,]

from_date <- strptime("2007/12/01 0:00:01","%Y/%m/%d %H:%M:%S")
until_date <- strptime("2007/12/31 23:59:59","%Y/%m/%d %H:%M:%S")
ggplot(df, aes(x=as.POSIXct(timestamp, origin = "1970-01-01", tz = "GMT"),y=value,color=variable)) + 
  geom_point() +
  scale_x_datetime(limits=c(as.POSIXct(from_date), as.POSIXct(until_date)),labels = date_format("%Y/%m")) +
  scale_y_log10() +
  facet_wrap(~variable,ncol=1)

lm_num_pfx <- lm(num_pfx~timestamp,data=bgp.stats[bgp.stats$num_pfx>0,])
lm_num_pfx_ips <- lm(num_pfx_ips~timestamp,data=bgp.stats[bgp.stats$num_pfx_ips>0,])
lm_num_asn <- lm(num_asn~timestamp,data=bgp.stats[bgp.stats$num_asn>0,])
