bgp.stats <- read.csv("/home/meiling/Developers/bgp-stats/src/python/1311.rv2.stats",
                      col.names=c("f1","ts","mt","st","p1","p2","p3","p4","p5","p6","p7","p8","p9","p10",
                                  "p11","p12","p13","p14","p15","p16","p17","p18","p19","p20",
                                  "p21","p22","p23","p24","p25","p26","p27","p28","p29","p30","p31","p32",
                                  "numips","bogips","moapfx"),
                      sep=' ', header=F, comment.char=c("#"))

bgp.stats.melt <- melt(bgp.stats, id.vars=c("f1","ts","mt","st"))

pfx <- c("p1","p2","p3","p4","p5","p6","p7","p8","p9","p10",
         "p11","p12","p13","p14","p15","p16","p17","p18","p19","p20",
         "p21","p22","p23","p24","p25","p26","p27","p28","p29","p30",
         "p31","p32")
df <- bgp.stats.melt[bgp.stats.melt$variable %in% pfx,]

ggplot(df, aes(x=ts,y=value)) + 
  geom_point() +
  scale_y_log10() +
  facet_wrap(~variable,nrow=8)

ggplot(bgp.stats[bgp.stats$numips > 1000000,],aes(x=ts,y=numips/1000000)) + geom_point()
ggplot(bgp.stats[bgp.stats$numips > 1000000,],aes(x=ts,y=bogips)) + geom_point()