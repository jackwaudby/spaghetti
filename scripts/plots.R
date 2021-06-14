library(ggplot2)
library(readr)

dat <- read_csv(file = './results.csv')

dat = dat[dat$cores <= 30,]
sf1 = dat[dat$sf == 1,]
sf3 = dat[dat$sf == 3,]


ggplot(data=sf1, aes(x=cores, y=commits/(total_time/cores/1000), group=protocol, colour=protocol)) +
  geom_line() +
  ylab("thpt") +
  ggtitle("SmallBank - High Contention (100 accounts)") +
  theme_bw() 

ggplot(data=sf3, aes(x=cores, y=commits/(total_time/cores/1000), group=protocol, colour=protocol)) +
  geom_line() +
  ylab("thpt") +
  ggtitle("SF3: Throughput") +
  theme_bw() 

ggplot(data=sf1, aes(x=cores, y=aborts/(commits+aborts), group=protocol, colour=protocol)) +
  geom_line() +
  ylab("abort rate") +
  ggtitle("SF1: Abort Rate") +
  theme_bw() 

ggplot(data=sf3, aes(x=cores, y=aborts/(commits+aborts), group=protocol, colour=protocol)) +
  geom_line() +
  ylab("abort rate") +
  ggtitle("SF3: Abort Rate") +
  theme_bw() 


