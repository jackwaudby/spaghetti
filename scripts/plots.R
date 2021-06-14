library(ggplot2)
library(readr)
library(dplyr)

waudby <- read_csv(file = './results.csv',col_names = c("sf","protocol","workload","cores","total_time","commits","aborts"))
durner <- read_delim(file = './durner_results.csv', delim = ";",col_names = F)
durner = durner[,c(1,2,3,5,8,9,11)]
colnames(durner) <- c("workload","protocol","sf","cores","total_time","commits","aborts")
durner$protocol = "durner"
dat = bind_rows(waudby,durner)
sf1 = dat %>% filter((sf == 1) | (sf == 100))
sf3 = dat %>% filter((sf == 3) | (sf == 10000))

ggplot(data=sf1, aes(x=cores, y=commits/(total_time/cores/1000), group=protocol, colour=protocol)) +
  geom_line() +
  ylab("thpt") +
  ggtitle("SmallBank - High Contention (100 accounts)") +
  theme_bw() 

ggplot(data=sf3, aes(x=cores, y=commits/(total_time/cores/1000), group=protocol, colour=protocol)) +
  geom_line() +
  ylab("thpt") +
  ggtitle("SmallBank - Low Contention (10000 accounts)") +
  theme_bw() 

ggplot(data=sf1, aes(x=cores, y=aborts/(commits+aborts), group=protocol, colour=protocol)) +
  geom_line() +
  ylab("abort rate") +
  ggtitle("SmallBank - High Contention (100 accounts)") +
  theme_bw() 

ggplot(data=sf3, aes(x=cores, y=aborts/(commits+aborts), group=protocol, colour=protocol)) +
  geom_line() +
  ylab("abort rate") +
  ggtitle("SmallBank - Low Contention (10000 accounts)") +
  theme_bw() 


