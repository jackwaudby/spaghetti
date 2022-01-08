# Produces plots for thpt, av.latency, and abort rate.
library(ggplot2)
library(readr)
library(dplyr)

waudby <- read_csv(file = './results.csv',col_names = c("sf","protocol","workload","cores","total_time","commits","aborts","errors","total_latency"))
waudby
durner <- read_delim(file = './durner_sgt.csv', delim = ";",col_names = F)
#durner_2pl <- read_delim(file = './durner_2pl.csv', delim = ";",col_names = F)
#durner = durner[,c(1,2,3,5,8,9,11,10,15,18)]
#durner_2pl = durner_2pl[,c(1,2,3,5,8,9,11,10,15,18)]
#colnames(durner) <- c("workload","protocol","sf","cores","total_time","commits","aborts","errors","txn_time","latency")
#colnames(durner_2pl) <- c("workload","protocol","sf","cores","total_time","commits","aborts","errors","txn_time","latency")
#durner$total_latency <- durner$txn_time + durner$latency 
#durner_2pl$total_latency <- durner_2pl$txn_time + durner_2pl$latency 
#durner = durner[,-c(9,10)]
#durner_2pl = durner_2pl[,-c(9,10)]
#durner$protocol = "durner"
#durner_2pl$protocol = "2pl"
#dat = bind_rows(waudby,durner,durner_2pl)
#dat = bind_rows(waudby,durner_2pl)
dat = bind_rows(waudby)
sf1 = dat %>% filter((sf == 1) | (sf == 100))
sf3 = dat %>% filter((sf == 3) | (sf == 10000))
#sf1 = sf1 %>% filter(cores <= 40)
#sf3 = sf3 %>% filter(cores <= 40)


ggplot(data=sf1, aes(x=cores, y=commits/(total_time/cores/1000)/1000000, group=workload, colour=workload)) +
  geom_line() +
  ylab("thpt (million/s)") +
  # ggtitle(paste0(dat$workload[1]," - sf1")) +
  ggtitle(paste0("sf1 - 100 rows")) +
  theme_bw() 

# ggsave(paste0("./graphics/",dat$workload[1],"_thpt_sf1.png"))
ggsave(paste0("./graphics/thpt_sf1.png"))

ggplot(data=sf3, aes(x=cores, y=commits/(total_time/cores/1000)/1000000, group=workload, colour=workload)) +
  geom_line() +
  ylab("thpt (million/s)") +
  # ggtitle(paste0(dat$workload," - sf3")) +
  ggtitle(paste0("sf3 - 10000 rows")) +
  theme_bw()

ggsave(paste0("./graphics/thpt_sf3.png"))

ggplot(data=sf1, aes(x=cores, y=errors/(commits+errors), group=workload, colour=workload)) +
  geom_line() +
  ylab("abort rate") +
  ggtitle(paste0("sf1 - 100 rows")) +
  theme_bw()

ggsave("./graphics/error_sf1.png")

ggplot(data=sf3, aes(x=cores, y=errors/(commits+errors), group=workload, colour=workload)) +
  geom_line() +
  ylab("abort rate") +
  ggtitle(paste0("sf3 - 10000 rows")) +
  theme_bw()

ggsave("./graphics/error_sf3.png")

# ggplot(data=sf1, aes(x=cores, y=commits+errors, group=workload, colour=workload)) +
#   geom_line() +
#   ylab("abort rate") +
#   ggtitle(paste0("sf1 - 100 rows")) +
#   theme_bw()
# 
# ggsave("./graphics/complete_sf1.png")

# ggplot(data=sf1, aes(x=cores, y=aborts/(commits+aborts), group=workload, colour=workload)) +
#   geom_line() +
#   ylab("abort rate") +
#   ggtitle(paste0("sf1 - 100 rows")) +
#   theme_bw()
# 
# ggsave("./graphics/abort_sf1.png")

# ggplot(data=sf3, aes(x=cores, y=aborts/(commits+aborts), group=protocol, colour=protocol)) +
#   geom_line() +
#   ylab("abort rate") +
#   ggtitle("SmallBank - Low Contention (10000 accounts)") +
#   theme_bw() 
# 
# ggsave("./graphics/smallbank_abort_low.png")


# Durner: txn time + latency/completed
# ggplot(data=sf1, aes(x=cores, y=total_latency/(commits+aborts+errors), group=protocol, colour=protocol)) +
#   geom_line() +
#   ylab("av latency (ms)") +
#   ggtitle("SmallBank - High Contention (100 accounts)") +
#   theme_bw() 
# 
# ggsave("./graphics/smallbank_lat_high.png")
# 
# 
# ggplot(data=sf3, aes(x=cores, y=total_latency/(commits+aborts+errors), group=protocol, colour=protocol)) +
#   geom_line() +
#   ylab("av latency (ms)") +
#   ggtitle("SmallBank - Low Contention (10000 accounts)") +
#   theme_bw() 
# 
# ggsave("./graphics/smallbank_lat_low.png")
