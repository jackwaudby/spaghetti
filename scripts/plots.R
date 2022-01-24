# Produces plots for thpt, av.latency, and abort rate.
library(ggplot2)
library(readr)
library(dplyr)

# Investigation: sgt vs msgt
# Parameters: smallbank, sf1, median of 5 runs, 1 million txns
raw = read_csv(file = './data/22_01_13_sgt_msgt_smallbank_sf1.csv',col_names = c("sf","protocol","workload","cores","total_time","commits","aborts","errors","total_latency"))
raw$thpt = raw$commits/(raw$total_time/raw$cores/1000)/1000000
raw$abr = raw$errors/(raw$commits + raw$errors)
raw$lat = raw$total_latency/(raw$commits + raw$errors + raw$aborts)

dat = raw %>%
  group_by(protocol,cores) %>%
  summarise(thpt = median(thpt), abr = median(abr), lat = median(lat))

dat

ggplot(data=dat, aes(x=cores, y=thpt, group=protocol, colour=protocol)) +
  geom_line() +
  ylab("thpt (million/s)") +
  ggtitle(paste0("smallbank sf1")) +
  theme_bw()

ggplot(data=dat, aes(x=cores, y=abr, group=protocol, colour=protocol)) +
  geom_line() +
  ylab("abort rate") +
  ggtitle(paste0("smallbank sf1")) +
  theme_bw() 

ggplot(data=dat, aes(x=cores, y=lat, group=protocol, colour=protocol)) +
  geom_line() +
  ylab("av latency (ms)") +
  ggtitle(paste0("sgt smallbank sf1")) +
  theme_bw() 

sgt <- read_csv(file = './data/22_01_12_sgt_smallbank_sf1_sf3.csv',col_names = c("sf","protocol","workload","cores","total_time","commits","aborts","errors","total_latency"))
msgt <- read_csv(file = './data/22_01_12_msgt_smallbank_sf1_sf3.csv',col_names = c("sf","protocol","workload","cores","total_time","commits","aborts","errors","total_latency"))
sgt_sf1 = sgt %>% filter(sf == 1)
msgt_sf1 = msgt %>% filter(sf == 1)
sgt_sf1$thpt = sgt_sf1$commits/(sgt_sf1$total_time/sgt_sf1$cores/1000)/1000000
msgt_sf1$thpt = msgt_sf1$commits/(msgt_sf1$total_time/msgt_sf1$cores/1000)/1000000
sgt_sf1$abr = sgt_sf1$errors/(sgt_sf1$commits+sgt_sf1$errors)
msgt_sf1$abr = msgt_sf1$errors/(msgt_sf1$commits+msgt_sf1$errors)

sgt_sf1 = sgt_sf1 %>%
  group_by(cores) %>%
  summarise(thpt = median(abr))

msgt_sf1 = msgt_sf1 %>%
  group_by(cores) %>%
  summarise(thpt = median(abr))

((msgt_sf1$thpt/sgt_sf1$thpt) - 1)*100



sgt_sf1$protocol = rep("sgt",7)
msgt_sf1$protocol = rep("msgt",7)

dat = rbind(msgt_sf1,sgt_sf1)
dat$protocol <- as.factor(dat$protocol)

head(dat)

ggplot(data=dat, aes(x=cores, y=thpt, group=protocol, colour=protocol)) +
  geom_line() +
  ylab("thpt (million/s)") +
  # ggtitle(paste0(dat$workload[1]," - sf1")) +
  ggtitle(paste0("sgt smallbank sf1")) +
  theme_bw() 

ggsave(paste0("./graphics/22_01_12_nocc_tatp_sf1.png"))

sf3 = dat %>% filter(sf == 3)
sf3$thpt = sf3$commits/(sf3$total_time/sf3$cores/1000)/1000000
sf3 = sf3 %>%
  group_by(cores) %>%
  summarise(thpt = median(thpt))

ggplot(data=sf3, aes(x=cores, y=thpt)) +
  geom_line() +
  ylab("thpt (million/s)") +
  # ggtitle(paste0(dat$workload[1]," - sf3")) +
  ggtitle(paste0("sgt smallbank sf1")) +
  theme_bw() 
ggsave(paste0("./graphics/22_01_12_nocc_smallbank_sf3.png"))




waudby <- read_csv(file = './results.csv',col_names = c("sf","protocol","workload","cores","total_time","commits","aborts","errors","total_latency"))
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
dat$sf = as.factor(dat$sf)

sf1 = dat %>% filter((sf == 1) | (sf == 100))
sf3 = dat %>% filter((sf == 3) | (sf == 10000))
#sf1 = sf1 %>% filter(cores <= 40)
#sf3 = sf3 %>% filter(cores <= 40)

ggplot(data=dat, aes(x=cores, y=commits/(total_time/cores/1000)/1000000, group=sf, colour=sf)) +
  geom_line() +
  ylab("thpt (million/s)") +
  # ggtitle(paste0(dat$workload[1]," - sf1")) +
  ggtitle(paste0("dummy")) +
  theme_bw() 


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


raw = read_csv(file = './data/22_01_13_old.csv',col_names = c("sf","protocol","workload","cores","total_time","commits","restarted","aborted","total_latency"))
raw = read_csv(file = './data/22_01_13_old.csv',col_names = c("sf","protocol","workload","cores","total_time","commits","aborts","errors","total_latency"))
raw$thpt = raw$commits/(raw$total_time/raw$cores/1000)/1000000

ggplot(data=raw,aes(x=cores, y=thpt, group=protocol, colour=protocol)) +
  geom_line() +
  ylab("thpt (million/s)") +
  ggtitle(paste0("smallbank sf1")) +
  theme_bw()

# Isolation breakdown

cores = c(10,20,30,40,50,60)

msgt_ru = c(7900,49318,157417,357359,659623,1043204)
msgt_rc = c(16496,108639,361963,848857,1596211,2541636)
msgt_s = c(53066,273932,746512,1496097,2545877,3735399)
total = msgt_ru + msgt_rc + msgt_s

sgt_ru = c(35870,182023,472259,925095,1509435,2241890)
sgt_rc = c(70824,365181,945414,1850348,3016117,4474513)
sgt_s = c(71349,364745,944946,1849372,3016714,4475011)
sgt_total = sgt_ru + sgt_rc + sgt_s

msgt_ru = data.frame(cores, aborts = msgt_ru/total, iso = rep("msgt-ru",6))
msgt_rc = data.frame(cores, aborts = msgt_rc/total, iso = rep("msgt-rc",6))
msgt_s = data.frame(cores, aborts = msgt_s/total, iso = rep("msgt-s",6))
msgt = data.frame(rbind(msgt_ru,msgt_rc,msgt_s))

sgt_ru = data.frame(cores, aborts = sgt_ru/sgt_total, iso = rep("sgt-ru",6))
sgt_rc = data.frame(cores, aborts = sgt_rc/sgt_total, iso = rep("sgt-rc",6))
sgt_s = data.frame(cores, aborts = sgt_s/sgt_total, iso = rep("sgt-s",6))
sgt = data.frame(rbind(sgt_ru,sgt_rc,sgt_s))

dat = data.frame(rbind(sgt,msgt))

ggplot(data=dat, aes(x=cores, y=aborts, group=iso, colour=iso)) +
  geom_line() +
  ylab("thpt (million/s)") +
  ggtitle(paste0("smallbank sf1")) +
  theme_bw()



msgt_ca = c(32631,176045,513448,1134919,2099962,3377140)
msgt_cyc = c(44831,255844,752444,1567394,2701749,3943099)

sgt_ca = c(72142,347277,870526,1707934,2818415,4291545)
sgt_cyc = c(105901,564672,1492093,2916881,4723851,6899869)

msgt_ca = data.frame(cores, ca = msgt_ca/total, iso = rep("msgt-cas",6))
msgt_cyc = data.frame(cores, ca = msgt_cyc/total, iso = rep("msgt-cyc",6))

sgt_ca = data.frame(cores, ca = sgt_ca/sgt_total, iso = rep("sgt-cas",6))
sgt_cyc = data.frame(cores, ca = sgt_cyc/sgt_total, iso = rep("sgt-cyc",6))

dat2 = data.frame(rbind(sgt_ca,sgt_cyc,msgt_ca,msgt_cyc))

ggplot(data=dat2, aes(x=cores, y=ca, group=iso, colour=iso)) +
  geom_line() +
  ylab("thpt (million/s)") +
  ggtitle(paste0("smallbank sf1")) +
  theme_bw()

