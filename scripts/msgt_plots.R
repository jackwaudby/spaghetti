library(ggplot2)
library(stringr)
library(readr)
library(dplyr)
library(patchwork)
library(scales)
library(latex2exp)

dir_root = "../msgt/paper/figures/"

# scalability
# read only
# cores=1-40; U=0.0; omega=0.0; theta=0.0
file = "./data/22_02_03_ycsb_scalabilty_1.csv"

# medium contention
# cores=1-40; U=0.5; omega=0.5; theta=0.6
file = "./data/22_02_03_ycsb_scalabilty_2.csv"

# high contention
# cores=1-40; U=0.5; omega=0.5; theta=0.7
file = "./data/22_02_03_ycsb_scalabilty_3.csv"

# high update rate, contention. low serializable rate
# cores=1-40; U=0.9; omega=0.2; theta=0.7
file = "./data/22_02_03_ycsb_scalability_4.csv"

# isolation
# cores=40; U=0.5; omega=0.0-1.0; theta=0.7
file = "./data/22_02_03_ycsb_isolation_1.csv"

# update rate
# cores=40; U=0.0-1.0; omega=0.2; theta=0.6
file = "./data/22_02_03_ycsb_update_rate_1.csv"

# contention
# cores=40; U=0.5; omega=0.2; theta=0.6-0.9
file = "./data/22_02_03_ycsb_contention_1.csv"

# load data
file = "./experiment.csv"
file = "./exp-isolation-results.csv"

col_names = c("sf","protocol","workload","cores",
              "theta","serializable_rate","update_rate",
              "dfs",
              "runtime","commits","aborts","not_found",
              "txn_time","commit_time","wait_time","latency")
raw = read_csv(file = file, col_names = col_names)

for (i in 1:nrow(raw)) {
  if (raw[i,8] == "relevant" && raw[i,2] == "msgt") {
    raw[i,2] = "msgt-rel"
  }
  
  if (raw[i,8] == "restricted" && raw[i,2] == "msgt") {
    raw[i,2] = "msgt-res"
  }
  
  if (raw[i,8] == "reduced" && raw[i,2] == "msgt") {
    raw[i,2] = "msgt-red"
  }
}

raw = raw %>% filter(dfs != "restricted")
raw$thpt = ((raw$commits + raw$not_found) / (raw$runtime / 1000)) / 1000000
raw$abr = (raw$aborts / (raw$commits + raw$not_found+ raw$aborts))*100
raw$lat = (raw$txn_time + raw$latency) / (raw$commits + raw$not_found)
raw$com = (raw$commit_time) / (raw$commits + raw$not_found)


ggplot(data = raw, 
       aes(x = serializable_rate, 
           y = thpt, 
           group = protocol, 
           colour = protocol)) +
  geom_line()

ggplot(data = raw, 
       aes(x = serializable_rate, 
           y = abr, 
           group = protocol, 
           colour = protocol)) +
  geom_line()

ggplot(data = raw, 
       aes(x = serializable_rate, 
           y = lat, 
           group = protocol, 
           colour = protocol)) +
  geom_line()


  ggplot(data = raw, 
       aes(x = serializable_rate, 
           y = com, 
           group = protocol, 
           colour = protocol)) +
  geom_line()


#### SCALABILITY ####

msgt = raw[raw$protocol == "msgt",]
sgt = raw[raw$protocol == "sgt",]
((msgt$thpt / sgt$thpt ) - 1)*100
(sgt$lat/msgt$lat)

sf1 = raw %>%
  group_by(protocol, cores) %>%
  summarise(thpt = median(thpt),
            abr = median(abr),
            lat = median(lat))



(s1 = ggplot(data = sf1, aes(x = cores,y = thpt,group = protocol,colour = protocol)) + 
  geom_line() + ylab("thpt (million/s)") + labs(color="") + theme_bw() + 
    theme(legend.position="top",text = element_text(size = 18)))

(s2 = ggplot(data = sf1, aes(x = cores,y = abr,group = protocol,colour = protocol)) +
  geom_line() + ylab("abort rate") + labs(color="") + theme_bw() + 
    theme(legend.position="top",text = element_text(size = 18)))

(s3 = ggplot(data = sf1, aes(x = cores,y = lat,group = protocol,colour = protocol)) +
  geom_line() + ylab("av latency (ms)") + labs(color="")+ theme_bw() + scale_y_log10() + 
    theme(legend.position="top",text = element_text(size = 18)))

combined <- s1 + s2 + s3 & theme(legend.position = "top",text = element_text(size = 20))
(sAll = combined + plot_layout(guides = "collect"))

s_root = "ycsb_scalability"
ggsave(paste0(dir_root,s_root,"_thpt.pdf"), s1, width = 6, height = 4,device = "pdf")
ggsave(paste0(dir_root,s_root,"_abr.pdf"), s2, width = 6, height = 4,device = "pdf")
ggsave(paste0(dir_root,s_root,"_lat.pdf"), s3, width = 6, height = 4,device = "pdf")
ggsave(paste0(dir_root,".pdf"),sAll,width = 18, height = 6,device = "pdf")

#### ISOLATION ####

raw = raw %>% filter(serializable_rate <= 0.8)

msgt = raw[raw$protocol == "msgt",]
sgt = raw[raw$protocol == "sgt",]

(msgt$thpt)/sgt$thpt

sgt$abr/(msgt$abr)

ggplot(data = raw, aes(x = serializable_rate, y = thpt, group = protocol, colour = protocol)) +
  geom_line()

ggplot(data = raw, aes(x = serializable_rate, y = abr, group = protocol, colour = protocol)) +
  geom_line()

ggplot(data = raw, aes(x = serializable_rate, y = lat, group = protocol, colour = protocol)) +
  geom_line()

(p1 = ggplot(data = raw, aes(x = serializable_rate, y = thpt, group = protocol, colour = protocol)) +
  geom_line() + xlab(TeX('% of Serializable Transactions ($\\omega$)')) + ylab("thpt (million/s)") + 
    labs(color="") + theme_bw() + theme(legend.position="top",text = element_text(size = 18)))

(p2 = ggplot(data = raw, aes(x = serializable_rate, y = abr, group = protocol, colour = protocol)) +
  geom_line() + xlab(TeX('% of Serializable Transactions ($\\omega$)')) + ylab("abort rate") +
  labs(color="") + theme_bw() + theme(legend.position="top",text = element_text(size = 18)))

(p3 = ggplot(data = raw, aes(x = serializable_rate, y = lat, group = protocol, colour = protocol)) +
  geom_line() + xlab(TeX('% of Serializable Transactions ($\\omega$)')) + ylab("av latency (ms)") + scale_y_log10() +
  labs(color="") + theme_bw() + theme(legend.position="top",text = element_text(size = 18)))

combined <- p1 + p2 + p3 & theme(legend.position = "top", text = element_text(size = 20))
combined + plot_layout(guides = "collect")


file_root = "ycsb_isolation"

ggsave(paste0(dir_root,file_root,"_thpt.pdf"), p1, width = 6, height = 4,device = "pdf")
ggsave(paste0(dir_root,file_root,"_abr.pdf"), p2, width = 6, height = 4,device = "pdf")
ggsave(paste0(dir_root,file_root,"_lat.pdf"), p3, width = 6, height = 4,device = "pdf")
ggsave(paste0(dir_root,".pdf"),width = 18, height = 6,device = "pdf")

#### UPDATE ####

(u1 = ggplot(data = raw, aes(x = update_rate,y = thpt,group = protocol,colour = protocol)) +
  geom_line() + xlab(TeX('% of Update Transactions ($U$)')) + ylab("thpt (million/s)") + 
   labs(color="") + theme_bw() + theme(legend.position="top",text = element_text(size = 18)))

(u2 = ggplot(data = raw, aes(x = update_rate,y = abr,group = protocol,colour = protocol)) +
  geom_line() + xlab(TeX('% of Update Transactions ($U$)')) + ylab("abort rate") + 
  labs(color="") + theme_bw() + theme(legend.position="top",text = element_text(size = 18)))

(u3 = ggplot(data = raw, aes(x = update_rate,y = lat,group = protocol,colour = protocol)) +
  geom_line() + xlab(TeX('% of Update Transactions ($U$)')) + ylab("av latency (ms)") + theme_bw() + scale_y_log10() + 
  labs(color="") + theme_bw() + theme(legend.position="top",text = element_text(size = 18)))

combined <- u1 + u2 + u3 & theme(legend.position = "top",text = element_text(size = 20))
uAll = combined + plot_layout(guides = "collect")

ur_root = "ycsb_update_rate"
ggsave(paste0(dir_root,ur_root,"_thpt.pdf"), u1, width = 6, height = 4,device = "pdf")
ggsave(paste0(dir_root,ur_root,"_abr.pdf"), u2, width = 6, height = 4,device = "pdf")
ggsave(paste0(dir_root,ur_root,"_lat.pdf"), u3, width = 6, height = 4,device = "pdf")
ggsave(paste0(dir_root,".pdf"),uAll,width = 18, height = 6,device = "pdf")


#### CONTENTION ####

msgt = raw[raw$protocol == "msgt",]
sgt = raw[raw$protocol == "sgt",]
((msgt$thpt / sgt$thpt ) - 1)*100

sgt$abr/msgt$abr
sgt$abr - msgt$abr
(1-msgt$lat/sgt$lat)*100

(c1 = ggplot(data = raw, aes(x = theta,y = thpt,group = protocol,colour = protocol)) +
  geom_line() + ylab("thpt (million/s)") + xlab(TeX('Skew Factor ($\\theta$)')) +
  labs(color="") + theme_bw() + theme(legend.position="top",text = element_text(size = 18)))

(c2 = ggplot(data = raw, aes(x = theta,y = abr,group = protocol,colour = protocol)) +
  geom_line() + xlab(TeX('Skew Factor ($\\theta$)')) + ylab("abort rate") +
  labs(color="") + theme_bw() + theme(legend.position="top",text = element_text(size = 18)))

(c3 = ggplot(data = raw, aes(x = theta,y = lat,group = protocol,colour = protocol)) +
  geom_line() + xlab(TeX('Skew Factor ($\\theta$)')) + ylab("av latency (ms)") + scale_y_log10() +
    labs(color="") + theme_bw() + theme(legend.position="top",text = element_text(size = 18)))

combined <- c1 + c2 + c3 & theme(legend.position = "top", text = element_text(size = 20))
cAll = combined + plot_layout(guides = "collect")

c_root = "ycsb_contention"
ggsave(paste0(dir_root,c_root,"_thpt.pdf"), c1, width = 6, height = 4,device = "pdf")
ggsave(paste0(dir_root,c_root,"_abr.pdf"), c2, width = 6, height = 4,device = "pdf")
ggsave(paste0(dir_root,c_root,"_lat.pdf"), c3, width = 6, height = 4,device = "pdf")
ggsave(paste0(dir_root,".pdf"),cAll,width = 18, height = 6,device = "pdf")
