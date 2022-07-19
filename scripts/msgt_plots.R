library(ggplot2)
library(stringr)
library(readr)
library(dplyr)
library(patchwork)
library(scales)
library(latex2exp)
library(tidyverse)

col_names = c("sf","protocol","workload","cores",
              "theta","serializable_rate","update_rate",
              "queries","dfs",
              "runtime","commits","aborts","not_found",
              "txn_time","commit_time","wait_time","latency",
              "rw","wr","rw","g0","g1","g2","path")

renameProtocols <- function(df) {
  for (i in 1:nrow(df)) {
    if (df[i,9] == "relevant" && df[i,2] == "msgt") {
      df[i,2] = "msgt-rel"
    }
    
    if (df[i,9] == "restricted" && df[i,2] == "msgt") {
      df[i,2] = "msgt-res"
    }
    
    if (df[i,9] == "reduced" && df[i,2] == "msgt") {
      df[i,2] = "msgt-red"
    }
  }
  
  return(df)
}
renameProtocolsTPCTC <- function(df) {
  for (i in 1:nrow(df)) {
    if (df[i,2] == "msgt") {
      df[i,2] = "MSGT"
    }
    
    if (df[i,2] == "sgt") {
      df[i,2] = "SGT"
    }
   
  }
  
  return(df)
}
computeMetrics <- function(df) {
  df$thpt = ((df$commits + df$not_found) / (df$runtime / 1000)) / 1000000
  df$abr = (df$aborts / (df$commits + df$not_found+ df$aborts))*100
  df$lat = (df$txn_time + df$latency) / (df$commits + df$not_found)
  df$com = df$commit_time / df$commits
  df$apl = df$path / (df$g0 + df$g1 + df$g2)
  
  return(df)
}

#### Overhead Experiment ####
file = "../results/exp-overhead-results.csv"
df = read_csv(file = file, col_names = col_names)
df = renameProtocols(df) 
df = computeMetrics(df)

for (w in c("smallbank","ycsb","tatp")) {
  temp = df[which(df$workload == w),]
  cat(paste0("workload: ",w,"\n"))
  
  (noccThpt = temp[which(temp$protocol == "nocc"),]$thpt)
  (sgtThpt = temp[which(temp$protocol == "sgt"),]$thpt)
  (msgtThpt = temp[which(temp$protocol == "msgt-red"),]$thpt)
  (sgtOverhead = ((noccThpt - sgtThpt) / noccThpt)*100)
  (msgtOverhead = ((noccThpt - msgtThpt) / noccThpt)*100)
  
  cat(paste0("nocc thpt: ",round(noccThpt, 2),"M\n"))
  cat(paste0("sgt thpt: ",round(sgtThpt, 2),"M\n"))
  cat(paste0("msgt thpt: ",round(msgtThpt, 2),"M\n"))
  cat(paste0("sgt overhead: ",round(sgtOverhead, 2),"%\n"))
  cat(paste0("msgt overhead: ",round(msgtOverhead, 2),"%\n"))
  cat("\n")
}

(noccThpt = df[which(df$protocol == "nocc"),]$thpt)
(sgtThpt = df[which(df$protocol == "sgt"),]$thpt)
(msgtThpt = df[which(df$protocol == "msgt-red"),]$thpt)
(sgtOverhead = ((noccThpt - sgtThpt) / noccThpt)*100)
(msgtOverhead = ((noccThpt - msgtThpt) / noccThpt)*100)


#### Isolation Experiment ####
file = "../results/exp-isolation-results.csv"
df = read_csv(file = file, col_names = col_names)
df = renameProtocolsTPCTC(df) 
df = computeMetrics(df)

(p1 = ggplot(data = df, aes(x = serializable_rate, y = thpt, group = protocol, colour = protocol)) +
    geom_line() + xlab(TeX('% of Serializable Transactions ($\\omega$)')) + ylab("thpt (million/s)") + 
    labs(color="") + theme_bw() + theme(legend.position="top",text = element_text(size = 18)) + 
    scale_color_manual(values=c("#CC6666", "#055099")))

ggplot(data = df, 
       aes(x = serializable_rate, 
           y = abr, 
           group = protocol, 
           colour = protocol)) +
  geom_line()

ggplot(data = df, 
       aes(x = serializable_rate, 
           y = lat, 
           group = protocol, 
           colour = protocol)) +
  geom_line()


ggplot(data = df, 
       aes(x = serializable_rate, 
           y = com, 
           group = protocol, 
           colour = protocol)) +
        geom_line()

msgt = df[1:6,]
total = msgt$g0 + msgt$g1 + msgt$g2

msgt$g0 = (msgt$g0 / total) * 100
msgt$g1 = (msgt$g1 / total) * 100
msgt$g2 = (msgt$g2 / total) * 100

ggplot(data = msgt, 
       aes(x = serializable_rate)) +
  geom_line(aes(y = g0)) +
  geom_line(aes(y = g1)) +
  geom_line(aes(y = g2))



ggplot(data = df[1:6,], 
       aes(x = serializable_rate, 
           y = apl)) +
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
