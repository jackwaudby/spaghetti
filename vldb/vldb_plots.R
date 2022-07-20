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
    if (df[i,2] == "msgt") {
      df[i,2] = "MSGT"
    }
    
    if (df[i,2] == "sgt") {
      df[i,2] = "SGT"
    }
    
    # if (df[i,9] == "relevant" && df[i,2] == "msgt") {
    #   df[i,2] = "msgt-rel"
    # }
    # 
    # if (df[i,9] == "restricted" && df[i,2] == "msgt") {
    #   df[i,2] = "msgt-res"
    # }
    # 
    # if (df[i,9] == "reduced" && df[i,2] == "msgt") {
    #   df[i,2] = "msgt-red"
    # }
    # 
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
dat_file = "./data/exp-scalability-results.csv"
df = read_csv(file = dat_file, col_names = col_names)
df = renameProtocols(df) 
df = computeMetrics(df)
scal_file_root = "./graphics/ycsb_scalability"

# Throughput 
(s1 = ggplot(data = df, aes(x = cores,y = thpt,group = protocol,colour = protocol)) + 
    geom_line() + ylab("thpt (million/s)") + labs(color="") + theme_bw() + 
    theme(legend.position="top",text = element_text(size = 18)) +
    scale_color_manual(values=c("#CC6666", "#055099")))

# Abort rate 
(s2 = ggplot(data = df, aes(x = cores,y = abr,group = protocol,colour = protocol)) +
    geom_line() + ylab("abort rate") + labs(color="") + theme_bw() + 
    theme(legend.position="top",text = element_text(size = 18)) +
    scale_color_manual(values=c("#CC6666", "#055099")))

combined <- s1 + s2 & theme(legend.position = "top",text = element_text(size = 20))
(sAll = combined + plot_layout(guides = "collect"))

ggsave(paste0(file_root,".pdf"),sAll,width = 18, height = 6,device = "pdf")

#### ISOLATION ####
dat_file = "./data/exp-isolation-results.csv"
df = read_csv(file = dat_file, col_names = col_names)
df = renameProtocols(df) 
df = computeMetrics(df)
file_root = "./graphics/ycsb_isolation"

# Throughput 
(p1 = ggplot(data = df, aes(x = serializable_rate, y = thpt, group = protocol, colour = protocol)) +
    geom_line() + 
    xlab(TeX('% of Serializable Transactions ($\\omega$)')) + 
    ylab("thpt (million/s)") + 
    labs(color="") + 
    theme_bw() + theme(legend.position="top",text = element_text(size = 18)) + 
    scale_color_manual(values=c("#CC6666", "#055099")))

# Abort rate 
(p2 = ggplot(data = df, aes(x = serializable_rate, y = abr, group = protocol, colour = protocol)) +
    geom_line() + 
    xlab(TeX('% of Serializable Transactions ($\\omega$)')) + 
    ylab("abort rate (%)") +
    labs(color="") + 
    theme_bw() + theme(legend.position="top",text = element_text(size = 18)) + 
    scale_color_manual(values=c("#CC6666", "#055099")))

# Latency
(p3 = ggplot(data = df, aes(x = serializable_rate, y = lat, group = protocol, colour = protocol)) +
    geom_line() + 
    xlab(TeX('% of Serializable Transactions ($\\omega$)')) + 
    ylab("av latency (ms)") + 
    labs(color="") + 
    theme_bw() + theme(legend.position="top",text = element_text(size = 18)) +
    scale_color_manual(values=c("#CC6666", "#055099")))

combined <- p1 + p2 + p3 & theme(legend.position = "top", text = element_text(size = 20))
(combined + plot_layout(guides = "collect"))

ggsave(paste0(file_root,".pdf"),width = 18, height = 6,device = "pdf")

#### UPDATE ####
dat_file = "./data/exp-update-rate-results.csv"
df = read_csv(file = dat_file, col_names = col_names)
df = renameProtocols(df) 
df = computeMetrics(df)
ur_file_root = "./graphics/ycsb_update_rate"

# Throughput 
(u1 = ggplot(data = df, aes(x = update_rate,y = thpt,group = protocol,colour = protocol)) +
    geom_line() + xlab(TeX('% of Update Transactions ($U$)')) + ylab("thpt (million/s)") + 
    labs(color="") + theme_bw() + theme(legend.position="top",text = element_text(size = 18)) +
    scale_color_manual(values=c("#CC6666", "#055099")))

ggsave(paste0(ur_file_root,"_thpt.pdf"), u1, width = 6, height = 4,device = "pdf")

#### CONTENTION ####
dat_file = "./data/exp-contention-results.csv"
df = read_csv(file = dat_file, col_names = col_names)
df = renameProtocols(df) 
df = computeMetrics(df)
con_file_root = "./graphics/ycsb_contention"

# Throughput 
(c1 = ggplot(data = df, aes(x = theta,y = thpt,group = protocol,colour = protocol)) +
    geom_line() + ylab("thpt (million/s)") + 
    xlab(TeX('Skew Factor ($\\theta$)')) +
    labs(color="") + theme_bw() + 
    theme(legend.position="top",text = element_text(size = 18)) +
    scale_color_manual(values=c("#CC6666", "#055099")))

# Abort rate 
(c2 = ggplot(data = df, aes(x = theta,y = abr,group = protocol,colour = protocol)) +
    geom_line() + xlab(TeX('Skew Factor ($\\theta$)')) + ylab("abort rate") +
    labs(color="") + theme_bw() + theme(legend.position="top",text = element_text(size = 18))+
    scale_color_manual(values=c("#CC6666", "#055099")))

# Latency 
(c3 = ggplot(data = df, aes(x = theta,y = lat,group = protocol,colour = protocol)) +
    geom_line() + xlab(TeX('Skew Factor ($\\theta$)')) + ylab("av latency (ms)") +
    labs(color="") + theme_bw() + theme(legend.position="top",text = element_text(size = 18))+
    scale_color_manual(values=c("#CC6666", "#055099")))

combined <- c1 + c2 + c3 & theme(legend.position = "top", text = element_text(size = 20))
(cAll = combined + plot_layout(guides = "collect"))

ggsave(paste0(con_file_root,".pdf"),cAll,width = 18, height = 6,device = "pdf")
