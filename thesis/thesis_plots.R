library(ggplot2)
library(stringr)
library(readr)
library(dplyr)
library(patchwork)
library(scales)
library(latex2exp)

renameProtocols <- function(df) {
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

col_names = c("sf","protocol","workload","cores",
              "theta","serializable_rate","update_rate",
              "queries","dfs","runtime","commits","aborts",
              "not_found","txn_time","commit_time","wait_time",
              "latency","rw","wr","rw","g0","g1","g2","path")

#### Isolation Experiment ####
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

ggsave(paste0(file_root,"_thpt.pdf"), p1, width = 6, height = 4,device = "pdf")
ggsave(paste0(file_root,"_abr.pdf"), p2, width = 6, height = 4,device = "pdf")
ggsave(paste0(file_root,"_lat.pdf"), p3, width = 6, height = 4,device = "pdf")


#### Contention Experiment ####
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

ggsave(paste0(con_file_root,"_thpt.pdf"), c1, width = 6, height = 4,device = "pdf")
ggsave(paste0(con_file_root,"_abr.pdf"), c2, width = 6, height = 4,device = "pdf")
ggsave(paste0(con_file_root,"_lat.pdf"), c3, width = 6, height = 4,device = "pdf")

#### Scalability Experiment ####
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

# Latency 
(s3 = ggplot(data = df, aes(x = cores,y = lat,group = protocol,colour = protocol)) +
    geom_line() + ylab("av latency (ms)") + labs(color="")+ theme_bw() + scale_y_log10() + 
    theme(legend.position="top",text = element_text(size = 18)) +
    scale_color_manual(values=c("#CC6666", "#055099")))

ggsave(paste0(scal_file_root,"_thpt.pdf"), s1, width = 6, height = 4,device = "pdf")
ggsave(paste0(scal_file_root,"_abr.pdf"), s2, width = 6, height = 4,device = "pdf")
ggsave(paste0(scal_file_root,"_lat.pdf"), s3, width = 6, height = 4,device = "pdf")

#### Update Rate Experiment ####
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

# Abort rate 
(u2 = ggplot(data = df, aes(x = update_rate,y = abr,group = protocol,colour = protocol)) +
    geom_line() + xlab(TeX('% of Update Transactions ($U$)')) + ylab("abort rate") + 
    labs(color="") + theme_bw() + theme(legend.position="top",text = element_text(size = 18)) +
    scale_color_manual(values=c("#CC6666", "#055099")))

# Latency
(u3 = ggplot(data = df, aes(x = update_rate,y = lat,group = protocol,colour = protocol)) +
    geom_line() + xlab(TeX('% of Update Transactions ($U$)')) + ylab("av latency (ms)") + theme_bw() + scale_y_log10() + 
    labs(color="") + theme_bw() + theme(legend.position="top",text = element_text(size = 18)) +
    scale_color_manual(values=c("#CC6666", "#055099")))

ggsave(paste0(ur_file_root,"_thpt.pdf"), u1, width = 6, height = 4,device = "pdf")
ggsave(paste0(ur_file_root,"_abr.pdf"), u2, width = 6, height = 4,device = "pdf")
ggsave(paste0(ur_file_root,"_lat.pdf"), u3, width = 6, height = 4,device = "pdf")

#### SMALLBANK ####
dat_file = "./data/exp-smallbank-results.csv"
df = read_csv(file = dat_file, col_names = col_names)
df = renameProtocols(df) 
df = computeMetrics(df)
smb_file_root = "./graphics/smallbank"

# Throughput 
(sb1 = ggplot(data = df, aes(x = cores,y = thpt,group = protocol,colour = protocol)) +
    geom_line() + ylab("thpt (million/s)") + 
    xlab(TeX('cores')) +
    labs(color="") + theme_bw() + 
    theme(legend.position="top",text = element_text(size = 18)) +
    scale_color_manual(values=c("#CC6666", "#055099")))

# Abort rate 
(sb2 = ggplot(data = df, aes(x = cores,y = abr,group = protocol,colour = protocol)) +
    geom_line() + ylab("abort rate") + labs(color="") + theme_bw() + 
    theme(legend.position="top",text = element_text(size = 18)) +
    scale_color_manual(values=c("#CC6666", "#055099")))

# Latency
(sb3 = ggplot(data = df, aes(x = cores,y = lat,group = protocol,colour = protocol)) +
    geom_line() + ylab("av latency (ms)") +
    labs(color="") + theme_bw() + theme(legend.position="top",text = element_text(size = 18))+
    scale_color_manual(values=c("#CC6666", "#055099")))


ggsave(paste0(smb_file_root,"_thpt.pdf"), sb1, width = 6, height = 4,device = "pdf")
ggsave(paste0(smb_file_root,"_abr.pdf"), sb2, width = 6, height = 4,device = "pdf")
ggsave(paste0(smb_file_root,"_lat.pdf"), sb3, width = 6, height = 4,device = "pdf")

#### TATP ####
dat_file = "./data/exp-tatp-results.csv"
df = read_csv(file = dat_file, col_names = col_names)
df = renameProtocols(df) 
df = computeMetrics(df)
tatp_file_root = "./graphics/tatp"

# Throughput 
(t1 = ggplot(data = df, aes(x = cores,y = thpt,group = protocol,colour = protocol)) +
    geom_line() + ylab("thpt (million/s)") + 
    xlab(TeX('cores')) +
    labs(color="") + theme_bw() + 
    theme(legend.position="top",text = element_text(size = 18)) +
    scale_color_manual(values=c("#CC6666", "#055099")))

# Abort rate 
(t2 = ggplot(data = df, aes(x = cores,y = abr,group = protocol,colour = protocol)) +
    geom_line() + ylab("abort rate") + labs(color="") + theme_bw() + 
    theme(legend.position="top",text = element_text(size = 18)) +
    scale_color_manual(values=c("#CC6666", "#055099")))

# Latency
(t3 = ggplot(data = df, aes(x = cores,y = lat,group = protocol,colour = protocol)) +
    geom_line() + ylab("av latency (ms)") +
    labs(color="") + theme_bw() + theme(legend.position="top",text = element_text(size = 18))+
    scale_color_manual(values=c("#CC6666", "#055099")))

ggsave(paste0(tatp_file_root,"_thpt.pdf"), t1, width = 6, height = 4,device = "pdf")
ggsave(paste0(tatp_file_root,"_abr.pdf"), t2, width = 6, height = 4,device = "pdf")
ggsave(paste0(tatp_file_root,"_lat.pdf"), t3, width = 6, height = 4,device = "pdf")



#### OVERHEAD ####
dat_file = "./data/exp-overhead-results.csv"
df = read_csv(file = dat_file, col_names = col_names)
df = renameProtocols(df) 
df = computeMetrics(df)

computeOverhead <- function(df, workload) {
  temp = df[df$workload == workload,]
  
  noccThpt = temp[temp$protocol == "nocc",]$thpt
  msgtThpt = temp[temp$protocol == "MSGT",]$thpt
  sgtThpt = temp[temp$protocol == "SGT",]$thpt
  
  msgtOverhead = ((noccThpt - msgtThpt) / noccThpt) * 100
  sgtOverhead = ((noccThpt - sgtThpt) / noccThpt) * 100
  
  return(list(noccThpt = noccThpt,
              msgtThpt = msgtThpt,
              sgtThpt = sgtThpt,
              sgtOverhead = sgtOverhead,
              msgtOverhead = msgtOverhead))
}

computeOverhead(df,"smallbank")
computeOverhead(df,"tatp")
computeOverhead(df,"ycsb")
