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
dat_file = "./exp-isolation-results.csv"
df = read_csv(file = dat_file, col_names = col_names)
df = renameProtocols(df) 
df = computeMetrics(df)
file_root = "ycsb_isolation"

# Throughput 
((df$thpt[1:6] / df$thpt[7:12]) - 1) * 100

(p1 = ggplot(data = df, aes(x = serializable_rate, y = thpt, group = protocol, colour = protocol)) +
    geom_line() + 
    xlab(TeX('% of Serializable Transactions ($\\omega$)')) + 
    ylab("thpt (million/s)") + 
    labs(color="") + 
    theme_bw() + theme(legend.position="top",text = element_text(size = 18)) + 
    scale_color_manual(values=c("#CC6666", "#055099")))

ggsave(paste0(file_root,"_thpt.pdf"), p1, width = 6, height = 4,device = "pdf")
ggsave(paste0(file_root,"_thpt.png"), p1, width = 6, height = 4,device = "png")

# Abort rate 
(df$abr[7:12]/ df$abr[1:6])

(p2 = ggplot(data = df, aes(x = serializable_rate, y = abr, group = protocol, colour = protocol)) +
    geom_line() + 
    xlab(TeX('% of Serializable Transactions ($\\omega$)')) + 
    ylab("abort rate (%)") +
    labs(color="") + 
    theme_bw() + theme(legend.position="top",text = element_text(size = 18)) + 
    scale_color_manual(values=c("#CC6666", "#055099")))

ggsave(paste0(file_root,"_abr.pdf"), p2, width = 6, height = 4,device = "pdf")
ggsave(paste0(file_root,"_abr.png"), p2, width = 6, height = 4,device = "png")

# Latency
(p3 = ggplot(data = df, aes(x = serializable_rate, y = lat, group = protocol, colour = protocol)) +
    geom_line() + 
    xlab(TeX('% of Serializable Transactions ($\\omega$)')) + 
    ylab("av latency (ms)") + 
    labs(color="") + 
    theme_bw() + theme(legend.position="top",text = element_text(size = 18)) +
    scale_color_manual(values=c("#CC6666", "#055099")))

ggsave(paste0(file_root,"_lat.pdf"), p3, width = 6, height = 4,device = "pdf")
ggsave(paste0(file_root,"_lat.png"), p3, width = 6, height = 4,device = "png")

#### Contention Experiment ####
dat_file = "./exp-contention-results.csv"
df = read_csv(file = dat_file, col_names = col_names)
df = renameProtocols(df) 
df = computeMetrics(df)
con_file_root = "ycsb_contention"

# Throughput 
((df$thpt[1:6] / df$thpt[7:12]) - 1) * 100

(c1 = ggplot(data = df, aes(x = theta,y = thpt,group = protocol,colour = protocol)) +
    geom_line() + ylab("thpt (million/s)") + 
    xlab(TeX('Skew Factor ($\\theta$)')) +
    labs(color="") + theme_bw() + 
    theme(legend.position="top",text = element_text(size = 18)) +
    scale_color_manual(values=c("#CC6666", "#055099")))

ggsave(paste0(con_file_root,"_thpt.pdf"), c1, width = 6, height = 4,device = "pdf")
ggsave(paste0(con_file_root,"_thpt.png"), c1, width = 6, height = 4,device = "png")

# Abort rate 
100 * ((df$abr[7:12] -df$abr[1:6]) / df$abr[1:6])

(c2 = ggplot(data = df, aes(x = theta,y = abr,group = protocol,colour = protocol)) +
    geom_line() + xlab(TeX('Skew Factor ($\\theta$)')) + ylab("abort rate") +
    labs(color="") + theme_bw() + theme(legend.position="top",text = element_text(size = 18))+
    scale_color_manual(values=c("#CC6666", "#055099")))

ggsave(paste0(con_file_root,"_abr.pdf"), c2, width = 6, height = 4,device = "pdf")
ggsave(paste0(con_file_root,"_abr.png"), c2, width = 6, height = 4,device = "png")

# Latency 
((df$lat[1:6] - df$lat[7:12]) /df$lat[1:6]) * 100

(c3 = ggplot(data = df, aes(x = theta,y = lat,group = protocol,colour = protocol)) +
    geom_line() + xlab(TeX('Skew Factor ($\\theta$)')) + ylab("av latency (ms)") +
    labs(color="") + theme_bw() + theme(legend.position="top",text = element_text(size = 18))+
  scale_color_manual(values=c("#CC6666", "#055099")))

ggsave(paste0(con_file_root,"_lat.pdf"), c3, width = 6, height = 4,device = "pdf")
ggsave(paste0(con_file_root,"_lat.png"), c3, width = 6, height = 4,device = "png")

#### Scalability Experiment ####
dat_file = "./exp-scalability-results.csv"
df = read_csv(file = dat_file, col_names = col_names)
df = renameProtocols(df) 
df = computeMetrics(df)
scal_file_root = "ycsb_scalability"

# Throughput 
# percentage increase = ((new - old)/old)*100
((df$thpt[1:5] - df$thpt[6:10])/ df$thpt[6:10]) * 100

(s1 = ggplot(data = df, aes(x = cores,y = thpt,group = protocol,colour = protocol)) + 
   geom_line() + ylab("thpt (million/s)") + labs(color="") + theme_bw() + 
   theme(legend.position="top",text = element_text(size = 18)) +
    scale_color_manual(values=c("#CC6666", "#055099")))

ggsave(paste0(scal_file_root,"_thpt.pdf"), s1, width = 6, height = 4,device = "pdf")
ggsave(paste0(scal_file_root,"_thpt.png"), s1, width = 6, height = 4,device = "png")

# Abort rate 
((df$abr[6:10] - df$abr[1:5] ) / df$abr[1:5]) * 100

(s2 = ggplot(data = df, aes(x = cores,y = abr,group = protocol,colour = protocol)) +
    geom_line() + ylab("abort rate") + labs(color="") + theme_bw() + 
    theme(legend.position="top",text = element_text(size = 18)) +
    scale_color_manual(values=c("#CC6666", "#055099")))

ggsave(paste0(scal_file_root,"_abr.pdf"), s2, width = 6, height = 4,device = "pdf")
ggsave(paste0(scal_file_root,"_abr.png"), s2, width = 6, height = 4,device = "png")

# Latency 
(s3 = ggplot(data = df, aes(x = cores,y = lat,group = protocol,colour = protocol)) +
    geom_line() + ylab("av latency (ms)") + labs(color="")+ theme_bw() + scale_y_log10() + 
    theme(legend.position="top",text = element_text(size = 18)) +
    scale_color_manual(values=c("#CC6666", "#055099")))

ggsave(paste0(scal_file_root,"_lat.pdf"), s3, width = 6, height = 4,device = "pdf")
ggsave(paste0(scal_file_root,"_lat.png"), s3, width = 6, height = 4,device = "png")

