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
dat_file = "./data/exp-isolation-results-sf2.csv"
df = read_csv(file = dat_file, col_names = col_names)
df = renameProtocols(df) 
df = computeMetrics(df)
file_root = "./graphics/ycsb_isolation"

# Metrics 
((df[1:6,]$thpt - df[7:12,]$thpt) / df[7:12,]$thpt) * 100

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
