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
