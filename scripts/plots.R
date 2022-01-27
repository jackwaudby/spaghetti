# Produces plots for thpt, av.latency, and abort rate for a workload
library(ggplot2)
library(stringr)
library(readr)
library(dplyr)
library(patchwork)

# get file path
args = commandArgs(trailingOnly = TRUE)
file = args[1]
include_2pl = args[2]
include_sgt = args[3]

file = "./data/22_01_27_all.csv"

# load data
col_names = c(
  "sf",
  "protocol",
  "workload",
  "cores",
  "total_time",
  "commits",
  "aborts",
  "errors",
  "total_latency"
)
raw = read_csv(file = file, col_names = col_names)

raw = raw %>% 
    mutate(protocol = str_replace(protocol, "owhtt", "wh-mc-tt")) %>% 
     mutate(protocol = str_replace(protocol, "owh", "wh-mc"))

# get workload
workload = str_to_title(raw$workload[1])

# include durner results
if (include_2pl == "true") {
  durner_2pl = read_delim(file = './data/durner_2pl.csv',
                          delim = ";",
                          col_names = F)
  durner_2pl = durner_2pl[, c(1, 2, 3, 5, 8, 9, 11, 10, 15, 18)]
  colnames(durner_2pl) <-
    c(
      "workload",
      "protocol",
      "sf",
      "cores",
      "total_time",
      "commits",
      "aborts",
      "errors",
      "txn_time",
      "latency"
    )
  durner_2pl$total_latency <-
    durner_2pl$txn_time + durner_2pl$latency
  durner_2pl = durner_2pl[, -c(9, 10)]
  durner_2pl$protocol = "2pl"
  durner_2pl$workload = workload
  durner_2pl = durner_2pl[, c(3, 2, 1, 4, 5, 6, 7, 8, 9)]
  raw = bind_rows(raw, durner_2pl)
}

if (include_sgt == "true") {
  durner <-
    read_delim(file = './data/durner_sgt.csv',
               delim = ";",
               col_names = F)
  durner = durner[, c(1, 2, 3, 5, 8, 9, 11, 10, 15, 18)]
  colnames(durner) <-
    c(
      "workload",
      "protocol",
      "sf",
      "cores",
      "total_time",
      "commits",
      "aborts",
      "errors",
      "txn_time",
      "latency"
    )
  durner$total_latency <- durner$txn_time + durner$latency
  durner = durner[, -c(9, 10)]
  durner$protocol = "durner-sgt"
  durner$workload = workload
  durner = durner[, c(3, 2, 1, 4, 5, 6, 7, 8, 9)]
  raw = bind_rows(raw, durner)
}

# compute thpt, av.latency, and abort rate.
raw$thpt = raw$commits / (raw$total_time / raw$cores / 1000) / 1000000
raw$abr = raw$errors / (raw$commits + raw$errors)
raw$lat = raw$total_latency / (raw$commits + raw$errors + raw$aborts)

# remove cores above 40
raw = raw %>% filter(cores <= 40)

# Divide into scale factor data sets
raw$sf = as.factor(raw$sf)
sf1 = raw %>% filter((sf == 1) | (sf == 100))
sf3 = raw %>% filter((sf == 3) | (sf == 10000))

# compute median
sf1 = sf1 %>%
  group_by(protocol, cores) %>%
  summarise(thpt = median(thpt),
            abr = median(abr),
            lat = median(lat))
sf3 = sf3 %>%
  group_by(protocol, cores) %>%
  summarise(thpt = median(thpt),
            abr = median(abr),
            lat = median(lat))

if (nrow(sf1) > 0) {
  p1 = ggplot(data = sf1, aes(
    x = cores,
    y = thpt,
    group = protocol,
    colour = protocol
  )) +
    geom_line() +
    ylab("thpt (million/s)") +
    theme_bw()
  
  p2 = ggplot(data = sf1, aes(
    x = cores,
    y = abr,
    group = protocol,
    colour = protocol
  )) +
    geom_line() +
    ylab("abort rate") +
    theme_bw()
  
  p3 = ggplot(data = sf1, aes(
    x = cores,
    y = lat,
    group = protocol,
    colour = protocol
  )) +
    geom_line() +
    ylab("av latency (ms)") +
    theme_bw()
  
  combined <- p1 + p2 + p3 & theme(legend.position = "top")
  combined + plot_layout(guides = "collect")
  
  ggsave(paste0("./graphics/", tolower(workload), "_sf1.png"),width = 18, height = 6)
}

if (nrow(sf3) > 0) {
  p1 = ggplot(data = sf3, aes(
    x = cores,
    y = thpt,
    group = protocol,
    colour = protocol
  )) +
    geom_line() +
    ylab("thpt (million/s)") +
     theme_bw()
  
  p2 = ggplot(data = sf3, aes(
    x = cores,
    y = abr,
    group = protocol,
    colour = protocol
  )) +
    geom_line() +
    ylab("abort rate") +
    theme_bw()
  
  p3 = ggplot(data = sf3, aes(
    x = cores,
    y = lat,
    group = protocol,
    colour = protocol
  )) +
    geom_line() +
    ylab("av latency (ms)") +
    theme_bw()
  
  combined <- p1 + p2 + p3 & theme(legend.position = "top")
  combined + plot_layout(guides = "collect")
  
  ggsave(paste0("./graphics/", tolower(workload), "_sf3.png"),width = 18, height = 6)
}

