library(ggplot2)
library(stringr)
library(readr)
library(dplyr)
library(patchwork)
library(scales)
library(latex2exp)

file = "./data/22_02_03_ycsb_scalabilty_1.csv"
file = "./data/22_02_03_ycsb_scalabilty_2.csv"
file = "./data/22_02_03_ycsb_scalabilty_3.csv"
file = "./data/22_02_03_ycsb_scalability_4.csv"
file = "./data/22_02_03_ycsb_isolation_1.csv"
file = "./data/22_02_03_ycsb_update_rate_1.csv"
file = "./data/22_02_03_ycsb_contention_1.csv"

# load data
col_names = c(
  "sf",
  "protocol",
  "workload",
  "cores",
  "total_time",
  "commits",
  "external",
  "internal",
  "total_latency",
  "theta",
  "update_rate",
  "serializable_rate"
)
raw = read_csv(file = file, col_names = col_names)
raw = raw %>% filter(cores <= 40)
workload = str_to_title(raw$workload[1])

raw$thpt = raw$commits / (raw$total_time / raw$cores / 1000) / 1000000
raw$abr = raw$external / (raw$commits + raw$external+ raw$internal)
raw$lat = raw$total_latency / (raw$commits + raw$external + raw$internal)

raw = raw %>% 
  mutate(protocol = str_replace(protocol, "msgt-std", "msgt"))

#### SCALABILITY ####

sf1 = raw %>%
  group_by(protocol, cores) %>%
  summarise(thpt = median(thpt),
            abr = median(abr),
            lat = median(lat))

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
  theme_bw() +
  scale_y_log10()

combined <- p1 + p2 + p3 & theme(legend.position = "top",
                                 text = element_text(size = 20)
)
combined + plot_layout(guides = "collect")
ggsave(paste0("../msgt/paper/figures/ycsb_scalability.pdf"),width = 18, height = 6,device = "pdf")

#### ISOLATION ####

p1 = ggplot(data = raw, aes(
  x = serializable_rate,
  y = thpt,
  group = protocol,
  colour = protocol
)) +
  geom_line() +
  xlab(TeX('% of Serializable Transactions ($\\omega$)')) +
  ylab("thpt (million/s)") +
  theme_bw()

p2 = ggplot(data = raw, aes(
  x = serializable_rate,
  y = abr,
  group = protocol,
  colour = protocol
)) +
  geom_line() +
  xlab(TeX('% of Serializable Transactions ($\\omega$)')) +
  ylab("abort rate") +
  theme_bw()

p3 = ggplot(data = raw, aes(
  x = serializable_rate,
  y = lat,
  group = protocol,
  colour = protocol
)) +
  geom_line() +
  xlab(TeX('% of Serializable Transactions ($\\omega$)')) +
  ylab("av latency (ms)") +
  theme_bw() +
  scale_y_log10()

combined <- p1 + p2 + p3 & theme(legend.position = "top",
                                 text = element_text(size = 20)
)
combined + plot_layout(guides = "collect")
ggsave(paste0("../msgt/paper/figures/ycsb_isolation.pdf"),width = 18, height = 6,device = "pdf")

raw

#### UPDATE ####

p1 = ggplot(data = raw, aes(
  x = update_rate,
  y = thpt,
  group = protocol,
  colour = protocol
)) +
  geom_line() +
  xlab(TeX('% of Update Transactions ($U$)')) +
  ylab("thpt (million/s)") +
  theme_bw()

p2 = ggplot(data = raw, aes(
  x = update_rate,
  y = abr,
  group = protocol,
  colour = protocol
)) +
  geom_line() +
  xlab(TeX('% of Update Transactions ($U$)')) +
  ylab("abort rate") +
  theme_bw()

p3 = ggplot(data = raw, aes(
  x = update_rate,
  y = lat,
  group = protocol,
  colour = protocol
)) +
  geom_line() +
  xlab(TeX('% of Update Transactions ($U$)')) +
  ylab("av latency (ms)") +
  theme_bw() +
  scale_y_log10()

combined <- p1 + p2 + p3 & theme(legend.position = "top",
                                 text = element_text(size = 20)
)
combined + plot_layout(guides = "collect")
ggsave(paste0("../msgt/paper/figures/ycsb_update.pdf"),width = 18, height = 6,device = "pdf")


#### CONTENTION ####

p1 = ggplot(data = raw, aes(
  x = theta,
  y = thpt,
  group = protocol,
  colour = protocol
)) +
  geom_line() +
  ylab("thpt (million/s)") +
  xlab(TeX('Skew Factor ($\\theta$)')) +
  theme_bw()

p2 = ggplot(data = raw, aes(
  x = theta,
  y = abr,
  group = protocol,
  colour = protocol
)) +
  geom_line() +
  xlab(TeX('Skew Factor ($\\theta$)')) +
  ylab("abort rate") +
  theme_bw()

p3 = ggplot(data = raw, aes(
  x = theta,
  y = lat,
  group = protocol,
  colour = protocol
)) +
  geom_line() +
  xlab(TeX('Skew Factor ($\\theta$)')) +
  ylab("av latency (ms)") +
  theme_bw() +
  scale_y_log10()

combined <- p1 + p2 + p3& theme(legend.position = "top",
                                 text = element_text(size = 20)
)
combined + plot_layout(guides = "collect")
ggsave(paste0("../msgt/paper/ycsb_contention.pdf"),width = 18, height = 6,device = "pdf")

