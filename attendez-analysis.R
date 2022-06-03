library(ggplot2)
library(stringr)
library(readr)
library(dplyr)
library(patchwork)
library(scales)

col_names = c("sf","protocol","workload","cores","total_time",
              "commits","external","internal","total_latency",
              "theta","update_rate","serializable_rate")

raw = read_csv(file = "exp.csv", col_names = col_names)

raw$thpt = raw$commits / (raw$total_time / raw$cores / 1000) / 1000000
raw$abr = (raw$external / (raw$commits + raw$external+ raw$internal))*100
raw$lat = raw$total_latency / (raw$commits + raw$external + raw$internal)

(g1 = ggplot(data = raw, aes(x = cores,y = thpt,group = protocol,colour = protocol)) + 
    geom_line() + ylab("thpt (million/s)") + labs(color="") + theme_bw() + 
    theme(legend.position="top",text = element_text(size = 18)))

(g2 = ggplot(data = raw, aes(x = cores, y = abr, group = protocol, colour = protocol)) +
    geom_line()  + ylab("abort rate") + labs(color="") + theme_bw() + 
    theme(legend.position="top",text = element_text(size = 18)))

(g3 = ggplot(data = raw, aes(x = cores, y = lat, group = protocol, colour = protocol)) +
    geom_line() + ylab("av latency (ms)") + scale_y_log10() + labs(color="") + theme_bw() + 
    theme(legend.position="top",text = element_text(size = 18)))

(combined <- g1 + g2 + g3 & theme(legend.position = "top", text = element_text(size = 20)))
(g = combined + plot_layout(guides = "collect"))

ggsave("attendez-analysis.pdf", g, width = 24, height = 6,device = "pdf")
