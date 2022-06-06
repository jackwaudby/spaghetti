library(ggplot2)
library(stringr)
library(readr)
library(dplyr)
library(patchwork)
library(scales)

col_names = c("sf","protocol","workload","cores","total_time",
              "commits","external","internal","total_latency",
              "theta","update_rate","serializable_rate","row_dirty","cascade","exceeded_watermark","commit_time")

raw = read_csv(file = "exp.csv", col_names = col_names)

raw$row_dirty[2:5] = raw$row_dirty[2:5] / raw$external[2:5]
raw$cascade[2:5] = raw$cascade[2:5] / raw$external[2:5]
raw$exceeded_watermark[2:5] = raw$exceeded_watermark[2:5] / raw$external[2:5]

raw$thpt = raw$commits / (raw$total_time / raw$cores / 1000) / 1000000
raw$abr = (raw$external / (raw$commits + raw$external+ raw$internal))*100
raw$lat = raw$total_latency / (raw$commits + raw$external + raw$internal)

(g0 = ggplot(data = raw, aes(x = cores,y = commit_time,group = protocol,colour = protocol)) + 
    geom_line() + ylab("thpt (million/s)") + labs(color="") + theme_bw() + 
    theme(legend.position="top",text = element_text(size = 18)))

(g1 = ggplot(data = raw, aes(x = cores,y = thpt,group = protocol,colour = protocol)) + 
    geom_line() + ylab("thpt (million/s)") + labs(color="") + theme_bw() + 
    theme(legend.position="top",text = element_text(size = 18)))

(g2 = ggplot(data = raw, aes(x = cores, y = abr, group = protocol, colour = protocol)) +
    geom_line()  + ylab("abort rate") + labs(color="") + theme_bw() + 
    theme(legend.position="top",text = element_text(size = 18)))

(g3 = ggplot(data = raw, aes(x = cores, y = lat, group = protocol, colour = protocol)) +
    geom_line() + ylab("av latency (ms)") + scale_y_log10() + labs(color="") + theme_bw() + 
    theme(legend.position="top",text = element_text(size = 18)))

(g4 = ggplot(raw, aes(cores)) + 
  geom_line(aes(y = row_dirty, colour = "row_dirty")) + 
  geom_line(aes(y = cascade, colour = "cascade")) + 
  geom_line(aes(y = exceeded_watermark, colour = "exceeded_watermark")) + labs(color="") + theme_bw())


(combined <- g1 + g2 + g3 + g4 & theme(legend.position = "top", text = element_text(size = 20)))
(g = combined + plot_layout(guides = "collect"))

ggsave("attendez-analysis.pdf", g, width = 16, height = 10,device = "pdf")
