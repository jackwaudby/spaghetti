library(ggplot2)
library(stringr)
library(readr)
library(dplyr)
library(patchwork)
library(scales)

col_names = c("sf","protocol","workload","cores","thpt","abr","lat","write_lat")

raw = read_csv(file = "exp.csv", col_names = col_names)

(g1 = ggplot(data = raw, aes(x = cores,y = thpt,group = protocol,colour = protocol)) + 
    geom_line() + ylab("thpt (million/s)") + labs(color="") + theme_bw() + 
    theme(legend.position="top",text = element_text(size = 18)))

(g2 = ggplot(data = raw, aes(x = cores, y = abr, group = protocol, colour = protocol)) +
    geom_line()  + ylab("abort rate") + labs(color="") + theme_bw() + 
    theme(legend.position="top",text = element_text(size = 18)))

(g3 = ggplot(data = raw, aes(x = cores, y = lat, group = protocol, colour = protocol)) +
    geom_line() + ylab("av latency (us)") + scale_y_log10() + labs(color="") + theme_bw() + 
    theme(legend.position="top",text = element_text(size = 18)))

(g4 = ggplot(data = raw, aes(x = cores, y = write_lat, group = protocol, colour = protocol)) +
    geom_line() + ylab("av write op (us)") + scale_y_log10() + labs(color="") + theme_bw() + 
    theme(legend.position="top",text = element_text(size = 18)))

(combined <- g1 + g2 + g3 + g4 & theme(legend.position = "top", text = element_text(size = 20)))
(g = combined + plot_layout(guides = "collect"))

ggsave("attendez-analysis.pdf", g, width = 16, height = 10,device = "pdf")
