args = commandArgs(trailingOnly = TRUE)

# test if there is at least one argument: if not, return an error
if (length(args) < 2) {
  stop("Two arguments must be supplied: <protocol> <workload>", call.=FALSE)
}

protocol = args[1]
workload = args[2]

fileName = paste0(protocol,"-",workload,"-report.pdf")

Sys.setenv(RSTUDIO_PANDOC="/Applications/RStudio.app/Contents/MacOS/pandoc")

rmarkdown::render(input = "report.Rmd",
                  output_file = fileName,
                  output_dir = "../",
                  params = list(protocol=protocol,workload=workload))