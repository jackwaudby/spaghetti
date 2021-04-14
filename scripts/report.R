args = commandArgs(trailingOnly=TRUE)

if (length(args)==1) {
  workload = args[1]
  cat(paste0("generating report for ",workload," workload\n"))
  today = Sys.Date()
  fileName = paste0(today, "-",workload,"-report.pdf")
  Sys.setenv(RSTUDIO_PANDOC = "/Applications/RStudio.app/Contents/MacOS/pandoc")
  rmarkdown::render(input = "report.Rmd",
                    output_file = fileName,
                    params = list(workload = workload),
                    output_dir = "../reports/")
  
} else {
  cat("please supply workload\n")
}


