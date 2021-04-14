today = Sys.Date()
fileName = paste0(today,"-all-report.pdf")
Sys.setenv(RSTUDIO_PANDOC = "/Applications/RStudio.app/Contents/MacOS/pandoc")
rmarkdown::render(input = "report.Rmd",
                    output_file = fileName,
                    output_dir = "../reports/")

