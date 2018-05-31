sink(stderr(), type = "output")
sink(stderr(), type = "message")
library(shiny)
cat(file=stderr(),"library(shiny)")
#library('SparkR')
library(ggplot2)
cat(file=stderr(),"library(ggplot2)")
library(dplyr)
cat(file=stderr(),"library(dplyr)")
library(sparklyr)
cat(file=stderr(),"library(sparklyr)")
Sys.setenv(SPARK_HOME = '/root/spark/spark-2.2.1-bin-hadoop2.7')
print(spark_installed_versions())
library(sequila)
cat(file=stderr(),"library(sequila)")
#Sys.setenv(SPARK_HOME = "/usr/local/spark")
#Sys.setenv(SPARK_HOME = "/usr/local/spark")
#library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
#sc <- sparkR.session(master="local", sparkJars = "/home/kacper/bdg-sequila/target/scala-2.11/bdg-sequila_2.11-0.4-SNAPSHOT.jar")
#sc <- sparkR.session(master="local")
#df<-as.data.frame(read.parquet( "coverage_dataset.parquet"))
#df<-dfConst

ss <- sequila_connect("local[1]")
cat(file=stderr(),"ss <- sequila_connect('local[1]')")
sequila_sql(ss,'reads',paste("CREATE TABLE IF NOT EXISTS reads USING org.biodatageeks.datasources.BAM.BAMDataSource OPTIONS(path '/tmp/NA12878.slice.bam')"))
cat(file=stderr(),"sequila_sql")
df<-sequila_sql(ss, 'coverage', "SELECT * FROM coverage_hist('reads')") %>% as.data.frame()
#sequila_disconnect(ss)
cat(file=stderr(),"df<-sequila_sql")




ui <- fluidPage(
  
  # App title ----
  titlePanel("Hello Shiny!"),
  
  # Sidebar layout with input and output definitions ----
  sidebarLayout(
    
    # Sidebar panel for inputs ----
    sidebarPanel(
      
      #fileInput("file1", "Choose BAM File",multiple=FALSE,
      #          accept = c(                           ".bam")),
      textOutput("txt"),
      
      
      
      # Input: Slider for the number of bins ----
      sliderInput(inputId = "bins",
                  label = "Number of bins:",
                  min = 1,
                  max = 50,
                  value = 30),
      sliderInput(inputId ="sliderRange", label="Position range",
                  min = min(df$position), max = max(df$position), value = c(quantile(df$position, 0.1),quantile(df$position, 0.75)))
      
      
    ),
    
    # Main panel for displaying outputs ----
    mainPanel(
      
      # Output: Histogram ----
      plotOutput(outputId = "covTotalPlot",click = "plot_click"),
      verbatimTextOutput("info"),
      
      textOutput("infoTest"),
      
      plotOutput(outputId = "covDetailedPlot",click = "plot_click")
    )
  )
)

server <- function(input, output,session) {
  
  #output$ui <- renderUI({
  #  if (is.null(input$file))
  #    return()
  #  sliderInput(inputId ="sliderRange", label="Position range",
  #              min = min(df$position), max = max(df$position), value = c(quantile(df$position, 0.1),quantile(df$position, 0.75)))
    
  #})

  
 # observeEvent(input$file, {
   # inFile <- parseFilePaths(roots=c(wd='.'), input$file)
    #ss <- sequila_connect("local[1]")
    
    #sequila_sql(ss,'reads',paste("CREATE TABLE IF NOT EXISTS reads USING org.biodatageeks.datasources.BAM.BAMDataSource OPTIONS(path '",inFile,"')"))
    
    #df<-sequila_sql(ss, 'coverage', "SELECT * FROM coverage_hist('reads')") %>% collect()%>% mutate(coverage=lapply(strsplit(coverage, split=","),as.numeric)) %>% as.data.frame()
    #sequila_disconnect(ss)
  #})
  
  output$covTotalPlot <- renderPlot({
    
    req(df)
    
    #ss <- sequila_connect("local[1]")
    
    #sequila_sql(ss,'reads',paste("CREATE TABLE IF NOT EXISTS reads USING org.biodatageeks.datasources.BAM.BAMDataSource OPTIONS(path '",input$file1$datapath,"')"))
    
    #df<-sequila_sql(ss, 'coverage', "SELECT * FROM coverage_hist('reads')") %>% collect()%>% mutate(coverage=lapply(strsplit(coverage, split=","),as.numeric)) %>% as.data.frame()
    #sequila_disconnect(ss)
    
    #x    <- tbl_df(faithful$waiting) %>% filter(val>input$sliderRange[1], val <input$sliderRange[2])
    dfCovTotalFiltered <- df[df$position>input$sliderRange[1] & df$position<input$sliderRange[2],] 
    x    <-   dfCovTotalFiltered$coverageTotal
    bins <- seq(min(x), max(x), length.out = input$bins + 1)
    
    ggplot(data=dfCovTotalFiltered, aes(dfCovTotalFiltered$coverageTotal)) + geom_histogram(fill="cornflowerblue",bins=input$bins)
    #ggplot(data=faithful, aes(x)) + geom_histogram(fill="cornflowerblue",bins=input$bins)
    #hist(x, breaks = bins, col = "#75AADB", border = "white",
    #     xlab = "Waiting time to next eruption (in mins)",
    #    main = "Histogram of waiting times")
  })
  
  output$info <- renderPrint({
    req(df)
    # With base graphics, need to tell it what the x and y variables are.
    nearPoints(df, input$plot_click, xvar="coverageTotal", yvar="count")
    # nearPoints() also works with hover and dblclick events
  })
  
  output$infoTest <- renderText({ 
    paste("You have selected this: " , input$sliderRange[1], " - ",  input$sliderRange[2])
    paste("You have selected this@@@@@@@@: " , input$detailedCoverage)
  })

  
  output$covDetailedPlot <- renderPlot({
    req(df)
    #x    <- tbl_df(faithful$waiting) %>% filter(val>input$sliderRange[1], val <input$sliderRange[2])
    dfCovDetailedFiltered <- df[df$position>input$sliderRange[1] & df$position<input$sliderRange[2],] 
    x    <-   dfCovDetailedFiltered
    bins <- seq(min(x), max(x), length.out = input$bins + 1)
    
    ggplot(data=dfCovDetailedFiltered, aes(dfCovDetailedFiltered$coverageTotal)) + geom_histogram(fill="cornflowerblue",bins=input$bins)
    #ggplot(data=faithful, aes(x)) + geom_histogram(fill="cornflowerblue",bins=input$bins)
    #hist(x, breaks = bins, col = "#75AADB", border = "white",
    #     xlab = "Waiting time to next eruption (in mins)",
    #    main = "Histogram of waiting times")
  })
  
}

shinyApp(ui = ui, server = server)
