library(shiny)
#library('SparkR')
library(ggplot2)
library(dplyr)
library(sequila)
#Sys.setenv(SPARK_HOME = "/usr/local/spark")
#Sys.setenv(SPARK_HOME = "/usr/local/spark")
#library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
#sc <- sparkR.session(master="local", sparkJars = "/home/kacper/bdg-sequila/target/scala-2.11/bdg-sequila_2.11-0.4-SNAPSHOT.jar")
#sc <- sparkR.session(master="local")
#df<-as.data.frame(read.parquet( "coverage_dataset.parquet"))
#df<-dfConst

#ss <- sequila_connect("local[1]")

#sequila_sql(ss,'reads',paste("CREATE TABLE IF NOT EXISTS reads USING org.biodatageeks.datasources.BAM.BAMDataSource OPTIONS(path '/tmp/NA12878.slice.bam')"))

#df<-sequila_sql(ss, 'coverage', "SELECT * FROM coverage_hist('reads')") %>% as.data.frame()
#sequila_disconnect(ss)

ui <- fluidPage(

  tags$style(type="text/css",
                 ".shiny-output-error { visibility: hidden; }",
                 ".shiny-output-error:before { visibility: hidden; }"
  ),
  titlePanel("bdg-sequila-shiny"),
  sidebarLayout(
    
    # Sidebar panel for inputs ----
    sidebarPanel(
      
	

      #fileInput("file1", "Choose BAM File",multiple=FALSE,
      #          accept = c(                           ".bam")),
      #textOutput("txt"),
      
      actionButton("go", "Go"),
      textInput("txt","Path to bam file","/tmp/NA12878.slice.bam"),
      
      
      # Input: Slider for the number of bins ----
      sliderInput(inputId = "bins",
                  label = "Number of bins:",
                  min = 1,
                  max = 50,
                  value = 30),
      
      selectInput("mapQSize", "MapQ buckets size:",
                  c(1,2,4,8,16,32,64,128),selected = 32),
      
      uiOutput("sliderRange"),
      uiOutput("detailedCoverage")
      
    ),
    
    # Main panel for displaying outputs ----
    mainPanel(
      
      # Output: Histogram ----
      plotOutput(outputId = "covTotalPlot",click = "plot_click"),
      
      plotOutput(outputId = "covTotalPositionPlot",click = "plot_click"),
      
      plotOutput(outputId = "covDetailedPlot",click = "plot_click")
    )
  )
)

server <- function(input, output,session) {
  
  ##observeEvent(input$go, {
  
  ##ss <- sequila_connect("local[1]")
  
  ##sequila_sql(ss,'reads',paste("CREATE TABLE IF NOT EXISTS reads USING org.biodatageeks.datasources.BAM.BAMDataSource OPTIONS(path '/tmp/NA12878.slice.bam')"))
  
  ##df <<- sequila_sql(ss, 'coverage', "SELECT * FROM coverage_hist('reads')") %>% collect() %>% as.data.frame()
  
  ##output$sliderRange <- renderUI({
  ##  sliderInput(inputId ="sliderRange", label="Position range",
  ##              min = min(df$position), max = max(df$position), value = c(quantile(df$position, 0.1),quantile(df$position, 0.75)))
  ##})
  
  ##})
  
  df <- eventReactive(input$go, {
    
    ss <- sequila_connect("local[1]")
    
    print(input$txt)
    print(input$mapQSize)
    print(paste("CREATE TABLE IF NOT EXISTS reads USING org.biodatageeks.datasources.BAM.BAMDataSource OPTIONS(path '",input$txt,"')", sep = ""))
    sequila_sql(ss,'reads',"DROP TABLE IF EXISTS reads")
    sequila_sql(ss,'reads',paste("CREATE TABLE IF NOT EXISTS reads USING org.biodatageeks.datasources.BAM.BAMDataSource OPTIONS(path '",input$txt,"')", sep = ""))
    
    sequila_sql(ss, 'coverage', paste("SELECT * FROM coverage_hist('reads',",input$mapQSize,")", sep = "")) %>% collect() %>% as.data.frame()
    
  })
  
  output$sliderRange <- renderUI({
    sliderInput(inputId ="sliderRange", label="Position range",
                min = min(df()$position), max = max(df()$position), value = c(quantile(df()$position, 0.1),quantile(df()$position, 0.75)))
  })
  
  output$detailedCoverage <- renderUI({
    sliderInput(inputId ="detailedCoverage", label=paste("Coverage for position: ",quantile(df()$position, 0.5)),
                min = min(df()$position), max = max(df()$position), value = quantile(df()$position, 0.3))
  })
  
  # observeEvent(input$file, {
  # inFile <- parseFilePaths(roots=c(wd='.'), input$file)
  #ss <- sequila_connect("local[1]")
  
  #sequila_sql(ss,'reads',paste("CREATE TABLE IF NOT EXISTS reads USING org.biodatageeks.datasources.BAM.BAMDataSource OPTIONS(path '",inFile,"')"))
  
  #df<-sequila_sql(ss, 'coverage', "SELECT * FROM coverage_hist('reads')") %>% collect()%>% mutate(coverage=lapply(strsplit(coverage, split=","),as.numeric)) %>% as.data.frame()
  #sequila_disconnect(ss)
  #})
  
  output$covTotalPlot <- renderPlot({
    
    #x    <- tbl_df(faithful$waiting) %>% filter(val>input$sliderRange[1], val <input$sliderRange[2])
    dfCovTotalFiltered <- df()[df()$position>input$sliderRange[1] & df()$position<input$sliderRange[2],] 
    x    <-   dfCovTotalFiltered$coverageTotal
    bins <- seq(min(x), max(x), length.out = input$bins + 1)
    
    ggplot(data=dfCovTotalFiltered, aes(dfCovTotalFiltered$coverageTotal)) + geom_histogram(fill="cornflowerblue",bins=input$bins)
  })
  
  observe({
    updateSliderInput(session, "detailedCoverage", label = paste("Coverage for position: ", input$detailedCoverage))
  })
  
  output$covDetailedPlot <- renderPlot({
    
    dfCovDetailedInitFiltered <- df()[df()$position<input$detailedCoverage[1]+300000 & df()$position>input$detailedCoverage[1]-300000 ,] 
    dfCovDetailedFiltered <- dfCovDetailedInitFiltered %>% mutate(closestRows=-abs(dfCovDetailedInitFiltered$position-input$detailedCoverage[1])) %>% top_n(8)
    #dfBucketHist<-data.frame(bucket=rep(c(1:4)),coverage=unlist(dfCovDetailedFiltered$coverage),position=rep(dfCovDetailedFiltered$position,each=4))
    covLength <- length(dfCovDetailedFiltered$coverage[[1]])
    dfBucketHist<-data.frame(bucket=rep(c(1:covLength)),coverage=unlist(dfCovDetailedFiltered$coverage),position=rep(dfCovDetailedFiltered$position,each=covLength))
    ggplot(data=dfBucketHist, aes(bucket,coverage)) + geom_bar(stat="identity",fill="cornflowerblue") + facet_wrap( ~ position, nrow=3)
  })
  
  output$covTotalPositionPlot <- renderPlot({
    dfCovTotalFiltered <- df()[df()$position>input$sliderRange[1] & df()$position<input$sliderRange[2],]
    ggplot(data=dfCovTotalFiltered, aes(position,coverageTotal)) + geom_line(color="cornflowerblue")
  })
  
}

shinyApp(ui = ui, server = server)
