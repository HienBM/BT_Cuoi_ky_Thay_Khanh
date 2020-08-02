
# load packages cần thiết
library(sparklyr)
library(tidyverse)
library(readxl)
library(lubridate)
library(ggplot2)
library(plotly)
library(dplyr)
library(ggthemes)
library(DT)
library(leaflet)
library(rworldmap)
library(shiny)
library(semantic.dashboard)

# Connect to spark
sc <- spark_connect("local")

# Print the version of Spark
spark_version(sc)

# Load dataset Online Sales
sales_transactions <- read_excel("D:/Draft/Online Retail.xlsx")

# view dataset
head(sales_transactions)
glimpse(sales_transactions)
summary(sales_transactions)

# import data into spark dataframe
sales_transactions_tbl <- copy_to(sc, sales_transactions, "sales_transactions", overwrite = TRUE)

# check if the table is available on Spark
src_tbls(sc)

# view spark data
head(sales_transactions_tbl)

# check số lượng record, số lượng hóa đơn, sl hóa đơn ghi không có record, số lượng khách hàng theo ID,
# số lượng khách hàng không có ID
sales_transactions_tbl %>%
  summarise(n_records = n(),
            n_invoices = n_distinct(InvoiceNo),
            n_missing_inv_rec = sum(as.integer(is.na(InvoiceNo)),na.rm = TRUE),
            n_dis_customers_id = n_distinct(CustomerID),
            n_missing_cust_id = sum(as.integer(is.na(CustomerID)),na.rm = TRUE))

# check sl mã kho, sl mã kho bị thiếu, số lượng sản phẩm, giá trị bị thiếu cột số lượng, giá trị bị
# thiếu cột prices
sales_transactions_tbl %>%
  summarise(n_dist_stocks = n_distinct(StockCode),
            n_missing_stocks = sum(as.integer(is.na(StockCode)),na.rm = TRUE),
            n_dist_desc = n_distinct(Description),
            n_missing_desc = sum(as.integer(is.na(Description)),na.rm = TRUE),
            n_missing_quant = sum(as.integer(is.na(Quantity)),na.rm = TRUE),
            n_missing_prices = sum(as.integer(is.na(UnitPrice)),na.rm = TRUE))

# loại bỏ các giá trị null
sales_transactions_tbl_not_null <- sales_transactions %>%
  filter(!is.na(CustomerID), !is.na(Description))

# Biểu đồ cột so sánh doanh thu theo thời gian
dt_theo_ngay <- sales_transactions_tbl_not_null %>%
  mutate(Revenue = round(Quantity * UnitPrice,digits = 0),
         Date = format(as.Date(InvoiceDate),"%Y-%m")) %>%
  filter(Revenue >= 0) %>%
  group_by(Date) %>%
  arrange(Date) %>%
  summarize(Revenue = sum(Revenue)) %>%
  ggplot(aes(x = Date, y = Revenue))+
  geom_bar(stat = "identity", position = "dodge", fill = "#08306b") +
  geom_label(aes(label = Revenue))+
  theme_bw()

ggplotly(dt_theo_ngay)

# Doanh thu theo country
percent_dt_theo_country <- sales_transactions_tbl_not_null %>%
  mutate(Revenue = round(Quantity * UnitPrice, digits = 0),
         Date = format(as.Date(InvoiceDate),"%Y-%m")) %>%
  filter(Revenue >= 0) %>%
  mutate(Percent_ratio = Revenue/sum(Revenue)*100) %>%
  group_by(Country) %>%
  summarize(Percent_ratio = sum(Percent_ratio)) %>%
  ggplot(aes(x = reorder(Country,Percent_ratio), y = Percent_ratio)) +
  geom_bar(stat = "identity", fill = "#08306b", position = "dodge") +
  coord_flip() +
  xlab("% Revenue") +
  ylab("Country") +
  theme_bw()

ggplotly(percent_dt_theo_country)

# Doanh thu theo country (view map)
rev_by_country <- sales_transactions_tbl_not_null %>%
  mutate(Revenue = round(Quantity * UnitPrice, digits = 0)) %>%
  filter(Revenue >= 0) %>%
  group_by(Country) %>%
  summarize(Revenue = sum(Revenue)) %>%
  collect()

sPDF <- joinCountryData2Map(rev_by_country
                            ,joinCode = "NAME"
                            ,nameJoinColumn = "Country", verbose = FALSE)

existing_countries <- subset(sPDF, !is.na(Country))

bins <- c(0, 50000, 100000, 150000, 200000, 250000, 300000, Inf)
## assign a color to each of the classes
pal <- colorBin("YlOrRd", domain = existing_countries$Revenue, bins = bins)

## create labels with actual revenue amounts per country, for hover info
labels <- paste0("<strong>", existing_countries$Country, "</strong><br/>",
                 format(existing_countries$Revenue, digits = 0, big.mark = ".", decimal.mark = ",", scientific = FALSE),
                 " GBP") %>% lapply(htmltools::HTML)

## create the cloropleth map
dt_by_country <- leaflet(existing_countries) %>%
  addTiles() %>%  # Add default OpenStreetMap map tiles
  addPolygons(
    fillColor = ~pal(Revenue),
    weight = 1,
    opacity = 1,
    color = "white",
    dashArray = "3",
    fillOpacity = 0.7,
    highlight = highlightOptions(
      weight = 2,
      color = "#666",
      dashArray = "",
      fillOpacity = 0.7,
      bringToFront = TRUE),
    label = labels,
    labelOptions = labelOptions(
      style = list("font-weight" = "normal", padding = "3px 8px"),
      textsize = "15px",
      direction = "auto")) %>%
  addLegend(pal = pal, values = ~Revenue, opacity = 0.7, title = NULL,
            position = "topright") %>%
  setView(17,34,2)

dt_by_country

# tỉ lệ cancel
ti_le_cancel <- sales_transactions_tbl_not_null %>%
  mutate(Status = ifelse(substr(InvoiceNo,1,1) %in% letters | substr(InvoiceNo,1,1) %in% LETTERS
                         ,"Cancel","Completed")) %>%
  group_by(Status) %>%
  summarize(Volumn = length(unique(InvoiceNo))) %>%
  mutate(Volume_ratio = round(Volumn/sum(Volumn)*100,digits = 0)) %>%
  ggplot(aes(x = Status, y = Volume_ratio)) +
  geom_bar(stat = "identity", position = "dodge", fill = "#08306b") +
  geom_label(aes(label = Volume_ratio)) +
  theme_bw()

ggplotly(ti_le_cancel)

# top 10 item có doanh thu cao nhất
top_10_items_rev <- sales_transactions_tbl_not_null %>%
  group_by(Description) %>%
  summarize(Revenue = sum(Quantity * UnitPrice)) %>%
  arrange(desc(Revenue)) %>%
  top_n(10) %>%
  ggplot(aes(x = reorder(Description, Revenue), y = Revenue)) +
  geom_bar(stat = "identity", position = "dodge", fill = "#08306b") +
  xlab("Description") +
  ylab("Revenue") +
  coord_flip() +
  theme_bw()

top_10_items_rev

# top 10 item bán nhiều nhất
top_10_items_volumn <- sales_transactions_tbl_not_null %>%
  filter(Quantity >= 0) %>%
  group_by(Description) %>%
  summarize(Quantity = sum(Quantity)) %>%
  arrange(desc(Quantity)) %>%
  top_n(10) %>%
  ggplot(aes(x = reorder(Description, Quantity), y = Quantity)) +
  geom_bar(stat = "identity", position = "dodge", fill = "#08306b") +
  xlab("Description") +
  ylab("Quantity") +
  coord_flip() +
  theme_bw()

top_10_items_volumn

# Import to shiny Dashboard
ui <- dashboardPage(
  dashboardHeader(color = "blue",title = "Dashboard Demo", inverted = TRUE),
  dashboardSidebar(
    size = "thin", color = "teal",
    sidebarMenu(
      menuItem(tabName = "main", "Top 10 items", icon = icon("table")),
      menuItem(tabName = "extra", "Revenue theo Country", icon = icon("table")),
      menuItem(tabName = "extra1", "Revenue theo tháng", icon = icon("table")),
      menuItem(tabName = "extra2", "% Return & % Revenue", icon = icon("table"))
    )
  ),
  dashboardBody(
    tabItems(
      selected = 1,
      tabItem(
        tabName = "main",
        fluidRow(
          box(width = 8,
              title = "Top 10 items by Revenue",
              color = "green", ribbon = TRUE, title_side = "top left",
              column(width = 8,
                     plotlyOutput("plot1", height = 500)
              )
          ),
          box(width = 8,
              title = "Top 10 items by Volume",
              color = "red", ribbon = TRUE, title_side = "top left",
              column(width = 8,
                     plotlyOutput("plot2", height = 500)
              )
          )
        )
      ),
      tabItem(
        tabName = "extra",
        fluidRow(
          box(width = 16,
              title = "Revenue by Country",
              color = "green", ribbon = TRUE, title_side = "top left",
              column(width = 16,
                     leafletOutput("plot3", height = 600))
              )
        )
      ),
      tabItem(
        tabName = "extra1",
        fluidRow(
          box(width = 16,
              title = "Revenue theo tháng",
              color = "green", ribbon = TRUE, title_side = "top left",
              column(width = 16,
                     plotlyOutput("plot4", height = 500))
              )
        )
      ),
      tabItem(
        tabName = "extra2",
        fluidRow(
          box(width = 10,
              title = "% Revenue by Country",
              color = "green", ribbon = TRUE, title_side = "top left",
              column(width = 10,
                     plotlyOutput("plot5", height = 600)
                     )
          ),
          box(width = 6,
              title = "% Return",
              color = "green", ribbon = TRUE, title_side = "top left",
              column(width = 6,
                     plotlyOutput("plot6", height = 600)
                     )
              )
        )
      )
    )
  )
)

server <- shinyServer(function(input, output, session) {

  output$plot1 <- renderPlotly({
    top_10_items_rev
  })

  output$plot2 <- renderPlotly({
    top_10_items_volumn
  })
  output$plot3 <- renderLeaflet({
    dt_by_country
  })
  output$plot4 <- renderPlotly({
    dt_theo_ngay
  })
  output$plot5 <- renderPlotly({
    percent_dt_theo_country
  })
  output$plot6 <- renderPlotly({
    ti_le_cancel
  })
})

# shinyApp(ui, server)


# spark_disconnect(sc)
