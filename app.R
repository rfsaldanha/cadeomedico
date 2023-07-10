library(shinydashboard)
library(tidyverse)
library(DBI)
library(DT)
options(DT.options = list(
  language = list(url = '//cdn.datatables.net/plug-ins/1.10.19/i18n/Portuguese-Brasil.json', decimal = ",")
))
library(glue)

# tabMun
load("data/tabMun.RData")

# Connection to database
conn <- dbConnect(RSQLite::SQLite(), "data/cnes.db")

# Tables
st_data <- tbl(conn, "st_data")
pf_data <- tbl(conn, "pf_data")

ui <- dashboardPage(
  skin = "green",
  dashboardHeader(title = "Quem trabalha aqui?"),
  dashboardSidebar(disable = TRUE),
  dashboardBody(
    fluidRow(
      box(
        status = "primary", solidHeader = TRUE, collapsible = TRUE, width = 12,
        title = "Onde você está?",
        uiOutput("estado"),
        uiOutput("municipio"),
        uiOutput("unidade_saude")
      ),
      box(
        status = "primary", solidHeader = TRUE, collapsible = TRUE, width = 12,
        title = "Sobre a unidade de saúde",
        uiOutput("tp_unid"),
        uiOutput("tpgestao"),
        uiOutput("nat_jur"),
        uiOutput("turno_at"),
        uiOutput("qtleitp1"),
        uiOutput("qtleitp2"),
        uiOutput("qtleitp3")
      ),
      box(
        status = "primary", solidHeader = TRUE, collapsible = TRUE, width = 12,
        title = "Quem trabalha aqui?",
        uiOutput("profissao"),
        dataTableOutput("profissionais"),
        p("CH: Carga horária semanal")
      ),
      box(
        status = "warning", solidHeader = TRUE, collapsible = TRUE, width = 12,
        title = "Sobre os dados",
        p("Competência: maio de 2023."),
        p(),
        p("Os dados utilizados neste projetos são do Cadastro Nacional de Estabelecimentos de Saúde (CNES) do DataSUS / Ministério da Saúde."),
        p("São exibidos apenas as unidades de saúde com algum vínculo com o Sistema Único de Saúde (SUS)."),
        p("É responsabilidade do estabelecimento vincular os profissionais que fazem parte do corpo clínico, sócios e colaboradores."),
        p("Os dados estão errados? Peça para a unidade atualizar o seu cadastro no CNES."),
        p("Dúvidas? Mande um e-mail para raphael.saldanha@icict.fiocruz.br")
      )
    )
  )
)

server <- function(input, output) {
  
  output$estado <- renderUI({
    uf <- tabMun %>%
      filter(munResStatus != "IGNOR") %>%
      distinct(munResUf) %>%
      arrange(munResUf)
    
    uf <- setNames(uf$munResUf, uf$munResUf)
    
    selectInput("estado", label = "Estado", choices = uf)
  })
  
  output$municipio <- renderUI({
    req(input$estado)
    
    municipio <- tabMun %>%
      filter(munResUf == !!input$estado) %>%
      filter(munResStatus != "IGNOR") %>%
      select(munResCod, munResNome)
    
    municipio <- setNames(municipio$munResCod, municipio$munResNome)
    
    selectInput("municipio", label = "Município", choices = municipio)
  })
  
  output$unidade_saude <- renderUI({
    req(input$municipio)
    
    st <- st_data %>%
      filter(CODUFMUN == !!input$municipio) %>%
      select(CNES, FANTASIA) %>%
      arrange(FANTASIA) %>%
      as_tibble() %>%
      na.omit()
    
    st <- setNames(st$CNES, st$FANTASIA)
    
    selectInput("unidade_saude", label = "Unidade de saúde", choices = st)
  })
  
  unidade_saude_df <- reactive({
    req(input$unidade_saude)
    
    st_data %>%
      filter(CNES == !!input$unidade_saude) %>%
      select(TP_UNID, TPGESTAO, NAT_JUR, TURNO_AT, QTLEITP1, QTLEITP2, QTLEITP3) %>%
      as_tibble()
  })
  
  output$tp_unid <- renderText({
    glue("<b>Tipo</b>: {unidade_saude_df()$TP_UNID}")
  })
  
  output$tpgestao <- renderText({
    glue("<b>Gestão</b>: {unidade_saude_df()$TPGESTAO}")
  })
  
  output$nat_jur <- renderText({
    glue("<b>Natureza jurídica</b>: {unidade_saude_df()$NAT_JUR}")
  })
  
  output$turno_at <- renderText({
    glue("<b>Atendimento</b>: {unidade_saude_df()$TURNO_AT}")
  })
  
  output$qtleitp1 <- renderText({
    glue("<b>Quantidade de leitos cirúrgicos existentes</b>: {unidade_saude_df()$QTLEITP1}")
  })
  
  output$qtleitp2 <- renderText({
    glue("<b>Quantidade de leitos clínicos existentes</b>: {unidade_saude_df()$QTLEITP2}")
  })
  
  output$qtleitp3 <- renderText({
    glue("<b>Quantidade de leitos complementares existentes</b>: {unidade_saude_df()$QTLEITP3}")
  })
  
  
  
  profissionais_df <- reactive({
    req(input$unidade_saude)
    
    pf_data %>%
      filter(CNES == !!input$unidade_saude) %>%
      select(OCUPACAO, NOMEPROF, DS_VINC, HORAHOSP, HORA_AMB, HORAOUTR, TURNO_AT) %>%
      arrange(OCUPACAO, NOMEPROF) %>%
      as_tibble()
  })
  
  output$profissao <- renderUI({
    prof <- profissionais_df() %>%
      select(OCUPACAO) %>%
      distinct(OCUPACAO) %>%
      arrange(OCUPACAO) %>%
      as_tibble()
    
    prof <- setNames(prof$OCUPACAO, prof$OCUPACAO)
    
    selectInput("profissao", label = "Ocupação", choices = prof, multiple = FALSE)
  })
  
  output$profissionais <- renderDataTable({
    req(input$profissao)
    
    profissionais_df() %>%
      filter(OCUPACAO %in% !!input$profissao) %>%
      select("Nome" = NOMEPROF,
             "Vínculo" = DS_VINC, 
             "CH Hospital" = HORAHOSP, 
             "CH Ambulatório" = HORA_AMB,
             "CH Outros" = HORAOUTR,
             "Turno" = TURNO_AT)
  }, options = list(scrollX = TRUE))
  
}

shinyApp(ui, server)