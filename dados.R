# Packages
library(tidyverse)
library(microdatasus)
library(RSQLite)
library(DBI)

# Download reference tables
ref_url <- "https://datasus-ftp-mirror.nyc3.cdn.digitaloceanspaces.com/CNES/200508_/Auxiliar/TAB_CNES.zip"
temp_file <- tempfile()
temp_dir <- tempdir()
download.file(url = ref_url, destfile = temp_file, mode = "wb")
unzip(zipfile = temp_file, exdir = temp_dir)

# Reference tables
vinc <- foreign::read.dbf(file = paste0(temp_dir, "/DBF/VINCULO.dbf"), as.is = TRUE) %>%
  mutate(CHAVE = as.character(as.numeric(CHAVE))) %>%
  mutate(DS_REGRA = str_remove_all(DS_REGRA, "[:digit:]")) %>%
  mutate(DS_REGRA = str_remove_all(DS_REGRA, "/")) %>%
  mutate(DS_REGRA = str_squish(DS_REGRA))

cbo <- foreign::read.dbf(file = paste0(temp_dir, "/DBF/CBO.dbf"), as.is = TRUE)
cadger <- foreign::read.dbf(file = paste0(temp_dir, "/DBF/CADGERBR.dbf"), as.is = TRUE) %>%
  select(CNES, FANTASIA, RSOC_MAN)

# Download data
options(timeout=500)

cnes_st <- fetch_datasus(year_start = 2024, month_start = 9, year_end = 2024, month_end = 9, information_system = "CNES-ST")
saveRDS(cnes_st, "raw_data/cnes_st.rds")

cnes_pf <- fetch_datasus(year_start = 2024, month_start = 9, year_end = 2024, month_end = 9, information_system = "CNES-PF")
saveRDS(cnes_pf, "raw_data/cnes_pf.rds")

# Pre-process data
st <- cnes_st %>%
  select(CODUFMUN, CNES, PF_PJ, VINC_SUS, TP_UNID, TPGESTAO, NAT_JUR, TURNO_AT, QTLEITP1, QTLEITP2, QTLEITP3) %>%
  process_cnes(., "CNES-ST", nomes = FALSE) %>%
  left_join(cadger, by =  "CNES") %>%
  filter(PF_PJ == "Pessoa jurídica") %>%
  filter(VINC_SUS == "Sim") %>%
  select(CODUFMUN, CNES, FANTASIA, TP_UNID, TPGESTAO, NAT_JUR, TURNO_AT, QTLEITP1, QTLEITP2, QTLEITP3)


pf <- cnes_pf %>%
  select(CNES, NOMEPROF, HORAHOSP, HORA_AMB, HORAOUTR, TURNO_AT, CBO, VINCULAC) %>%
  process_cnes(., "CNES-PF", nomes = FALSE) %>%
  left_join(cbo, by = c("CBO")) %>%
  rename(OCUPACAO = DS_CBO) %>%
  left_join(vinc, by = c("VINCULAC" = "CHAVE")) %>%
  rename(DS_VINC = DS_REGRA) %>%
  select(CNES, OCUPACAO, NOMEPROF, DS_VINC, HORAHOSP, HORA_AMB, HORAOUTR, TURNO_AT)

conn <- dbConnect(RSQLite::SQLite(), "../cadeomedico/data/cnes.db")
dbWriteTable(conn, "st_data", st, overwrite = TRUE)
dbWriteTable(conn, "pf_data", pf, overwrite = TRUE)
dbDisconnect(conn)



