# Packages
library(dplyr)
library(tibble)
library(stringr)
library(lubridate)
library(curl)
library(fs)
library(read.dbc)
library(microdatasus)
library(RSQLite)
library(DBI)

s3_base <- "https://datasus-ftp-mirror.nyc3.cdn.digitaloceanspaces.com"

year_query <- "24"
month_query <- "09"

# Download reference tables
ref_url <- path(s3_base, "/CNES/200508_/Auxiliar/TAB_CNES.zip")
temp_file <- file_temp()
temp_dir <- path_temp()
curl_download(url = ref_url, destfile = temp_file, mode = "wb", quiet = FALSE)

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

# Download CNES data
options(timeout=500)
ufs <- c("AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT","MS","MG","PA","PB","PR","PE","PI","RJ","RN","RS","RO","RR","SC","SP","SE","TO")

st_files <- paste0("ST", ufs, year_query, month_query, ".dbc")
pf_files <- paste0("PF", ufs, year_query, month_query, ".dbc")

res_st <- multi_download(
  urls = path(s3_base, "/CNES/200508_/Dados/ST/", st_files), 
  destfiles = path(temp_dir, st_files), 
  progress = TRUE
)

res_pf <- multi_download(
  urls = path(s3_base, "/CNES/200508_/Dados/PF/", pf_files), 
  destfiles = path(temp_dir, pf_files), 
  progress = TRUE
)

# Read CNES data
cnes_st <- tibble()
for(f in res_st$destfile){
  tmp_st <- read.dbc(f, as.is = TRUE)
  cnes_st <- bind_rows(cnes_st, tmp_st)
  rm(tmp_st)
}
rm(f)

cnes_pf <- tibble()
for(f in res_pf$destfile){
  tmp_pf <- read.dbc(f, as.is = TRUE)
  cnes_pf <- bind_rows(cnes_pf, tmp_pf)
  rm(tmp_pf)
}
rm(f)


# Save raw data
saveRDS(cnes_st, "raw_data/cnes_st.rds")
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



