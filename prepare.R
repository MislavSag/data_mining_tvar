library(data.table)
library(arrow)
library(duckdb)
library(AzureStor)
library(finfeatures)


# SET UP ------------------------------------------------------------------
# Define symbols
symbols = c("spy")

# Globals
get_ohlcv_by_symbols_duckdb = function(path, symbols) {
  con = dbConnect(duckdb::duckdb())
  symbols_string = paste(sprintf("'%s'", symbols), collapse=", ")
  query = sprintf("
  SELECT *
  FROM '%s'
  WHERE Symbol IN (%s)
", path, symbols_string)
  prices_dt = dbGetQuery(con, query)
  dbDisconnect(con, shutdown = TRUE)
  setDT(prices_dt)
  return(prices_dt)
}

# Define paths
PATH = "F:/strategies/tvar"


# DATA --------------------------------------------------------------------
# Predictors
file_ = file.path(PATH, "ohlcv_predictors_sample_by_symbols.parquet")
if (!file.exists(file_)) {
  KEY = Sys.getenv("KEY")
  ENDPOINT = "https://snpmarketdata.blob.core.windows.net/"
  endpoint = storage_endpoint(ENDPOINT, key = KEY)
  cont = storage_container(endpoint, "data")
  download_blob(container = cont,
                src = "ohlcv_predictors_sample_by_symbols.parquet",
                dest = file_
                )
}
predictors = read_parquet(file_)
predictors = predictors[symbol %in% symbols]

# Generate other Ohlcv predictors
# OHLCV Prices
prices = get_ohlcv_by_symbols_duckdb("F:/strategies/momentum/prices.csv", symbols)
ohlcv = Ohlcv$new(prices)
ohlcv_features_daily = OhlcvFeaturesDaily$new(
  at = NULL,
  windows = c(5, 10, 22, 44, 66, 132, 252, 504, 756),
  quantile_divergence_window = c(22, 44, 66, 132, 252, 504, 756)
)
ohlcv_predictors = ohlcv_features_daily$get_ohlcv_features(ohlcv$X)

# Merge prices and all predictors
dt = merge(ohlcv_predictors, predictors, by = c("symbol", "date"), all = TRUE)

# Inspect
dim(dt)
head(colnames(dt), 20)
tail(colnames(dt), 20)

# Remove columns we don't need
cols_remove = c(
  "liquid_500", "liquid_200", "liquid_100", "liquid_50", "open", "high", "low",
  "close", "close_raw", "volume", "returns"
)
dt[, (cols_remove) := NULL]

# Features space from features raw
colnames(dt)
cols_non_features = c("symbol", "date")
cols_features = setdiff(colnames(dt), cols_non_features)
cols = c(cols_non_features, cols_features)

# Convert columns to numeric. This is important only if we import existing features
chr_to_num_cols = setdiff(colnames(dt[, .SD, .SDcols = is.character]), cols_non_features)
if (length(chr_to_num_cols) != 0) {
  dt = dt[, (chr_to_num_cols) := lapply(.SD, as.numeric), .SDcols = chr_to_num_cols]
}
log_to_num_cols = setdiff(colnames(dt[, .SD, .SDcols = is.logical]), cols_non_features)
if (length(log_to_num_cols) != 0) {
  dt = dt[, (log_to_num_cols) := lapply(.SD, as.numeric), .SDcols = log_to_num_cols]
}

# Remove duplicates
any_duplicates = any(duplicated(dt[, .(symbol, date)]))
if (any_duplicates) dt = unique(dt, by = c("symbol", "date"))

# Remove columns with many NA
keep_cols = names(which(colMeans(!is.na(dt)) > 0.70))
print(paste0("Removing columns with many NA values: ", setdiff(colnames(dt),
                                                               keep_cols)))
dt = dt[, .SD, .SDcols = keep_cols]

# Remove Inf and Nan values if they exists
is.infinite.data.frame = function(x) do.call(cbind, lapply(x, is.infinite))
keep_cols = names(which(colMeans(!is.infinite(as.data.frame(dt))) > 0.95))
print(paste0("Removing columns with Inf values: ", setdiff(colnames(dt), keep_cols)))
dt = dt[, .SD, .SDcols = keep_cols]

# Remove inf values
n_0 = nrow(dt)
dt = dt[is.finite(rowSums(dt[, .SD, .SDcols = is.numeric], na.rm = TRUE))]
n_1 = nrow(dt)
print(paste0("Removing ", n_0 - n_1, " rows because of Inf values"))

# Final checks
dt[, max(date)]

# Create padobran.sh file
sh_file = sprintf("
#!/bin/bash

#PBS -N TVAR
#PBS -l ncpus=4
#PBS -l mem=8GB
#PBS -J 3-%d
#PBS -o logs
#PBS -j oe

cd ${PBS_O_WORKDIR}
apptainer run image.sif tvar.R
", ncol(dt))
sh_file_name = "padobran.sh"
file.create(sh_file_name)
writeLines(sh_file, sh_file_name)

# Save data and transfer it to padobran
fwrite(dt, file.path(PATH, "tvar.csv"))
