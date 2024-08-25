library(data.table)
library(parallel)
library(runner)
library(tsDyn)


# PREPARE DATA ------------------------------------------------------------
print("Prepare data")

# read predictors
if (interactive()) {
  library(fs)
  pead_file_local = list.files("F:/predictors/spyml_dt", full.names = TRUE)
  dates = as.Date(gsub(".*-", "", path_ext_remove(path_file(pead_file_local))),
                  format = "%Y%m%d")
  DT = fread(pead_file_local[which.max(dates)])
} else {
  DT = fread("spyml-predictors-20240129.csv")
}

# Define predictors
cols_non_features = c("date", "open", "high", "low", "close", "volume")
targets = c("targetRet1", "targetRet6", "targetRet12")
cols_features = setdiff(colnames(DT), c(cols_non_features, targets))

# Convert columns to numeric. This is important only if we import existing features
chr_to_num_cols = setdiff(colnames(DT[, .SD, .SDcols = is.character]), c("symbol"))
print(chr_to_num_cols)
if (length(chr_to_num_cols) > 0) {
  DT[, (chr_to_num_cols) := lapply(.SD, as.numeric), .SDcols = chr_to_num_cols]
}
# Remove observations with missing target
# Probably different for LIVE
DT = na.omit(DT, cols = targets)

# Get index
if (interactive()) {
  print(paste0("Number of indecies: ", length(cols_features)))
  i = 100
} else {
  length(cols_features)
  i = as.integer(Sys.getenv('PBS_ARRAY_INDEX'))
}


# TVAR --------------------------------------------------------------------
# prepare data
var =  cols_features[i]
cols = c("date", "returns1", var)
X_vars = as.data.frame(na.omit(DT[, ..cols]))
# window_lengths = c(7 * 22 * 2, 7 * 22 * 6, 7 * 22 * 12, 7 * 22 * 24)
window_lengths = c(7 * 22 * 2, 7 * 22 * 6, 7 * 22 * 12)

# Prepare cluster
cl = makeCluster(8)
clusterExport(cl, "X_vars", envir = environment())
clusterEvalQ(cl, {library(tsDyn)})

# Rolling TVAR
roll_preds = lapply(window_lengths, function(x) {
  runner(
    x = X_vars,
    f = function(x) {
      # debug
      # x = X_vars[1:500, ]

      # # TVAR (1)
      tv1 = tryCatch(
        TVAR(
          data = x[, 2:3],
          lag = 3,
          # Number of lags to include in each regime
          model = "TAR",
          # Whether the transition variable is taken in levels (TAR) or difference (MTAR)
          nthresh = 2,
          # Number of thresholds
          thDelay = 1,
          # 'time delay' for the threshold variable
          trim = 0.05,
          # trimming parameter indicating the minimal percentage of observations in each regime
          mTh = 2,
          # ombination of variables with same lag order for the transition variable. Either a single value (indicating which variable to take) or a combination
          plot = FALSE
        ),
        error = function(e)
          NULL
      )
      if (is.null(tv1)) {
        tv1_pred_onestep = NA
      } else {
        # tv1$coeffmat
        tv1_pred = predict(tv1)[, 1]
        names(tv1_pred) = paste0("predictions_", 1:5)
        thresholds = tv1$model.specific$Thresh
        names(thresholds) = paste0("threshold_", seq_along(thresholds))
        coef_1 = tv1$coefficients$Bdown[1, ]
        names(coef_1) = paste0(names(coef_1), "_bdown")
        coef_2 = tv1$coefficients$Bmiddle[1, ]
        names(coef_2) = paste0(names(coef_2), "_bmiddle")
        coef_3 = tv1$coefficients$Bup[1, ]
        names(coef_3) = paste0(names(coef_3), "_bup")
        cbind.data.frame(date = tail(x$date, 1),
                         as.data.frame(as.list(thresholds)),
                         as.data.frame(as.list(tv1_pred)),
                         data.frame(aic = AIC(tv1)),
                         data.frame(bic = BIC(tv1)),
                         data.frame(loglik = logLik(tv1)),
                         as.data.frame(as.list(coef_1)),
                         as.data.frame(as.list(coef_2)),
                         as.data.frame(as.list(coef_3)))
      }
    },
    k = x,
    lag = 0L,
    cl = cl,
    at = 4000:4010,
    na_pad = TRUE
  )
})
stopCluster(cl)
gc()

# save results
file_name = paste0(var, ".rds")
dir_name = "tvar_results"
if (!dir.exists(dir_name)) {
  dir.create(dir_name)
}
file_name = file.path(dir_name, file_name)
saveRDS(roll_preds, file_name)
