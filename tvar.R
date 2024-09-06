library(data.table)
library(parallel)
library(runner)
library(tsDyn)



# IMPORT DATA -------------------------------------------------------------
# Define file name
if (interactive()) {
  file_ = file.path("F:/strategies/tvar", "tvar.csv")
} else {
  file_ = "tvar.csv"
}

# Get index
if (interactive()) {
  i = 3
} else {
  i = as.integer(Sys.getenv('PBS_ARRAY_INDEX'))
}

# Read predictors
if (interactive()) {
  PATH = "F:/strategies/tvar"
  DT = fread(file.path(PATH, "tvar.csv"), select = c(2, 11, i))
} else {
  DT = fread("tvar.csv", select = c(2, 11, i))
}


# TVAR --------------------------------------------------------------------
# prepare data
DT = as.data.frame(na.omit(DT))
window_lengths = c(22 * 6, 252, 252 * 2)

# Prepare cluster
cl = makeCluster(8)
clusterExport(cl, "DT", envir = environment())
clusterEvalQ(cl, {library(tsDyn)})

# Rolling TVAR
roll_preds = lapply(window_lengths, function(x) {
  runner(
    x = DT,
    f = function(x) {
      # debug
      # x = DT[1:500, ]

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
          # combination of variables with same lag order for the transition variable. Either a single value (indicating which variable to take) or a combination
          plot = FALSE
        ),
        error = function(e)
          NULL
      )
      if (is.null(tv1)) {
        tv1_pred_onestep = NA
      } else {
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
    # at = 1000:1010,
    na_pad = TRUE,
    simplify = FALSE
  )
})
stopCluster(cl)
gc()

# Clean data
roll_preds = lapply(roll_preds, function(x) {
  x[sapply(x, function(y) !all(is.na(y)))]
})
roll_preds = lapply(roll_preds, function(x) rbindlist(x))
roll_preds = lapply(seq_along(window_lengths), function(i) {
  cbind(window = window_lengths[[i]], roll_preds[[i]])
})
roll_preds = rbindlist(roll_preds)

# save results
file_name = paste0(colnames(DT)[3], ".rds")
dir_name = "tvar_results"
if (!dir.exists(dir_name)) {
  dir.create(dir_name)
}
file_name = file.path(dir_name, file_name)
saveRDS(roll_preds, file_name)
