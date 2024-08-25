# read results
list.files("D:/features", pattern = "exuberv3_threshold2_kurtosis_bsadf_log_", full.names = TRUE)
list.files("D:/features", pattern = "exuberv3_threshold2_sd", full.names = TRUE)
# roll_preds <- readRDS("D:/features/exuberv3_threshold2_kurtosis_bsadf_log_20230711074304.rds")
# roll_preds <- readRDS("D:/features/exuberv3_threshold2_sd_radf_sum_20230804080720.rds")
length(roll_preds)
roll_preds[[1]][[2000]]
varvar = "sd_radf_sum"

# extract info from object
roll_results <- lapply(roll_preds, function(x) lapply(x, as.data.table))
roll_results <- lapply(roll_results, rbindlist, fill = TRUE)
roll_results[[1]]
cols = c("date", "returns", varvar)
tvar_res <- lapply(roll_results, function(x){
  cbind(backtest_data[1:nrow(x), ..cols], x)
})

# visualize
ggplot(tvar_res[[1]], aes(date)) +
  geom_line(aes(y = threshold_1), color = "green") +
  geom_line(aes(y = threshold_2), color = "red") +
  geom_line(aes(y = kurtosis_bsadf_log ))
ggplot(tvar_res[[1]], aes(date)) +
  geom_line(aes(y = bic), color = "green")
ggplot(tvar_res[[1]][date %between% c("2020-01-01", "2022-10-01")], aes(date)) +
  geom_line(aes(y = threshold_1), color = "green") +
  geom_line(aes(y = threshold_2), color = "red") +
  geom_line(aes(y = kurtosis_bsadf_log))
ggplot(tvar_res[[1]][date %between% c("2022-01-01", "2023-06-01")], aes(date)) +
  geom_line(aes(y = threshold_1), color = "green") +
  geom_line(aes(y = threshold_2), color = "red") +
  geom_line(aes(y = kurtosis_bsadf_log))

# threshold based backtest
tvar_backtest <- function(tvar_res_i) {
  # tvar_res_i <- tvar_res[[2]]
  returns <- tvar_res_i$returns
  threshold_1 <- tvar_res_i$threshold_1
  threshold_2 <- tvar_res_i$threshold_2
  coef_1_up <- tvar_res_i$sd_radf_sum..1_bup
  coef_1_down <- tvar_res_i$sd_radf_sum..1_bdown
  coef_ret_1_up <- tvar_res_i$returns..1_bup
  coef_ret_1_down <- tvar_res_i$returns..1_bdown
  coef_ret_1_middle <- tvar_res_i$returns..1_bmiddle
  coef_ret_2_up <- tvar_res_i$returns..2_bup
  coef_ret_2_down <- tvar_res_i$returns..2_bdown
  coef_ret_2_middle <- tvar_res_i$returns..2_bmiddle
  coef_ret_3_up <- tvar_res_i$returns..3_bup
  coef_ret_3_down <- tvar_res_i$returns..3_bdown
  coef_ret_3_middle <- tvar_res_i$returns..3_bmiddle
  predictions_1 = tvar_res_i$predictions_1
  aic_ <- tvar_res_i$aic
  indicator <- tvar_res_i$sd_radf_sum
  predictions <- tvar_res_i$predictions_1
  sides <- vector("integer", length(predictions))
  sides <- vector("integer", length(returns))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(threshold_2[i-1]) || is.na(threshold_2[i-2])) {
      sides[i] <- NA
    } else if (indicator[i-1] > threshold_2[i-1] & coef_ret_1_up[i-1] < 0) {
      sides[i] <- 0
    } else if (indicator[i-1] > threshold_1[i-1] & coef_ret_1_middle[i-1] < 0 & coef_ret_2_middle[i-1] < 0) {
      sides[i] <- 0
      # } else if (indicator[i-1] < threshold_1[i-1] & coef_ret_1_down[i-1] < 0 & coef_ret_2_down[i-1] < 0 & coef_ret_3_down[i-1] < 0) {
      #   sides[i] <- 0
    } else {
      sides[i] <- 1
    }
  }
  sides <- ifelse(is.na(sides), 1, sides)
  returns_strategy <- returns * sides
  returns_strategy
  # Return.cumulative(returns_strategy)
}
lapply(tvar_res, function(x) Return.cumulative(tvar_backtest(x)))
res_ = tvar_res[[1]]
returns_strategy <- tvar_backtest(res_)
PerformanceAnalytics::Return.cumulative(returns_strategy)
charts.PerformanceSummary(xts(cbind(res_$returns, returns_strategy), order.by = res_$date))
charts.PerformanceSummary(xts(tail(cbind(returns, returns_strategy), 2000), order.by = tail(res_$time, 2000)))

# threshold based backtest with multiple windows
tvar_backtest_windows <- function(tvar_res_i) {
  # tvar_res_i <- tvar_res[[1]]
  returns <- tvar_res_i$returns
  threshold_1 <- tvar_res_i$threshold_1
  threshold_2 <- tvar_res_i$threshold_2
  coef_1_up <- tvar_res_i$sd_radf_sum..1_bup
  coef_1_down <- tvar_res_i$sd_radf_sum..1_bdown
  coef_ret_1_up <- tvar_res_i$returns..1_bup
  coef_ret_1_down <- tvar_res_i$returns..1_bdown
  coef_ret_1_middle <- tvar_res_i$returns..1_bmiddle
  coef_ret_2_up <- tvar_res_i$returns..2_bup
  coef_ret_2_down <- tvar_res_i$returns..2_bdown
  coef_ret_2_middle <- tvar_res_i$returns..2_bmiddle
  coef_ret_3_up <- tvar_res_i$returns..3_bup
  coef_ret_3_down <- tvar_res_i$returns..3_bdown
  coef_ret_3_middle <- tvar_res_i$returns..3_bmiddle
  predictions_1 = tvar_res_i$predictions_1
  aic_ <- tvar_res_i$aic
  indicator <- tvar_res_i$sd_radf_sum
  predictions <- tvar_res_i$predictions_1
  sides <- vector("integer", length(predictions))
  sides <- vector("integer", length(returns))
  for (i in seq_along(sides)) {
    if (i %in% c(1) || is.na(threshold_2[i-1]) || is.na(threshold_2[i-2])) {
      sides[i] <- NA
    } else if (indicator[i-1] > threshold_2[i-1] & coef_ret_1_up[i-1] < 0) {
      sides[i] <- 0
    } else if (indicator[i-1] > threshold_1[i-1] & coef_ret_1_middle[i-1] < 0 & coef_ret_2_middle[i-1] < 0) {
      sides[i] <- 0
      # } else if (indicator[i-1] < threshold_1[i-1] & coef_ret_1_down[i-1] < 0 & coef_ret_2_down[i-1] < 0 & coef_ret_3_down[i-1] < 0) {
      #   sides[i] <- 0
    } else {
      sides[i] <- 1
    }
  }
  sides <- ifelse(is.na(sides), 1, sides)
  return(sides)
}
sides = lapply(tvar_res[1:3], function(x) tvar_backtest_windows(x))
sides <- Map(cbind, sides[[1]], sides[[2]], sides[[3]])
sides = lapply(sides, as.data.table)
sides = rbindlist(sides)
setnames(sides, c("tvar_308", "tvar_924", "tvar_1848"))
sides$sides_any <- rowSums(sides)
sides[, sides_any := ifelse(sides_any == 3, 1, 0)]
sides[, sides_all := ifelse(sides_any > 0, 1, 0)]
returns <- tvar_res[[1]]$returns
returns_strategies <- returns * sides
returns_strategies = cbind(tvar_res[[1]][, .(date)], returns_strategies)
charts.PerformanceSummary(returns_strategies)
