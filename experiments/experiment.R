# Copyright 2019 Cloudera, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# # Example R Code for Demonstrating Experiments in CDSW

# Import the cdsw package, which is included with CDSW
# and has some built-in functions that you can use
# with experiments:

library(cdsw)

# Parse the command-line arguments. This script expects
# one argument: a numeric value between 0 and 1:

arguments <- commandArgs(trailingOnly = TRUE)
if (length(arguments) > 0) {
  train_proportion <- as.numeric(arguments[1])
} else {
  train_proportion <- 0.7
}


# Run code to fit and evaluate a linear regression model.
# The code below is excerpted from `example.R`; the only
# important difference below is that the `train` and 
# `test` parameters to `sdf_partition()` are set
# according to the value of `train_proportion`.

library(sparklyr)
library(dplyr)

spark <- spark_connect(
  master = "yarn",
  app_name = "cdsw-experiments-training"
)

flights <- spark_read_csv(
  sc = spark,
  name = "flights",
  path = "flights/",
  header = TRUE,
  infer_schema = TRUE
)

flights_to_model <- flights %>%
  select(dep_delay, arr_delay, distance) %>%
  na.omit()

samples <- flights_to_model %>%
  sdf_partition(
    train = train_proportion,
    test = 1 - train_proportion
  )

model <- samples$train %>%
  ml_linear_regression(arr_delay ~ dep_delay)

pred <- model %>%
  ml_predict(samples$test)

R_squared <- pred %>%
  summarise(r_squared = cor(arr_delay, prediction)^2) %>%
  pull(r_squared)

spark_disconnect(spark)


# Track the value of R-squared (rounded to four decimal
# places) to compare experiment results:

track.metric("R_squared", round(R_squared, 4))
