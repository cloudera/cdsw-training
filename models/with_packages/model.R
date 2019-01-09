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

# # Example R CDSW Model Using a Package

# Load the package required by the code below:

library(lubridate)

# Define the function that will be called when the
# model is run:

pred_arr_time <- function(args) {
  sched_arr_time <- args$sched_arr_time
  dep_delay <- args$dep_delay
  sched_arr_hour <- as.integer(sched_arr_time / 100)
  sched_arr_min <- sched_arr_time %% 100
  pred_arr_delay <- dep_delay * 1.02 - 5.9
  pred_arr_time <-
    lubridate::ymd("2019-01-01") +
    lubridate::hours(sched_arr_hour) +
    lubridate::minutes(sched_arr_min) +
    lubridate::dminutes(pred_arr_delay)
  result <- as.integer(format(pred_arr_time, "%H%M"))
  list(pred_arr_time = result)
} 
