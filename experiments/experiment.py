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

# # Example Python Code for Demonstrating Experiments in CDSW

# Import the `cdsw` module, which is included with CDSW
# and has some built-in functions that you can use
# with experiments:

import cdsw

# Parse the command-line arguments. This script expects
# one argument: the string `true` or `false`:

if len(sys.argv) > 1 and sys.argv[1].lower() == 'false':
  fit_intercept = False
else:
  fit_intercept = True


# Run code to fit and evaluate a linear regression model.
# The code below is excerpted from `example.py`; the only
# important difference below is that the `fitIntercept`
# parameter to `LinearRegression()` is specified. It is
# set to the value of `fit_intercept`.

from pyspark.sql import SparkSession

spark = SparkSession.builder \
  .master("yarn") \
  .appName("cdsw-experiments-training") \
  .getOrCreate()

flights = spark.read.csv(
  "flights/",
  header=True,
  inferSchema=True
)

flights_to_model = flights \
  .select("dep_delay", "arr_delay") \
  .dropna()

from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler(
  inputCols=["dep_delay"],
  outputCol="features"
)

flights_assembled = assembler.transform(flights_to_model)

(train, test) = flights_assembled.randomSplit([0.7, 0.3])

from pyspark.ml.regression import LinearRegression

lr = LinearRegression(
  featuresCol="features",
  labelCol="arr_delay",
  fitIntercept=fit_intercept
)

lr_model = lr.fit(train)

lr_summary = lr_model.evaluate(test)

spark.stop()


# Track the value of R-squared (rounded to four decimal
# places) to compare experiment results:

cdsw.track_metric("R_squared", round(lr_summary.r2, 4))
