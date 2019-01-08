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

# # Example Python Code for Demonstrating Models in CDSW

# Import functions required by the code below:

from datetime import datetime, timedelta

# Define the function that will be called when the
# model is run:

def pred_arr_time(args):
  sched_arr_time = args['sched_arr_time']
  sched_arr_hour = int(sched_arr_time / 100)
  sched_arr_min = sched_arr_time % 100
  dep_delay = args['dep_delay']
  pred_arr_delay = dep_delay * 1.02 - 5.9
  pred_arr_time = datetime(
    2019, 1, 1,
    sched_arr_hour, sched_arr_min
  ) + timedelta(minutes=pred_arr_delay)
  result = int(str(pred_arr_time.hour)
    + str(pred_arr_time.minute).zfill(2))
  return {'pred_arr_time': result}
