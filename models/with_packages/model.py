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

# # Example Python CDSW Model Using a Package

# Import the module required by the code below:

import pendulum

# Define the function that will be called when the
# model is run:

def pred_arr_time(args):
  sched_arr_time = args['sched_arr_time']
  dep_delay = args['dep_delay']
  sched_arr_hour = int(sched_arr_time / 100)
  sched_arr_min = sched_arr_time % 100
  pred_arr_delay = dep_delay * 1.02 - 5.9
  pred_arr_time = pendulum \
    .datetime(2019, 1, 1) \
    .add(hours=sched_arr_hour) \
    .add(minutes=sched_arr_min) \
    .add(minutes=pred_arr_delay)
  result = int(pred_arr_time.format('HHmm'))
  return {'pred_arr_time': result}
