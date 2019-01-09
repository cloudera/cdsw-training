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

# Example build script used to customize the engine 
# environment for CDSW Experiments and Models

# Install pendulum for Python 2:

pip install pendulum

# Install pendulum for Python 3:

pip3 install pendulum

# Install lubridate for R: 

Rscript -e "install.packages(repos='https://cloud.r-project.org', 'lubridate')"
