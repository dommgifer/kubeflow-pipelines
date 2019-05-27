# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import argparse
import os
from datetime import datetime
from kfp import Client
import utils

###### Input/Output Instruction ######
# input: yaml
# output: local file path


# Parsing the input arguments
def parse_arguments():
  """Parse command line arguments."""

  parser = argparse.ArgumentParser()
  parser.add_argument('--input',
                      type=str,
                      required=True,
                      help='The path of a pipeline package that will be submitted.')
  parser.add_argument('--result',
                      type=str,
                      required=True,
                      help='The path of the test result that will be exported.')
  parser.add_argument('--output',
                      type=str,
                      required=True,
                      help='The path of the test output')
  parser.add_argument('--testname',
                      type=str,
                      required=True,
                      help="Test name")
  parser.add_argument('--namespace',
                      type=str,
                      default='kubeflow',
                      help="namespace of the deployed pipeline system. Default: kubeflow")
  parser.add_argument('--param',
                      dest="params",
                      action="append",
                      nargs=2,
                      default=[],
                      metavar=("key", "value"),
                      help="Extra param to pass to the run (can be given multiple times)")
  args = parser.parse_args()
  return args

def main():
  args = parse_arguments()
  test_cases = []
  test_name = args.testname + ' Sample Test'

  ###### Initialization ######
  host = 'ml-pipeline.%s.svc.cluster.local:8888' % args.namespace
  client = Client(host=host)

  ###### Check Input File ######
  utils.add_junit_test(test_cases, 'input generated yaml file', os.path.exists(args.input), 'yaml file is not generated')
  if not os.path.exists(args.input):
    utils.write_junit_xml(test_name, args.result, test_cases)
    print('Error: job not found.')
    exit(1)

  ###### Create Experiment ######
  experiment_name = args.testname + ' sample experiment'
  print('Creating experiment %s...' % experiment_name)
  response = client.create_experiment(experiment_name)
  experiment_id = response.id
  print('Created experiment %s with ID %s' % (experiment_name, experiment_id))
  utils.add_junit_test(test_cases, 'create experiment', True)

  ###### Create Job ######
  job_name = args.testname +'_sample'
  params = {}
  for k, v in args.params:
    params[k] = v
  print('Creating pipeline run %s...' % job_name)
  response = client.run_pipeline(experiment_id, job_name, args.input, params)
  run_id = response.id
  print('Created pipeline run %s with ID %s' % (job_name, run_id))
  utils.add_junit_test(test_cases, 'create pipeline run', True)

  ###### Monitor Job ######
  try:
    start_time = datetime.now()
    print('Waiting run to complete...')
    response = client.wait_for_run_completion(run_id, 1200)
    succ = (response.run.status.lower()=='succeeded')
    print('Run completed with status %s' % response.run.status)
    end_time = datetime.now()
    elapsed_time = (end_time - start_time).seconds
    utils.add_junit_test(test_cases, 'job completion', succ, 'waiting for job completion failure', elapsed_time)
  finally:
    ###### Output Argo Log for Debugging ######
    workflow_json = client._get_workflow_json(run_id)
    workflow_id = workflow_json['metadata']['name']
    argo_log, _ = utils.run_bash_command('argo logs -n {} -w {}'.format(args.namespace, workflow_id))
    print("=========Argo Workflow Log=========")
    print(argo_log)

  if not succ:
    utils.write_junit_xml(test_name, args.result, test_cases)
    exit(1)

  ###### Delete Job ######
  #TODO: add deletion when the backend API offers the interface.

  ###### Write out the test result in junit xml ######
  utils.write_junit_xml(test_name, args.result, test_cases)

if __name__ == "__main__":
  main()
