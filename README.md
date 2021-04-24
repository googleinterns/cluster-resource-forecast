# Overcommit Simulator

Our  simulator  is  written  in  Python  using  [Apache  Beam](https://beam.apache.org/). The simulator uses the Google's publicly-available cluster usage trace [[link]](https://github.com/google/cluster-data.), and mimics Google’s production environment in which Borg [[link]](https://research.google/pubs/pub49065/) operates. A Python script,named simulator-setup-walkthrough, develops a Beam pipeline to convert the Google workload trace to the desired format outlined in `table_schema.json`. Another Python script,named `fortune-teller-setup-walkthrough`, builds a Beam   pipeline   to   configure   simulation   scenarios   and   run   various   predictors   (see   Section   A.5 from the paper  further  details).  The  heart  of  the  simulator  is  in `simulator/fortune_teller.py`,  which  implements  the logic  to  run peak-oracle and  other  practical  predictors.The  configurations  for  the  simulator  are  defined  using protobuf’s text format. The format for the configuration files is described in the `simulator/config.proto`.

## Software Dependencies

Our  simulator  defines data  processing  pipelines  using  Apache  Beam.   Beam pipelines can be run on selected distributed processing back-ends that include Apache Flink, Apache Spark, and GoogleCloud Dataflow. Our simulator should run on any cloud platform or compute cluster, with slight modifications, that supports the aforementioned processing back-ends. How-ever, we have only tested the simulator with Dataflow onGCP. When running on GCP, the simulator uses the Compute Engine, BigQuery (BQ), Google Cloud Storage, and Dataflow APIs. We plan to test the simulator on other cloud platforms and the university-owned compute clusters in the future.


## Installation
Instructions for setting up the environment are available in
`/docs/installation/`. The  instructions  for setting  up  the  GCP project  and enabling the required APIs  are  available  under `/docs/installation/gcp-project-config`. The instructions for creating a Google Compute Engine virtual machine (VM) instance with required permissions are available under `/docs/installation/gcp-vm-config`. Finally, the instructions for setting up a Python virtual environment with required dependencies for the simulator are available under   `/docs/installation/simulator-config`.

## Experiment Workflow
There are three major steps for evaluating the performance of a predictor following the data pre-processing, simulation, and data post-processing framework: Join the table, then run the simulator on the joined table, and data analysis. 
The first two steps can be merged into a single beam pipeline, technically. But because the joining tables take lots of time and resources, and its results can be reused, we choose to put it into a separate pipeline so that users can run it once and not worry about it later. 

### Data pre-processing 

The Google workload trace provides event and usage information for instances in two separate tables: `InstanceEvents` table and `InstanceUsage` table. Our simulator expects this information in a single table. The instructions for joining Google trace tables are available under  `/docs/experiments/joining-tables`. The output is saved to a BQ table and is used as an input for the next step. 

### Simulation 

Our simulator enables user to configure various simulation scenarios and run multiple predictors in parallel. The instructions for configuring the simulation scenarios are available under `/docs/experiments/simulation-scenarios`. 
The instructions for configuring and running various peak predictors are available under `/docs/experiments/running-predictors`. Each predictor saves its output to a BQ table, which contain the per-machine usage, sum of limits for all tasks running on each machine, and predicted peak for each machine, along with machine-specific information.


### Data post-processing
The output BQ tables from simulations are processed in a Colab notebook to compute evaluation metrics and visualize the results. The instructions to using the Colab notebook and notebook's link are available under `/docs/visualization/`. 


## Experiment Customization
One of the key goals for our simulator is to enable future work on designing overcommit policies. We enable users to customize the parameters for the existing overcommit policies as well as contribute entirely new predictors to the simulator. The instruction for customizing the existing predictors are available under `/docs/customize-predictors`. Our simulator facilitates integrating new predictors and standardizes their interface to ease their later conversion to a production environment. We implement supporting classes in `simulator/predictor.py` that define the interfaces to implement new peak predictors. Users can add any data-driven, machine learning-based predictors as long as they use the specified interfaces. The instructions for contributing new predictors are available under `/docs/contribute`. 