Our work uses the instance usage and instance events tables from Google trace v3. We provide a script to combine the two tables. You can find the script in the `scripts` folder in the main repository folder. The script can be run by executing the following command. 

`./join_google_trace_tables cell_id start_time end_time gcp_region output_table_name`

Here,   
* **cell_id:** is the cell identifiers used in Google trace and our paper. It takes one of the 8 letters from a-to-h.  
* **start_time:** it is the start time for the cluster trace data in microseconds. It take a value between 0 and 30x24x60x60x10^6=2,592,000,000,000. Here, 30 corresponds to 30 days data in Google’s trace. 
* **end_time:** it is the start time for the cluster trace data in microseconds. It take a value between 0 and 30x24x60x60x10^6=2,592,000,000,000. 
* **gcp_region:** it is the region of GCP where you want to run your experiments. CloudDataFlow supports only us-east1, us-east4, us-west1, us-central1.
* **output_table:** it is the name of the BigQuery table where the joint table will be stored. 

Users can generate the joint table for any number of days. However, it should be kept in mind that the join operation for the two tables is very time-consuming and may take more than 4 days if run on GCP trial account (GCP trial account limits Dataflow workers to a maximum value of 8).
