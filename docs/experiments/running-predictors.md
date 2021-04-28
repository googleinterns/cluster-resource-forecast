This document introduces the reader to configuring different predictors and running the simulations. Just like configuring simulation scenarios, we use protobuf-based configuration files. The predictor configurations parameters are specified in the same `config.proto` file we discussed in the previous section. We next discuss how to write a predictor configuration file. 

    scheduled_samples {
        input {
            dataset: "artifact_evaluations"
            table: "scheduled_samples_cell_a_first_week"
        }
    }
    fortune_teller {
        name: "n_sigma_5_7200_7200_first_week_cell_a"
        save_samples: false
        predictor {
            n_sigma_predictor {
                min_num_samples: 24
                cap_to_limit: true
                num_history_samples: 24
                n: 5
            }
        }
    }
    fortune_teller {
        name: "oracle_24h_first_week_cell_a"
        save_samples: false
        oracle {
            horizon_in_seconds: 86400
            cap_to_limit: true
        }
    }
    simulation_result {
        dataset: "artifact_evaluations"
    }

Here,   
* **input:** the scheduled_samples table we saved in the last step.
* **fortune_teller:** this allows you to specify the name of the predictor and its configurations. 
* **name:** this field allows you to set the name for your predictor or oracle. 
* **n_sigma_predictor:** it is one of the predictors used in our papers, for other predictors list, please review the `config.proto` file. 
* **min_num_samples:** the minimum amount of history needed to run the predictor. It is configured as a number of samples where each sample spans 5 minutes. The total time in seconds is 24x5x60=7200. Please find further details in Section 4 of the paper. 
* **num_history_samples:** this is the maximum number of samples that our predictor will keep for each task. Please find further details in Section 4. 
* **cap_to_limit:** the default value of this parameter is true. Please find further details in Section 4. 
* **n:** is one of the configuration parameters for the `n_sigma predictor`. Please find more details in Section 4.
* **horizon_in_seconds:** it specifies the horizon for the oracle, i.e. how far into the future our peak oracle algorithm can see. We use 1day as the default parameter.
* **simulation_results:** users need to only specify the output dataset. Our pipeline creates individual tables inside the dataset with the name provided for each predictor. 

