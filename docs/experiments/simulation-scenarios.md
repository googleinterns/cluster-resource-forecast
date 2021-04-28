The next step is to configure various simulation scenarios. We use Google’s Protocol Buffer (protobuf) Python to configure various simulation scenarios. Our `config.proto` file is shown below. It can also be found at Github’s cluster resource forecast repo as well as inside the VM at `/simulator/config.proto`.

Using the `config.proto` file, we write configurations using protobuf text format. We show an example config file here to explain how the reviewers can write their own simulation configurations. 

    first_week_simulation_config.txt
    input {
        dataset: "artifact_evaluations"
        table: "cell_a_first_week"
    }
    filter {
        remove_non_top_level_vms: true
        priority_range {
            lower_bound: 0
            upper_bound: 400
        }
        scheduling_class_range {
            lower_bound: 2
            upper_bound: 3
        }
    }
    metric {
        cpu_usage_percentile: 90
    }
    reset_and_shift {
        reset_time_to_zero: false
        random_shift {
            lower_bound: 0
            upper_bound: 1000
        }
    }
    scheduler {
        by_machine_id: true
    }
    scheduled_samples {
        output {
            dataset: "artifact_evaluations"
            table: "scheduled_samples_cell_a_first_week"
        }
    }

Here, 

* **input:** specifies the location of joint table from step 1
output: allows users to specify the output location of the scheduled_samples intermediate table. 
* **filter:** allows the user to remove/keep top level VMs and filter VMs based on priority and/or scheduling class. This example config file contains the default values and we encourage reviewers to keep the filter specifications the same.
* **metric:** this allows you to set a certain percentile usage as CPU usage. We use 90p CPU usage as our CPU usage; setting of this parameter is justified in Figure 6 of paper.  
* **reset_and_shift:** this allows users to generate more data by shuffling the existing data. We do not use this ability of the simulator for this paper. 
* **scheduler:** this allows you to set the scheduler. The simulator allows three options: `by_machine_id` (default parameters for almost all of experiments) `by_vm_unique_id` (used in Figure 1 to simulate task-level peak scenario) `at_random` (not used in any of the results). 
