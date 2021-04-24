All of the following steps use the GCP project that we created in the `gcp-project-config` guide. 

1. Please use the guide on [Creating and starting a VM instance](https://cloud.google.com/compute/docs/instances/create-start-instance) to start a basic VM instance. The VM will only be used to run the Apache Beam guidelines on Google Cloud Dataflow. Therefore, even `f1-micro` VM should suffice. 

2. You need to create an SSH key pair and username for the system that you want to use for login. See [Creating a new SSH key](https://cloud.google.com/compute/docs/instances/adding-removing-ssh-keys#createsshkeys) for details (2 minutes). Please follow the [Formatting your public SSH key files](https://cloud.google.com/compute/docs/instances/adding-removing-ssh-keys#sshkeyformat) to properly format your SSH keys before the next step. 

3. Please follow the [Providing public SSH keys to instances](https://cloud.google.com/compute/docs/instances/connecting-advanced#provide-key) to enable SSH access to the VM instance. 

4. In your local mac or linus terminal, use the ssh command along with your private SSH key file, the username, and the external IP address of the instance to connect. For example:   
&ensp;&ensp;&ensp;&ensp;&ensp;`ssh -i PATH_TO_PRIVATE_KEY USERNAME@EXTERNAL_IP`  
The VM instance has access to all the GCP APIs that are needed to run the experiments. 
