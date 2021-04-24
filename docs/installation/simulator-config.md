1. The experimental setup requires Python 3.6 or greater. See Installing Python for further details. (10 minutes)

2. You need to create a virtual environment to manage dependencies for this project. Please use the following command to create a virtual environment.  
&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;<code>python3 -m venv overcommit_sim_env</code>\
`venv` virtual environment manager is included in the Python standard library (version 3.6 or greater) and requires no additional installation. 


3. You can activate the virtual environment using the following command.   

&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;`source tutorial_env/bin/activate`

4. Please visit the following github repository: [cluster-resource-forecast](https://github.com/googleinterns/cluster-resource-forecast). Clone the `master` branch of the [overcommit simulator Github repository](https://github.com/googleinterns/cluster-resource-forecast) to your desired directory using the `git clone` command. Please see [Cloning a repository](https://docs.github.com/en/github/creating-cloning-and-archiving-repositories/cloning-a-repository) for further details.

5. Please navigate to the `cluster-resource-forecast` folder and install all the dependencies using the following command.  

&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;`pip install -r requirements.txt`


6. Once done, you can test if the setup is working properly using a test script by running the following command.  
&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;
`sh test_script.sh`  
This will run an example apache beam pipeline that counts words in a given file and stores the results to a Google Cloud Storage (GCS) bucket. This pipeline should finish in 5-6 human-minutes. The runtime will be quite verbose, but you should see the “JOB_STATE_DONE” as the last phrase. This successful completion of this test ensures that your setup is complete.

