# Project Overview
The goal of this project is to analyse five search terms in Google Trends - "vpn", "hack", "cyber", "security", "wifi", and see how their stats and rank change weekly by region. Out of these five search terms, comparison is done between "vpn" and other four terms, with rankings calculated accordingly. Ranking is done based on search term share where highest search term share is ranked as the highest, or first, and lowest search term share is last. If "vpn' and one or more search term have the same search share, "vpn" should be ranked lowest, otherwise - ranking is done in alphabetical order.

To start the analysis, a pipeline has to be created to extract, ingest, prepare and transform Google Trends data. Data is extracted on weekly basis, deduplicated, restructured and transformed in a way that the data can be seen by country, extraction start and end dates, with ranking for each search term, based on the requirements. For this project, Airflow was chosen to create and orchestrate the pipeline.

# Prerequisites
- Python version 3.10 - can be downloaded here: https://www.python.org/downloads/release/python-3100/. Airfow does not support latest Python 3.11 version.
- Access to Google Cloud Platform - one of the team members must grant access to the Service Account. Once that is done, go to http://console.cloud.google.com/ and log into Google Account. Navigate to the top left and click on "Navigation menu" -> "APIs & Services" -> "Credentials" -> click at the bottom on a Service Account email -> on the top bar 'KEYS' -> 'ADD KEY' -> 'Create new key' -> leave Key type as JSON and click on 'CREATE'. Once done, keep the JSON file as it will be needed to configure connection for Airflow.
- POSIX-compliant Operatin System. More information can be found here: https://airflow.apache.org/docs/apache-airflow/stable/installation/prerequisites.html.

# Setup
First, clone this repostitory to any folder. Later on, these files will be moved to new 'airflow' folder, which will be created after package installation. To clone this repository, run "git clone https://github.com/tautvydas-v/homework.git".

After Python installation, it's advised to create a virtual environment so that each project's modules could be handled separately and there would be no conflict between the modules. 
- To create a virtual environment, open a terminal (run "python3 --version" just to make sure that Python is installed), navigate to your preferred folder and run "python3 -m venv venv". 
- After environment is created, run "source venv/bin/activate/" to activate the virtual environment. Before installing any modules, run "pip install --upgrade pip".
- Navigate in terminal to the cloned repository's "Requirements" folder and run "pip install -r requirements.txt".

Airflow setup:
Since this Airflow will be hosted and run locally, setup for Airflow will be kept to a minimum. It will be sufficient to run the pipelines completely. Note: this setup should not be used for production environment and production deployments as it's not 

1. Make sure virtual environment is activated and requirements.txt is installed. Navigate to any folder you prefer and run "airflow db init". Airflow folder will be created. Move to created 'airflow' folder.
2. Before starting anything else, open 'airflow.cfg' file, find 'load_examples' and change value to False.
3. Run "airflow users create --username admin --firstname airflow --lastname airflow --role Admin --email airfow@airlow.example.com". A prompt with password creation input will come up - for simplicity sake, enter 'admin' and confirm with the same 'admin'. 
4. While still inside the 'airflow' folder, open up a second terminal (and navigate to 'airflow' folder if needed). In one terminal run "airflow webserver", in another run "airflow scheduler". 
5. Open any browser (for example Google Chrome) and go to "http://localhost:8080/". As a first time sign in, Username and Password is needed. For both, enter "admin", unless another username and password was created in previous steps. Once logged in, Airflow UI will be visible. 
Note: it might take a minute or two to parse the DAG file. If after several minutes nothing is seen, kill scheduler and webserver inside terminal (control + C) and run both commands once again.
6. Connection setup - downloaded JSON file (refer to Prerequisites above) will be used to store the connection for Google Cloud. To setup Connection, in Airflow UI navigate to "Admin" -> "Connections". Find 'Conn Id' which is "google_cloud_default" and click on the left side 'Edit Record'. Inside, fill in these fields:
  - Project ID : homework-data2020
  - Keyfile Path : delete this value
  - Keyfile JSON : open downloaded JSON file in text editor, copy everything and paste into Keyfile JSON field
  - Scopes : https://www.googleapis.com/auth/cloud-platform
Once done, click 'Save'.
7. Before importing any DAG, Airflow Variables need to be set up, otherwise there will be DAG Import Error. To setup Variables, in Airflow UI navigate to "Admin" -> "Variables". Inside, click on blue "+" sign to add a new variable. Each variable has to be setup separately. Key stands for the name of the variable and Val stands for the value of the variable. Variables needed are (format below is Key : Val):
  - project_id : homework-data2020
  - dataset : data_engineer
  - raw_table : tv_raw_google_trends
  - staging_table : tv_staging_google_trends
  - final_table : tv_final_transformed_google_trends
    
    To run a fresh pipeline, use valid "project_id" and "dataset" which can be accessed with the provided credentials and also setup "raw_table",       "staging_table" and "final_table" as needed.
 
8. Copy from the cloned repository's 'dags' folder into 'airflow' folder. It will include both the DAG and SQL templates. 









