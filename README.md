# Project Overview
The goal of this project is to analyse five search terms in Google Trends - "vpn", "hack", "cyber", "security", "wifi", and see how their stats and rank change weekly by region. Out of these five search terms, comparison is done between "vpn" and other four terms, with rankings calculated accordingly. Ranking is done based on search term share where highest search term share is ranked as the highest, or first, and lowest search term share is last. If "vpn' and one or more search term have the same search share, "vpn" should be ranked lowest, otherwise - ranking is done in alphabetical order.

To start the analysis, a pipeline has to be created to extract, ingest, prepare and transform Google Trends data. Data is extracted on weekly basis, deduplicated, restructured and transformed in a way that the data can be seen by country, extraction start and end dates, with ranking for each search term, based on the requirements. For this project, Airflow was chosen to create and orchestrate the pipeline.

# Prerequisites
- Python version 3.10 - can be downloaded here: https://www.python.org/downloads/release/python-3100/. Airfow does not support latest Python 3.11 version.
- Access to Google Cloud Platform - one of the team members must grant access to the Service Account. Once that is done, go to http://console.cloud.google.com/ and log into Google Account. Navigate to the top left and click on "Navigation menu" -> "APIs & Services" -> "Credentials" -> cick at the bottom on a Service Account email -> on the top bar 'KEYS' -> 'ADD KEY' -> 'Create new key' -> leave Key type as JSON and click on 'CREATE'. Once done, keep the JSON file as it will be needed to configure connection for Airflow.
- POSIX-compliant Operatin System. More information can be found here: https://airflow.apache.org/docs/apache-airflow/stable/installation/prerequisites.html.

# Setup
After Python installation, it's advised to create a virtual environment so that each project's modules could be handled separately and there would be no conflict between the modules. To create a virtual environment, open a terminal (run "python3 --version" just to make sure that Python is installed), navigate to your preferred folder and run "python3 -m venv venv". After environment is created, run "source venv/bin/activate/" to activate the virtual environment.






