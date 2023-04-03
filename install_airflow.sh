# installing pip
sudo apt-get update
sudo apt-get install python-setuptools
sudo apt install python3-pip
sudo -H pip3python install --upgrade pip

# installing airflow dependencies
sudo apt-get install libmysqlclient-dev
sudo apt-get install libssl-dev
sudo apt-get install libkrb5-dev

export AIRFLOW_HOME=~/airflow
pip3 install apache-airflow
pip3 install apache-airflow-providers-postgres
pip3 install typing_extensions

# initialize the database
airflow db init

# create a user
airflow users create --username admin --password admin --firstname FirstName --lastname LastName --role Admin --email admin@mail.com
mkdir ~/airflow/dags/

# cp -r dags ~/airflow
# airflow webserver -p 8080
# airflow scheduler