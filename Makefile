export AIRFLOW_HOME=./airflow

test ! -d .ve && virtualenv -p python3.7 .ve
. .ve/bin/activate
pip install apache-airflow

airflow initdb

airflow webserver -p 8080