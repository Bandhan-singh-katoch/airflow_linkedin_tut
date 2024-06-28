pip install 'apache-airflow==2.9.1' \
 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.1/constraints-3.8.txt"

export AIRFLOW_HOME=~/environment/airflow
airflow db init


airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com


airflow users list
airflow webserver & airflow scheduler & airflow webserver
airflow webserver -p 8082
sql_alchemy_conn = postgresql+psycopg2://bandhan:Band%%401234@localhost:5432/airflow_db

airflow dags test pipe_line_name 2024-06-16





sudo apt update
sudo apt upgrade
sudo apt install postgresql
sudo service postgresql <status,start>
sudo su - postgres 
psql
\du list of users
create user bandhan with password 'Band1234' SUPERUSER;
psql -U bandhan -d Band@1234
pip install psycopg2-binary
--pip install apache-airflow-providers-postgres

CREATE DATABASE airflow_db WITH OWNER bandhan;
\dt : list of tables
\d table_name : describe table

\l : list of database
\c master : enter into dtabase
