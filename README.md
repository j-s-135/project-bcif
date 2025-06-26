local installation with sqlite database (non-parallel workflow):

make project folder

make python virtual environment in project folder

activate environment (henceforth do everything from same environment)

install apache airflow with pip (sqlite database, non-parallel)

```
pip3 uninstall apache-airflow
pip3 install apache-airflow
airflow db install
mkdir ~/airflow/dags
airflow users create --username admin --firstname James --lastname Smith --role Admin --email james.smith@rcsb.org  --password pspspsps
```

cd to ~/airflow (note that it installed globally although you installed with pip in virtual environment)

make dags folder

start new terminal

activate virtual environment again

```
airflow scheduler
```

start another terminal

activate virtual environment again

```
airflow webserver
```

open browser to 127.0.0.1:8080

```
cd ~/airflow/dags
```

make example dag

wait for it to appear in webserver (refresh periodically)

test in webserver
