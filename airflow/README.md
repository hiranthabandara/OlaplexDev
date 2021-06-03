# Olaplex-Airflow
This repo contains Airflow DAGs for Olaplex's Data Warehouse. This project uses Apache Airflow version **1.10.9**.
For more details, refer the [Official Documentation](https://airflow.apache.org/docs/1.10.9/).
## Useful commands:
Airflow's **Scheduler** and **Webserver** run as systemd daemons and they are automatically started when the EC2 instance boots. 
Run these commands to see the status of these services:
```shell
sudo service airflow-webserver status
sudo service airflow-scheduler status
```
Use the following commands to manually start/stop the services.
```shell
sudo service airflow-webserver start
sudo service airflow-scheduler start
sudo service airflow-webserver stop
sudo service airflow-scheduler stop
```
Command to clean up the locally stored airflow log files older than 30 days:
```shell
find /home/ubuntu/airflow/logs/ -type d -mtime +30 | xargs rm -rf
```