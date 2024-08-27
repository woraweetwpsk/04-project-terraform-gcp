## Project Terraform and Google Cloud Platform

เป็น Project ที่ใช้ Terraform สร้าง Google Cloud Storage, Google Cloud Composer, Dataproc และ BigQuery เพื่อมาสร้าง Data Pipeline 
โดยใช้ข้อมูลจาก [Kaggle][Kaggle_Link] นำมาทำความสะอาดข้อมูลโดยใช้ Dataproc แล้วนำมาแสดงเป็น Dashboard

[Kaggle_Link]:https://www.kaggle.com/datasets/artermiloff/steam-games-dataset

### Architecture

![architecture](https://github.com/woraweetwpsk/04-project-terraform-gcp/blob/main/images/architecture.png?raw=true)

**Tools & Technologies**

- Terraform
- Google Cloud Storage
- Google Cloud Composer (Aiflow)
- Dataproc (Spark)
- BigQuery
- Looker Studio

### DAG in AIRFLOW UI

![dag](https://github.com/woraweetwpsk/04-project-terraform-gcp/blob/main/images/dag.png?raw=true)

### Overview


### Dashboard

![dashboard](https://github.com/woraweetwpsk/04-project-terraform-gcp/blob/main/images/dashboard.png?raw=true)
