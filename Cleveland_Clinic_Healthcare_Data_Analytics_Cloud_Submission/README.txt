CLEVELAND CLINIC - HEALTHCARE DATA ANALYTICS - CLOUD TRACK SUBMISSION
======================================================

Region:
  ap-south-1 (Asia Pacific - Mumbai)

Implementation summary:
  Raw CSV -> S3 raw/ -> Glue Crawler/Data Catalog
  -> Glue PySpark ETL -> S3 processed/
  -> Glue Processed Crawler/Data Catalog
  -> Athena analytics
  -> Lambda daily report -> S3 reports/
  -> EventBridge Scheduler daily trigger

AWS resources:
  S3 bucket: medical-data-engineering-oh
  Glue database: medical_data_engineering
  Glue ETL job: medical-data-engineering-etl
  Lambda: medical-data-engineering-daily-report
  EventBridge Scheduler: medical-data-engineering-daily-report

Raw datasets:
  patients, appointments, billing, branches, doctors,
  lab_reports, prescriptions, surgeries

Processed datasets:
  patients, appointments, billing, branches, doctors,
  lab_reports, prescriptions, surgeries, doctor_other_branches

Validation:
  appointments: 12000
  billing: 5000
  branches: 100
  doctor_other_branches: 384
  doctors: 200
  lab_reports: 7500
  patients: 2300
  prescriptions: 9000
  surgeries: 1500

Analytics:
  Total visits: 12000
  Total billed revenue: 128137627.30
  Unique consultation dates: 1344
  Top diagnosis by appointment count in the final query: Arthritis (1260)

Security notes:
  - S3 bucket is private and public access is blocked.
  - S3 default encryption is enabled.
  - Glue and Lambda use IAM roles/policies.
  - No AWS credentials, passwords, access keys, or .env files are included.
  - Lambda reads the Glue Data Catalog and processed S3 data through IAM permissions.

Important implementation note:
  The cloud track uses S3 + Glue + Athena for the cloud analytics path.

Contents:
  01_Architecture_Approach/
  02_Screenshots/
  03_ETL_Scripts/
  04_SQL/
  05_Reports/
  06_Validation/
