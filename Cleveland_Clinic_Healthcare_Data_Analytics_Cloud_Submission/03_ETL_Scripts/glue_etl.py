import sys
import re

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


RAW_BASE = "s3://medical-data-engineering-oh/raw"
PROCESSED_BASE = "s3://medical-data-engineering-oh/processed"


def standardize_column_name(column_name):
    name = column_name.strip().lower()
    name = re.sub(r"[^a-z0-9]+", "_", name)
    name = re.sub(r"_+", "_", name)
    return name.strip("_")


def standardize_columns(df):
    for old_name in df.columns:
        new_name = standardize_column_name(old_name)
        if old_name != new_name:
            df = df.withColumnRenamed(old_name, new_name)
    return df


def trim_string_columns(df):
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, F.trim(F.col(field.name)))
    return df


def read_csv(table_name):
    path = f"{RAW_BASE}/{table_name}/"
    print(f"Reading: {path}")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("mode", "PERMISSIVE")
        .csv(path)
    )

    df = standardize_columns(df)
    df = trim_string_columns(df)

    print(f"{table_name} columns: {df.columns}")
    print(f"{table_name} row count: {df.count()}")
    return df


def write_processed(df, table_name):
    output_path = f"{PROCESSED_BASE}/{table_name}/"
    print(f"Writing: {output_path}")

    (
        df.write
        .mode("overwrite")
        .option("header", True)
        .csv(output_path)
    )


patients = read_csv("patients")
appointments = read_csv("appointments")
billing = read_csv("billing")
branches = read_csv("branches")
doctors = read_csv("doctors")
lab_reports = read_csv("lab_reports")
prescriptions = read_csv("prescriptions")
surgeries = read_csv("surgeries")


def convert_date_columns(df):
    """
    Convert date-like columns to Spark DateType.
    Supports the source formats used by the project.
    """
    for column_name in df.columns:
        if (
            "date" in column_name
            or column_name.endswith("_dob")
            or column_name == "dob"
        ):
            df = df.withColumn(
                column_name,
                F.coalesce(
                    F.to_date(F.trim(F.col(column_name)), "M/d/yyyy"),
                    F.to_date(F.trim(F.col(column_name)), "MM/dd/yyyy"),
                    F.to_date(F.trim(F.col(column_name)), "yyyy-MM-dd")
                )
            )
    return df


patients = convert_date_columns(patients)
appointments = convert_date_columns(appointments)
billing = convert_date_columns(billing)
branches = convert_date_columns(branches)
doctors = convert_date_columns(doctors)
lab_reports = convert_date_columns(lab_reports)
prescriptions = convert_date_columns(prescriptions)
surgeries = convert_date_columns(surgeries)


def convert_numeric_columns(df):
    for column_name in df.columns:
        if column_name in [
            "case_id", "serial_no", "claim_id",
            "report_id", "bill_id", "surgery_id"
        ]:
            df = df.withColumn(
                column_name, F.col(column_name).cast("long")
            )
        elif any(
            keyword in column_name
            for keyword in [
                "amount", "bill", "revenue", "cost",
                "salary", "rating", "duration"
            ]
        ):
            df = df.withColumn(
                column_name, F.col(column_name).cast("double")
            )
    return df


patients = convert_numeric_columns(patients)
appointments = convert_numeric_columns(appointments)
billing = convert_numeric_columns(billing)
branches = convert_numeric_columns(branches)
doctors = convert_numeric_columns(doctors)
lab_reports = convert_numeric_columns(lab_reports)
prescriptions = convert_numeric_columns(prescriptions)
surgeries = convert_numeric_columns(surgeries)


# Normalize the multi-valued other-branch relationship.
print("Creating doctor_other_branches table...")

possible_columns = [
    column_name
    for column_name in doctors.columns
    if "other" in column_name and "branch" in column_name
]

if possible_columns and "doctor_id" in doctors.columns:
    other_branch_column = possible_columns[0]

    doctor_other_branches = (
        doctors
        .select(F.col("doctor_id"), F.col(other_branch_column))
        .filter(
            F.col(other_branch_column).isNotNull()
            & (F.trim(F.col(other_branch_column)) != "")
        )
        .withColumn(
            "branch_id",
            F.explode(
                F.split(
                    F.regexp_replace(
                        F.col(other_branch_column),
                        r"\s+",
                        ""
                    ),
                    ","
                )
            )
        )
        .select("doctor_id", "branch_id")
        .filter(F.col("branch_id") != "")
        .dropDuplicates(["doctor_id", "branch_id"])
    )
else:
    doctor_other_branches = spark.createDataFrame(
        [], "doctor_id string, branch_id string"
    )

if possible_columns:
    doctors = doctors.drop(possible_columns[0])


patients = patients.dropDuplicates()
appointments = appointments.dropDuplicates()
billing = billing.dropDuplicates()
branches = branches.dropDuplicates()
doctors = doctors.dropDuplicates()
lab_reports = lab_reports.dropDuplicates()
prescriptions = prescriptions.dropDuplicates()
surgeries = surgeries.dropDuplicates()
doctor_other_branches = doctor_other_branches.dropDuplicates()


write_processed(patients, "patients")
write_processed(appointments, "appointments")
write_processed(billing, "billing")
write_processed(branches, "branches")
write_processed(doctors, "doctors")
write_processed(lab_reports, "lab_reports")
write_processed(prescriptions, "prescriptions")
write_processed(surgeries, "surgeries")
write_processed(doctor_other_branches, "doctor_other_branches")


print("")
print("==========================================")
print("FINAL ETL VALIDATION")
print("==========================================")

print(f"patients:                {patients.count()}")
print(f"appointments:            {appointments.count()}")
print(f"billing:                 {billing.count()}")
print(f"branches:                {branches.count()}")
print(f"doctors:                 {doctors.count()}")
print(f"lab_reports:             {lab_reports.count()}")
print(f"prescriptions:           {prescriptions.count()}")
print(f"surgeries:               {surgeries.count()}")
print(f"doctor_other_branches:   {doctor_other_branches.count()}")

print("")
print("ETL processing completed successfully.")
print(f"Processed data written to: {PROCESSED_BASE}")

job.commit()
