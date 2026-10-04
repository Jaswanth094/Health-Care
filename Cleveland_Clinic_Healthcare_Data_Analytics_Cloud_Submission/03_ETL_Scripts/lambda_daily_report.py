import boto3
import time
from datetime import datetime

athena = boto3.client("athena", region_name="ap-south-1")
s3 = boto3.client("s3", region_name="ap-south-1")

DATABASE = "medical_data_engineering"
BUCKET = "medical-data-engineering-oh"
ATHENA_OUTPUT = f"s3://{BUCKET}/athena-results/"
REPORT_PREFIX = "reports/"


def run_athena_query(query):
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT}
    )

    query_execution_id = response["QueryExecutionId"]
    print(f"Started Athena query: {query_execution_id}")

    while True:
        status_response = athena.get_query_execution(
            QueryExecutionId=query_execution_id
        )
        state = status_response["QueryExecution"]["Status"]["State"]
        print(f"Athena query status: {state}")

        if state == "SUCCEEDED":
            break

        if state in ["FAILED", "CANCELLED"]:
            reason = status_response["QueryExecution"]["Status"].get(
                "StateChangeReason", "Unknown error"
            )
            raise Exception(f"Athena query {state}: {reason}")

        time.sleep(2)

    return athena.get_query_results(
        QueryExecutionId=query_execution_id
    )


def get_first_value(results):
    rows = results["ResultSet"]["Rows"]
    if len(rows) < 2:
        return None
    return rows[1]["Data"][0].get("VarCharValue")


def lambda_handler(event, context):
    report_date = datetime.now().strftime("%Y-%m-%d")

    visits_results = run_athena_query("""
        SELECT COUNT(*) AS total_visits
        FROM processed_appointments
    """)
    total_visits = get_first_value(visits_results)

    revenue_results = run_athena_query("""
        SELECT SUM(total_bill) AS total_revenue
        FROM processed_billing
    """)
    total_revenue = get_first_value(revenue_results)

    diagnosis_results = run_athena_query("""
        SELECT diagnosis, COUNT(*) AS diagnosis_count
        FROM processed_appointments
        GROUP BY diagnosis
        ORDER BY diagnosis_count DESC
        LIMIT 10
    """)

    top_diagnoses = []
    for row in diagnosis_results["ResultSet"]["Rows"][1:]:
        values = row["Data"]
        diagnosis = values[0].get("VarCharValue", "Unknown")
        count = values[1].get("VarCharValue", "0")
        top_diagnoses.append(f"{diagnosis}: {count}")

    report_lines = [
        "MEDICAL DATA ENGINEERING - DAILY REPORT",
        "=" * 50,
        f"Report Date: {report_date}",
        "",
        "SUMMARY",
        "-" * 50,
        f"Total Patient Visits: {total_visits}",
        f"Total Billed Revenue: {total_revenue}",
        "",
        "TOP 10 COMMON DIAGNOSES",
        "-" * 50
    ]

    for index, diagnosis in enumerate(top_diagnoses, start=1):
        report_lines.append(f"{index}. {diagnosis}")

    report = "\n".join(report_lines)
    print(report)

    report_key = f"{REPORT_PREFIX}daily_report_{report_date}.txt"

    s3.put_object(
        Bucket=BUCKET,
        Key=report_key,
        Body=report.encode("utf-8"),
        ContentType="text/plain"
    )

    print(f"Report saved to s3://{BUCKET}/{report_key}")

    return {
        "statusCode": 200,
        "body": {
            "report_date": report_date,
            "total_visits": total_visits,
            "total_revenue": total_revenue,
            "report_location": f"s3://{BUCKET}/{report_key}"
        }
    }
