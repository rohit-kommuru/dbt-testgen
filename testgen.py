import argparse
from google.cloud import bigquery
import os
import subprocess

# Set up argument parser with a description
parser = argparse.ArgumentParser(description="This script processes a single argument in the format project-id.dataset-id.table-id")
parser.add_argument("input", type=str, help="Input in the format project-id.dataset-id.table-id")

# Parse arguments
args = parser.parse_args()

# Split the input into three parts
input_parts = args.input.split('.')

    
if len(input_parts) == 3:
    project_id, dataset_id, table_id = input_parts
    print(f"Project ID: {project_id}")
    print(f"Dataset ID: {dataset_id}")
    print(f"Table ID: {table_id}")
    table_ids = [table_id]
elif len(input_parts) == 2: 
    project_id, dataset_id = input_parts
    print(f"Project ID: {project_id}")
    print(f"Dataset ID: {dataset_id}")

    client = bigquery.Client(project=project_id)
    query_job = client.query(
        f"""SELECT *
            FROM `{project_id}.{dataset_id}.__TABLES__`;"""
    )

    results = query_job.result()  
    table_ids = [row['table_id'] for row in results]

else:
    print("Error: Run the file with an additional argument in the format of project-id.dataset-id.table-id or  project-id.dataset-id")

for table_id in table_ids:
    print(table_id)
    table_name = f"`{project_id}.{dataset_id}.{table_id}`"
    model = " {{ config(materialized='table') }} \n select * from " + table_name
    
    dir_name = f"{table_id}"
    os.makedirs(os.path.join('./models', dir_name), exist_ok=True)

    with open(os.path.join(os.path.join('./models', dir_name), f"model_{table_id}.sql"), "w") as file:
        file.write(model)

    print("dbt compile -q --inline \"{{ testgen.get_test_suggestions(ref(" + f"\'model_{table_id}\'" + ")) }}\"")
    run_command = "dbt run --select " + f"{table_id}"
    generate_command = "dbt compile -q --inline \"{{ testgen.get_test_suggestions(ref(" + f"\'{table_id}\'" + ")) }}\""

    # Run the command and capture the output
    result = subprocess.run(run_command, shell=True, capture_output=True, text=True)
    if result.returncode == 0: 
        result = subprocess.run(generate_command, shell=True, capture_output=True, text=True)

        # Store the output of dbt-testgen
        yaml_file = result.stdout
        with open(os.path.join(os.path.join('./models', dir_name), "schema.yaml"), "w") as file:
            file.write(yaml_file)