import boto3
import csv
from decimal import Decimal

#CUSTOM VARIABLES - PLEASE MODIFY
dynamodb_table_name = 'language-game'
csv_output_filename = "language-game-export.csv"

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(dynamodb_table_name)

# Function to recursively scan all items
def scan_table(table):
    response = table.scan()
    data = response['Items']

    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])

    return data

# Convert Decimal to float for CSV export
def convert_types(item):
    for key, value in item.items():
        if isinstance(value, Decimal):
            item[key] = float(value)
    return item

# Main export function
def export_to_csv(filename):
    data = scan_table(table)
    if not data:
        print("No data found in table.")
        return

    # Get all unique keys from all items to handle inconsistent schema
    all_keys = set()
    for item in data:
        all_keys.update(item.keys())

    all_keys = list(all_keys)

    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=all_keys)
        writer.writeheader()

        for item in data:
            writer.writerow(convert_types(item))

    print(f"Exported {len(data)} items to {filename}")

# Run the export
export_to_csv(csv_output_filename)
