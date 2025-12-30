import boto3

# AWS Config
athena = boto3.client('athena', region_name='us-east-1')
DATABASE = 'market_intel_gold'
TABLE = 'dashboard_master_view'

response = athena.get_table_metadata(
    CatalogName='AwsDataCatalog',
    DatabaseName=DATABASE,
    TableName=TABLE
)

print(f"\n--- Columns in {TABLE} ---")
for col in response['TableMetadata']['Columns']:
    print(f"- {col['Name']} ({col['Type']})")
print("----------------------------\n")