import sys
import boto3
import psycopg2
from botocore.exceptions import NoCredentialsError

def upload_to_s3(file_path, bucket_name, s3_file_name):
    s3 = boto3.client('s3')
    try:
        s3.upload_file(file_path, bucket_name, s3_file_name)
        print(f"File {file_path} uploaded to S3 bucket {bucket_name} as {s3_file_name}")
    except FileNotFoundError:
        print("The file was not found")
    except NoCredentialsError:
        print("Credentials not available")

def load_data_to_redshift(table_name, s3_file_name, bucket_name, cluster_endpoint, dbname, user, password, port, iam_role):
    conn_string = f"dbname='{dbname}' port='{port}' user='{user}' password='{password}' host='{cluster_endpoint}'"
    try:
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()
        copy_cmd = f"""
        COPY {table_name}
        FROM 's3://{bucket_name}/{s3_file_name}'
        IAM_ROLE '{iam_role}'
        FORMAT AS CSV;
        """
        cursor.execute(copy_cmd)
        conn.commit()
        print(f"Data from {s3_file_name} loaded into table {table_name}")
    except Exception as e:
        print(f"Error loading data to Redshift: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python redshift_data_loader.py <path_to_database_file>")
        sys.exit(1)

    file_path = sys.argv[1]
    s3_file_name = "transformed.dat"
    bucket_name = "transformed-data"
    table_name = "Data"
    cluster_endpoint = "redshift-cluster-1.cdqtggyc2vob.us-east-1.redshift.amazonaws.com:5439/dev"  # Your Redshift cluster endpoint
    dbname = "dev"
    user = "awsuser"
    password = "your-master-password"
    port = "5439"  # Default Redshift port
    iam_role = "arn:aws:iam::your-account-id:role/your-redshift-role"  # Your IAM role for Redshift

    upload_to_s3(file_path, bucket_name, s3_file_name)
    load_data_to_redshift(table_name, s3_file_name, bucket_name, cluster_endpoint, dbname, user, password, port, iam_role)
