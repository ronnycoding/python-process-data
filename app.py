import mysql.connector
import csv

# Connection details (update these with your actual credentials)
mysql_config = {
    'user': 'test',
    'password': 'test',
    'host': 'localhost',
    'database': 'test'
}

def fetch_data(mysql_config):
    try:
        cnx = mysql.connector.connect(**mysql_config)
        cursor = cnx.cursor()
        query = "SELECT * FROM Data WHERE Flag = TRUE"
        cursor.execute(query)
        data = cursor.fetchall()
        headers = [i[0] for i in cursor.description]
        return headers, data
    finally:
        cursor.close()
        cnx.close()

def export_data(filename, headers, data):
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        writer.writerows(data)

def transform_data(input_file, output_file):
    with open(input_file, 'r', newline='') as infile, open(output_file, 'w', newline='') as outfile:
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()
        for row in reader:
            row['StringA'] = row['StringB'].upper() + '_'
            writer.writerow(row)

try:
    headers, data = fetch_data(mysql_config)
    export_data('exported.dat', headers, data)
    transform_data('exported.dat', 'transformed.dat')
except Exception as e:
    print(e)

