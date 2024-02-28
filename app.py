import mysql.connector
import csv

# Connection details (update these with your actual credentials)
mysql_config = {
    'user': 'test',
    'password': 'test',
    'host': 'localhost',
    'database': 'test'
}

try:
    # Connect to MySQL
    cnx = mysql.connector.connect(**mysql_config)
    cursor = cnx.cursor()

    # Select data where Flag is true
    query = "SELECT * FROM Data WHERE Flag = TRUE"
    cursor.execute(query)

    # Export data to a local file
    with open('exported.dat', 'w', newline='') as file:
        writer = csv.writer(file)
        headers = [i[0] for i in cursor.description]  # Capture headers
        writer.writerow(headers)  # write headers
        writer.writerows(cursor)

    # Assuming 'StringA' and 'StringB' are columns in your exported data
    # Read data from the exported file, transform it, and write to 'transformed.dat'
    with open('exported.dat', 'r', newline='') as infile, open('transformed.dat', 'w', newline='') as outfile:
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()
        
        for row in reader:
            # Transform 'StringA' based on 'StringB'
            row['StringA'] = row['StringB'].upper() + '_'
            writer.writerow(row)

except mysql.connector.Error as err:
    print(f"Error: {err}")
finally:
    if cnx.is_connected():
        cursor.close()
        cnx.close()

