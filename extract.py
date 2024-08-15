!pip install faker google-cloud-storage
import csv
import string
import random
from faker import Faker
import pandas as pd
from google.cloud import storage

# Initialize Faker
fake = Faker()

# Function to generate unique employee IDs
def generate_unique_ids(num_records):
    return random.sample(range(100000, 999999), num_records)

# Function to generate employee data
def generate_employee_data(num_records=10):
    employee_data = []
    password_characters = string.ascii_letters + string.digits + 'm'

    for _ in range(num_records):
        employee_data.append({
            "employee_id": random.randint(100000, 999999),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "job_title": fake.job(),
            "department": fake.job(),  # Generate department-like data using the job() method
            "email": fake.email(),
            "address": fake.address().replace("\n", " "),  # Replace newlines in address
            "phone_number": fake.phone_number(),
            "salary": str(fake.random_number(digits=5)),  # Generate a random 5-digit salary
            "password": ''.join(random.choice(password_characters) for _ in range(8))  # Generate an 8-character password
        })
    return employee_data

# Function to save data to CSV
def save_to_csv(data, filename):
    fieldnames = ['employee_id', 'first_name', 'last_name', 'job_title', 'department', 'email', 'address', 'phone_number', 'salary', 'password']
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        for row in data:
            writer.writerow(row)

# Function to upload file to GCS
def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print(f"File {source_file_name} uploaded to {destination_blob_name} in bucket {bucket_name}.")

if __name__ == "__main__":
    num_records = 100  
    employees = generate_employee_data(num_records)
    csv_filename = "employee_data.csv"
    save_to_csv(employees, csv_filename)
    bucket_name = "bkt-employee-data"  
    upload_to_gcs(bucket_name, csv_filename, csv_filename)
