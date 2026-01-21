import argparse
from google.cloud import storage

def main():
    parser = argparse.ArgumentParser(description="Create a Google Cloud Storage bucket.")
    parser.add_argument("bucket_name", type=str, help="The name of the bucket to create.")
    args = parser.parse_args()

    bucket_name = args.bucket_name
    print(f"bucket_name received: {bucket_name}")
    storage_client = storage.Client(project="gcp-data-engineer-curso-485002")
    bucket = storage_client.bucket(bucket_name)
    bucket.storage_class = "STANDARD"
    new_bucket = storage_client.create_bucket(bucket, location="us-central1")
    print(f"Bucket {new_bucket.name} created in {new_bucket.location} with storage class {new_bucket.storage_class}")

if __name__ == "__main__":
    main()
    