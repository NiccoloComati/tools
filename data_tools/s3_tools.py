import os
import zipfile
import boto3

def process_zip_files(filenames, bucket, data_dir="data"):
    if isinstance(filenames, str):
        filenames = [filenames]
    
    s3 = boto3.client("s3")

    for filename in filenames:
        zip_path = os.path.join(data_dir, filename)
        csv_target = os.path.join(data_dir, filename[:-4] + ".csv")

        if filename in os.listdir(data_dir):
            if os.path.exists(csv_target):
                print(f"{csv_target} already extracted")
            else:
                print(f"{filename} already downloaded, extracting...")
                with zipfile.ZipFile(zip_path, "r") as zip_ref:
                    extracted_files = zip_ref.namelist()
                    zip_ref.extractall(data_dir)
                    if len(extracted_files) == 1:
                        old_path = os.path.join(data_dir, extracted_files[0])
                        os.rename(old_path, csv_target)
                    else:
                        print(f"Warning: {filename} contained multiple files, skipping rename")
        else:
            print(f"{filename} not found, downloading...")
            with open(zip_path, "wb") as f:
                s3.download_fileobj(bucket, filename, f)
            print(f"{filename} downloaded, extracting...")
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                extracted_files = zip_ref.namelist()
                zip_ref.extractall(data_dir)
                if len(extracted_files) == 1:
                    old_path = os.path.join(data_dir, extracted_files[0])
                    os.rename(old_path, csv_target)
                else:
                    print(f"Warning: {filename} contained multiple files, skipping rename")