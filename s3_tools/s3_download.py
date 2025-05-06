import os
import boto3
import zipfile
from tqdm import tqdm
from botocore.exceptions import NoCredentialsError, ClientError

class ProgressPercentage:
    def __init__(self, filename, filesize):
        self.filename = filename
        self.filesize = filesize
        self._seen_so_far = 0
        self._tqdm = tqdm(total=filesize, unit='B', unit_scale=True, desc=filename)

    def __call__(self, bytes_amount):
        self._seen_so_far += bytes_amount
        self._tqdm.update(bytes_amount)

def download_and_extract_s3_zip(bucket, s3_key, local_path, extract_to=None):
    """
    Downloads a file from S3 (with progress), and extracts if it's a zip.
    - Skips download if already present locally.
    - Only extracts if the file is a ZIP and extract_to is given.

    Args:
        bucket (str): S3 bucket name
        s3_key (str): Path in bucket
        local_path (str): Where to save locally
        extract_to (str): Where to extract (if file is a zip)
    """
    # Download if needed
    if os.path.exists(local_path):
        print(f"File already exists: {local_path}")
    else:
        try:
            print(f"Downloading {s3_key} from {bucket}...")
            s3 = boto3.client("s3")
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            filesize = s3.head_object(Bucket=bucket, Key=s3_key)['ContentLength']
            s3.download_file(bucket, s3_key, local_path, Callback=ProgressPercentage(s3_key, filesize))
            print("Download complete.")
        except NoCredentialsError:
            print("ERROR: AWS credentials not found.")
            return
        except ClientError as e:
            print(f"ERROR: S3 error: {e}")
            return

    # Only extract if it’s a zip and user asked to extract
    if extract_to:
        if zipfile.is_zipfile(local_path):
            if os.path.exists(extract_to) and os.listdir(extract_to):
                print(f"Already extracted to: {extract_to}")
            else:
                print(f"Extracting to: {extract_to}")
                with zipfile.ZipFile(local_path, 'r') as zip_ref:
                    os.makedirs(extract_to, exist_ok=True)
                    zip_ref.extractall(extract_to)
                print("Extraction complete.")
        else:
            print(f"File is not a ZIP archive — skipping extraction.")