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

def s3_download(bucket, s3_key, download_dir=None):
    """
    Downloads a file from S3 and extracts it if it's a zip.
    
    Args:
        bucket (str): S3 bucket name.
        s3_key (str): Key of the file in S3.
        download_dir (str): Optional base directory to save & extract files.
                            Defaults to script directory.
    """
    # Set base directory
    base_dir = download_dir or os.getcwd()

    # Determine local file path
    filename = os.path.basename(s3_key)
    local_path = os.path.join(base_dir, filename)

    # Default extraction directory (same as zip filename)
    extract_to = os.path.splitext(local_path)[0]

    # Download if not already there
    if os.path.exists(local_path):
        print(f"[Skip] File already exists: {local_path}")
    else:
        try:
            print(f"[Download] {s3_key} from {bucket}")
            s3 = boto3.client("s3")
            os.makedirs(base_dir, exist_ok=True)
            filesize = s3.head_object(Bucket=bucket, Key=s3_key)['ContentLength']
            s3.download_file(bucket, s3_key, local_path, Callback=ProgressPercentage(filename, filesize))
            print("[Done] Download complete.")
        except NoCredentialsError:
            print("❌ AWS credentials not found.")
            return
        except ClientError as e:
            print(f"❌ S3 error: {e}")
            return

    # Extract if it's a zip
    if zipfile.is_zipfile(local_path):
        if os.path.exists(extract_to) and os.listdir(extract_to):
            print(f"[Skip] Already extracted to: {extract_to}")
        else:
            print(f"[Extract] Extracting to: {extract_to}")
            with zipfile.ZipFile(local_path, 'r') as zip_ref:
                os.makedirs(extract_to, exist_ok=True)
                zip_ref.extractall(extract_to)
            print("[Done] Extraction complete.")
    else:
        print(f"[Skip] Not a zip — no extraction needed.")
