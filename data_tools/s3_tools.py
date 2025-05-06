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

def s3_download(bucket, s3_key, local_path=None, extract_to=None):
    """
    Downloads a file from S3 (with progress), and extracts if it's a zip.
    - If local_path not given, saves in current dir mirroring S3 structure.
    - If extract_to not given and file is zip, extracts next to zip with same name.
    - Skips download if file already exists.
    - Skips extraction if already done.
    """
    # Set local_path if not provided
    if local_path is None:
        local_path = os.path.join(os.getcwd(), s3_key.replace("/", os.sep))
        print(f"[Info] local_path not provided — using: {local_path}")

    # Set extract_to if not provided but file is a zip
    if extract_to is None and local_path.endswith(".zip"):
        extract_to = os.path.splitext(local_path)[0]  # Same name, no .zip
        print(f"[Info] extract_to not provided — will extract to: {extract_to}")

    # Download if file doesn't exist
    if os.path.exists(local_path):
        print(f"[Skip] File already exists: {local_path}")
    else:
        try:
            print(f"[Download] {s3_key} from {bucket}")
            s3 = boto3.client("s3")
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            filesize = s3.head_object(Bucket=bucket, Key=s3_key)['ContentLength']
            s3.download_file(bucket, s3_key, local_path, Callback=ProgressPercentage(s3_key, filesize))
            print("[Done] Download complete.")
        except NoCredentialsError:
            print("❌ AWS credentials not found.")
            return
        except ClientError as e:
            print(f"❌ S3 error: {e}")
            return

    # Extraction logic
    if extract_to:
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
            print(f"[Skip] File is not a ZIP archive — no extraction done.")
