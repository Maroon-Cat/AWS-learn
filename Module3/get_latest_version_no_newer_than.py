import argparse
from datetime import timezone

import boto3
from dateutil import parser as date_parser

parser = argparse.ArgumentParser(description="Download S3 file version not newer than given date.")
parser.add_argument("--key", required=True, help="S3 object key (file path/name)")
parser.add_argument("--cutoff_date", required=True, help="Cutoff datetime in ISO format YYYY-MM-DDTHH:MM:SS (e.g. 2025-04-10T15:00:00)")
parser.add_argument("--output", default="downloaded.txt", help="Local file path and name to save the version")
parser.add_argument("--AWSprofile", default="S3User_1", help="AWS profile name to use")
parser.add_argument("--bucket", default="semyon-koltsov-tset-2-bucket", help = "Bucket name in AWS (ARN)")
args = parser.parse_args()

bucket = args.bucket
profile_name = args.AWSprofile
key = args.key
output_file = args.output
cutoff_date = date_parser.parse(args.cutoff_date)
if cutoff_date.tzinfo is None:
    cutoff_date = cutoff_date.replace(tzinfo=timezone.utc)


session = boto3.Session(profile_name=profile_name)
s3 = session.client("s3")


versions = s3.list_object_versions(Bucket=bucket, Prefix=key).get("Versions", [])
candidates = [
    v for v in versions
    if v["Key"] == key and v["LastModified"] <= cutoff_date
]

if not candidates:
    print("❌ No suitable version found.")
else:
    latest_version = sorted(candidates, key=lambda v: v["LastModified"], reverse=True)[0]
    version_id = latest_version["VersionId"]

    print(f"✅ Downloading version {version_id} from {latest_version['LastModified']} to {output_file}")
    s3.download_file(Bucket=bucket, Key=key, Filename=output_file, ExtraArgs={"VersionId": version_id})
    print("✅ Done.")