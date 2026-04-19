import boto3
import json
import re
import logging
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

def lambda_handler(event, context):
    """
    Creates a private S3 bucket for a new user.

    Expected event payload:
    {
        "user_id": "usr_abc123",
        "email": "jane@example.com",
        "region": "us-east-1"   # optional, defaults to us-east-1
    }
    """

    # --- 1. Parse and validate input ---
    user_id = event.get("user_id")
    email   = event.get("email", "unknown")
    region  = event.get("region", "us-east-1")

    if not user_id:
        return error_response(400, "Missing required field: user_id")

    # Sanitize user_id to meet S3 naming rules (lowercase, alphanumeric + hyphens)
    safe_id     = re.sub(r"[^a-z0-9\-]", "-", user_id.lower())
    bucket_name = f"userdata-{safe_id}"

    logger.info(f"Creating bucket '{bucket_name}' for user '{user_id}' ({email})")

    # --- 2. Create the bucket ---
    try:
        if region == "us-east-1":
            s3.create_bucket(Bucket=bucket_name)
        else:
            s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region}
            )
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "BucketAlreadyOwnedByYou":
            logger.warning(f"Bucket '{bucket_name}' already exists — continuing.")
        elif code == "BucketAlreadyExists":
            return error_response(409, f"Bucket name '{bucket_name}' is taken globally.")
        else:
            logger.error(f"Failed to create bucket: {e}")
            return error_response(500, f"AWS error: {str(e)}")

    # --- 3. Block all public access ---
    try:
        s3.put_public_access_block(
            Bucket=bucket_name,
            PublicAccessBlockConfiguration={
                "BlockPublicAcls":       True,
                "IgnorePublicAcls":      True,
                "BlockPublicPolicy":     True,
                "RestrictPublicBuckets": True,
            }
        )
    except ClientError as e:
        return error_response(500, f"Bucket created but security config failed: {str(e)}")

    # --- 4. Enable versioning ---
    try:
        s3.put_bucket_versioning(
            Bucket=bucket_name,
            VersioningConfiguration={"Status": "Enabled"}
        )
    except ClientError as e:
        logger.warning(f"Versioning not applied: {e}")

    # --- 5. Tag the bucket with owner metadata ---
    try:
        s3.put_bucket_tagging(
            Bucket=bucket_name,
            Tagging={"TagSet": [
                {"Key": "owner_id",    "Value": user_id},
                {"Key": "owner_email", "Value": email},
                {"Key": "env",         "Value": "production"},
            ]}
        )
    except ClientError as e:
        logger.warning(f"Tagging failed: {e}")

    logger.info(f"Bucket '{bucket_name}' ready for user '{user_id}'")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message":     "Bucket created successfully",
            "bucket_name": bucket_name,
            "user_id":     user_id,
            "region":      region,
        })
    }


def error_response(status_code, message):
    logger.error(message)
    return {
        "statusCode": status_code,
        "body":       json.dumps({"error": message})
    }
