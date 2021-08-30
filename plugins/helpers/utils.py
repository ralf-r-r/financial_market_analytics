import boto3
from io import BytesIO
import pandas as pd

__all__ = ["upload_df_to_S3"]

def upload_df_to_S3(df: pd.DataFrame,
                    aws_access_key_id: str,
                    aws_secret_access_key: str,
                    region: str,
                    bucket: str,
                    key: str):

    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, header=True, index=False)
    csv_buffer.seek(0)

    s3_client = boto3.client('s3',
                             aws_access_key_id=aws_access_key_id,
                             aws_secret_access_key=aws_secret_access_key,
                             region_name=region)

    s3_client.upload_fileobj(csv_buffer, bucket, key)

    return None
