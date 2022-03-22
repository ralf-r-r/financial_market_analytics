from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


def month_to_quarter(month: int) -> str:
    """
    Converts the month number of a date to a string for the quarter e.g. Q4
    :param month: int, month of the date
    :return: str, the quarter of the year, e.g. Q2
    """
    quarter = (month - 1) // 3 + 1
    return "Q" + str(quarter)

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ('date',)

    @apply_defaults
    def __init__(self,
                 aws_credentials_id: str,
                 rs_conn_id: str,
                 rs_target_table: str,
                 s3_bucket: str,
                 date: str,
                 s3_key: str = '',
                 s3_region: str = 'eu-central-1',
                 use_date=True,
                 *args,
                 **kwargs):
        """
        :param aws_credentials_id: str, aws credentials id
        :param rs_conn_id: str, redshift connection id
        :param rs_target_table: str, the name of the target table
        :param s3_bucket: str, name of the aws s3 bucket
        :param s3_key: str, name of the s3 bucket key
        :param s3_region: str, the region of the s3 bucket
        """
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.aws_credentials_id = aws_credentials_id
        self.rs_conn_id = rs_conn_id
        self.rs_target_table = rs_target_table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_region = s3_region
        self.date = date
        self.use_date = use_date

    def execute(self, context):

        # get year and quarter from date
        date_str = self.date.format(**context)
        year, month, day = date_str.split("-")
        quarter = month_to_quarter(int(month))

        # set bucket path
        bucket_path = self.s3_bucket + self.s3_key
        if self.use_date:
            bucket_path = bucket_path.replace("year",str(year))
            bucket_path = bucket_path.replace("quarter", quarter)

        self.log.info('starting staging from ' + bucket_path + ' to ' + self.rs_target_table)

        redshift = PostgresHook(postgres_conn_id=self.rs_conn_id)
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()

        # - delete rows in the staging table
        delete_rows = """DELETE {}""".format(self.rs_target_table)
        redshift.run(delete_rows)

        # - load new data from s3 bucket into the staging table
        staging_copy = """
                COPY {}
                FROM '{}'
                ACCESS_KEY_ID '{}'
                SECRET_ACCESS_KEY '{}'
                REGION AS '{}'
                FORMAT AS CSV 
                EMPTYASNULL
                BLANKSASNULL
                COMPUPDATE OFF
                IGNOREHEADER 1
        """

        staging_copy_formatted = staging_copy.format(self.rs_target_table,
                                                     bucket_path,
                                                     credentials.access_key,
                                                     credentials.secret_key,
                                                     self.s3_region)

        redshift.run(staging_copy_formatted)

        self.log.info('loading from ' + self.s3_bucket + self.s3_key + ' to ' + self.rs_target_table + ' completed')