from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadRedShiftOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 rs_conn_id: str,
                 prior_truncate: bool,
                 rs_table_name: str,
                 sql_insert: str,
                 *args,
                 **kwargs):
        """
        :param rs_conn_id: str, redshift connection id
        :param prior_truncate: bool, whether to truncate the table before inserting data
        :param rs_table_name: str, the name of the table
        :param sql_insert: str, sql statement to insert data
        """
        super(LoadRedShiftOperator, self).__init__(*args, **kwargs)

        self.rs_conn_id = rs_conn_id
        self.rs_table_name = rs_table_name
        self.prior_truncate = prior_truncate
        self.sql_insert = sql_insert

    def execute(self, context):
        self.log.info('LoadFactOperator: Starting to copy data into ' + self.rs_table_name)

        redshift = PostgresHook(postgres_conn_id=self.rs_conn_id)

        if self.prior_truncate:
            redshift.run("TRUNCATE TABLE {} ".format(self.rs_table_name))
            redshift.run(self.sql_insert)
        else:
            redshift.run(self.sql_insert)

        self.log.info('LoadFactOperator: Copying data into ' + self.rs_table_name + ' completed')