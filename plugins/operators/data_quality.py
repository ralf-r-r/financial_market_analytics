from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import operator


class sqlDataQualityCheck():
    """
    this class allows to create data quality checks from sql statements
    """

    def __init__(self,
                 sql_query: str,
                 expected_result: dict,
                 error_message: str,
                 success_message: str
                 ):

        operator_dict = {'>': operator.lt,
                         '<=': operator.le,
                         '>=': operator.ge,
                         '<': operator.gt,
                         '==': operator.eq,
                         '!=': operator.ne
                         }

        self.operator_dict = operator_dict
        self.sql_query = sql_query
        self.expected_result = expected_result
        self.error_message = error_message
        self.success_message = success_message

    def run_check(self, postgres_hook: PostgresHook, logger):
        """
        :param postgres_hook: airflow.hooks PostgresHook
        :param logger: logger object
        :return: None
        """
        result_of_query = postgres_hook.get_records(self.sql_query)[0]
        if self.operator_dict[self.expected_result["condition"]](result_of_query[0], self.expected_result["value"]):
            logger.info(self.success_message)
        else:
            logger.info(self.error_message)
            logger.info("result of query:" + str(result_of_query))
            raise ValueError(self.error_message)

        return None


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 rs_conn_id: str,
                 data_quality_checks: list,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.rs_conn_id = rs_conn_id
        self.data_quality_checks = data_quality_checks

    def execute(self, context):
        self.log.info('Quality checks: starting')

        # create data quality checks:
        quality_checks_list = []
        for quality_check in self.data_quality_checks:
            quality_checks_list.append(sqlDataQualityCheck(**quality_check))

        # create postgreshook
        redshift = PostgresHook(postgres_conn_id=self.rs_conn_id)

        # run data quality checks:
        for quality_test in quality_checks_list:
            quality_test.run_check(redshift, self.log)

        self.log.info('Quality checks: finished')
