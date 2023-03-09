from airflow import DAG
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryCheckOperator
from airflow.models import Variable

from datetime import datetime, timedelta
import os 

# Local machine throws "Negsignal.SIGSEGV" error if this isn't specified. 
os.environ["no_proxy"]="*"


class ExtractDataFromGoogleTrends(BaseOperator):
    '''
    Extracts Google Trends data into a dataframe based on extract_start and extract_end dates.
    After extraction, uploads the dataframe to BigQuery table.

    :param extract_start: Start date for Google Trends time interval.
    :type extract_start: str
    :param extract_end: End date for Google Trends time interval.
    :type extract_end: str
    :param project_id: Project ID in BigQuery
    :type project_id: str
    :param dataset: Dataset in BigQuery
    :type dataset: str
    :param table_name: Target table where the data will be uploaded
    :type table_name: str
    :param kw_list: Keyword list which are used as search terms for Google Trends 
    :type kw_list: List[str]
    '''
    template_fields = ('extract_start', 'extract_end', 'project_id', 'dataset', 'table_name')

    def __init__(
            self,
            extract_start: str,
            extract_end: str,
            project_id: str,
            dataset: str,
            table_name: str,
            kw_list: list = ['vpn', 'hack', 'cyber', 'security', 'wifi'],
            **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.extract_start = extract_start
        self.extract_end = extract_end
        self.project_id = project_id
        self.dataset = dataset
        self.table_name = table_name
        self.kw_list = kw_list

    
    def execute(self, context: dict):
        from pytrends.request import TrendReq

        self.log.info(f'Start date: {self.extract_start}, End date: {self.extract_end}')

        pytrend = TrendReq()
        
        pytrend.build_payload(kw_list=self.kw_list,
                            timeframe=f"{self.extract_start} {self.extract_end}")
        
        df = pytrend.interest_by_region(inc_low_vol=True)
        self.log.info('Data has been fetched from Google Trends.')

        df = df.reset_index()

        df['geoName'] = df['geoName'].astype('str')
        df['start_date'] = datetime.strptime(self.extract_start, '%Y-%m-%d').date()
        df['end_date'] = datetime.strptime(self.extract_end, '%Y-%m-%d').date()
        df['batch_date'] = datetime.today().date()

        bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default', use_legacy_sql=False).get_client()

        try:
            bq_hook.get_table(f'{self.project_id}.{self.dataset}.{self.table_name}')
        except:
            self.create_google_trends_table(table_hook=bq_hook)

        table_name = bq_hook.get_table(f'{self.project_id}.{self.dataset}.{self.table_name}')

        bq_hook.insert_rows_from_dataframe(table_name, df, table_name.schema)

        return True

    def create_google_trends_table(self, table_hook):
        '''
        Creates a dedicated table for ingesting Google Trends data.
        '''
        table_hook.query(f'''
            CREATE TABLE IF NOT EXISTS {self.project_id}.{self.dataset}.{self.table_name} (
                geoName STRING,
                vpn INT64,
                hack INT64,
                cyber INT64,
                security INT64,
                wifi INT64,
                start_date DATE,
                end_date DATE,
                batch_date DATE
            )
            PARTITION BY start_date;''')

    def drop_google_trends_table(self, table_hook):
        '''
        Drops Google Trends table.
        '''
        table_hook.query(f'DROP TABLE IF EXISTS {self.project_id}.{self.dataset}.{self.table_name};')


default_args = {
    'owner' : 'airflow',
    'retries' : 1,
    'retry_delay' : timedelta(seconds=10),
    'email_on_failure' : False, 
    'email_on_retry' : False,
    'depends_on_past' : False
}

def create_dag(
        dag_id,
        description,
        max_active_runs,
        start_date,
        schedule_interval,
        dagrun_timeout,
        catchup,
        default_args,
        doc_md
):
    dag = DAG(
        dag_id=dag_id,
        description=description,
        max_active_runs=max_active_runs,
        start_date=start_date,
        schedule_interval=schedule_interval,
        dagrun_timeout=dagrun_timeout,
        catchup=catchup,
        default_args=default_args,
        doc_md=doc_md
    )

    with dag:
        start_date = '{{ dag_run.conf["start_date"] if dag_run.conf else data_interval_start | ds }}'
        end_date = '{{ dag_run.conf["end_date"] if dag_run.conf else data_interval_end | ds }}'

        ExtractGoogleTrendsData = ExtractDataFromGoogleTrends(
            task_id='extract_google_trends_data',
            extract_start=f'{start_date}',
            extract_end=f'{end_date}',
            project_id='{{ var.value.project_id }}',
            dataset='{{ var.value.dataset }}',
            table_name='{{ var.value.raw_table }}',
            doc_md='''Extract Google Trends data and upload it to "raw" table inside BigQuery.'''
        )

        PrepareData = BigQueryInsertJobOperator(
            task_id='prepare_data',
            gcp_conn_id='google_cloud_default',
            configuration={
                'query' : {
                    'query' : '{% include "templates/clean_transpose_data.sql" %}',
                    'useLegacySql' : False
                }
            },
            params={
                'project_id': Variable.get('project_id'),
                'dataset': Variable.get('dataset'),
                'raw_table': Variable.get('raw_table'),
                'staging_table': Variable.get('staging_table')
            },
            trigger_rule='all_done',
            doc_md='Deduplicates data, changes the structure of the dataset and insert data into "staging" table.'
        )

        TransformData = BigQueryInsertJobOperator(
            task_id='transform_data',
            gcp_conn_id='google_cloud_default',
            configuration={
                'query' : {
                    'query' : '{% include "templates/transformed_data.sql" %}',
                    'useLegacySql' : False
                }
            },
            params={
                'project_id': Variable.get('project_id'),
                'dataset': Variable.get('dataset'),
                'staging_table': Variable.get('staging_table'),
                'final_table': Variable.get('final_table')
            },
            doc_md='Calculates ranking for each search term based on search term interest and inserts data into "final" table.'
        )

        CheckRawDuplicates = BigQueryCheckOperator(
            task_id='check_raw_table_duplicates',
            gcp_conn_id='google_cloud_default',
            sql=f'''SELECT 
                        COUNT(*) = 0
                    FROM (
                        SELECT 
                            geoName,
                            start_date,
                            end_date,
                            count(*)
                        FROM 
                            {Variable.get('project_id')}.{Variable.get('dataset')}.{Variable.get('raw_table')}
                        GROUP BY
                            geoName,
                            start_date,
                            end_date
                        HAVING COUNT(*) > 1);''',
            use_legacy_sql=False,
            doc_md='Validation if there are no duplicates in the raw data set, based on geoName, start_date and end_date'
        )

        CheckStagingDuplicates = BigQueryCheckOperator(
            task_id='check_staging_table_duplicates',
            gcp_conn_id='google_cloud_default',
            sql=f'''SELECT 
                        COUNT(*) = 0
                    FROM (
                        SELECT 
                            country,
                            search_term,
                            start_date,
                            end_date,
                            count(*)
                        FROM 
                            {Variable.get('project_id')}.{Variable.get('dataset')}.{Variable.get('staging_table')}
                        GROUP BY
                            country,
                            search_term,
                            start_date,
                            end_date
                        HAVING COUNT(*) > 1);''',
            use_legacy_sql=False,
            doc_md='Validation if there are no duplicates in the staging data set, based on country, search_term, start_date and end_date'
        )

        CheckTransformedDuplicates = BigQueryCheckOperator(
            task_id='check_transformed_tableduplicates',
            gcp_conn_id='google_cloud_default',
            sql=f'''SELECT 
                        COUNT(*) = 0
                    FROM (
                        SELECT 
                            country,
                            search_term,
                            start_date,
                            end_date,
                            rank,
                            count(*)
                        FROM 
                            {Variable.get('project_id')}.{Variable.get('dataset')}.{Variable.get('final_table')}
                        GROUP BY
                            country,
                            search_term,
                            start_date,
                            end_date,
                            rank
                        HAVING COUNT(*) > 1);''',
            use_legacy_sql=False,
            doc_md='Validation if there are no duplicates in the transformed data set, based on country, start_date and end_date'
        )

        ExtractGoogleTrendsData >> PrepareData >> TransformData >> [CheckRawDuplicates, CheckStagingDuplicates, CheckTransformedDuplicates]
    
    return dag


globals()["google_trends_weekly_dag"] = create_dag(
    dag_id='google_trends_weekly_dag',
    description='DAG that extracts weekly google trends data',
    max_active_runs=1,
    start_date=datetime(2023, 2, 26),
    schedule_interval="@weekly",
    dagrun_timeout=timedelta(hours=1),
    catchup=False,
    default_args=default_args,
    doc_md='''
    This DAG ingests weekly Google Trends data and uploads it to the "raw" table. 
    Table name is stored in Airflow UI, along with Project ID and Dataset ID, provided that "gcp_conn_id" can access the table.
    Data ingestion is not idempotent, but data inserts to "staging" and "final" tables are.
    '''
)

globals()["google_trends_backfill_dag"] = create_dag(
    dag_id='google_trends_backfill_dag',
    description='DAG that extracts historical google trends data',
    max_active_runs=1,
    start_date=datetime(2020, 1, 1),
    schedule_interval="@weekly",
    dagrun_timeout=timedelta(hours=1),
    catchup=True,
    default_args=default_args,
    doc_md='''
    This DAG backfills Google Trends data and uploads it to the "raw" table. Connections and variables are the same as for Weekly DAG.
    Manual DAG runs can be triggered, providing "start_date" and "end_date" (strings must be doube quote), e.g.
    {"start_date":"2023-01-01", "end_date":"2023-01-08"}
    '''
)