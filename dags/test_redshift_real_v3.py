from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago
import pandas as pd
import requests
import datetime
import os

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    dag_id='dust_data_pipeline',
    default_args=default_args,
    description='A complex data pipeline with multiple stages',
    schedule_interval='@hourly',
    start_date=days_ago(1),
    catchup=False,
)

def fetch_data_from_api():
    # 현재 날짜를 YYYYMMDDHH 형식으로 얻기 (예: 1시간 전 데이터 요청), 분은 꼭 00분으로 고정되어야함 안그러면 안됨.
    current_datetime = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime("%Y%m%d%H00")
    
    # API URL (json 형식으로 요청)
    api_url = f"http://openapi.seoul.go.kr:8088/4b746a725579756e35386955445a73/json/TimeAverageCityAir/1/100/{current_datetime}"
    response = requests.get(api_url)
    try:
        response.raise_for_status()  # HTTP 응답 상태 코드 확인
        data = response.json()
    except requests.exceptions.HTTPError as http_err:
        raise ValueError(f"HTTP error occurred: {http_err}")
    except requests.exceptions.RequestException as req_err:
        raise ValueError(f"Request exception occurred: {req_err}")
    except ValueError as json_err:
        raise ValueError(f"JSON decode error: {json_err}")
    
    if "TimeAverageCityAir" not in data or "row" not in data["TimeAverageCityAir"]:
        raise ValueError("No data returned from API")
    
    items = data["TimeAverageCityAir"]["row"]
    if not items:
        raise ValueError("No data available for the requested date and time.")
    
    df = pd.DataFrame(items)

    # 컬럼명을 ERD의 영어 이름으로 변경, 추후 redshift에 적재하기 편한 형태로
    df.columns = [
        'date', 'region_code', 'region_name', 'office_code', 'office_name',
        'dust_1h', 'dust_24h', 'ultradust', 'O3', 'NO2', 'CO', 'SO2'
    ]
    # 데이터프레임을 UTF-8 인코딩으로 CSV 형식의 문자열로 변환,utf-8로하면 안되고 꼭 sig를 붙여 줘야함.
    csv_data = df.to_csv(index=False, encoding='utf-8-sig')
    
    # 현재 작업 디렉토리를 사용하여 파일 저장
    file_path = os.path.join(os.getcwd(), 'api_raw_data.csv')
    with open(file_path, 'w', encoding='utf-8-sig') as f:
        f.write(csv_data)
    
    return file_path

def create_raw_data_temp_table():
    # redshift 연결
    redshift_hook = PostgresHook(postgres_conn_id='redshift_test_dev')
    
    # api_raw_data_temp 테이블 생성
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS yusuyeon678.api_raw_data_temp (
        date BIGINT NOT NULL,
        region_code INT NOT NULL,
        region_name VARCHAR(20) NOT NULL,
        office_code INT NOT NULL,
        office_name VARCHAR(20) NOT NULL,
        dust FLOAT NOT NULL,
        dust_24h FLOAT NOT NULL,
        ultradust FLOAT NOT NULL,
        O3 FLOAT NOT NULL,
        NO2 FLOAT NOT NULL,
        CO FLOAT NOT NULL,
        SO2 FLOAT NOT NULL
    );
    """
    redshift_hook.run(create_table_sql)

def load_data_into_raw_data_temp():
    # redshift 연결
    redshift_hook = PostgresHook(postgres_conn_id='redshift_test_dev')
    aws_hook = S3Hook(aws_conn_id='aws_s3')
    credentials = aws_hook.get_credentials()
    
    # s3로부터 데이터 copy해오기
    load_sql = f"""
    COPY yusuyeon678.api_raw_data_temp
    FROM 's3://dust-dag/dataSource/api_raw_data.csv'
    credentials
    'aws_access_key_id={credentials.access_key};aws_secret_access_key={credentials.secret_key}'
    csv
    IGNOREHEADER 1;
    """
    redshift_hook.run(load_sql)

def deduplicate_and_insert_data():
    # redshift 연결
    redshift_hook = PostgresHook(postgres_conn_id='redshift_test_dev')
    conn = redshift_hook.get_conn()
    cursor = conn.cursor()
    
    # 중복 데이터 제거 및 삽입 쿼리 실행
    cursor.execute("""
        BEGIN;
        
        -- 중복되지 않은 데이터만 삽입
        INSERT INTO yusuyeon678.raw_data_test_youngjun
        SELECT * FROM yusuyeon678.api_raw_data_temp
        EXCEPT
        SELECT * FROM yusuyeon678.raw_data_test_youngjun;
        
        -- 임시 테이블 데이터 삭제
        DELETE FROM yusuyeon678.api_raw_data_temp;
        
        COMMIT;
    """)
    
    conn.close()

def convert_date_format():
    # redshift 연결
    redshift_hook = PostgresHook(postgres_conn_id='redshift_test_dev')
    
    # raw_data 삭제 후, 재생성
    create_table_query = """
    DROP TABLE IF EXISTS yusuyeon678.raw_data;
    CREATE TABLE yusuyeon678.raw_data (
        date TIMESTAMP NOT NULL,
        region_code INT NOT NULL,
        region_name VARCHAR(20) NOT NULL,
        office_code INT NOT NULL,
        office_name VARCHAR(20) NOT NULL,
        dust FLOAT NOT NULL,
        dust_24h FLOAT NOT NULL,
        ultradust FLOAT NOT NULL,
        O3 FLOAT NOT NULL,
        NO2 FLOAT NOT NULL,
        CO FLOAT NOT NULL,
        SO2 FLOAT NOT NULL,
        constraint raw_data_pk PRIMARY KEY (date, region_code, region_name, office_code, office_name)
    );
    """
    redshift_hook.run(create_table_query)
    
    # date 컬럼의 형식 변경 후, raw_data_temp 테이블에서부터 raw_data로 데이터 복제
    sql_query = """
    INSERT INTO yusuyeon678.raw_data (date, region_code, region_name, office_code, office_name, dust, dust_24h, ultradust, O3, NO2, CO, SO2)
    SELECT
        TO_TIMESTAMP(CAST(date AS VARCHAR), 'YYYYMMDDHH24MI') AS date,
        region_code,
        region_name,
        office_code,
        office_name,
        dust,
        dust_24h,
        ultradust,
        O3,
        NO2,
        CO,
        SO2
    FROM yusuyeon678.raw_data_test_youngjun;
    """
    
    redshift_hook.run(sql_query)

def populate_region_table():
    # redshift 연결
    redshift_hook = PostgresHook(postgres_conn_id='redshift_test_dev')
    
    # group_by_region 테이블 삭제 후, 재생성
    create_table_query = """
    DROP TABLE IF EXISTS yusuyeon678.group_by_region;
    CREATE TABLE yusuyeon678.group_by_region (
        date TIMESTAMP NOT NULL,
        region_code INT NOT NULL,
        region_name VARCHAR(20) NOT NULL,
        dust FLOAT NOT NULL,
        ultradust FLOAT NOT NULL,
        O3 FLOAT NOT NULL,
        NO2 FLOAT NOT NULL,
        CO FLOAT NOT NULL,
        SO2 FLOAT NOT NULL,
        CONSTRAINT group_by_region_pk PRIMARY KEY (date, region_code)
    );
    """
    redshift_hook.run(create_table_query)
    
    # raw_data 테이블로부터 데이터를 가져와 지역별, 시간별로 그룹 지어 평균 낸 group_by_region 테이블 생성
    sql_query = """
    INSERT INTO yusuyeon678.group_by_region (date, region_code, region_name, dust, ultradust, O3, NO2, CO, SO2)
    SELECT
        date,
        region_code,
        region_name,
        ROUND(AVG(dust), 2) AS dust,
        ROUND(AVG(ultradust), 2) AS ultradust,
        ROUND(AVG(o3), 2) AS o3,
        ROUND(AVG(no2), 2) AS no2,
        ROUND(AVG(co), 2) AS co,
        ROUND(AVG(so2), 3) AS so2
    FROM yusuyeon678.raw_data
    GROUP BY date, region_code, region_name;
    """
    
    redshift_hook.run(sql_query)

def populate_office_table():
    # redshift 연결
    redshift_hook = PostgresHook(postgres_conn_id='redshift_test_dev')
    
    # group_by_office 테이블 삭제 후, 재생성
    create_table_query = """
    DROP TABLE IF EXISTS yusuyeon678.group_by_office;
    CREATE TABLE yusuyeon678.group_by_office (
        date TIMESTAMP NOT NULL,
        office_code INT NOT NULL,
        office_name VARCHAR(20) NOT NULL,
        dust FLOAT NOT NULL,
        ultradust FLOAT NOT NULL,
        O3 FLOAT NOT NULL,
        NO2 FLOAT NOT NULL,
        CO FLOAT NOT NULL,
        SO2 FLOAT NOT NULL,
        CONSTRAINT group_by_office_pk PRIMARY KEY (date, office_code)
    );
    """
    redshift_hook.run(create_table_query)
    
    # raw_data 테이블로부터 데이터를 가져와 측정소별, 시간별로 그룹 지어 평균 낸 group_by_office 테이블 생성
    sql_query = """
    INSERT INTO yusuyeon678.group_by_office (date, office_code, office_name, dust, ultradust, O3, NO2, CO, SO2)
    SELECT
        date,
        office_code,
        office_name,
        ROUND(AVG(dust), 2) AS dust,
        ROUND(AVG(ultradust), 2) AS ultradust,
        ROUND(AVG(o3), 2) AS o3,
        ROUND(AVG(no2), 2) AS no2,
        ROUND(AVG(co), 2) AS co,
        ROUND(AVG(so2), 3) AS so2
    FROM yusuyeon678.raw_data
    GROUP BY date, office_code, office_name;
    """
    
    redshift_hook.run(sql_query)

def populate_dust_summary_table():
    # redshift 연결
    redshift_hook = PostgresHook(postgres_conn_id='redshift_test_dev')
    
    # seoul_dust_summary 테이블 삭제 후, 재생성
    create_table_query = """
    DROP TABLE IF EXISTS yusuyeon678.seoul_dust_summary;
    CREATE TABLE yusuyeon678.seoul_dust_summary (
        date TIMESTAMP primary key,
        dust FLOAT NOT NULL
    );
    """
    redshift_hook.run(create_table_query)
    
    # raw_data 테이블로부터 데이터를 가져와 시간별로 미세먼지의 평균량을 제공하는 seoul_dust_summary 테이블 생성
    sql_query = """
    INSERT INTO yusuyeon678.seoul_dust_summary (date, dust)
    SELECT
        date,
        ROUND(AVG(dust), 2) AS dust
    FROM yusuyeon678.raw_data
    GROUP BY date;
    """
    
    redshift_hook.run(sql_query)

def populate_ultradust_summary_table():
    # redshift 연결
    redshift_hook = PostgresHook(postgres_conn_id='redshift_test_dev')
    
    # seoul_ultradust_summary 테이블 삭제 후, 재생성
    create_table_query = """
    DROP TABLE IF EXISTS yusuyeon678.seoul_ultradust_summary;
    CREATE TABLE yusuyeon678.seoul_ultradust_summary (
        date TIMESTAMP primary key,
        ultradust FLOAT NOT NULL
    );
    """
    redshift_hook.run(create_table_query)
    
    # raw_data 테이블로부터 데이터를 가져와 시간별로 초미세먼지의 평균량을 제공하는 seoul_dust_summary 테이블 생성
    sql_query = """
    INSERT INTO yusuyeon678.seoul_ultradust_summary (date, ultradust)
    SELECT
        date,
        ROUND(AVG(ultradust), 2) AS ultradust
    FROM yusuyeon678.raw_data
    GROUP BY date;
    """
    
    redshift_hook.run(sql_query)

# s3 용량 관리를 위해 주기적으로 s3 데이터 삭제
def delete_s3_files():
    s3_hook = S3Hook(aws_conn_id='aws_s3')
    s3_hook.delete_objects(bucket='dust-dag', keys='dataSource/api_raw_data.csv')



with dag:
    with TaskGroup('ELT') as elt:
        with TaskGroup('data_extraction') as data_extraction:
            fetch_data_task = PythonOperator(
                task_id='fetch_data_from_api',
                python_callable=fetch_data_from_api,
            )

            upload_to_s3_task = LocalFilesystemToS3Operator(
                task_id='upload_data_to_s3',
                filename="{{ task_instance.xcom_pull(task_ids='ELT.data_extraction.fetch_data_from_api') }}",
                dest_bucket='dust-dag',
                dest_key='dataSource/api_raw_data.csv',
                aws_conn_id='aws_s3',
                replace=True  # 해당 파일이 이미 존재한다면, 덮어 쓰기
            )

            fetch_data_task >> upload_to_s3_task

        with TaskGroup('data_loading') as data_loading:
            create_temp_table_task = PythonOperator(
                task_id='create_raw_data_temp_table',
                python_callable=create_raw_data_temp_table
            )

            load_temp_redshift_task = PythonOperator(
                task_id='load_data_into_raw_data_temp',
                python_callable=load_data_into_raw_data_temp
            )

            create_temp_table_task >> load_temp_redshift_task

        with TaskGroup('data_transformation') as data_transformation:
            deduplicate_insert_task = PythonOperator(
                task_id='deduplicate_and_insert_data',
                python_callable=deduplicate_and_insert_data
            )

            change_format_task = PythonOperator(
                task_id='convert_date_format',
                python_callable=convert_date_format
            )

            deduplicate_insert_task >> change_format_task
            
        data_extraction >> data_loading >> data_transformation

    delete_s3_files_task = PythonOperator(
        task_id='delete_s3_files',
        python_callable=delete_s3_files
    )

    with TaskGroup('ETL') as etl:
        populate_region_task = PythonOperator(
            task_id='populate_region_table',
            python_callable=populate_region_table
        )

        populate_office_task = PythonOperator(
            task_id='populate_office_table',
            python_callable=populate_office_table
        )

        populate_dust_summary_task = PythonOperator(
            task_id='populate_dust_summary_table',
            python_callable=populate_dust_summary_table
        )

        populate_ultradust_summary_task = PythonOperator(
            task_id='populate_ultradust_summary_table',
            python_callable=populate_ultradust_summary_table
        )

        [populate_region_task, populate_office_task, populate_dust_summary_task, populate_ultradust_summary_task]

    elt >> delete_s3_files_task >> etl