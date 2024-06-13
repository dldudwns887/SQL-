from airflow import DAG
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

# DAG 정의
with DAG(
    'api_to_s3_to_redshift',
    default_args=default_args,
    description='Fetch data from API, upload to S3, and load into Redshift with deduplication',
    schedule_interval='@hourly',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # 1. API 호출하여 데이터프레임 형태로 변환하는 함수
    def fetch_api_data():
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

    fetch_data = PythonOperator(
        task_id='fetch_api_data',
        python_callable=fetch_api_data,
    )

    # 2. 데이터프레임 형태로 변환한 데이터를 S3에 업로드
    upload_to_s3 = LocalFilesystemToS3Operator(
        task_id='upload_to_s3',
        filename="{{ task_instance.xcom_pull(task_ids='fetch_api_data') }}",
        dest_bucket='dust-dag',
        dest_key='dataSource/api_raw_data.csv',
        aws_conn_id='aws_s3',
        replace=True  # 파일이 이미 존재하는 경우 덮어쓰기
    )

    # 3. Redshift에 임시 테이블 생성
    def create_temp_redshift_table():
        redshift_hook = PostgresHook(postgres_conn_id='redshift_test_dev')
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

    create_temp_table = PythonOperator(
        task_id='create_temp_redshift_table',
        python_callable=create_temp_redshift_table,
    )

    # 4. S3 데이터를 임시 테이블에 적재
    def load_to_temp_redshift():
        redshift_hook = PostgresHook(postgres_conn_id='redshift_test_dev')
        aws_hook = S3Hook(aws_conn_id='aws_s3')
        credentials = aws_hook.get_credentials()
        load_sql = f"""
        COPY yusuyeon678.api_raw_data_temp
        FROM 's3://dust-dag/dataSource/api_raw_data.csv'
        credentials
        'aws_access_key_id={credentials.access_key};aws_secret_access_key={credentials.secret_key}'
        csv
        IGNOREHEADER 1;
        """
        redshift_hook.run(load_sql)

    load_temp_redshift = PythonOperator(
        task_id='load_temp_redshift',
        python_callable=load_to_temp_redshift,
    )

    # 5. Redshift 데이터 중복 검사 및 삽입
    def deduplicate_and_insert():
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

    deduplicate_insert = PythonOperator(
        task_id='deduplicate_and_insert',
        python_callable=deduplicate_and_insert,
    )

    # 6. S3 파일 삭제
    def delete_s3_file():
        s3_hook = S3Hook(aws_conn_id='aws_s3')
        s3_hook.delete_objects(bucket='dust-dag', keys='dataSource/api_raw_data.csv')

    delete_file = PythonOperator(
        task_id='delete_s3_file',
        python_callable=delete_s3_file,
    )

    # Task 순서 정의
    fetch_data >> upload_to_s3 >> create_temp_table >> load_temp_redshift >> deduplicate_insert >> delete_file
