import pendulum
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

@dag(
    dag_id='chap7_2_sw',
    start_date=pendulum.datetime(2024, 8, 25, tz="Asia/Seoul"),
    schedule="@once",  # 한 번만 실행
    catchup=False
)
def aws_s3():

    @task
    def check_s3():
        # S3 연결 확인
        hook = S3Hook('aws_default')  # Airflow UI에서 설정된 AWS 연결 ID 사용
        bucket_name = 'de6-team8-bucket'  # 확인할 버킷 이름 (설정된 버킷 이름을 사용)
        
        # S3 버킷에서 파일 목록 가져오기
        files = hook.list_keys(bucket_name=bucket_name)

        if files:
            for file in files:
                print(f"파일 목록: {file}")  # 파일 목록 출력
        else:
            print("버킷에 파일이 없습니다.")  # 파일이 없으면 출력

    @task
    def create_file():
        load_file_name = '/tmp/s3_test.txt'  # 생성할 파일 경로
        with open(load_file_name, 'w', encoding='utf-8') as f:
            f.write("s3 test file~")  # 파일 내용 작성
        return load_file_name  # 생성된 파일 경로 반환

    @task
    def upload_to_s3(load_file_name):
        hook = S3Hook('aws_default')  # S3 연결
        # 파일을 S3에 업로드 (파일 경로, 버킷 이름, 키 설정)
        hook.load_file(filename=load_file_name, bucket_name='de6-team8-bucket', key='test/s3_test.txt')  # `test/` 폴더 아래에 업로드
        print(f"파일 {load_file_name}이(가) S3에 업로드되었습니다.")  # 업로드 완료 메시지

    check_s3()  # S3에 파일이 있는지 확인
    file_name = create_file()  # 파일 생성
    upload_to_s3(file_name)  # 파일을 S3에 업로드

aws_s3()  # DAG 실행
