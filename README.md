# IT 제품 통합 검색 & 분석 플랫폼

본 프로젝트는 다나와, 퀘이사존, 네이버 쇼핑 API 데이터를 자동 수집하여 통합 검색 + 비교 분석 + 시각화를 제공하는 것을 목표로 합니다.

## 프로젝트 개요

### 목표

- 다양한 쇼핑몰·커뮤니티 데이터를 자동 수집하는 ETL 파이프라인 구축

- 정제된 데이터를 기반으로 제품 비교·트렌드 분석 대시보드 제공

- 사용자가 웹에서 직접 조건 검색이 가능한 검색 서비스 구현

### 기술 스택 

| 분류            | 기술 / 도구                            | 사용 목적                      |
| ------------- | ---------------------------------- | -------------------------- |
| **언어**        | Python                             | 크롤링, ETL 로직, 데이터 처리        |
| **웹 크롤링**     | Requests, BeautifulSoup, Selenium  | 다나와·퀘이사존 데이터 수집            |
| **API**       | Naver Shopping Open API            | 네이버 쇼핑몰 가격·상품 정보 수집            |
| **워크플로우**     | Apache Airflow                 | ETL 파이프라인 스케줄링 및 자동화       |
| **클라우드**      | AWS EC2                        | Airflow, 웹 서비스 서버 운영       |
|               | AWS S3                         | 원본 데이터 및 변환 데이터 저장         |
|               | AWS Lambda                    | Naver API 수집 및 DAG 배포 자동화  |
|               | AWS Glue                       | JSON → Parquet 변환 ETL      |
|               | AWS RDS (PostgreSQL)           | Airflow 메타데이터 및 웹 서비스 DB   |
| **데이터 웨어하우스** | Snowflake                     | 정제 데이터 적재 및 분석             |
| **메시지 브로커**   | Redis                              | Airflow CeleryExecutor 브로커 |
| **데이터 포맷**    | JSON, Parquet                      | 원본/분석용 데이터 포맷              |
| **ML / 매칭**   | Sentence-Transformers, Levenshtein | 제품명 임베딩 및 퍼지 매칭            |
| **시각화**       | Supereset                             | 대시보드 및 데이터 분석              |
| **백엔드**       | Django                             | 검색 API 및 웹 서비스             |
| **프론트엔드**     | Vue.js                             | 사용자 검색 UI                  |
| **웹 서버**      | Nginx                              | Reverse Proxy 및 정적 파일 서빙   |
| **컨테이너**      | Docker                             | 개발·배포 환경 표준화               |
| **CI/CD**     | GitHub Actions                     | DAG 및 코드 자동 배포             |
| **형상 관리**     | GitHub                             | 소스 코드 및 버전 관리              |


### 시스템 아키텍처
<img width="1000" height="450" alt="image" src="https://github.com/user-attachments/assets/3fe48f4e-eac7-491a-aa25-e7506f82fe0f" />

- **EC2**
    - airflow-main : airflow webserver 과 scheduler를 위한 인스턴스
    - ariflow-worker : airflow worker 인스턴스
    - webserver : 웹서비스 인스턴스
      
- **Airflow**
    1. executor: CeleryExecutor
    2. Meta DB: RDS PostgreSQL
    3. broker : redis
    4. Logging : S3
       
- **S3 bucket**

    ```python
    s3://de6-team8-bucket/
    ├── glue/
    │   ├── scripts/
    │   │   ├── naver/
    │   │   │   └── naver_json_to_parquet.py
    │   │   ├── danawa/
    │   │   │   └── danawa_json_to_parquet.py
    │   │   └── quasarzone/
    │   │       └── quasarzone_json_to_parquet.py
    │   └── logs/
    │       └── (Glue Job 로그 또는 에러 기록용)
    │
    ├── raw_data/
    │   ├── naver/
    │   │   └── *.json (예: naver-2025-07-09.json)
    │   ├── danawa/
    │   │   └── *.json
    │   └── quasarzone/
    │       └── *.json
    │
    └── processed_data/
        ├── naver/
    		│   ├── jsonl
    		│   │     └── *.json
        │   └── parquet
        │         └── *.parquet
        ├── danawa/
        │   ├── jsonl
    		│   │     └── *.json
        │   └── parquet
        │         └── *.parquet
        └── quasarzone/
    				├── jsonl
    			  │     └── *.json
    	      └── parquet
    	            └── *.parquet
    ```
    
- **RDS**
    | 사용 목적 | 설명 | 역할 |
    | --- | --- | --- |
    | **1. Airflow 메타데이터 DB** | DAG 실행 이력, Task 상태, 로그 등 관리 | 스케줄러/웹서버 간 상태 공유 |
    | **2. 웹서비스용 백엔드 DB** | 사용자 요청, 제품정보, 리뷰 등 저장 | 정형화된 데이터 관리 Django와 연동 |
  
- **Lambda & Glue**
    | 구성 요소 | 사용 이유 | 역할 |
    | --- | --- | --- |
    | **Lambda** | GitHub에서 S3로 업로드된 DAG 파일을 감지해 **자동으로 EC2에 싱크** (SSM으로 전달) | DAG 동기화 자동화 |
    | **Glue** | S3의 **raw JSON → Parquet** 변환을 위한 **서버리스 ETL** 수행 | 대규모 데이터 변환에 최적화|

### 데이터 파이프라인

<img width="1000" height="500" alt="image" src="https://github.com/user-attachments/assets/2c0c6a26-d98a-4b56-bae4-0b95c859e2cb" />

1️. 데이터 수집

- Web Crawling / Open API 방식으로 IT 제품 데이터 수집

- Python 기반 수집 로직 구현

- 수집 결과를 JSON 형태로 표준화하여 생성

2️. 원본 데이터 저장

- 수집된 JSON 데이터를 AWS S3 Raw 영역에 저장

- 날짜 기준 파일 관리로 이력 추적 및 재처리 가능

- s3://bucket/raw_data/{source}/{date}.json

3️. 데이터 변환

- AWS Glue Job을 사용하여 서버리스 ETL 수행

- JSON 데이터를 Parquet 포맷으로 변환

- 컬럼 정제, 타입 변환, 필드 표준화 처리

4️. 분석용 데이터 적재

- S3의 Parquet 데이터를 기반으로 Snowflake 외부 테이블 생성

- 기존 테이블과 MERGE 방식으로 중복 제거 후 적재

- 분석에 최적화된 정형 데이터 구조 유지

5️. 워크플로우 자동화

- Apache Airflow DAG로 전체 ETL 프로세스 오케스트레이션

- DAG별 매일 오전 9시(UTC) 기준으로 자동 실행

- catchup=False, max_active_runs=1 설정으로 안정성 확보

6️. 운영 및 배포 자동화

- GitHub → S3 → Lambda → EC2(Airflow) 구조의 CI/CD 구성

- DAG 코드 변경 시 Airflow 재시작 없이 자동 반영

- 이벤트 기반 배포로 운영 리스크 최소화

### 데이터 구조
<img width="800" height="570" alt="image" src="https://github.com/user-attachments/assets/5ee779a3-75fb-4510-a130-e0cf68186955" />

### 대시보드 시각화 & 분석
대시보드를 통해 트렌드 추세 확인하고, 통합 검색 UI를 제공해 제품 상세 스펙 및 리뷰를 조회할 수 있습니다.

<img width="1001" height="500" alt="image" src="https://github.com/user-attachments/assets/56c4847e-4fba-4aff-8dd1-3f4d1781cc99" />
