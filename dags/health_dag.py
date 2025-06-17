import pandas as pd
from pydantic import BaseModel, Field, UUID4
from typing import Optional
from datetime import datetime
from enum import Enum

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import os

from health_check import DuplicateDetectionService

class DuplicateStatus(str, Enum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class ControlHealthCheckFileBase(BaseModel):
    id: UUID4
    file_name: str
    s3_path: str
    status: str
    airflow_dag_run_id: Optional[str] = None
    user_id: Optional[UUID4] = None
    mapping_json: Optional[dict] = None
    created_at: datetime
    updated_at: datetime


class ControlHealthCheckDetailBase(BaseModel):
    name: str
    description: Optional[str] = None
    objective: Optional[str] = None
    type: str
    class_: str = Field(..., alias="class")
    status: str = DuplicateStatus.PENDING.value
    match_percentage: Optional[int] = None


class ControlHealthCheckDetailCreate(ControlHealthCheckDetailBase):
    external_id: UUID4
    
    class Config:
        populate_by_name = True


class ControlHealthCheckDetail(ControlHealthCheckDetailBase):
    id: UUID4
    external_id: UUID4
    created_at: datetime
    updated_at: datetime
    
    class Config:
        populate_by_name = True
        from_attributes = True


class ControlHealthCheckDetailWithFile(ControlHealthCheckDetail):
    external: ControlHealthCheckFileBase

    class Config:
        populate_by_name = True
        from_attributes = True


@dag(tags=["health_check"])
def health_dag():

    @task
    def query_health_check_file(id=None):
        pg_hook = PostgresHook(postgres_conn_id="postgres_default")

        if id:
            # Query for a specific health check file by ID
            sql = """
            SELECT id, file_name, s3_path, airflow_dag_run_id, status, 
                   user_id, mapping_json, created_at, updated_at
            FROM control_health_check_files
            WHERE id = %s;
            """
            params = (id,)
        else:
            # Get the first health check file
            sql = """
            SELECT id, file_name, s3_path, airflow_dag_run_id, status, 
                   user_id, mapping_json, created_at, updated_at
            FROM control_health_check_files
            LIMIT 1;
            """
            params = None

        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(sql, params)
        result = cursor.fetchone()
        cursor.close()
        conn.close()

        if not result:
            print("Control Health Check File not found.")
            return None

        # Convert result to a dictionary
        columns = [
            "id",
            "file_name",
            "s3_path",
            "airflow_dag_run_id",
            "status",
            "user_id",
            "mapping_json",
            "created_at",
            "updated_at",
        ]
        health_check_file = dict(zip(columns, result))

        # Extract the S3 object path
        s3_object = health_check_file["file_name"]
        print(f"Found health check file: {s3_object}")

        return health_check_file

    @task
    def download_health_check_file():
        local_file = "dags/duplicates_4be25b5b-b03a-42a2-bf9d-e82c58c5f533.xlsx"
        if os.path.exists(local_file):
            df = pd.read_excel(local_file)
            service = DuplicateDetectionService()
            results = service.detect_duplicates(df, df.columns.tolist())
            details = []
            for item in results:
                idx = item.get("index")
                if idx is not None and 0 <= idx < len(df):
                    row = df.iloc[idx]
                    detail = ControlHealthCheckDetail(
                        external_id=id,
                        name=row.get("name", ""),
                        description=row.get("description", ""),
                        objective=row.get("objective", ""),
                        type=row.get("type", ""),
                        class_=row.get("class", ""),
                        match_percentage=item.get("match_percentage"),
                    )
                    details.append(detail)

            return details
    query_health_check_file() >> download_health_check_file()


health_dag()
