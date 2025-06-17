import pandas as pd
from pydantic import BaseModel, Field, UUID4
from typing import Optional
from datetime import datetime
from enum import Enum
from uuid import uuid4

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
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
    def process_excel_file():
        CHUNK_SIZE = 1000  # Adjust based on available memory
        excel_path = "dags/duplicates_4be25b5b-b03a-42a2-bf9d-"
        excel_path += "e82c58c5f533.xlsx"
        
        if not os.path.exists(excel_path):
            raise FileNotFoundError(f"Excel file not found: {excel_path}")
            
        service = DuplicateDetectionService()
        all_details = []
        
        # Read the Excel file
        df = pd.read_excel(excel_path)
        total_rows = len(df)
        
        # Process the dataframe in chunks
        for chunk_start in range(0, total_rows, CHUNK_SIZE):
            chunk_end = min(chunk_start + CHUNK_SIZE, total_rows)
            df_chunk = df.iloc[chunk_start:chunk_end]
            
            # Detect duplicates in this chunk
            chunk_results = service.detect_duplicates(
                df_chunk,
                df_chunk.columns.tolist()
            )
            
            # Process the duplicate results
            chunk_details = []
            for item in chunk_results:
                idx = item.get("index")
                if idx is not None and 0 <= idx < len(df_chunk):
                    row = df_chunk.iloc[idx]
                    now = datetime.utcnow()
                    detail = ControlHealthCheckDetail(
                        id=uuid4(),
                        external_id=uuid4(),
                        name=row.get("name", ""),
                        description=row.get("description", ""),
                        objective=row.get("objective", ""),
                        type=row.get("type", ""),
                        class_=row.get("class", ""),
                        status=DuplicateStatus.PENDING.value,
                        match_percentage=int(
                            round(item.get("match_percentage", 0))
                        ),
                        created_at=now,
                        updated_at=now
                    )
                    chunk_details.append(detail)
            
            all_details.extend(chunk_details)
            chunk_num = chunk_start // CHUNK_SIZE + 1
            total_chunks = (total_rows + CHUNK_SIZE - 1) // CHUNK_SIZE
            print(f"Processed chunk {chunk_num}/{total_chunks}")
        
        print(f"Total duplicates found: {len(all_details)}")
        # Convert Pydantic models to dictionaries and ensure UUIDs are strings
        serializable_details = []
        for detail in all_details:
            detail_dict = detail.model_dump()
            # Convert UUIDs to strings
            detail_dict['id'] = str(detail_dict['id'])
            detail_dict['external_id'] = str(detail_dict['external_id'])
            serializable_details.append(detail_dict)
        return serializable_details

    @task
    def process_health_check_details(details_dicts):
        if not details_dicts:
            print("No details to process")
            return
        
        BATCH_SIZE = 1000  # Adjust this based on your needs
        pg_hook = PostgresHook(postgres_conn_id="postgres_default")
        
        # Prepare the SQL statement for batch insert
        insert_sql = """
        INSERT INTO control_health_check_details (
            id, external_id, name, description, objective, type, class,
            status, match_percentage, created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        );
        """
        
        # Convert dictionaries to list of tuples for batch insert
        records = [
            (
                str(detail["id"]),
                str(detail["external_id"]),
                detail["name"],
                detail["description"],
                detail["objective"],
                detail["type"],
                detail["class_"],
                detail["status"],
                detail["match_percentage"],
                detail["created_at"],
                detail["updated_at"]
            )
            for detail in details_dicts
        ]
        
        # Process in batches
        total_records = len(records)
        processed = 0
        
        try:
            with pg_hook.get_conn() as conn:
                with conn.cursor() as cur:
                    while processed < total_records:
                        batch = records[processed:processed + BATCH_SIZE]
                        cur.executemany(insert_sql, batch)
                        processed += len(batch)
                        print(f"Inserted {processed}/{total_records}")
                conn.commit()
            print(f"Successfully processed {total_records} records")
            
        except Exception as e:
            print(f"Error inserting health check details: {str(e)}")
            raise
    
    # Define the DAG structure
    # Create the task instances
    excel_task = process_excel_file()
    db_task = process_health_check_details(excel_task)


health_dag()
