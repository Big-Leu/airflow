Airflow releases: https://airflow.apache.org/docs/apach...

4. docker-compose up airflow-init
5. docker-compose up

Cleanup - below command will delete all the conainer and free up the volumes and memory
`Run docker-compose down --volumes --remove-orphans`
command in the directory you downloaded the docker-compose.yaml file

airflow-scheduler - The scheduler monitors all tasks and DAGs, then triggers the task instances once their dependencies are complete.
airflow-webserver - The webserver is available at http://localhost:8080.
airflow-worker - The worker that executes the tasks given by the scheduler.
airflow-triggerer - The triggerer runs an event loop for deferrable tasks.
airflow-init - The initialization service.
postgres - The database.
redis - The redis - broker that forwards messages from scheduler to worker.

Reference: https://airflow.apache.org/docs/apach...


# Stop and remove all containers
docker-compose down

# Remove any orphaned volumes
docker volume prune -f

# Try starting again
docker-compose up


docker-compose down -v
docker-compose --env-file .env up


    # @task
    # def query_health_check_file(id=None):
    #     pg_hook = PostgresHook(postgres_conn_id="postgres_default")

    #     if id:
    #         # Query for a specific health check file by ID
    #         sql = """
    #         SELECT id, file_name, s3_path, airflow_dag_run_id, status, 
    #                user_id, mapping_json, created_at, updated_at
    #         FROM control_health_check_files
    #         WHERE id = %s;
    #         """
    #         params = (id,)
    #     else:
    #         # Get the first health check file
    #         sql = """
    #         SELECT id, file_name, s3_path, airflow_dag_run_id, status, 
    #                user_id, mapping_json, created_at, updated_at
    #         FROM control_health_check_files
    #         LIMIT 1;
    #         """
    #         params = None

    #     conn = pg_hook.get_conn()
    #     cursor = conn.cursor()
    #     cursor.execute(sql, params)
    #     result = cursor.fetchone()
    #     cursor.close()
    #     conn.close()

    #     if not result:
    #         print("Control Health Check File not found.")
    #         return None

    #     # Convert result to a dictionary
    #     columns = [
    #         "id",
    #         "file_name",
    #         "s3_path",
    #         "airflow_dag_run_id",
    #         "status",
    #         "user_id",
    #         "mapping_json",
    #         "created_at",
    #         "updated_at",
    #     ]
    #     health_check_file = dict(zip(columns, result))

    #     # Extract the S3 object path
    #     s3_object = health_check_file["file_name"]
    #     print(f"Found health check file: {s3_object}")

    #     return health_check_file


    Run docker compose build to build the image, or add --build flag to docker compose up or docker compose run commands to build the image automatically as needed.

    docker-compose --env-file .env up