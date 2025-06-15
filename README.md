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