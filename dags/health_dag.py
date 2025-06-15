from airflow.sdk import dag, task


@dag
def health_dag():
    @task
    def check_health():
        # Simulate a health check
        return {"status": "healthy"}

    @task
    def log_health(health_status):
        print(f"Health Status: {health_status['status']}")

    health_status = check_health()
    log_health(health_status)


health_dag()