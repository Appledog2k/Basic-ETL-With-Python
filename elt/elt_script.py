import subprocess
import time

def wait_for_postgres(host, max_retries=5, delay_seconds=5):
    retries = 0
    while retries < max_retries:
        try:
            result = subprocess.run(
                ["pg_isready", "-h", host],
                check=True,
                capture_output=True,
                text=True,
            )
            if "accepting connections" in result.stdout:
                print("Success: Postgres is ready to accept connections")
                return True
        except subprocess.CalledProcessError as e:
            print(f"Error: Postgres is not ready: {e}")
            retries += 1
            print(f"Retrying in {delay_seconds} seconds (Attempt {retries}/{max_retries})")
            time.sleep(delay_seconds)
    print("Error: Max retries reached, Postgres is not ready")
    return False

if not wait_for_postgres(host="source_postgres"):
    print("Error: Postgres is not ready, exiting...")
    exit(1)

print("Success: Postgres is ready, continuing with ETL...")

source_config = {
    "dbname": "source_db",
    "user": "postgres",
    "password": "Hung1234",
}