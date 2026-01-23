<!--  -->

# Install Python
apt update && apt install python3
# Check Python version
python3 -V <!-- Tells Python to print its version and exit. -->
# pip (python package manager)
pip install pandas pyarrow <!-- Install globally on your system. Don't do it. -->
pip install uv <!-- virtual environment - an isolated Python environment that keeps dependencies for this project separate from other projects and from your system Python -->
uv init --python=3.13 <!-- This creates a pyproject.toml file. It describes the Python project’s metadata, dependencies, and build system in a standardized way. Similar to Dockerfile is to Docker. -->
uv run python -V <!-- Tells Python to print its version and exit. -->
uv add pandas <!-- A Python library used for data manipulation and analysis, providing data structures like DataFrames to work with structured data. -->
uv add pyarrow <!-- A Python library that enables efficient in-memory columnar data processing and provides support for the Apache Arrow and Parquet formats. -->
uv run python pipeline.py 10 <!-- Running python script -->

# Install Jupyter
uv add --dev jupyter <!-- Jupyter is an interactive computing environment used mainly for data analysis, data science, and machine learning. Jupyter lets you write and run code in small interactive cells, see the results immediately, and mix code with text, tables, and plots in the same document.-->
uv run jupyter notebook <!-- It starts a Jupyter Notebook server using the Python environment managed by uv. Accesed via web browser on localhost:port -->
uv add sqlalchemy <!-- A Python library that provides a high-level, Pythonic way to work with databases, including SQL generation, connections, and ORM (Object Relational Mapping). -->
uv add psycopg2-binary <!-- A PostgreSQL database driver that allows Python programs to connect to and communicate with a PostgreSQL database. -->
# Convert Notebook to Script then move and rename
uv run jupyter nbconvert --to=script notebook.ipynb
mv notebook.py ingest_data.py
# Running python script
uv run python ingest_data.py \
  --pg_user=root \
  --pg_pass=root \
  --pg_host=localhost \
  --pg_port=5432 \
  --pg_db=ny_taxi \
  --target_table=yellow_taxi_trips


# Docker
docker --version <!-- Checks docker version -->
docker run ubuntu <!-- Creates a container from the Ubuntu image, runs its default command, and exits immediately because there is no interactive terminal. -->
docker run -it ubuntu <!-- Creates a container from the Ubuntu image and starts an interactive terminal session inside it. -->
docker run -it --rm ubuntu <!-- --rm removes the container when it exits. -->
docker ps -a <!-- Lists all Docker containers, including running and stopped ones. -->
docker rm $(docker ps -aq) <!-- Removes all Docker containers by deleting every container ID returned by docker ps -aq. -->
docker container prune <!-- Remove all stopped containers -->
docker images <!-- List all images. -->
docker rmi taxi_ingest:v001 <!-- Remove specific image. -->
docker image prune -a <!-- Remove all unused images -->
docker volume ls <!-- List volumes. -->
docker volume rm volume_name <!-- Remove specific volumes -->
docker volume prune <!-- Remove all unused volumes. -->
docker network ls <!-- List volumes. -->
docker network rm network_name <!-- Remove specific network. -->
docker network prune <!-- Remove all unused networks. -->

docker run -it \ 
 --rm \ 
 -v $(pwd)/test:/app/test \ <!-- Mounts the local "test" directory into "/app/test" inside the container. -->
 --entrypoint=bash \ <!-- Overrides the default entrypoint to start a Bash shell. -->
 python:3.9.16-slim <!-- Uses the lightweight Python 3.9.16 slim image as the base container. -->

<!-- Executes list_files.py which is inside the mounted "test" directory and lists the files inside of it. -->
cd /app/test
ls -la
cat file1.txt
python list_files.py

# Docker build based on Dockerfile
docker build -t test:pandas .
docker run -it test:pandas some_number

docker build -f Dockerfile.uv -t test:pandas_uv . <!-- Explicit Dockerfile file, otherwise takes Dockerfile. -->
docker run -it test:pandas_uv some_number

# Running PostgreSQL in a Docker Container inside a Network
docker run -it --rm \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v ny_taxi_postgres_data:/var/lib/postgresql \
  -p 5432:5432 \
  --network=pg-network \
  --name pgdatabase \
  postgres:18
<!-- 
    -e sets environment variables (user, password, database name)
    -v ny_taxi_postgres_data:/var/lib/postgresql creates a named volume
    Docker manages this volume automatically
    Data persists even after container is removed
    Volume is stored in Docker's internal storage
    -p 5432:5432 maps port 5432 from container to host
    postgres:18 uses PostgreSQL version 18 (latest as of Dec 2025) 

    Named volume (name:/path): Managed by Docker, easier. "ny_taxi_postgres_data:/var/lib/postgresql"
    Bind mount (/host/path:/container/path): Direct mapping to host filesystem, more control. "$(pwd)/ny_taxi_postgres_data:/var/lib/postgresql"
-->


# -- Running Ingestion in a Docker Container inside a Network
docker run -it --rm \
  --network=pipeline_default  \
  taxi_ingest:v001 \
  --pg_user=root \
  --pg_pass=root \
  --pg_host=pgdatabase \
  --pg_port=5432 \
  --pg_db=ny_taxi \
  --target_table=yellow_taxi_trips_2021_1 \
  --year=2021 \
  --month=1 \
  --chunksize=100000


# -- Running pgAdmin in a Docker Contaoner inside a Network
docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -v pgadmin_data:/var/lib/pgadmin \
  -p 8085:80 \
  --network=pg-network \
  --name pgadmin \
  dpage/pgadmin4


# Connect to postgres container using pgcli 
uv run pgcli -h localhost -p 5432 -u root -d ny_taxi <!-- This command connects from the host shell to a Postgres server running inside a Docker container via a published port. -->


# Docker Compose
<!-- docker-compose allows us to launch multiple containers using a single configuration file, so that we don't have to run multiple complex docker run commands separately. -->
docker-compose up
docker-compose up -d <!-- If you want to run the containers again in the background rather than in the foreground (thus freeing up your terminal), you can run them in detached mode. -->
docker-compose down <!-- Stop All Running Containers. -->
docker-compose logs <!-- View logs. -->
docker-compose down -v <!-- Stop and remove volumes. -->
docker nerwork ls 


# CleanUP
docker-compose down
docker container prune
docker image prune -a
docker volume prune
docker network prune