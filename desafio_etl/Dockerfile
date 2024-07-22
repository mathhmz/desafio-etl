FROM python:3.10-slim

# Checkout and install dagster libraries needed to run the gRPC server
# exposing your repository to dagster-webserver and dagster-daemon, and to load the DagsterInstance

RUN pip install \
    dagster \
    dagster-postgres \
    dagster-docker \
    dagster-aws \
    pandas

# Add repository code
WORKDIR /opt/dagster/app

# Copy the requirements.txt first
COPY requirements.txt /opt/dagster/app/

# Install dependencies from requirements.txt
RUN pip install -r requirements.txt

# Create a data directory
RUN mkdir data

# Copy the rest of the application code
COPY . /opt/dagster/app/desafio_etl

# Run dagster gRPC server on port 4000
EXPOSE 4000

# CMD allows this to be overridden from run launchers or executors that want
# to run other commands against your repository
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "desafio_etl"]