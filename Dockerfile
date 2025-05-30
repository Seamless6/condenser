FROM python:3.9-slim

# Install PostgreSQL 16 client tools and MySQL client
RUN apt-get update && apt-get install -y \
    gnupg2 \
    lsb-release \
    wget \
    default-mysql-client \
    ssh \
    && wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /usr/share/keyrings/postgresql-keyring.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/postgresql-keyring.gpg] http://apt.postgresql.org/pub/repos/apt/ $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list \
    && apt-get update \
    && apt-get install -y postgresql-client-16 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY . .

# Set environment variables for database paths
ENV POSTGRES_PATH=/usr/bin
ENV MYSQL_PATH=/usr/bin

# Run the container forever
CMD ["tail", "-f", "/dev/null"]