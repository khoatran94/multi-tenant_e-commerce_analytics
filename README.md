# multi-tenant_e-commerce_analytics

Prerequisites: Ubuntu, Docker, Python  
Note: for the ease of reproducibility, I push some files that should be kept as secrets in production env (postgres credentials, dbt profiles.yml, etc.)

## 0 Clone the repo:
```sh
git clone https://github.com/khoatran94/multi-tenant_e-commerce_analytics.git
cd multi-tenant_e-commerce_analytics/
```

## 1 PostgreSQL local:
Spin up a Docker container and create 3 schemas for 3 different tenants, namely raw_*:

```sh
docker run -d --name postgres \
-e POSTGRES_USER=user -e POSTGRES_PASSWORD=password -e POSTGRES_DB=ecommerce \
-p 5432:5432 postgres:15
docker cp raw_schema.sql postgres:/raw_schema.sql
docker exec postgres psql -U user -d ecommerce -f /raw_schema.sql
```

## 2 Install airbyte locally
```sh
curl -LsfS https://get.airbyte.com | bash -
abctl local install
```

## 3 Set up the connections:
I'm so sorry, it seems there is no export/import option for transferring the set up connections in a local airbyte instance to another (or maybe I am not aware of) 
Please have a look at the folder `📁 airbyte_workspace/` where I place 3 .json files downloaded from local airbyte API
Basically:
- the source is Google Drive connector (so sorry again, I couldn't use the File Connector with Local Filesystem option so I hosted the files on my Google Drive, and I cannot push the service account credentials here)

- the destination is the containerized PostgreSQL
- 3 connections, each with 9 streams (9 files from the Kaggle's Olist dataset) ingested to the 3 schemas in Step 1)
Open: http://localhost:8000 and log in with credentials from
```sh
abctl local credentials
```
to manually create the connections

## 4 Install dbt and prefect:
### 4.1 Create a venv:
```sh
python3 -m venv elt-env
source elt-env/bin/activate
```
### 4.2 Install dbt and prefect:
```sh
pip install --upgrade pip
pip install dbt-postgres prefect "prefect[dbt]"
```
## 5 Create Staging/Intermediate/Analytics Layer:

```sh
mkdir -p ~/.dbt
cp profiles.yml ~/.dbt/profiles.yml
cd ecommerce_analytics/
dbt debug
# Should say: "All checks passed!"
```

### 5.1 Staging Layer (Bronze):
```sh
dbt run --select staging --vars "tenant_id: amazon"
dbt run --select staging --vars "tenant_id: etsy"
dbt run --select staging --vars "tenant_id: shopify"
```
### 5.2 Intermediate Layer (Silver):
```sh
dbt run --select intermediate --vars "tenant_id: amazon"
dbt run --select intermediate --vars "tenant_id: etsy"
dbt run --select intermediate --vars "tenant_id: shopify"
```
### 5.3 Analytics Layer (Gold):

```sh
dbt run --select analytics --vars "tenant_id: amazon"
dbt run --select analytics --vars "tenant_id: etsy"
dbt run --select analytics --vars "tenant_id: shopify"
```

Or, the scheduling of dbt runs can be done with **prefect**:
### 5.4 Prefect Flow register and Flow run execution:
```sh
python flows/dbt_multi_tenant_refresh.py &
prefect deployment run 'Multi-Tenant dbt Run/multi_tenant_deployment'
```
Note: we must keep the python script running with & so that the created worker can pick up the flow run created by **prefect deployment run**
Another way is to run the python script, close it, execute the **prefect deployment run** and run the python script again

## 6 Superset:

### 6.1 Superset quickstart:
```sh
cd ~
git clone https://github.com/apache/superset
cd superset
git checkout tags/5.0.0
docker compose -f docker-compose-image-tag.yml up
```
### 6.2 Login:

Log in with the default created account on http://localhost:8088:
```sh
username: admin
password: admin
```
### 6.3 Import dashboards:
After login, on the **Dashboards** tab, click **Import dashboard** and use the .zip files in the folder `📁 superset_dashboards/`






