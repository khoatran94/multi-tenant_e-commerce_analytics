# multi-tenant_e-commerce_analytics

Prerequisites: Ubuntu, Docker, Python
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
docker exec postgres psql -U user -d ecommerce -f /raw_schema_sql
```

## 2 Install airbyte locally
```sh
curl -LsfS https://get.airbyte.com | bash -
abvtl local install
```

## 3 Set up the connections:
I'm so sorry, it seems there is no export/import option for transferring the set up connections in a local airbyte instance to another (or maybe I am not aware of) 
Please have a look at the folder airbyte_workspace/ where I place 3 .json files downloaded from local airbyte API
Basically:
- the source is Google Drive connector (so sorry again, I couldn't use the File Connector with Local Filesystem option so I hostes the files on my Google Drive)
- the destination is the containerized PostgreSQL
- 3 connections, each with 9 streams (9 files from the Kaggle's Olist dataset) ingested to the 3 schemas in Step 1)
