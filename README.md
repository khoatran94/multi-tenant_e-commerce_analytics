# multi-tenant_e-commerce_analytics

Prerequisites: Ubuntu, Docker, Python
## 0 Clone the repo:
```sh
git clone https://github.com/khoatran94/multi-tenant_e-commerce_analytics.git
cd multi-tenant_e-commerce_analytics/
```

## 1 PostgreSQL local:
```sh
docker run -d --name postgres \
-e POSTGRES_USER=user -e POSTGRES_PASSWORD=password -e POSTGRES_DB=ecommerce\
-p 5432:5432 postgres:15
```

