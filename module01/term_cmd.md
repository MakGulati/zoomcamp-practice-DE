```bash
docker run -it \
-e POSTGRES_USER=postgres \
-e POSTGRES_PASSWORD=postgres \
-e POSTGRES_DB=ny_taxi \
-v ny_taxi_postgres_data:/var/lib/postgresql/data \
-p 5436:5432 \
--network=pg-network \
--name pg-database \
postgres
```
```bash
docker run -it \
-e PGADMIN_DEFAULT_EMAIL="pgadmin@pgadmin.com" \
-e PGADMIN_DEFAULT_PASSWORD="pgadmin" \
-p 8080:80 \
--network=pg-network \
--name pgadmin-clt \
dpage/pgadmin4:latest
```
```bash
    container_name: pgadmin
    image: dpage/pgadmin4:latest
    environment:
      PGADMIN_DEFAULT_EMAIL: "pgadmin@pgadmin.com"
      PGADMIN_DEFAULT_PASSWORD: "pgadmin"
    ports:
      - "8080:80"
```