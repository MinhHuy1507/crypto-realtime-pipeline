## Local Running Instructions

> **Full model catalog, SQL logic, incremental strategies, and data quality tests:** See [dbt Transformation Guide](../docs/architecture/dbt_models.md).

#### Step 1: 
```bash
docker exec -it dbt_dev /bin/bash
```
#### Step 2: 
To run the entire project:
```bash
dbt build
```

To run a specific layer:
```bash
dbt run --select staging
dbt run --select intermediate
dbt run --select marts
```

To run a specific model and its children:
```bash
dbt run --select +mart_crypto__technical_analysis
```

## Project Documentation

To view detailed documentation about models, lineage, and descriptions:

**View Documentation (Docker):**

```bash
# 1. Access the container
docker exec -it dbt_dev /bin/bash

# 2. Generate documentation
dbt docs generate

# 3. Start the server
dbt docs serve
```
Access: `http://localhost:8580/` (configured in docker-compose.yml)
