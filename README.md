
# PRICING

### 1. Setting development environment

1. docker build -t pricing_image .
2. set env variables in docker-compose.yml (optional)
3. docker-compose --verbose up -d
4. Access jupyter at http://127.0.0.1:8888 (pass: pricing123)
5. Example notebook at scripts/Experiment_Update.ipynb
5. docker exec -it pricing_dev /bin/bash (optional - access docker's terminal)
