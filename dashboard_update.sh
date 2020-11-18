#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

docker run -v "${DIR}":/home/pricing -v /var/log:/var/log -e PYTHONPATH=/home/pricing/src pricing_image python src/pricing/dashboard/data_update.py
cd $DIR && docker-compose down
cd $DIR && DASHBOARD_MODE="PROD" LOGGER_FILE="True" LOGGER_LEVEL="INFO" PRICING_API_HOST=$PRICING_API_HOST PRICING_API_USER=$PRICING_API_USER PRICING_API_PASSWORD=$PRICING_API_PASSWORD docker-compose up -d
