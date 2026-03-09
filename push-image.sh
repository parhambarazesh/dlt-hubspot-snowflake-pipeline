snow connection test --connection myinstance > /dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "Connection to Snowflake failed. Please check your connection settings."
    exit 1
fi
REGISTRY_URL=irxihve-qpb91970.registry.snowflakecomputing.com/productionpoc/poc/image_repo

snow spcs image-registry login --connection myinstance

docker build -t hubspot_pipeline:latest .
docker tag hubspot_pipeline:latest $REGISTRY_URL/hubspot_pipeline:latest
docker push $REGISTRY_URL/hubspot_pipeline:latest