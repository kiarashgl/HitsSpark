# Hits Algorithm on a graph using Spark
## Build
```bash
mvn clean package
```

## Run spark job
```bash
spark-submit --driver-memory 4g --num-executors 10 --executor-memory 4g --executor-cores 2 --class ca.uwaterloo.HitsMain target/Hits-1.0-SNAPSHOT.jar --input /path/to/input/graph --output /path/of/output
```

## Get Top Hubs and Authorities
```bash
spark-submit --driver-memory 4g --num-executors 10 --executor-memory 4g --executor-cores 2 --class ca.uwaterloo.HitsMain target/Hits-1.0-SNAPSHOT.jar --input /path/to/input/graph --output scores --titles /path/to/titles
```