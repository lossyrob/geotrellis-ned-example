DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $DIR/environment.sh

JAR=$CODE_TARGET/ingest-elevation-assembly-0.1.0.jar

DATA_NAME=ned-19
DST_CRS=EPSG:3857
SOURCE_BUCKET=azavea-datahub
SOURCE_PREFIX=raw/ned-13arcsec-geotiff/
TARGET=s3://geotrellis-test/elevation-ingest/

PARTITION_ARG='partitionCount=1115,'

ON_FAILURE=CONTINUE

LAYER_NAME=ned-13arcsec
CRS=EPSG:3857
TILE_SIZE=512
CELL_TYPE=float32
INGEST_CLASS=elevation.Ingest

TILE_ARGS="--deploy-mode,cluster"
TILE_ARGS=${TILE_ARGS},--class,$INGEST_CLASS,--driver-memory,$DRIVER_MEMORY,--executor-memory,$EXECUTOR_MEMORY,--num-executors,$NUM_EXECUTORS,--executor-cores,$EXECUTOR_CORES
TILE_ARGS=${TILE_ARGS},--conf,spark.yarn.executor.memoryOverhead=$EXECUTOR_YARN_OVERHEAD
TILE_ARGS=${TILE_ARGS},$JAR
TILE_ARGS=${TILE_ARGS},--input,s3,--format,geotiff,-I,"$PARTITION_ARG"bucket=$SOURCE_BUCKET,key=$SOURCE_PREFIX
TILE_ARGS=${TILE_ARGS},--output,render,-O,path=$TARGET,encoding=png
TILE_ARGS=${TILE_ARGS},--layer,$LAYER_NAME,--tileSize,$TILE_SIZE,--crs,$CRS
TILE_ARGS=${TILE_ARGS},--layoutScheme,tms,--cache,NONE,--pyramid

echo "TILE: $TILE_ARGS"

# http://geotrellis-test.s3.amazonaws.com/elevation-ingest/{z}/{x}/{y}.png

aws emr add-steps --cluster-id $CLUSTER_ID --steps \
  Name=Ingest-$LAYER_NAME,ActionOnFailure=$ON_FAILURE,Type=Spark,Jar=$JAR,Args=[$TILE_ARGS]

# Jar=s3://oam-tiler-emr/mosaic.jar,Args=[--deploy-mode,cluster,--driver-memory,$DRIVER_MEMORY,--num-executors,$NUM_EXECUTORS,--executor-memory,$EXECUTOR_MEMORY,--executor-cores,$EXECUTOR_CORES,--class,org.hotosm.oam.Main,s3://oam-server-tiler/emr/mosaic.jar,$WORKSPACE_URI/step1_result.json]

# Spark arguments added:
#     --executor-memory 10g (m3 xlarge has 15g but Spark limit is 11g)
#     --num-executors 6     (for 6-worker cluster; could investigate spark.dynamicAllocation.enabled)
#     --executor-cores 4    (m3 xlarge has 4 cores)
#
# Tile arguments added:
#    splitSize=100          (2500 files / (6 workers * 4 cores/worker) ~= 100 files/core)
