EMR_TARGET=s3://geotrellis-test/emr
CODE_TARGET=s3://geotrellis-test/jars

AWS_REGION="us-east-1"
SUBNET_ID="subnet-c5fefdb1"

KEY_NAME=geotrellis-emr

# If you want to have zeppelin, uncomment
# ZEPPELIN="Name=Zeppelin-Sandbox"

MASTER_INSTANCE=m3.xlarge
MASTER_PRICE=0.15
DRIVER_MEMORY=10g

# 40 m3.2xlarges, using 5 cores each,
WORKER_INSTANCE=m3.2xlarge
WORKER_PRICE=0.15
WORKER_COUNT=50

# M3.2XLARGE
#    8 cores
#   30 GB
#  160 GB SSD

NUM_EXECUTORS=400
EXECUTOR_MEMORY=2604m
EXECUTOR_CORES=1
EXECUTOR_YARN_OVERHEAD=520

CLUSTER_ID=j-10COP0DF65MDB
