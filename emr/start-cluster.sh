# Starts a long running ETL cluster.

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $DIR/environment.sh

# Ganglia is required for Accumulo dependencies

aws emr create-cluster \
  --name "Elevation Ingest" \
  --region $AWS_REGION \
  --log-uri $EMR_TARGET/logs/ \
  --release-label emr-4.5.0 \
  --use-default-roles \
  --ec2-attributes KeyName=$KEY_NAME,SubnetId=$SUBNET_ID \
  --applications Name=Ganglia Name=Spark $ZEPPELIN  \
  --instance-groups \
    Name=Master,InstanceCount=1,InstanceGroupType=MASTER,InstanceType=$MASTER_INSTANCE \
    Name=Workers,InstanceCount=$WORKER_COUNT,BidPrice=$WORKER_PRICE,InstanceGroupType=CORE,InstanceType=$WORKER_INSTANCE # \
 --configurations file://./emr.json \
 --bootstrap-action Path=$EMR_TARGET/bootstrap-geowave.sh # \
#  --steps Type=CUSTOM_JAR,Name=Wait,Jar=s3://elasticmapreduce/libs/script-runner/script-runner.jar,Args=["s3:$EMR_TARGET/post-boostrap.sh"]
