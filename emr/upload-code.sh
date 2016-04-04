DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $DIR/environment.sh

pushd ../
aws s3 cp ./target/scala-2.10/ingest-elevation-assembly-0.1.0.jar $CODE_TARGET/ --region $AWS_REGION
popd

aws s3 cp bootstrap.sh $EMR_TARGET/bootstrap.sh --region $AWS_REGION
