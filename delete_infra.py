from infra import pretty_redshift_props
import configparser
import boto3


config = configparser.ConfigParser()
config.read_file(open("dwh.cfg"))

"Get variable from config file"
KEY = config.get("AWS", "KEY")
SECRET = config.get("AWS", "SECRET")
DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")
DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
DWH_DB = config.get("CLUSTER", "DB_NAME")
DWH_DB_USER = config.get("CLUSTER", "DB_USER")
DWH_DB_PASSWORD = config.get("CLUSTER", "DB_PASSWORD")
DWH_PORT = config.get("CLUSTER", "DB_PORT")
DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")

ec2 = boto3.resource(
    "ec2", region_name="us-west-2", aws_access_key_id=KEY, aws_secret_access_key=SECRET
)

s3 = boto3.resource(
    "s3", region_name="us-west-2", aws_access_key_id=KEY, aws_secret_access_key=SECRET
)

iam = boto3.client(
    "iam", aws_access_key_id=KEY, aws_secret_access_key=SECRET, region_name="us-west-2"
)

redshift = boto3.client(
    "redshift",
    region_name="us-west-2",
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET,
)


def delete_redshift_cluster(cluster_identifier,  skip_final_cluster_snapshot):
    print('Redshift Cluster deletion now starting')
    redshift.delete_cluster(cluster_identifier, skip_final_cluster_snapshot)


myClusterProps = redshift.describe_clusters(
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER
    )["Clusters"][0]
pretty_redshift_props(myClusterProps)

status = myClusterProps["ClusterStatus"]
print(f"Current Redshift Cluster status is  {status}")
iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
arn = config["IAM_ROLE"]
# update arn role
arn["ARN"] = ''
# Write changes back to file
with open("dwh.cfg", "w") as conf:
    config.write(conf)
print('Policy and Roles now removed')
redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
print('Cluster has been deleted')



