import boto3
import json
import configparser
import pandas as pd
import logger

from botocore.exceptions import ClientError

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


def create_iam_role(DWH_IAM_ROLE_NAME):
    try:
        print("Creating a new IAM Role")
        dwhRole = iam.create_role(
            Path="/",
            RoleName=DWH_IAM_ROLE_NAME,
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {
                    "Statement": [
                        {
                            "Action": "sts:AssumeRole",
                            "Effect": "Allow",
                            "Principal": {"Service": "redshift.amazonaws.com"},
                        }
                    ],
                    "Version": "2012-10-17",
                }
            ),
        )
    except Exception as e:
        print(e)
    print("Attaching Policy")
    iam.attach_role_policy(
        RoleName=DWH_IAM_ROLE_NAME,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
    )["ResponseMetadata"]["HTTPStatusCode"]
    print("Get the IAM role ARN")
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)["Role"]["Arn"]
    print(roleArn)
    return roleArn


def create_redshift_cluster(
    DWH_CLUSTER_TYPE,
    DWH_NODE_TYPE,
    DWH_NUM_NODES,
    DWH_DB,
    DWH_CLUSTER_IDENTIFIER,
    DWH_DB_USER,
    DWH_DB_PASSWORD,
    roleArn,
):
    try:
        print("Starting to create Redshift Cluster")
        response = redshift.create_cluster(
            # HW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),
            # Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            # Roles (for s3 access)
            IamRoles=[roleArn],
        )
    except Exception as e:
        print(e)



def pretty_redshift_props(props):
    keys_to_show = [
        "ClusterIdentifier",
        "NodeType",
        "ClusterStatus",
        "MasterUsername",
        "DBName",
        "Endpoint",
        "NumberOfNodes",
        "VpcId",
    ]
    x = [(k, v) for k, v in props.items() if k in keys_to_show]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


def opening_tcp_connection(vpc_id, DWH_PORT):
    try:
        vpc = ec2.Vpc(id=vpc_id)
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp="0.0.0.0/0",
            IpProtocol="TCP",
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT),
        )
    except Exception as e:
        print(e)



def main():
    role_arn = create_iam_role(DWH_IAM_ROLE_NAME)
    create_redshift_cluster(
        DWH_CLUSTER_TYPE,
        DWH_NODE_TYPE,
        DWH_NUM_NODES,
        DWH_DB,
        DWH_CLUSTER_IDENTIFIER,
        DWH_DB_USER,
        DWH_DB_PASSWORD,
        role_arn,
    )

    myClusterProps = redshift.describe_clusters(
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER
    )["Clusters"][0]
    pretty_redshift_props(myClusterProps)
    status = myClusterProps["ClusterStatus"]
    print(f"Cluster is still {status}")
    while myClusterProps["ClusterStatus"] == "creating":
        myClusterProps = redshift.describe_clusters(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER
        )["Clusters"][0]
        pretty_redshift_props(myClusterProps)
    else:
        if myClusterProps["ClusterStatus"] == "available":
            print(f"Cluster is now available")
            myClusterProps = redshift.describe_clusters(
                ClusterIdentifier=DWH_CLUSTER_IDENTIFIER
            )["Clusters"][0]
            pretty_redshift_props(myClusterProps)
            vpc_id = myClusterProps["VpcId"]
            opening_tcp_connection(vpc_id, DWH_PORT)
            DWH_ENDPOINT = myClusterProps["Endpoint"]["Address"]
            DWH_ROLE_ARN = myClusterProps["IamRoles"][0]["IamRoleArn"]
            # get the ARN section from config file
            arn = config["IAM_ROLE"]
            # update arn role
            arn["ARN"] = DWH_ROLE_ARN
            # Write changes back to file
            with open("dwh.cfg", "w") as conf:
                config.write(conf)
            print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
            print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)
            print("Your Redshift Cluster is now created")
        else:
            print(myClusterProps["ClusterStatus"])


if __name__ == "__main__":
    main()
