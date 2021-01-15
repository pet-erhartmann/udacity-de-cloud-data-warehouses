import time
import boto3
import configparser


def create_client(config):
    # CREATE REDSHIFT CLIENT
    redshift = boto3.client('redshift',
                            region_name="us-west-2",
                            aws_access_key_id=config['AWS']['KEY'],
                            aws_secret_access_key=config['AWS']['SECRET']
                            )

    return redshift


def create_cluster(redshift_client, config):
    try:
        response = redshift_client.create_cluster(
            # HW
            ClusterType=config['REDSHIFT']['DWH_CLUSTER_TYPE'],
            NodeType=config['REDSHIFT']['DWH_NODE_TYPE'],
            NumberOfNodes=int(config['REDSHIFT']['DWH_NUM_NODES']),

            # Identifiers & Credentials
            DBName=config['CLUSTER']['DB_NAME'],
            ClusterIdentifier=config['REDSHIFT']['DWH_CLUSTER_IDENTIFIER'],
            MasterUsername=config['CLUSTER']['DB_USER'],
            MasterUserPassword=config['CLUSTER']['DB_PASSWORD'],

            # Roles (for s3 access)
            IamRoles=[config['IAM_ROLE']['ARN']]
        )
    except Exception as e:
        print(e)


def create_redshift_cluster():
    # init config read
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # CREATE CLIENT
    redshift_client = create_client(config)

    # CREATE CLUSTER
    create_cluster(redshift_client, config)

    # CHECK STATUS
    creating = True

    while creating:
        myClusterProps = redshift_client.describe_clusters(ClusterIdentifier=config['REDSHIFT']['DWH_CLUSTER_IDENTIFIER'])['Clusters'][0]

        if myClusterProps["ClusterStatus"] != 'creating':
            print("cluster creation completed")

            # WRITE HOST TO CFG FILE
            config.set('CLUSTER', 'HOST', myClusterProps["Endpoint"]["Address"])
            with open('dwh.cfg', 'w') as configfile:
                config.write(configfile)
            creating = False

        else:
            print(myClusterProps["ClusterStatus"])
            time.sleep(30)
