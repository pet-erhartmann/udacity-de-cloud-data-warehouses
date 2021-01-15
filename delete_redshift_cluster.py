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


def delete_cluster(redshift_client, config):
    redshift_client.delete_cluster(
        ClusterIdentifier=config['REDSHIFT']['DWH_CLUSTER_IDENTIFIER'],
        SkipFinalClusterSnapshot=True
    )


def delete_redshift_cluster():
    # init config read
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # CREATE CLIENT
    redshift_client = create_client(config)

    # DELETE CLUSTER
    delete_cluster(redshift_client, config)

    # CHECK STATUS
    deleting = True
    while deleting:
        try:
            myClusterProps = redshift_client.describe_clusters(ClusterIdentifier=config['REDSHIFT']['DWH_CLUSTER_IDENTIFIER'])['Clusters'][0]
            print(myClusterProps["ClusterStatus"])
            time.sleep(30)

        except Exception as e:
            print("delete completed")
            # REMOVE HOST TO CFG FILE
            config.set('CLUSTER', 'HOST', '')

            with open('dwh.cfg', 'w') as configfile:
                config.write(configfile)

            deleting = False
