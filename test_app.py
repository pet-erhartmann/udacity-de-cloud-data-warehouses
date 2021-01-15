import delete_redshift_cluster
import create_redshift_cluster
import create_tables
import boto3
import configparser

config = configparser.ConfigParser()
config.read('dwh.cfg')

def list_s3_file():
    s3 = boto3.client(
        's3',
        aws_access_key_id=config['AWS']['KEY'],
        aws_secret_access_key=config['AWS']['SECRET'],
    )

    for k in s3.list_objects(Bucket='udacity-dend')["Contents"]:
        print(k["Key"])


#create_redshift_cluster.create_redshift_cluster()
#delete_redshift_cluster.delete_redshift_cluster()
create_tables.main()
#list_s3_file()