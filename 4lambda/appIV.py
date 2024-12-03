import boto3
import time

# Información directamente proporcionada del clúster en ejecución
CLUSTER_ID = 'j-CN685OWRABYO'  # ID del clúster
LOG_URI = 's3://headlinesdyn/logdeemr/'  # Ruta de logs de S3
SCRIPT_PATH = 's3://aws-emr-studio-107657703304-us-east-1/1732816646285/e-B5N1FAALHV5E31M6XDEXYKSZ0/untitled.py'  # Ruta al script PySpark en S3
INSTANCE_TYPE = 'm5.xlarge'  # Tipo de instancia para master y slave
INSTANCE_COUNT = 3  # Número de instancias
SERVICE_ROLE = 'arn:aws:iam::107657703304:role/EMR_DefaultRole'  # Rol de servicio de EMR
INSTANCE_PROFILE = 'EMR_EC2_DefaultRole'  # Perfil de instancia EC2 (solo el nombre del rol)

# Cliente de Boto3
emr_client = boto3.client('emr', region_name='us-east-1')

BOOTSTRAP_SCRIPT_PATH = 's3://headlinesdyn/bootstrap/bootstrap.sh'

def create_cluster():
    cluster_config = {
        'MasterInstanceType': INSTANCE_TYPE,
        'SlaveInstanceType': INSTANCE_TYPE,
        'InstanceCount': INSTANCE_COUNT,
        'LogUri': LOG_URI,
        'ReleaseLabel': 'emr-6.9.0',  # Versión de EMR
        'Applications': [
            {'Name': 'Spark'},
            {'Name': 'Hadoop'},
            {'Name': 'JupyterHub'},
            {'Name': 'JupyterEnterpriseGateway'}
        ],
        'TerminationProtected': False
    }

    # Crear un nuevo clúster
    response = emr_client.run_job_flow(
        Name='NewEMRCluster',
        LogUri=cluster_config['LogUri'],
        ReleaseLabel=cluster_config['ReleaseLabel'],
        Applications=cluster_config['Applications'],
        Instances={
            'MasterInstanceType': cluster_config['MasterInstanceType'],
            'SlaveInstanceType': cluster_config['SlaveInstanceType'],
            'InstanceCount': cluster_config['InstanceCount'],
            'KeepJobFlowAliveWhenNoSteps': False,  # Terminar el clúster después de ejecutar el paso
            'TerminationProtected': cluster_config['TerminationProtected'],
            'Ec2KeyName': 'vockey',  # Nombre de la clave EC2
            'Ec2SubnetId': 'subnet-008ac3b1dd24a8210',  # Subnet ID
            'EmrManagedMasterSecurityGroup': 'sg-029abf80d1e92a4c5',
            'EmrManagedSlaveSecurityGroup': 'sg-0b25b79d8a1e54919',
        },
        BootstrapActions=[
            {
                'Name': 'Install Spark and Dependencies',
                'ScriptBootstrapAction': {
                    'Path': BOOTSTRAP_SCRIPT_PATH,
                    'Args': []  # Puedes incluir argumentos si tu script los necesita
                }
            }
        ],
        ServiceRole=SERVICE_ROLE,  # Rol de servicio de EMR
        Steps=[{
            'Name': 'Run PySpark Job',
            'ActionOnFailure': 'TERMINATE_CLUSTER',  # Terminar el clúster si el paso falla
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--master', 'yarn',
                    '--deploy-mode', 'cluster',
                    '--num-executors', '3',
                    '--executor-memory', '4G',
                    '--executor-cores', '2',
                    '--driver-memory', '4G',
                    '--conf', 'spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain',
                    '--conf', 'spark.sql.shuffle.partitions=50',
                    '--conf', 'spark.sql.parquet.writeLegacyFormat=true',
                    SCRIPT_PATH  # Ruta al archivo Python en S3
                ]
            }
        }],
        VisibleToAllUsers=True,
        JobFlowRole=INSTANCE_PROFILE  # Rol de instancia para el JobFlow
    )
    return response['JobFlowId']


# Función para monitorear el estado del clúster
def monitor_cluster(cluster_id):
    while True:
        response = emr_client.describe_cluster(ClusterId=cluster_id)
        state = response['Cluster']['Status']['State']
        print(f"Estado del clúster: {state}")
        if state == 'TERMINATED' or state == 'TERMINATED_WITH_ERRORS':
            break
        time.sleep(60)  # Revisa el estado cada minuto

# Función Lambda principal
def fIV(event, context):
    # Crear el nuevo clúster
    cluster_id = create_cluster()
    print(f"Nuevo clúster creado: {cluster_id}")

    # Monitorear el clúster y esperar a que termine
    monitor_cluster(cluster_id)
    print(f"Cluster {cluster_id} terminado.")

    return {
        'statusCode': 200,
        'body': f'EMR job completado y el clúster {cluster_id} ha terminado.'
    }