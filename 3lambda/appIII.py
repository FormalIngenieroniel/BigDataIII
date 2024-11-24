import boto3
import time

def fIII(event, context):
    # Nombre del crawler configurado como variable de entorno
    crawler_name = 'crawlers3pt3'
    glue_client = boto3.client('glue')

    # Intentar iniciar el crawler
    try:
        glue_client.start_crawler(Name=crawler_name)
        print(f"Crawler '{crawler_name}' iniciado correctamente.")
    except glue_client.exceptions.CrawlerRunningException:
        print(f"Crawler '{crawler_name}' ya está en ejecución.")
    except Exception as e:
        print(f"Error al iniciar el crawler: {e}")
        return {
            "statusCode": 500,
            "body": f"Error al iniciar el crawler: {str(e)}"
        }

    # Verificar el estado del crawler
    try:
        while True:
            response = glue_client.get_crawler(Name=crawler_name)
            state = response.get('Crawler', {}).get('State', None)

            if state is None:
                print("Estado del crawler no encontrado en la respuesta.")
                return {
                    "statusCode": 500,
                    "body": "Error: No se pudo determinar el estado del crawler."
                }

            print(f"Estado actual del crawler: {state}")
            
            if state == 'READY':
                print(f"Crawler '{crawler_name}' completó su ejecución.")
                break

            # Esperar antes de volver a consultar el estado
            time.sleep(10)
    except Exception as e:
        print(f"Error al obtener el estado del crawler: {e}")
        return {
            "statusCode": 500,
            "body": f"Error al obtener el estado del crawler: {str(e)}"
        }

    # Validar que el crawler realizó actualizaciones exitosas
    try:
        tables = glue_client.get_tables(DatabaseName='dbs3pnt3')['TableList']
        updated_table = next((table for table in tables if table['Name'] == 's3pnt3headlines'), None)

        if not updated_table:
            print(f"Tabla 's3pnt3headlines' no encontrada en la base de datos 'dbs3pnt3'.")
            return {
                "statusCode": 500,
                "body": "Error: No se encontró la tabla esperada en el catálogo."
            }

        print(f"Tabla 's3pnt3headlines' actualizada exitosamente en la base de datos 'dbs3pnt3'.")
    except Exception as e:
        print(f"Error al validar la tabla: {e}")
        return {
            "statusCode": 500,
            "body": f"Error al validar la tabla: {str(e)}"
        }

    # Respuesta exitosa
    return {
        "statusCode": 200,
        "body": f"Crawler '{crawler_name}' ejecutado exitosamente y la tabla fue actualizada."
    }
