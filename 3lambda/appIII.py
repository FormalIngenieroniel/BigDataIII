import boto3

glue_client = boto3.client('glue')

def fIII(event, context):
    # Nombre del crawler configurado como variable de entorno
    crawler_name = 'crawlers3pt3'
    
    # Intentar iniciar el crawler
    try:
        response = glue_client.start_crawler(Name=crawler_name)
        print(f"Crawler '{crawler_name}' iniciado correctamente: {response}")
    except glue_client.exceptions.CrawlerRunningException:
        print(f"Crawler '{crawler_name}' ya está en ejecución.")
    except glue_client.exceptions.EntityNotFoundException:
        print(f"Error: Crawler '{crawler_name}' no encontrado. Verifica el nombre.")
    except Exception as e:
        print(f"Error al iniciar el crawler: {e}")
        return {
            "statusCode": 500,
            "body": f"Error al iniciar el crawler: {str(e)}"
        }

    # Respuesta exitosa
    return {
        "statusCode": 200,
        "body": f"Crawler '{crawler_name}' iniciado correctamente o ya estaba en ejecución."
    }
