import boto3
from bs4 import BeautifulSoup
import csv

# Inicializar cliente S3
s3 = boto3.client('s3')

def detect_newspaper(key, soup):
    """
    Detecta el periódico basado en el nombre del archivo o la estructura del HTML.
    """
    # Verificar el nombre del archivo
    if "portafolio" in key:
        return "portafolio"
    elif "eltiempo" in key:
        return "eltiempo"

    # Heurística basada en estructuras HTML
    if soup.find('article', class_='c-article'):  # El Tiempo
        return "eltiempo"
    elif soup.find('article'):  # Portafolio
        return "portafolio"
    else:
        return "desconocido"

def extract_headlines_portafolio(soup):
    """
    Extrae titulares, categorías y enlaces del sitio web de Portafolio.
    """
    headlines = []
    # Buscar todos los artículos con las clases base conocidas
    for article in soup.find_all('article'):
        # Extraer categoría desde data-category o <p class="tarjeta__categoria">
        category = article.get('data-category', None)
        if not category:
            category_tag = article.find('p', class_='tarjeta__categoria')
            category = category_tag.get_text(strip=True) if category_tag else "Sin categoría"

        # Extraer título desde data-name o <p class="tarjeta__titulo">
        title = article.get('data-name', None)
        if not title:
            title_tag = article.find('p', class_='tarjeta__titulo')
            title = title_tag.get_text(strip=True) if title_tag else "Sin titular"

        # Escapar comillas en el título
        title = '"' + title.replace('"', '""') + '"'

        # Extraer enlace desde <a href="...">
        link = "Sin enlace"
        link_tag = article.find('a', href=True)
        if link_tag:
            link = "https://www.portafolio.co" + link_tag['href'] if link_tag['href'].startswith("/") else link_tag['href']

        # Agregar el resultado a la lista
        headlines.append((category, title, link))
    
    return headlines


def extract_headlines_eltiempo(soup):
    """
    Extrae titulares, categorías y enlaces del sitio web de El Tiempo.
    """
    headlines = []
    for article in soup.find_all('article', class_='c-article'):
        # Extraer la categoría principal desde data-category o el contenido visual
        category = article.get('data-category', 'Sin categoría')

        # Inicializar title_tag
        title_tag = None

        # Extraer el título desde data-name o el encabezado H3
        title = article.get('data-name', None)
        if not title:
            title_tag = article.find('h3', class_='c-article__title')
            title = title_tag.get_text(strip=True) if title_tag else "Sin titular"

        # Escapar comillas en el título
        title = '"' + title.replace('"', '""') + '"'

        # Extraer el enlace
        link = "Sin enlace"

        # Intentar extraer el enlace principal desde el <h3>
        if title_tag:
            link_tag = title_tag.find('a')
            if link_tag and 'href' in link_tag.attrs:
                link = link_tag['href']

        # Si no se encuentra en <h3>, buscar un enlace en el cuerpo del artículo
        if link == "Sin enlace":
            secondary_link_tag = article.find('a', href=True)
            if secondary_link_tag:
                link = secondary_link_tag['href']

        # Agregar el resultado a la lista
        headlines.append((category, title, link))
    
    return headlines


def process_and_store(bucket_name, key):
    try:
        print(f"Procesando archivo: {key} desde el bucket: {bucket_name}")

        # Descargar archivo desde S3
        response = s3.get_object(Bucket=bucket_name, Key=key)
        html_content = response['Body'].read().decode('utf-8')

        # Parsear contenido HTML con BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')

        # Detectar el periódico
        newspaper = detect_newspaper(key, soup)
        print(f"Periódico detectado: {newspaper}")

        # Extraer titulares según el periódico
        if newspaper == "portafolio":
            headlines = extract_headlines_portafolio(soup)
        elif newspaper == "eltiempo":
            headlines = extract_headlines_eltiempo(soup)
        else:
            print(f"No se pudo determinar el periódico para el archivo {key}")
            return

        # Generar ruta para el archivo CSV
        if key.startswith("headlines/raw/"):
            filename = key.replace("headlines/raw/", "").replace(".html", "")
            parts = filename.split("-")

            if len(parts) == 5:  # Validar el formato del nombre
                periodico = parts[1]
                year, month, day = parts[2], parts[3], parts[4]
                csv_key = f"headlines/final/periodico={periodico}/year={year}/month={month}/day={day}/headlines.csv"
                print(f"Ruta generada para el archivo CSV: {csv_key}")

                # Crear el archivo CSV
                csv_buffer = "Categoría,Titular,Enlace\n"
                for row in headlines:
                    csv_buffer += ",".join(row) + "\n"

                # Subir el archivo CSV a S3
                s3.put_object(Bucket=bucket_name, Key=csv_key, Body=csv_buffer, ContentType='text/csv')
                print(f"Archivo CSV guardado exitosamente en {csv_key}")
            else:
                print(f"El nombre del archivo {filename} no tiene el formato esperado.")

        else:
            print(f"El archivo {key} no está en la carpeta 'raw/'.")

    except Exception as e:
        print(f"Error al procesar {key}: {e}")


def lambda_handler(event, context):
    """
    Manejar eventos S3 que se activan al subir archivos a la carpeta 'raw/'.
    """
    try:
        print(f"Evento recibido: {event}")  # Log para depuración

        # Verificar si el evento tiene 'Records' (evento S3)
        if "Records" in event and event["Records"]:
            for record in event['Records']:
                bucket_name = record['s3']['bucket']['name']
                key = record['s3']['object']['key']

                print(f"Registro recibido: Bucket={bucket_name}, Key={key}")

                # Validar que el archivo esté en la carpeta 'raw/' y sea un archivo HTML
                if key.startswith("headlines/raw/") and key.endswith(".html"):
                    print(f"El archivo {key} está en la carpeta 'raw/' y es un archivo HTML.")
                    process_and_store(bucket_name, key)
                else:
                    print(f"El archivo {key} no pertenece a la carpeta 'raw/' o no es un archivo HTML.")
        else:
            print("Evento no soportado o no relacionado con S3.")

    except Exception as e:
        print(f"Error general en Lambda: {e}")