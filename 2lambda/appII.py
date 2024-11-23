import boto3
import csv
import os
from datetime import datetime
from bs4 import BeautifulSoup

# Cliente de S3
s3 = boto3.client('s3')

# Nombre del bucket
BUCKET_NAME = 'headlinesdyn'

def fII(event, context):
    for record in event['Records']:
        # Obtener información del evento S3
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        
        if 'raw' in key:  # Asegurar que es un archivo de la carpeta raw
            # Descargar el archivo HTML desde S3
            local_file = '/tmp/temp.html'
            s3.download_file(bucket, key, local_file)
            
            # Leer el archivo HTML con BeautifulSoup
            with open(local_file, 'r', encoding='utf-8') as file:
                soup = BeautifulSoup(file, 'html.parser')

            # Extraer información requerida
            headlines = []
            for article in soup.find_all('article'):
                categoria = article.find(class_='category-class').get_text(strip=True) if article.find(class_='category-class') else "Sin categoría"
                titular = article.find('h2').get_text(strip=True) if article.find('h2') else "Sin titular"
                enlace = article.find('a')['href'] if article.find('a') else "Sin enlace"
                headlines.append([categoria, titular, enlace])

            # Generar la ruta de destino en S3
            now = datetime.now()
            
            # Extraer el nombre del periódico de forma más robusta
            # Se considera que el archivo tiene un formato consistente, como "tiempo-raw.html" o "espectador-raw.html"
            nombre_periodico = key.split('/')[-1].split('-')[0].lower()  # Convertir a minúsculas para uniformidad
            
            # Validar si el nombre del periódico es válido
            if nombre_periodico not in ['tiempo', 'espectador']:
                nombre_periodico = 'desconocido'  # Manejo de casos inesperados
            
            s3_key = f"headlines/final/periodico={nombre_periodico}/year={now.year}/month={now.month:02}/day={now.day:02}/headlines.csv"
            
            # Escribir resultados a un archivo CSV
            csv_file = '/tmp/headlines.csv'
            with open(csv_file, 'w', encoding='utf-8', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(['Categoria', 'Titular', 'Enlace'])
                writer.writerows(headlines)

            # Subir el archivo CSV al bucket S3
            s3.upload_file(csv_file, BUCKET_NAME, s3_key)
            print(f"Archivo procesado y guardado en: {s3_key}")

    return {
        'statusCode': 200,
        'body': 'Archivos procesados exitosamente'
    }
