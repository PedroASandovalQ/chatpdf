import requests
import re
import os
import logging
import pandas as pd
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor
from time import sleep

logging.basicConfig(level=logging.INFO)

CSV_FILE = 'Directorio_Oficial_2023.csv'
BASE_URL = 'https://cdnsae.mineduc.cl/documentos/{}/ReglamentodeConvivencia{}.pdf'
DOWNLOAD_FOLDER = 'PDF'
MAX_WORKERS = 30 # Hilos para descargar paralelamente
 

try:
    df = pd.read_csv(CSV_FILE, sep=';') # El CSV usa separadores ; en vez de ,
except pd.errors.ParserError as e:
    print(f"Error al leer el archivo CSV: {e}")
    df = None

def download_pdf(session, number, counter):
    pdf_url = BASE_URL.format(number, number)
    
    try:
        with session.get(pdf_url, timeout=60) as response:
            if response.status_code == 200 and 'application/pdf' in response.headers.get('Content-Type', ''):
                pdf_filename = f'ReglamentoConvivencia_{number}.pdf'
                pdf_filename = re.sub(r'[^a-zA-Z0-9._-]', '_', pdf_filename)

                pdf_path = os.path.join(DOWNLOAD_FOLDER, pdf_filename)

                with open(pdf_path, 'wb') as pdf_file:
                    pdf_file.write(response.content)

                logging.info(f"PDF descargado: {pdf_path}")
                counter['Descargado'] += 1

            else:
                logging.warning(f"No se pudo descargar el PDF {number}. Código de estado: {response.status_code}")

    except requests.Timeout:
        logging.error(f"La solicitud de PDF {number} ha superado el tiempo de espera. Puede haber problemas con la conexión o el servidor.")
    except Exception as e:
        logging.error(f"Error al descargar el PDF {number}: {e}")

def create_folder():
    os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)

def main():
    create_folder()

    try:
        df = pd.read_csv(CSV_FILE, sep=';')
    except pd.errors.ParserError as e:
        logging.error(f"Error al leer el archivo CSV: {e}")
        df = None

    if df is not None:
        numeros_RBD = df['RBD'].tolist()
        total_archivos = len(numeros_RBD)
        counter = {'Descargado': 0}

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            session = requests.Session()

            for numero in numeros_RBD:
                executor.submit(download_pdf, session, numero, counter)

        logging.info(f"Cantidad total de archivos: {total_archivos}, Descargados: {counter['Descargado']}")

if __name__ == "__main__":
    main()
