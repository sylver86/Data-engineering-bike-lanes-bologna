

import geopandas
import os
import pandas as pd 
import logging
import sys

def ingest_geojson(path_file: str) -> geopandas.GeoDataFrame:                                                           # Funzione per caricare un file geojson in un GeoDataFrame       
    """
    Funzione per caricare un file geojson in un GeoDataFrame
    :param path_file: percorso del file geojson
    :return: GeoDataFrame
    """
    
    if not os.path.exists(path_file):
        logging.error(f"Ingest_data: File {path_file} non trovato.")                                                    # Controlla se il file esiste
        raise FileNotFoundError(f"Ingest_data: File {path_file} non trovato.")
    
    logging.info(f"Ingest_data: Caricamento del file {path_file}...")
    
    try:
        gdf = geopandas.read_file(path_file)                                                                            # Carica il file geojson in un GeoDataFrame
    except Exception as e:
        logging.exception(f"Ingest_data: Errore durante il caricamento del file {path_file}")                           # Gestione degli errori di lettura del file
        raise e
    logging.info(f"Ingest_data: File {path_file} caricato con successo.")
    
    return gdf
    
    


if __name__ == "__main__":
    
    FILE_NAME = 'piste-ciclopedonali.geojson'
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))                                                             # Ottieni la directory dello script corrente
    PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)                                                                          # Ottieni la directory principale del progetto (directory padre dello script)
    PATH = os.path.join(PROJECT_ROOT,'data','raw',FILE_NAME)                                                            # Costruisci il percorso completo del file 

    LOG_FILE = 'pipeline.log'
    LOG_PATH = os.path.join(PROJECT_ROOT, 'logs', LOG_FILE)                                                             # Costruisci il percorso completo del file di log

    
    logging.basicConfig(
        filename=LOG_PATH,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        filemode='w'                                                                                                    # Modalità di apertura del file di log (scrittura)
    )
    
    
    logging.info("Ingest_data: Inizio dell'esecuzione dello script.")                                                   # Messaggio di inizio dello script  
    
    '''1. FASE DI CARICAMENTO DEL FILE GEOJSON'''
    
    try:
        gdf = ingest_geojson(PATH)                                                                                      # Carica il file geojson in un GeoDataFrame
    except FileNotFoundError as e:
        logging.exception(f"Ingest_data: File non trovato: {e}")                                                        # Gestione dell'eccezione se il file non viene trovato
        sys.exit(1)
    except Exception as e:
        logging.exception(f"Ingest_data: Errore durante l'esecuzione dello script: {e}")                                # Gestione di altre eccezioni
        sys.exit(1)
    
    
    
    
    '''2. FASE DI VERIFICA DEL FILE GEOJSON'''
    
    logging.info("\n\n\n########Ingest_data: File geojson caricato con successo - VERIFICA FILE########################################")
    
    if  gdf.shape[0] == 0:
        logging.warning("Ingest_data: Il file geojson è vuoto.")                                                        # Messaggio di errore se il GeoDataFrame è vuoto
    if gdf.shape[1] < 2:
        logging.error("Ingest_data: Il file geojson non contiene colonne sufficienti.")                                 # Messaggio di errore se il GeoDataFrame non ha colonne sufficienti
        sys.exit(1)
    if gdf.isnull().sum().sum() > 0:
        logging.warning("Ingest_data: Il file geojson contiene valori nulli.")                                          # Messaggio di errore se il GeoDataFrame contiene valori nulli
    
    logging.info("\n\n\n########Ingest_data: File geojson FINE VERIFICA FILE##########################################################\n\n\n")
        
    
    
    
    '''3. FASE DI SALVATAGGIO DEL FILE GEOJSON IN PARQUET'''    
        
    OUTPUT_FILENAME_PARQUET = FILE_NAME.replace('.geojson','.parquet')                                                   # Nome del file di output in formato parquet
    OUTPUT_PARQUET_PATH = os.path.join(PROJECT_ROOT,'data','raw',OUTPUT_FILENAME_PARQUET)    
    
    try:    
        gdf.to_parquet(OUTPUT_PARQUET_PATH, index=False)                                                                # Salva il GeoDataFrame in un file parquet
        logging.info(f"Ingest_data: File parquet salvato con successo in {OUTPUT_PARQUET_PATH}")                        # Messaggio di log per il salvataggio del file parquet
    
    except Exception as e:
        logging.exception(f"Ingest_data: Errore durante il salvataggio del file parquet: {e}")                          # Gestione degli errori di salvataggio del file parquet
        sys.exit(1)
        
    logging.info("Ingest_data: Esecuzione dello script completata.")                                                    # Messaggio di completamento dello script
   