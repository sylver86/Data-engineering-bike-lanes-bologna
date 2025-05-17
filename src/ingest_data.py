

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
        logging.error(f"File {path_file} non trovato.")                                                                 # Controlla se il file esiste
        raise FileNotFoundError(f"File {path_file} non trovato.")
    
    logging.info(f"Caricamento del file {path_file}...")
    
    try:
        gdf = geopandas.read_file(path_file)                                                                            # Carica il file geojson in un GeoDataFrame
    except Exception as e:
        logging.exception(f"Errore durante il caricamento del file {path_file}")                                        # Gestione degli errori di lettura del file
        raise e
    logging.info(f"File {path_file} caricato con successo.")
    
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
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    
    
    logging.info("Inizio dell'esecuzione dello script.")                                                              # Messaggio di inizio dello script  
    
    try:
        gdf = ingest_geojson(PATH)                                                                                      # Carica il file geojson in un GeoDataFrame
    except FileNotFoundError as e:
        logging.exception(f"File non trovato: {e}")                                                                     # Gestione dell'eccezione se il file non viene trovato
        sys.exit(1)
    except Exception as e:
        logging.exception(f"Errore durante l'esecuzione dello script: {e}")                                             # Gestione di altre eccezioni
        sys.exit(1)
    
    logging.info("Esecuzione dello script completata.")                                                                 # Messaggio di completamento dello script
   