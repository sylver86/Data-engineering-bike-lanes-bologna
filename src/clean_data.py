import pandas as pd
import geopandas as gpd
import os
import sys
import logging
 

if __name__ == "__main__": 
    
    SRC_DIR = os.path.dirname(os.path.abspath(__file__))                                                                # Ottiene il percorso della cartella SRC_DIR
    PROJECT_DIR = os.path.dirname(SRC_DIR)                                                                              # Ottiene il percorso della cartella del progetto
    LOG_PATH = os.path.join(PROJECT_DIR,'logs','pipeline.log')                                                          # Percorso del file di log
    
    logging.basicConfig(                                                                                                # Set up logging
        filename=LOG_PATH,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        filemode='w'
    )
    
    logging.info("Clean Data: Inizio dell'esecuzione dello script di cleaning.")                                        # Messaggio di inizio dello script
    
    FILE_NAME = 'piste-ciclopedonali.parquet'                                                                           # Nome del file parquet
    SRC_DIR = os.path.dirname(os.path.abspath(__file__))
    PROJECT_DIR = os.path.dirname(SRC_DIR)                                                                              # Ottiene il percorso della cartella del progetto
    PATH_INPUT_PARQUET = os.path.join(PROJECT_DIR,'data','raw',FILE_NAME)                                               # Percorso del file parquet

    
    
    '''1. FASE DI CARICAMENTO DEL FILE PARQUET'''
    
    try:
        gdf = gpd.read_parquet(PATH_INPUT_PARQUET)                                                                      # Carica il file geojson in un GeoDataFrame
    except FileNotFoundError as e:
        logging.exception(f"Clean Data: ERRORE CRITICO - File parquet non trovato al percorso {PATH_INPUT_PARQUET}: {e}")                                                        # Controlla se il file esiste
        sys.exit(1)
        
    except Exception as e:
        logging.exception(f"Clean Data: Errore durante il caricamento del file {PATH_INPUT_PARQUET}: {e}")              # Gestione degli errori di lettura del file
        raise e
    
    
    
    '''2. FASE DI LETTURA DEL FILE PARQUET'''
    
    logging.info("\n\n\n########Clean Data: File parquet caricato con successo - ANTEPRIMA FILE########################################")  
    logging.info(f"Clean Data: Informazioni del file sorgente: \n")
    print(gdf.info())
    logging.info(f"Clean Data: Nomi delle colonne del GeoDataFrame:{gdf.columns.tolist()}")                             # Messaggio di log per i nomi delle colonne
    logging.info(f"Clean Data: N° di record vuoti:{gdf.isnull().sum()}")                                                # Messaggio di log per i record vuoti
    logging.info(f"Clean Data: Anteprima del Geodataframe:{gdf.head()}")                                                # Messaggio di log per l'anteprima del GeoDataFrame   
    logging.info(f"Clean Data: Shape del Geodataframe: {gdf.shape}")                                                    # Messaggio di log per la forma del GeoDataFrame
    logging.info(f"Clean Data: Conteggio per tipologia record: \n\n{gdf['dtipologia2'].value_counts()}")                # Messaggio di log per il conteggio per tipologia record
    logging.info("\n\n\n########Clean Data: File parquet FINE ANTEPRIMA FILE###########################################################\n\n\n")
     
    logging.info(f"Clean Data: Fine dell'esecuzione dello script.")                                                     # Messaggio di fine dello script
  