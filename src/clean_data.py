import pandas as pd
import geopandas as gpd
from tabulate import tabulate
import os
import sys
import logging


if __name__ == "__main__":

    SRC_DIR = os.path.dirname(os.path.abspath(__file__))                                                                                            # Ottiene il percorso della cartella SRC_DIR
    PROJECT_DIR = os.path.dirname(SRC_DIR)                                                                                                          # Ottiene il percorso della cartella del progetto
    LOG_PATH = os.path.join(PROJECT_DIR,'logs','pipeline.log')                                                                                      # Percorso del file di log
    FILE_NAME = 'piste-ciclopedonali.parquet'                                                                                                       # Nome del file parquet
    PATH_INPUT_PARQUET = os.path.join(PROJECT_DIR,'data','raw',FILE_NAME)                                                                           # Percorso del file parquet


    logging.basicConfig(                                                                                                                            # Set up logging
        filename=LOG_PATH,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        filemode='w'
    )

    logging.info("Clean Data: Inizio dell'esecuzione dello script di cleaning....\n\n\n")                                                           # Messaggio di inizio dello script




    '''1. FASE DI CARICAMENTO DEL FILE PARQUET---------------------------------------------------------------------------------------------------'''

    try:
        gdf = gpd.read_parquet(PATH_INPUT_PARQUET)                                                                                                  # Carica il file parquet in un GeoDataFrame
    except FileNotFoundError as e:
        logging.exception(f"Clean Data: ERRORE CRITICO - File parquet non trovato al percorso {PATH_INPUT_PARQUET}: {e}")                           # Controlla se il file esiste
        sys.exit(1)

    except Exception as e:
        logging.exception(f"Clean Data: Errore durante il caricamento del file {PATH_INPUT_PARQUET}: {e}")                                          # Gestione degli errori di lettura del file
        sys.exit(1)

    '''2. FASE DI LETTURA DEL FILE PARQUET-------------------------------------------------------------------------------------------------------'''

    logging.info("########Clean Data: File parquet caricato con successo - ANTEPRIMA FILE##########################################################")
    logging.info(f"Clean Data: Informazioni del file sorgente: \n")
    print(gdf.info())
    logging.info(f"Clean Data: Nomi delle colonne del GeoDataFrame:{gdf.columns.tolist()}")                                                         # Messaggio di log per i nomi delle colonne
    logging.info(f"Clean Data: N° di record vuoti:{gdf.isnull().sum()}")                                                                            # Messaggio di log per i record vuoti
    logging.info(f"Clean Data: Anteprima del Geodataframe:{tabulate(gdf.head(), headers='keys', tablefmt='psql')}")                                 # Messaggio di log per l'anteprima del GeoDataFrame
    logging.info(f"Clean Data: Shape del Geodataframe: {gdf.shape}")                                                                                # Messaggio di log per la forma del GeoDataFrame
    logging.info(f"Clean Data: Conteggio per tipologia record: \n\n{gdf['dtipologia2'].value_counts()}\n\n\n")                                      # Messaggio di log per il conteggio per tipologia record



    '''3. FASE DI PULIZIA DEL FILE PARQUET-------------------------------------------------------------------------------------------------------'''

    logging.info("########Clean Data: Inizio della fase di pulizia del file parquet################################################################")
    logging.info("Eliminazione delle colonne 'geo_point_2d','duso','length' in quanto ridondanti.")
    columns_to_drop = ['geo_point_2d','duso','length']
    gdf = gdf.drop(columns=columns_to_drop)                                                                                                         # Elimino le colonne definite in lista

    logging.info("Concatenazione di un campo unico della tipologia ed eliminazione delle colonne utilizzate.")
    gdf['tipologia'] = gdf['dtipologia2'] + ' - ' + gdf['tipologia2']                                                                               # Concateno i due campi in un unico
    gdf = gdf.drop(columns=['dtipologia2','tipologia2'])                                                                                            # Elimino i campi non piu utili utilizzati prima
    logging.info(f"DataFrame risultante: \n {tabulate(gdf.head(), headers='keys', tablefmt='psql')}\n\n\n")


    '''4. RIDENOMINAZIONE COLONNE----------------------------------------------------------------------------------------------------------------'''

    logging.info("########Clean Data: Ridenominazione delle colonne ##############################################################################")
    logging.info("Rinomino le colonne.")
    gdf = gdf.rename(columns={'codice':'code',                                                                                                      # Definisco un dizionario che rinomina le colonne
                              'anno':'year_of_data',
                              'lunghezza':'length_meters',
                              'tipologia':'type',
                              'nomequart':'neighborhood_name',
                              'zona_fiu':'zone_name'})

    logging.info(f"DataFrame post ridenominazione:\n{tabulate(gdf.head(), headers='keys',tablefmt='psql')}\n\n\n")


    '''5. GESTIONE VALORI MANCANTI---------------------------------------------------------------------------------------------------------------'''

    logging.info("########Clean Data: Gestione Valori Mancanti ###################################################################################")
    logging.info("Elaboriamo per le colonne 'year_of_data','type' l'eliminazione delle righe qualora troviamo valori null")
    logging.info(f"Numero record pre-eliminazione: {gdf.shape[0]}")                                                                                 # Estrazione dei record pre eliminazione
    gdf_post = gdf.dropna(subset=['type','year_of_data'])                                                                                           # Definizione DataFrame con eliminazione
    logging.info(f"Numero record post-eliminazione : {gdf_post.shape[0]}")
    concat = pd.concat([gdf, gdf_post])                                                                                                             # Concateniamo i record dei 2 Dataframe (pre e post)
    delete_record = concat.drop_duplicates(keep=False)                                                                                              # Eliminiamo i duplicati (cio' che resta sono gli eliminati)
    logging.info(f"Record eliminati: \n{tabulate(delete_record, headers='keys', tablefmt='psql')}")

