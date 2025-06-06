import pandas as pd
import geopandas as gpd
from tabulate import tabulate
import os
import sys
import logging
import re



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
    logging.info(f"Clean Data: Conteggio per tipologia record: \n\n{gdf['dtipologia2'].value_counts()}\n\n\n\n\n\n")                                # Messaggio di log per il conteggio per tipologia record



    '''3. FASE DI PULIZIA DEL FILE PARQUET-------------------------------------------------------------------------------------------------------'''

    logging.info("########Clean Data: Inizio della fase di pulizia del file parquet################################################################")
    logging.info("Eliminazione delle colonne 'geo_point_2d','duso','length' in quanto ridondanti.")
    columns_to_drop = ['geo_point_2d','duso','length']
    gdf = gdf.drop(columns=columns_to_drop)                                                                                                         # Elimino le colonne definite in lista

    logging.info("Concatenazione di un campo unico della tipologia ed eliminazione delle colonne utilizzate.")
    gdf['tipologia'] = gdf['dtipologia2'] + ' - ' + gdf['tipologia2']                                                                               # Concateno i due campi in un unico
    gdf = gdf.drop(columns=['dtipologia2','tipologia2'])                                                                                            # Elimino i campi non piu utili utilizzati prima
    logging.info(f"DataFrame risultante: \n {tabulate(gdf.head(), headers='keys', tablefmt='psql')}\n\n\n\n\n\n")



    '''4. RIDENOMINAZIONE COLONNE----------------------------------------------------------------------------------------------------------------'''

    logging.info("########Clean Data: Ridenominazione delle colonne ##############################################################################")
    logging.info("Rinomino le colonne.")
    gdf = gdf.rename(columns={'codice':'code',                                                                                                      # Definisco un dizionario che rinomina le colonne
                              'anno':'year_of_data',
                              'lunghezza':'length_meters',
                              'tipologia':'type',
                              'nomequart':'neighborhood_name',
                              'zona_fiu':'zone_name'})

    logging.info(f"DataFrame post ridenominazione:\n{tabulate(gdf.head(), headers='keys',tablefmt='psql')}\n\n\n\n\n\n")



    '''5. GESTIONE VALORI MANCANTI---------------------------------------------------------------------------------------------------------------'''

    logging.info("########Clean Data: Gestione Valori Mancanti ###################################################################################")
    logging.info("Elaboriamo per le colonne 'year_of_data','type' l'eliminazione delle righe qualora troviamo valori null")
    logging.info(f"Numero record pre-eliminazione: {gdf.shape[0]}")                                                                                 # Estrazione dei record pre eliminazione
    gdf_post = gdf.dropna(subset=['type','year_of_data'])                                                                                           # Definizione DataFrame con eliminazione
    logging.info(f"Numero record post-eliminazione : {gdf_post.shape[0]}")
    concat = pd.concat([gdf, gdf_post])                                                                                                             # Concateniamo i record dei 2 Dataframe (pre e post)
    delete_record = concat.drop_duplicates(keep=False)                                                                                              # Eliminiamo i duplicati (cio' che resta sono gli eliminati)
    logging.info(f"Record eliminati: \n{tabulate(delete_record, headers='keys', tablefmt='psql')}\n\n\n\n\n\n")



    '''6. STANDARDIZZAZIONE COLONNE--------------------------------------------------------------------------------------------------------------'''

    logging.info("########Clean Data: Standardizzazione Colonne ###################################################################################")
    logging.info("[1] - Standardizziamo il campo 'year_of_data' affinchè restituisca l'anno in formato numerico\n")

    def convert(text):                                                                                                                              # Definisco la funzione da applicare (apply) alla colonna
        try:
            return int(str(text[-4:]))
        except Exception as e:
            return 0

    logging.info(f"Tipologie del campo: {gdf['year_of_data'].value_counts()}")
    gdf['year_of_data'] = gdf['year_of_data'].apply(convert)                                                                                        # Applico la trasformazione di conversione in numero anno
    logging.info(f"Campo del DataFrame convertito: \n{tabulate(gdf.head(), headers='keys', tablefmt='psql')}\n\n")
    logging.info(f"Tipologie nuove del campo: {gdf['year_of_data'].value_counts()}\n\n")

    logging.info(f"[2] - Standardizziamo le colonne categoriali (minuscolo, rimozione spazi inizio e fine e caratteri speciali)")

    def remove_special_char(text):
        pattern = r"[^a-zA-Z0-9\s.\-]"                                                                                                              # Definisco il pattern dei caratteri che mantengo
        return re.sub(pattern,' ',str(text))                                                                                                        # Sostituisco i caratteri rilevati da 're' con spazi

    logging.info(f"Processo di eliminazione spazi inizio-fine + lower string...")
    gdf['type'] = gdf['type'].str.lower()                                                                                                           # Operazioni di lower sulla stringa
    gdf['type'] = gdf['type'].str.strip()                                                                                                           # Operazioni di eliminazione spazi inizio-fine stringa
    gdf['neighborhood_name'] = gdf['neighborhood_name'].str.lower()
    gdf['neighborhood_name'] = gdf['neighborhood_name'].str.strip()
    gdf['zone_name'] = gdf['zone_name'].str.lower()
    gdf['zone_name'] = gdf['zone_name'].str.strip()

    logging.info(f"Processo di eliminazione dei caratteri speciali sulle colonne 'type','neighborhood_name','zone_name'...")
    gdf['type'] = gdf['type'].apply(remove_special_char)                                                                                            # Applicazione del metodo di rimozione caratteri speciali
    gdf['neighborhood_name'] = gdf['neighborhood_name'].apply(remove_special_char)
    gdf['zone_name'] = gdf['zone_name'].apply(remove_special_char)

    logging.info(f"DataFrame post-intervento: \n{tabulate(gdf.head(), headers='keys', tablefmt='psql')}\n\n\n\n\n\n")


    '''7. SALVATAGGIO DATO PULITO E TRASFORMATO -------------------------------------------------------------------------------------------------'''

    logging.info("########Clean Data: Salvataggio Dato Pulito e Trasformato #######################################################################")
    logging.info(f"Riordinamento colonne del DataFrame:\n {tabulate(gdf.head(), headers='keys', tablefmt='psql')}")

    order_column = [                                                                                                                                # Definisco una lista per definire ordine colonne atteso
        'code',
        'year_of_data',
        'type',
        'zone_name',
        'neighborhood_name',
        'length_meters',
        'geometry'
    ]

    gdf = gdf[order_column]                                                                                                                         # Salvo l'ordinamento delle colonne
    logging.info(f"DataFrame post intervento:\n {tabulate(gdf.head(), headers='keys', tablefmt='psql')}\n\n\n")
    logging.info(f"Procedimento di salvataggio su cartella 'processed'...")


    try:
        gdf.to_parquet(os.path.join(PROJECT_DIR,'data','processed','piste-ciclopedonali_cleaned.parquet'))                                          # Salvo il DataFrame pulito ed elaborato
    except Exception as e:
        logging.exception(f"Impossibile salvare il DataFrame in file parquet su cartella .\processed - {e}")
        sys.exit(1)


    logging.info(f"DataFrame salvato in .\processed\piste-ciclopedonali_cleaned.parquet")
    logging.info(f"Fine processo di Clean_data.")
