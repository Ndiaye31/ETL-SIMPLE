import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv
import re
import psycopg2
from psycopg2 import sql

# Configuration initiale
load_dotenv()



pd.set_option('display.max_columns', None)

# Constantes
ID_PATTERN = r":\d+"
DATE_FORMATS = ["%m/%d/%y", "%d-%b-%y", "%Y-%m-%d"]
SELECTED_COLUMNS = [
    'id', 'country', 'shipment_mode', 'scheduled_delivery_date',
    'delivered_to_client_date', 'delivery_recorded_date',
    'product_group', 'sub_classification', 'vendor',
    'item_description', 'molecule_test_type', 'brand',
    'dosage_form', 'line_item_quantity',
    'line_item_value', 'unit_price', 'weight_kilograms',
    'freight_cost_usd', 'line_item_insurance_usd'
]

def load_csv_files(directory):
    """Charge tous les fichiers CSV d'un dossier dans une liste de DataFrames."""
    return [
        pd.read_csv(os.path.join(directory, file))
        for file in sorted(os.listdir(directory))
        if file.endswith(".csv")
    ]

def clean_column_names(dataframes):
    """Nettoie les noms de colonnes des DataFrames en appliquant des transformations."""
    transformations = str.maketrans({
        '-': '_', ' ': '_', '$': '', '?': '', '/': '_', '\\': '_',
        '%': '', ')': '', '(': ''
    })
    for df in dataframes:
        df.columns = [col.lower().translate(transformations) for col in df.columns]
    return dataframes

def find_date_columns(df):
    """Identifie les colonnes contenant 'date' dans leur nom."""
    return [col for col in df.columns if "date" in col.lower()]

def parse_date(date_str):
    """Tente de convertir une chaîne en date selon plusieurs formats."""
    if pd.isna(date_str) or date_str in ['Pre-PQ Process', 'Date Not Captured', '']:
        return pd.NaT
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(str(date_str).strip(), fmt)
        except ValueError:
            continue
    return pd.to_datetime(date_str, errors='coerce')

def convert_date_columns(df):
    """Convertit les colonnes de date en format datetime."""
    for col in find_date_columns(df):
        df[col] = df[col].astype(str).str.strip().apply(parse_date)
    return df

def resolve_id_reference(value, dataset, column):
    """Remplace les références d'ID (ex: ':123') par la valeur correspondante dans dataset."""
    
    if not isinstance(value, str): #verification type de données avec isintance.
        return value
    match = re.search(ID_PATTERN, value)
    if match:
        id_str = match.group().replace(":", "")
        filtered = dataset.query(f"id == {id_str}")
        if not filtered.empty:
            return filtered[column].iloc[0]
    return value

def setup_postgres_database(
    db_name="scms_db",
    user="postgres",
    password=os.getenv("PG_PASSWORD"),
    host="localhost",
    port="5433"
):
    try:
        # Connexion à la base "postgres"
        conn = psycopg2.connect(
            dbname="postgres",
            user=user,
            password=password,
            host=host,
            port=port
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Vérifie si la base existe
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
        exists = cursor.fetchone()

        if not exists:
            cursor.execute(sql.SQL("CREATE DATABASE {} ENCODING 'UTF8'").format(
                sql.Identifier(db_name)
            ))
            print(f"✅ Base de données '{db_name}' créée.")
        else:
            print(f"ℹ️ La base de données '{db_name}' existe déjà.")

        cursor.close()
        conn.close()

        # Connexion à la base nouvellement créée
        pg_conn = psycopg2.connect(
            dbname=db_name,
            user=user,
            password=password,
            host=host,
            port=port,
        )
        pg_conn.autocommit = True
        print(f"✅ Connexion établie à la base '{db_name}'.")
        return pg_conn

    except Exception as e:
        print(f"❌ Erreur : {e}")
        return None


def main():
    """Pipeline principal pour traiter les données."""
    # Étape 1 : Chargement des données
    dataframes = load_csv_files("dataset")
    print(f"{len(dataframes)} fichiers CSV chargés.")

    # Étape 2 : Nettoyage des noms de colonnes
    dataframes = clean_column_names(dataframes)

    # Étape 3 : Conversion des colonnes de date
    dataframes = [convert_date_columns(df) for df in dataframes]

    # Étape 4 : Sélection des colonnes pertinentes pour le premier DataFrame
    df = dataframes[0][SELECTED_COLUMNS].copy()

    # Étape 5 : Résolution des références dans freight_cost_usd et weight_kilograms
    dataset = df.copy()  # Référence pour les recherches d'ID
    for col in ["freight_cost_usd", "weight_kilograms"]:
            df[col] = df[col].astype(str).apply(lambda x: resolve_id_reference(x, dataset, col)).astype(float, errors='ignore')
    # Etape 6 elimination des lignes avec valeurs manquantes dans shpment et line_item_insurance
    df = df.dropna(subset=["shipment_mode", "line_item_insurance_usd"])
    # Calcul  cout total shipping
    # df['Total_Shipping_Cost'] = df['freight_cost_usd'] + df['line_item_insurance_usd']
    # # delai de livraison :
    # df['duration delivery (days)']=round((df['delivered_to_client_date']-df['scheduled_delivery_date']).dt.days,0)
    # df=df.drop(["freight_cost_usd","line_item_insurance_usd"],axis=1)

    deliveries=[]
    products=[]
    countries=[]
    shippment_mode=[]
    vendors=[]
    shippments=df[["id","shipment_mode"]]






    # Étape 7 : Affichage des informations
    # print("Colonnes nettoyées :", df.columns.tolist())
    # print("Colonnes avec 'date' :", find_date_columns(df))
    # print("Types de colonnes :\n", df.dtypes)
    # print("\nAperçu des données manquantes:\n", df.isna().sum() / df.shape[0])
    #print("\nAperçu des données :\n", df.head())
    print(shippments)
    

if __name__ == "__main__":
    main()