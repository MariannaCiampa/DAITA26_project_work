import src.common as common
from dotenv import load_dotenv
import psycopg
import os
import datetime
import pandas as pd

load_dotenv()
host = os.getenv("host")
dbname = os.getenv("dbname")
user = os.getenv("user")
password = os.getenv("password")
port = os.getenv("port")


def extract():
    print("questo è il metodo EXTRACT di orders")
    df = common.read_file()
    return df

def transform(df):
    #stampo il dataframe
    print(df)
    print()

    #controllo che i valori siano unici
    print("--Valori Unici--")
    print(df.nunique())
    print()

    #controllo che non ci siano valori nulli
    print("--Valori nulli--")
    print(df.isnull().sum())
    print()

    #controllo i vari tipi di stato degli ordini
    print(df.groupby(by="order_status").count())

    # controllo i tipi di dato
    print("--Informazioni--")
    print(df.info())
    print()

    #riempio i parametri vuoti con "2000-01-01 00:00:00" per standardizzare
    df["order_delivered_customer_date"] = df["order_delivered_customer_date"].fillna("2000-01-01 00:00:00")

    #controllo nuovamente che i valori siano unici
    print("--Valori Unici--")
    print(df.nunique())
    print()

    #controllo nuovamente che non ci siano valori nulli
    print("--Valori nulli--")
    print(df.isnull().sum())
    print()
    return df

def load(df):
    print("questo è il metodo LOAD dei customers\n")
    df["last_updated"] = datetime.datetime.now().isoformat(sep=" ", timespec="seconds")
    #bisogna convertire le colonne in formato DATA
    df["order_purchase_timestamp"] = pd.to_datetime(df["order_purchase_timestamp"])
    df["order_delivered_customer_date"] = pd.to_datetime(df["order_delivered_customer_date"])
    #print(df)
    with psycopg.connect(host=host, dbname=dbname, user=user, password=password, port=port) as conn:
        with conn.cursor() as cur:
                sql = """
                CREATE TABLE orders (
                    pk_order VARCHAR PRIMARY KEY,
                    fk_customer VARCHAR,
                    order_status VARCHAR,
                    order_purchase_timestamp TIMESTAMP,
                    order_delivered_customer_date TIMESTAMP,
                    order_estimated_delivery_date DATE,
                    last_updated TIMESTAMP,  
                    FOREIGN KEY(fk_customer) REFERENCES customers(pk_customer)
                    ON DELETE CASCADE
                    );
                    """

                try:
                    cur.execute(sql)  # Inserimento report nel database
                except psycopg.errors.DuplicateTable as ex:
                    conn.commit()
                    print(ex)
                    domanda = input("Vuoi cancellare la tabella? si/no").strip().lower()
                    if domanda == "si":
                        # se risponde si: cancellare tabella
                        sql_delete = """
                    DROP TABLE orders;
                    """
                        cur.execute(sql_delete)
                        conn.commit()
                        print("Ricreo la tabella orders")
                        cur.execute(sql)


                sql = """
                        INSERT INTO orders
                        (pk_order, fk_customer, order_status, order_purchase_timestamp, order_delivered_customer_date, order_estimated_delivery_date, last_updated)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (pk_order) DO UPDATE 
                        SET (fk_customer, order_status, order_purchase_timestamp, order_delivered_customer_date, order_estimated_delivery_date, last_updated) = (EXCLUDED.fk_customer, EXCLUDED.order_status, EXCLUDED.order_purchase_timestamp, EXCLUDED.order_delivered_customer_date, EXCLUDED.order_estimated_delivery_date, EXCLUDED.last_updated);
                    """

                common.caricamento_barra(df, cur, sql)
                conn.commit()


def main():
    print("questo è il metodo MAIN di orders")
    df = extract()
    df = transform(df)
    #print("Dati trasformati")
    print(df)
    load(df)

if __name__ == "__main__":
    main()