import src.common as common
from dotenv import load_dotenv
import psycopg
import os
import datetime

from src.common import save_processed

load_dotenv()
host = os.getenv("host")
dbname = os.getenv("dbname")
user = os.getenv("user")
password = os.getenv("password")
port = os.getenv("port")


def extract():
    print("questo è il metodo EXTRACT di products")
    df = common.read_file()
    return df


def transform(df):
    print("questo è il metodo TRANSFORM di products")
    df = common.format_category_column(df, ["category"])
    format_products(df)
    #save_processed(df)
    print(df)
    return df


def format_products(df):
    #riempio i parametri vuoti con la categoria "uncategorized"
    df["category"] = df["category"].fillna("uncategorized")

    # controllo il tipo di dato (object(str),float,int)
    print("--Informazioni--")
    print(df.info())
    print()

    # verifico che non ci siano valori = 0
    print(df.groupby(by="product_name_lenght").count())
    print()
    print(df.groupby(by="product_description_lenght").count())
    print()
    print(df.groupby(by="product_photos_qty").count())
    print()

    colonne_da_convertire = ['product_name_lenght', 'product_description_lenght', 'product_photos_qty']
    df[colonne_da_convertire] = df[colonne_da_convertire].apply(lambda x: x.astype(str)).apply(lambda x: x.str.replace("nan", "0")).apply(lambda x: x.str.replace(".0", "")).apply(lambda x: x.astype(int))
    return df


def load(df):
    df["last_updated"] = datetime.datetime.now().isoformat(sep=" ", timespec="seconds")
    print("questo è il metodo LOAD dei products\n")
    #print(df)
    with psycopg.connect(host=host, dbname=dbname, user=user, password=password, port=port) as conn:
        with conn.cursor() as cur:
                sql = """
                CREATE TABLE products (
                    pk_product VARCHAR PRIMARY KEY,
                    fk_category VARCHAR,
                    product_name_lenght INTEGER,
                    product_description_lenght INTEGER,
                    product_photos_qty INTEGER,
                    last_updated TIMESTAMP,
                    FOREIGN KEY (fk_category) REFERENCES categories(category_name) 
                    ON DELETE CASCADE
                    );
                    """

#_on delete_ cascade serve per elimicare le righe sia della tabella considerata che di quella collegata a cascata
#_drop cascade_ cancello la tabella e quindi viene eliminato il CONSTRAINT

                try:
                    cur.execute(sql)  # Inserimento report nel database
                except psycopg.errors.DuplicateTable as ex:
                    conn.commit()
                    print(ex)
                    domanda = input("Vuoi cancellare la tabella? si/no").strip().lower()
                    if domanda == "si":
                        # se risponde si: cancellare tabella
                        common.delete_table("products")
                        print("Ricreo la tabella products")
                        cur.execute(sql)


                sql = """
                        INSERT INTO products
                        (pk_product, fk_category, product_name_lenght, product_description_lenght, product_photos_qty,last_updated)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (pk_product) DO UPDATE 
                        SET (fk_category, product_name_lenght, product_description_lenght, product_photos_qty,last_updated) = (EXCLUDED.fk_category, EXCLUDED.product_name_lenght, EXCLUDED.product_description_lenght, EXCLUDED.product_photos_qty, EXCLUDED.last_updated);
                    """

                common.caricamento_barra(df, cur, sql)
                conn.commit()



def main():
    print("questo è il metodo MAIN di products")
    df = extract()
    df = transform(df)
    load(df)
    print(df)

# per usare questo file come fosse un MODULO (slide 4.2)
if __name__ == "__main__":
    main()