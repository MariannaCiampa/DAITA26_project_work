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
    print("questo è il metodo EXTRACT di categories")
    df = common.read_file()
    return df

def transform(df):
    print("questo è il metodo TRANSFORM di categories\n")
    #common.format_categories(df)
    common.format_category_column(df, ["product_category_name_english", "product_category_name_italian"])
    common.add_categories(df)
    common.drop_duplicates(df)
    save_processed(df)
    print(df)
    return df

def load(df):
    df["last_updated"] = datetime.datetime.now().isoformat(sep=" ", timespec="seconds")
    df = df.drop("product_category_name_italian", axis="columns")
    print("questo è il metodo LOAD di categories\n")
    # print(df)
    with psycopg.connect(host=host, dbname=dbname, user=user, password=password, port=port) as conn:
        with conn.cursor() as cur:
            sql = """
                CREATE TABLE categories (
                    pk_id_category SERIAL PRIMARY KEY,
                    category_name VARCHAR UNIQUE,
                    last_updated TIMESTAMP
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
                    common.delete_table("categories")
                    print("Ricreo la tabella categories")
                    cur.execute(sql)

            sql = """
                        INSERT INTO categories
                        (category_name, last_updated)
                        VALUES (%s, %s)
                        ;
                """

            common.caricamento_barra(df, cur, sql)
            conn.commit()



def main():
    print("questo è il metodo MAIN dei categories")
    df = extract()
    df = transform(df)
    #print("Dati trasformati")
    print(df)
    load(df)

if __name__ == "__main__":
    main()
