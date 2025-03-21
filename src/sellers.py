import src.common as common
from dotenv import load_dotenv
import psycopg
import os
import datetime

load_dotenv()
host = os.getenv("host")
dbname = os.getenv("dbname")
user = os.getenv("user")
password = os.getenv("password")
port = os.getenv("port")

def extract():
    print("questo è il metodo EXTRACT di sellers")
    #src.common.read_file()
    df = common.read_file()
    return df

def transform(df):
    print("questo è il metodo TRANSFORM dei customers\n")
    print(df)
    print()


    print("--Valori Unici--")
    print(df.nunique())
    print()

    print("--Informazioni--")
    print(df.info())
    print()


    print("--Valori nulli--")
    print(df.isnull().sum())
    print()
    return df


def load(df):
    df["last_updated"] = datetime.datetime.now().isoformat(sep=" ", timespec="seconds")
    print("questo è il metodo LOAD di sellers\n")
    # print(df)
    with psycopg.connect(host=host, dbname=dbname, user=user, password=password, port=port) as conn:
        with conn.cursor() as cur:
            sql = """
                CREATE TABLE sellers (
                    seller_id VARCHAR PRIMARY KEY,
                    region VARCHAR,
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
                    common.delete_table("sellers")
                    print("Ricreo la tabella sellers")
                    cur.execute(sql)

            sql = """
                        INSERT INTO sellers
                        (seller_id, region, last_updated)
                        VALUES (%s, %s, %s)
                        ;
                """

            common.caricamento_barra(df, cur, sql)
            conn.commit()



def main():
    df = extract()
    df = transform(df)
    load(df)
    common.format_region()


if __name__ == "__main__":
    main()