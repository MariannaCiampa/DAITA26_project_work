# 3 metodi ETL per customers
#from src.common import read_file -> con questa scrittura basta richiamare semplicemente il metodo "read_file()"
#import src.common
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
    print("questo è il metodo EXTRACT di customers")
    #src.common.read_file()
    df = common.read_file()
    return df

def transform(df):
    print("questo è il metodo TRANSFORM dei customers\n")
    print(df)
    df = common.drop_duplicates(df)
    df = common.check_nulls(df, ["customer_id"])
    df = common.format_string(df, ["region", "city"])
    df = common.format_cap(df)
    #common.save_processed(df)
    return df

def load(df):
    df["last_updated"] = datetime.datetime.now().isoformat(sep=" ", timespec="seconds")
    print("questo è il metodo LOAD dei customers\n")
    #print(df)
    with psycopg.connect(host=host, dbname=dbname, user=user, password=password, port=port) as conn:
        with conn.cursor() as cur:
                sql = """
                CREATE TABLE customers (
                    pk_customer VARCHAR PRIMARY KEY,
                    region VARCHAR,
                    city VARCHAR,
                    cap VARCHAR,
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
                        sql_delete = """
                    DROP TABLE customers;
                    """
                        cur.execute(sql_delete)
                        conn.commit()
                        print("Ricreo la tabella customers")
                        cur.execute(sql)


                sql = """
                        INSERT INTO customers
                        (pk_customer, region, city, cap, last_updated)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (pk_customer) DO UPDATE 
                        SET (region, city, cap,last_updated) = (EXCLUDED.region, EXCLUDED.city, EXCLUDED.cap, EXCLUDED.last_updated);
                    """

                common.caricamento_barra(df, cur, sql)
                conn.commit()


def complete_city_region():
        with psycopg.connect(host=host,
                             dbname=dbname,
                             user=user,
                             password=password,
                             port=port) as conn:
            with conn.cursor() as cur:

                sql = f"""
                    UPDATE customers AS c1 
                    SET region = c2.region,
                    last_updated = '{datetime.datetime.now().isoformat(sep=" ", timespec="seconds")}'
                    FROM customers AS c2
                    WHERE c1.cap = c2.cap
                    AND c1.cap <> 'NaN'
                    AND c2.cap <> 'NaN'
                    AND c1.region = 'NaN'
                    AND c2.region <> 'NaN'
                    RETURNING *
                    ; """

                cur.execute(sql)
                #conn.commit()

                print("Record con region aggiornata")
                #print(cur.rowcount)
                for record in cur:
                    print(record)


                sql = f"""
                    UPDATE customers AS c1 
                    SET city = c2.city,
                    last_updated = '{datetime.datetime.now().isoformat(sep=" ", timespec="seconds")}'
                    FROM customers AS c2
                    WHERE c1.cap = c2.cap
                    AND c1.cap <> 'NaN'
                    AND c2.cap <> 'NaN'
                    AND c1.city = 'NaN'
                    AND c2.city <> 'NaN'
                    RETURNING *
                    ; """

                cur.execute(sql)
                #conn.commit()

                print("Record con city aggiornata")
                #print(cur.rowcount)
                for record in cur:
                    print(record)





def main():
    print("questo è il metodo MAIN dei customers")
    df = extract()
    df = transform(df)
    print("Dati trasformati")
    print(df)

    load(df)

if __name__ == "__main__":
    main()