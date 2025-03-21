import src.common as common
from dotenv import load_dotenv
import psycopg
import os
import datetime
import pandas as pd

from src.common import execute_one_query

load_dotenv()
host = os.getenv("host")
dbname = os.getenv("dbname")
user = os.getenv("user")
password = os.getenv("password")
port = os.getenv("port")

def extract():
    print("questo è il metodo EXTRACT di orders_products")
    df = common.read_file()
    return df


def transform(df):
    # stampo il dataframe
    print(df)
    print()

    # controllo che i valori siano unici
    print("--Valori Unici--")
    print(df.nunique())
    print()

    # controllo che non ci siano valori nulli
    print("--Valori nulli--")
    print(df.isnull().sum())
    print()

    # controllo a cosa fa riferimento la colonna "order_item"
    print(df.groupby(by="order_item").count())
    print(df.loc[df['order_item'] == 21])
    print(df.loc[df['order_id'] == "8272b63d03f5f79c56e9e4120aec44ef"])
    return df


def load(df):
    df["last_updated"] = datetime.datetime.now().isoformat(sep=" ", timespec="seconds")
    print("questo è il metodo LOAD di orders_products\n")
    #print(df)
    with psycopg.connect(host=host, dbname=dbname, user=user, password=password, port=port) as conn:
        with conn.cursor() as cur:
                sql = """
                CREATE TABLE orders_products (
                    pk_order_product SERIAL PRIMARY KEY,
                    fk_order_id VARCHAR,
                    order_item INTEGER,
                    fk_product_id VARCHAR,
                    fk_seller_id VARCHAR,
                    price FLOAT, 
                    freight FLOAT,
                    last_updated TIMESTAMP,
                    FOREIGN KEY(fk_order_id) REFERENCES orders(pk_order)
                    ON DELETE CASCADE,
                    FOREIGN KEY(fk_product_id) REFERENCES products(pk_product)
                    ON DELETE CASCADE,
                    FOREIGN KEY(fk_seller_id) REFERENCES sellers(seller_id)
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
                        common.delete_table("orders_products")
                        print("Ricreo la tabella orders_products")
                        cur.execute(sql)


                sql = """
                        INSERT INTO orders_products
                        (fk_order_id , order_item, fk_product_id, fk_seller_id, price,freight, last_updated)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (pk_order_product) DO UPDATE 
                        SET (fk_order_id, order_item, fk_product_id, fk_seller_id, price, freight, last_updated) = (EXCLUDED.fk_order_id, EXCLUDED.order_item, EXCLUDED.fk_product_id, EXCLUDED.fk_seller_id, EXCLUDED.price, EXCLUDED.freight, EXCLUDED.last_updated);
                    """

                common.caricamento_barra(df, cur, sql)
                conn.commit()



def delete_invalid_orders():
    #TODO cancellare da orders_products i record che hanno status delivered e delivered_time nullo
    sql = """
    DELETE FROM orders_products 
    WHERE fk_order_id
    IN (SELECT pk_order 
    FROM orders 
    WHERE orders.order_delivered_customer_date IS NULL 
    AND order_status = 'delivered')
    RETURNING *;
    """
    execute_one_query(sql, result=True)
    #TODO cancellare da orders i record che hanno status delivered e delivered_time nulli
    sql = """
    DELETE
    FROM orders 
    WHERE orders.order_delivered_customer_date IS NULL 
    AND order_status = 'delivered'
    RETURNING *;
    """

    print("Ordini non validi cancellati")
    execute_one_query(sql, result=True)






def main():
    print("questo è il metodo MAIN dei orders_products")
    df = extract()
    df = transform(df)
    #print("Dati trasformati")
    print(df)
    load(df)

if __name__ == "__main__":
    main()


