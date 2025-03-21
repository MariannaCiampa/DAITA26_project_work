import src.customers as customers
import src.products as products
import src.common as common
import src.categories as categories
import src.products as products
import src.orders as orders
import src.orders_products as orders_products
import subprocess

from src.common import delete_all_tables

if __name__ == "__main__":
    risposta = ""
    while risposta != "0":

        risposta = input("""Che cosa vuoi fare?
        1 = esegui ETL di customers
        2 = esegui integrazione dati regione e città
        3 = esegui format regione per PowerBI
        4 = esegui ETL di categories
        5 = esegui ETL di products
        6 = esegui ETL di orders
        7 = esegui ETL di orders_products
        8 = esegui cancellazione ordini non validi
        9 = esegui jupyter -> clienti per regione
        10 = esegui jupyter -> clienti per città
        x = cancella tutte le tabelle
        0 = esci dal programma
        """)
        if risposta == "1":
            df_customers = customers.extract()
            df_customers = customers.transform(df_customers)
            customers.load(df_customers)
        elif risposta == "2":
            customers.complete_city_region()
        elif risposta == "3":
            common.format_region()
        elif risposta == "4":
            df_categories = categories.extract()
            df_categories = categories.transform(df_categories)
            categories.load(df_categories)
        elif risposta =="5":
            df_products = products.extract()
            df_products = products.transform(df_products)
            products.load(df_products)
        elif risposta == "6":
            df_orders = orders.extract()
            df_orders = orders.transform(df_orders)
            orders.load(df_orders)
        elif risposta == "7":
            df_orders_products = orders_products.extract()
            df_orders_products = orders_products.transform(df_orders_products)
            orders_products.load(df_orders_products)
        elif risposta == "8":
            orders_products.delete_invalid_orders()
        elif risposta == "9":
            subprocess.run(["jupyter", "notebook", "notebook/clienti_per_regione.ipynb"])
        elif risposta == "10":
            subprocess.run(["jupyter", "notebook", "notebook/clienti_per_città.ipynb"])
        elif risposta.lower() == "x":
            delete_all_tables()
        else:
            risposta = "0"


