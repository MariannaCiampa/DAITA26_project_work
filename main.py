import src.customers as customers
import src.products as products
import src.common as common

if __name__ == "__main__":
    risposta = ""
    while risposta != "0":

        risposta = input("""Che cosa vuoi fare?
        1 = esegui ETL
        2 = esegui integrazione dati regione e città
        3 = esegui format regione per PowerBI
        0 = esci dal programma""")
        if risposta == "1":
            df_customers = customers.extract()
            df_customers = customers.transform(df_customers)
            customers.load(df_customers)
        elif risposta == "2":
            customers.complete_city_region()
        elif risposta == "3":
            common.format_region()
        else:
            risposta = "0"




    #df_customers = customers.extract()
    #df_customers = customers.transform(df_customers)
    #print("Visualizzo i dati DOPO la trasformazione")
    #print(df_customers)
    #customers.load(df_customers)

    #customers.complete_city_region()