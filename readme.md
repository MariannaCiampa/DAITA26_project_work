md = mark down

# TITOLO

## esempio
* pippo
**pippo**
~~**pippo**~~
_**pippo**_


## **customers**
* pk_customer VARCHAR
* region VARCHAR
* city VARCHAR
* cap VARCHAR (si potrebbe eliminare)


## **categories**
* pk_category SERIAL
* name VARCHAR (in inglese)


## **products**
* pk_product VARCHAR 
* fk_category INTEGER
* name_length INTEGER (errore ortografico)
* description length INTEGER 
* imgs_qty INTEGER


## **orders**
* pk_order VARCHAR
* fk_customer VARCHAR 
* status VARCHAR
* purchase_timestamp TIMESTAMP
* delivered_timestamp TIMESTAMP
* estimated_date DATE


(per il momento non abbiamo questa tabella)
## **sellers**
* pk_seller VARCHAR 
* region VARCHAR 


## **orders_products**
* pk_order_product SERIAL
* fk_order VARCHAR
* fk_product VARCHAR
* fk_seller VARCHAR
* price FLOAT
* freight FLOAT (=costo di trasporto)

(abbiamo escluso order_item)

## TODO OPZIONALE
* copia del file in input alla cartella raw
* (fare in modo che il nome del file sia univoco, con data e ora)
* creare il database da python(quindi non a mano da pgadmin)

* controllo validità input
* controllo su user e password prima di cancellare tabella (variabili di ambiente)
* (chiedere in input, verificare validità, e solo nel caso autorizzare cancellazione della tabella)
* inserimento colonna per tenere traccia della data in cui i dati sono stati inseriti nel database
* integrare dati customers a partire dal cap
* gestione del tipo di valore da aggiornare in fill_null
* formattazione stringhe (no caratteri speciali all'inizio e alla fine)(select_dtypes("object"))
* codifica utf8
* maiuscole all'inizio
* solo lettere maiuscole e minuscole, apostrofo, spazio, trattino
* finito tutto, visualizziamo un menù delle opzioni disponibili da eseguire sul database
(tipo integrare dati su regioni-città,
visualizzare i prodotti di una certa categoria/regione
visualizzare clienti per regione etc)
* a seconda della scelta dell'utente, richiameremo il metodo con questa funzione