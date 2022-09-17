# python-api-large-data-collection

In these scripts, data from a online marketplace, https://www.mercadolivre.com.br , is collected via their API and saved in database. A custom script I had done for a client. All the info about account and access keys are blank for their privacy.

## Features

### To take away/learn from here.
1. Dynamic table creation with 250+ columns.
2. Creating dynamic SQL query for 250+ columns.
3. Mapping / sorting data accordingly to insert into database accordingly.
4. Using multithreading and collecting the data.

### The problems - (Were specific for this task)
1. Nearly a million of products data, goes over a million sometimes.
2. Too many attributes for a single products, 250+. That means 250+ data-table columns.
3. To get all of the data takes too long. Have to reduce time.

### Solution implemented
1. This API has some different method to get all products data. When getting the product IDs, the API creates a temporary cache for the ids, and gives us a 'scroll_id'. Have to send request with the 'scroll_id' periodically to get the data. Can't request all the product IDs at once.
2. For saving 250+ records for a single product, here used dynamic table creation. map_data function in the script handles it. Basically, the key-value pair is used to dynamically insert data. Key with combined with parent node key has been used to create column names, and with that data is sorted accordingly to insert automatically.
3. Finally used multithreading to get the product details faster.

