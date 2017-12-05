1. Please make sure the following files are in the ../data (some need to be unzipped)

* categories_indexed.npy - for looking up categories for products
* cats.txt - category name lookup
* ccm_general.npy - general co occurrence matrix (collaborative recommendations)
* cust_item_matrix.npy - customer x purchase matrix
* demo_matrix.npy - customer x demographics matrix
* dict_dir.npy - categories directory structure
* asin.npy - asin lookup
* season_price_instock_indexed.npy - asin x (season, price, instock) matrix
# rating_indexed.npy - content recommendations
* custids.npy - customer lookup

2. Make sure flask is installed

3. Run app.py 