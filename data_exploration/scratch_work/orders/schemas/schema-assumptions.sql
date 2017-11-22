--asin are unique
CREATE UNIQUE INDEX products_asin_uindex ON products (asin);

ALTER TABLE reviews
ADD CONSTRAINT reviews_products_asin_fk
FOREIGN KEY (asin) REFERENCES products (asin);

ALTER TABLE orderlines
ADD CONSTRAINT orderlines_products_productid_fk
FOREIGN KEY (productid) REFERENCES products (productid);

ALTER TABLE orderlines
ADD CONSTRAINT orderlines_orders_orderid_fk
FOREIGN KEY (orderid) REFERENCES orders (orderid);
