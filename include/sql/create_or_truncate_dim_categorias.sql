CREATE TABLE IF NOT EXISTS dim_categorias(

   id_categoria INT PRIMARY KEY,
   nome_categoria VARCHAR(80) NOT NULL

);

TRUNCATE TABLE dim_categorias;
