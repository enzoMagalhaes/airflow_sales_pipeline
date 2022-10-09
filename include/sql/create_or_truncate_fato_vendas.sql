CREATE TABLE IF NOT EXISTS fato_vendas(

   id_venda INT PRIMARY KEY,
   id_funcionario INT NOT NULL,
   id_categoria INT NOT NULL,
   data_venda DATE NOT NULL,
   venda INT NOT NULL
   
);

TRUNCATE TABLE fato_vendas;
