DROP TABLE IF EXISTS fato_venda;
CREATE TABLE fato_venda(

   id_venda INT PRIMARY KEY,
   id_funcionario INT NOT NULL,
   id_categoria INT NOT NULL,
   data_venda DATE NOT NULL,
   venda INT NOT NULL

   -- FOREIGN KEY (id_funcionario)
   --      REFERENCES dim_funcionarios (id_funcionario),
   --
   -- FOREIGN KEY (id_categoria)
   --      REFERENCES dim_categorias (id_categoria)

);
