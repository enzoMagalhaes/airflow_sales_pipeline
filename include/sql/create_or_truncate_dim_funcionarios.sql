CREATE TABLE IF NOT EXISTS dim_funcionarios(

   id_funcionario INT PRIMARY KEY,
   nome_funcionario VARCHAR(80) NOT NULL

);

TRUNCATE TABLE dim_funcionarios;
