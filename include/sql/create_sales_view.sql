CREATE OR REPLACE VIEW vendas AS
    SELECT
    		    fato_vendas.data_venda,
            fato_vendas.venda,
            dim_categorias.nome_categoria,
            dim_funcionarios.nome_funcionario

    FROM fato_vendas
    LEFT JOIN dim_categorias
    	ON dim_categorias.id_categoria = fato_vendas.id_categoria
    LEFT JOIN dim_funcionarios
    	ON dim_funcionarios.id_funcionario = fato_vendas.id_funcionario
    ORDER BY fato_vendas.data_venda DESC
;
