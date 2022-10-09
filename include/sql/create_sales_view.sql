CREATE VIEW venda AS

    SELECT
    		    fato_venda.data_venda,
            fato_venda.venda,
            dim_categorias.nome_categoria,
            dim_funcionarios.nome_funcionario

    FROM fato_venda
    LEFT JOIN dim_categorias
    	ON dim_categorias.id_categoria = fato_venda.id_categoria
    LEFT JOIN dim_funcionarios
    	ON dim_funcionarios.id_funcionario = fato_venda.id_funcionario
    ORDER BY data_venda DESC
;
