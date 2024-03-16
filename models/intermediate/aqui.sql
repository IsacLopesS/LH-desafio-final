/* 
CREATE VIEW relatorio_ceo AS
SELECT
    vendedor,
    regiao,
    COUNT(*) AS numero_de_vendas,
    SUM(valor_da_venda) AS total_de_vendas,
    AVG(valor_da_venda) AS venda_media,
    MODE() WITHIN GROUP (ORDER BY produto) AS produto_mais_vendido
FROM
    vendas
INNER JOIN
    vendedores ON vendas.vendedor_id = vendedores.id
INNER JOIN
    regioes ON vendas.regiao_id = regioes.id
GROUP BY
    vendedor,
    regiao;
*/