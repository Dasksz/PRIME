-- Ativa a extensão pg_stat_statements para monitorizar o desempenho das consultas.
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Otimização 1: Índice para filtragem comum na tabela de vendas detalhadas.
-- Ajuda em consultas que filtram por supervisor, vendedor ou cliente.
CREATE INDEX IF NOT EXISTS idx_data_detailed_filters ON data_detailed (superv, nome, codcli);

-- Otimização 2: Índice na data do pedido, crucial para séries temporais e filtros de período.
CREATE INDEX IF NOT EXISTS idx_data_detailed_dtped ON data_detailed (dtped);

-- Otimização 3: Índice na tabela de histórico para as mesmas otimizações.
CREATE INDEX IF NOT EXISTS idx_data_history_dtped ON data_history (dtped);

-- Função RPC principal para buscar dados agregados do dashboard.
-- Esta função substitui a necessidade de baixar as tabelas 'data_detailed' e 'data_history' inteiras para o cliente.
CREATE OR REPLACE FUNCTION get_initial_dashboard_data(
    supervisor_filter TEXT DEFAULT NULL,
    pasta_filter TEXT DEFAULT NULL,
    sellers_filter TEXT[] DEFAULT NULL,
    fornecedor_filter TEXT DEFAULT NULL,
    posicao_filter TEXT DEFAULT NULL,
    codcli_filter TEXT DEFAULT NULL
)
RETURNS JSONB AS $$
DECLARE
    result JSONB;
BEGIN
    WITH filtered_sales AS (
        -- CTE base para selecionar e filtrar os dados de vendas do mês atual.
        -- Todos os outros CTEs de agregação dependerão deste.
        SELECT
            s.*,
            c.ramo
        FROM data_detailed s
        LEFT JOIN data_clients c ON s.codcli = c.codigo_cliente
        WHERE
            (supervisor_filter IS NULL OR s.superv = supervisor_filter)
        AND (pasta_filter IS NULL OR s.observacaofor = pasta_filter)
        AND (sellers_filter IS NULL OR s.nome = ANY(sellers_filter))
        AND (fornecedor_filter IS NULL OR s.codfor = fornecedor_filter)
        AND (posicao_filter IS NULL OR s.posicao = posicao_filter)
        AND (codcli_filter IS NULL OR s.codcli = codcli_filter)
    ),
    kpis AS (
        -- CTE para agregar os KPIs principais em um único objeto JSON.
        SELECT jsonb_build_object(
            'total_faturamento', COALESCE(SUM(s.vlvenda), 0),
            'total_peso', COALESCE(SUM(s.totpesoliq), 0),
            'positivacao_count', COUNT(DISTINCT CASE WHEN s.vlvenda > 0 OR s.vlbonific > 0 THEN s.codcli END),
            'total_skus', COUNT(CASE WHEN s.vlvenda > 0 OR s.vlbonific > 0 THEN 1 END),
            'total_clients_for_coverage', (SELECT COUNT(DISTINCT codigo_cliente) FROM data_clients WHERE (razaosocial ILIKE '%AMERICANAS%' OR rca1 <> '53'))
        ) as data
        FROM filtered_sales s
    ),
    charts AS (
        -- CTE para construir um objeto JSON contendo os dados de todos os gráficos.
        SELECT jsonb_build_object(
            'sales_by_supervisor', (SELECT COALESCE(jsonb_agg(json_data), '[]'::jsonb) FROM (SELECT s.superv, SUM(s.vlvenda) AS total_faturamento FROM filtered_sales s GROUP BY s.superv ORDER BY total_faturamento DESC) AS json_data),
            'sales_by_pasta', (SELECT COALESCE(jsonb_agg(json_data), '[]'::jsonb) FROM (SELECT s.observacaofor, SUM(s.vlvenda) AS total_faturamento FROM filtered_sales s GROUP BY s.observacaofor ORDER BY total_faturamento ASC) AS json_data),
            'trend', (SELECT jsonb_build_object('avg_revenue', (SELECT AVG(monthly_revenue) FROM (SELECT SUM(vlvenda) AS monthly_revenue FROM data_history WHERE dtped >= date_trunc('month', NOW()) - INTERVAL '3 months' AND dtped < date_trunc('month', NOW()) GROUP BY date_trunc('month', dtped)) AS monthly_data), 'trend_revenue', (SELECT SUM(vlvenda) FROM filtered_sales) / GREATEST((SELECT value::INTEGER FROM data_metadata WHERE key = 'passed_working_days' LIMIT 1), 1) * 22)),
            'top_10_products_faturamento', (SELECT COALESCE(jsonb_agg(json_data), '[]'::jsonb) FROM (SELECT s.produto, s.descricao, SUM(s.vlvenda) AS faturamento FROM filtered_sales s GROUP BY s.produto, s.descricao ORDER BY faturamento DESC LIMIT 10) AS json_data),
            'top_10_products_peso', (SELECT COALESCE(jsonb_agg(json_data), '[]'::jsonb) FROM (SELECT s.produto, s.descricao, SUM(s.totpesoliq) AS peso FROM filtered_sales s GROUP BY s.produto, s.descricao ORDER BY peso DESC LIMIT 10) AS json_data)
        ) AS data
    ),
    filters AS (
        -- CTE para agregar os dados necessários para os filtros da UI.
        SELECT jsonb_build_object(
            'supervisors', (SELECT jsonb_agg(DISTINCT superv) FROM data_detailed WHERE superv IS NOT NULL),
            'sellers', (SELECT jsonb_agg(DISTINCT nome) FROM data_detailed WHERE nome IS NOT NULL),
            'suppliers', (SELECT jsonb_agg(DISTINCT jsonb_build_object('codfor', codfor, 'fornecedor', fornecedor)) FROM data_detailed WHERE codfor IS NOT NULL AND fornecedor IS NOT NULL)
        ) AS data
    ),
    metadata AS (
        -- CTE para obter metadados como a data da última atualização.
        SELECT
            (SELECT value FROM data_metadata WHERE key = 'last_update' LIMIT 1) AS last_update,
            (SELECT value::INTEGER FROM data_metadata WHERE key = 'passed_working_days' LIMIT 1) AS passed_working_days
    )
    -- Monta o resultado final combinando os JSONs de cada CTE.
    SELECT jsonb_build_object(
        'kpis', k.data,
        'charts', c.data,
        'filters', f.data,
        'last_update', m.last_update,
        'passed_working_days', m.passed_working_days
    )
    INTO result
    FROM kpis k, charts c, filters f, metadata m;

    RETURN result;
END;
$$ LANGUAGE plpgsql;
