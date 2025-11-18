-- Ativa a extensão pg_stat_statements para monitorizar o desempenho das consultas.
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Otimização 1: Índice composto principal para filtros comuns.
CREATE INDEX IF NOT EXISTS idx_data_detailed_filters ON data_detailed (superv, nome, codcli);

-- Otimização 2: Índices para colunas frequentemente usadas em GROUP BY e filtros individuais.
CREATE INDEX IF NOT EXISTS idx_data_detailed_dtped ON data_detailed (dtped);
CREATE INDEX IF NOT EXISTS idx_data_detailed_observacaofor ON data_detailed (observacaofor);
CREATE INDEX IF NOT EXISTS idx_data_detailed_codfor ON data_detailed (codfor);
CREATE INDEX IF NOT EXISTS idx_data_detailed_posicao ON data_detailed (posicao);

-- Otimização 3: Índice na tabela de histórico.
CREATE INDEX IF NOT EXISTS idx_data_history_dtped ON data_history (dtped);

-- Função RPC principal para buscar dados agregados do dashboard.
-- Esta versão foi reescrita para performance, usando CTEs granulares para evitar
-- múltiplas varreduras da tabela de vendas filtrada.
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
        SELECT
            s.vlvenda,
            s.totpesoliq,
            s.vlbonific,
            s.codcli,
            s.superv,
            s.observacaofor,
            s.produto,
            s.descricao
        FROM data_detailed s
        WHERE
            (supervisor_filter IS NULL OR s.superv = supervisor_filter)
        AND (pasta_filter IS NULL OR s.observacaofor = pasta_filter)
        AND (sellers_filter IS NULL OR s.nome = ANY(sellers_filter))
        AND (fornecedor_filter IS NULL OR s.codfor = fornecedor_filter)
        AND (posicao_filter IS NULL OR s.posicao = posicao_filter)
        AND (codcli_filter IS NULL OR s.codcli = codcli_filter)
    ),
    kpi_agg AS (
        -- Agrega todos os KPIs em uma única passagem sobre os dados filtrados.
        SELECT
            COALESCE(SUM(s.vlvenda), 0) AS total_faturamento,
            COALESCE(SUM(s.totpesoliq), 0) AS total_peso,
            COUNT(DISTINCT CASE WHEN s.vlvenda > 0 OR s.vlbonific > 0 THEN s.codcli END) AS positivacao_count,
            COUNT(CASE WHEN s.vlvenda > 0 OR s.vlbonific > 0 THEN 1 END) AS total_skus
        FROM filtered_sales s
    ),
    total_clients_agg AS (
        -- Subconsulta independente para o total de clientes da cobertura (não muda com os filtros).
        SELECT COUNT(DISTINCT codigo_cliente) AS total_clients_for_coverage
        FROM data_clients
        WHERE (razaosocial ILIKE '%AMERICANAS%' OR rca1 <> '53')
    ),
    sales_by_supervisor_agg AS (
        -- Agregação para o gráfico de vendas por supervisor.
        SELECT COALESCE(jsonb_agg(json_data ORDER BY total_faturamento DESC), '[]'::jsonb) AS data
        FROM (
            SELECT s.superv, SUM(s.vlvenda) AS total_faturamento
            FROM filtered_sales s
            GROUP BY s.superv
        ) AS json_data
    ),
    sales_by_pasta_agg AS (
        -- Agregação para o gráfico de vendas por pasta/categoria.
        SELECT COALESCE(jsonb_agg(json_data ORDER BY total_faturamento ASC), '[]'::jsonb) AS data
        FROM (
            SELECT s.observacaofor, SUM(s.vlvenda) AS total_faturamento
            FROM filtered_sales s
            GROUP BY s.observacaofor
        ) AS json_data
    ),
    top_10_products_faturamento_agg AS (
        -- Agregação para o top 10 produtos por faturamento.
        SELECT COALESCE(jsonb_agg(json_data), '[]'::jsonb) AS data
        FROM (
            SELECT s.produto, s.descricao, SUM(s.vlvenda) AS faturamento
            FROM filtered_sales s
            GROUP BY s.produto, s.descricao
            ORDER BY faturamento DESC
            LIMIT 10
        ) AS json_data
    ),
    top_10_products_peso_agg AS (
        -- Agregação para o top 10 produtos por peso.
        SELECT COALESCE(jsonb_agg(json_data), '[]'::jsonb) AS data
        FROM (
            SELECT s.produto, s.descricao, SUM(s.totpesoliq) AS peso
            FROM filtered_sales s
            GROUP BY s.produto, s.descricao
            ORDER BY peso DESC
            LIMIT 10
        ) AS json_data
    ),
    history_trend_agg AS (
        -- Subconsulta independente para a média de faturamento histórico para o gráfico de tendência.
        SELECT AVG(monthly_revenue) as avg_revenue
        FROM (
            SELECT SUM(vlvenda) AS monthly_revenue
            FROM data_history
            WHERE dtped >= date_trunc('month', NOW()) - INTERVAL '3 months'
              AND dtped < date_trunc('month', NOW())
            GROUP BY date_trunc('month', dtped)
        ) AS monthly_data
    ),
    filters_agg AS (
        -- Agrega dados para os filtros da UI. Executado de forma independente para sempre mostrar todas as opções.
        SELECT
            (SELECT jsonb_agg(DISTINCT superv) FROM data_detailed WHERE superv IS NOT NULL) AS supervisors,
            (SELECT jsonb_agg(DISTINCT nome) FROM data_detailed WHERE nome IS NOT NULL) AS sellers,
            (SELECT jsonb_agg(DISTINCT jsonb_build_object('codfor', codfor, 'fornecedor', fornecedor)) FROM data_detailed WHERE codfor IS NOT NULL AND fornecedor IS NOT NULL) AS suppliers
    ),
    metadata AS (
        -- Busca metadados essenciais.
        SELECT
            (SELECT value FROM data_metadata WHERE key = 'last_update' LIMIT 1) AS last_update,
            (SELECT value::INTEGER FROM data_metadata WHERE key = 'passed_working_days' LIMIT 1) AS passed_working_days
    )
    -- Monta o resultado final combinando os JSONs de cada CTE.
    SELECT jsonb_build_object(
        'kpis', (
            SELECT jsonb_build_object(
                'total_faturamento', k.total_faturamento,
                'total_peso', k.total_peso,
                'positivacao_count', k.positivacao_count,
                'total_skus', k.total_skus,
                'total_clients_for_coverage', tc.total_clients_for_coverage
            )
            FROM kpi_agg k, total_clients_agg tc
        ),
        'charts', (
            SELECT jsonb_build_object(
                'sales_by_supervisor', s_sup.data,
                'sales_by_pasta', s_pasta.data,
                'top_10_products_faturamento', top_fat.data,
                'top_10_products_peso', top_peso.data,
                'trend', jsonb_build_object(
                    'avg_revenue', h_trend.avg_revenue,
                    'trend_revenue', (SELECT k.total_faturamento FROM kpi_agg k) / GREATEST((SELECT m.passed_working_days FROM metadata m), 1) * 22
                )
            )
            FROM sales_by_supervisor_agg s_sup,
                 sales_by_pasta_agg s_pasta,
                 top_10_products_faturamento_agg top_fat,
                 top_10_products_peso_agg top_peso,
                 history_trend_agg h_trend
        ),
        'filters', (SELECT to_jsonb(f) FROM filters_agg f),
        'last_update', (SELECT last_update FROM metadata),
        'passed_working_days', (SELECT passed_working_days FROM metadata)
    )
    INTO result;

    RETURN result;
END;
$$ LANGUAGE plpgsql;
