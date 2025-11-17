
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
    -- Declara variáveis para armazenar os resultados das agregações.
    kpis JSONB;
    filters JSONB;
    charts JSONB;
    result JSONB;
BEGIN
    -- Utiliza Common Table Expressions (CTEs) para organizar a consulta.
    WITH filtered_sales AS (
        -- Esta CTE seleciona e filtra os dados de vendas do mês atual.
        -- Os filtros são aplicados apenas se os parâmetros correspondentes não forem nulos.
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
    kpi_agg AS (
        -- Agrega os KPIs principais a partir dos dados filtrados.
        SELECT
            COALESCE(SUM(vlvenda), 0) AS total_faturamento,
            COALESCE(SUM(totpesoliq), 0) AS total_peso,
            COUNT(DISTINCT CASE WHEN vlvenda > 0 OR vlbonific > 0 THEN codcli END) AS positivacao_count,
            COUNT(CASE WHEN vlvenda > 0 OR vlbonific > 0 THEN 1 END) AS total_skus
        FROM filtered_sales
    ),
    clients_for_coverage_agg AS (
        -- Calcula o número total de clientes relevantes para o cálculo da cobertura.
        SELECT COUNT(DISTINCT codigo_cliente) AS total_clients
        FROM data_clients
        WHERE (razaosocial ILIKE '%AMERICANAS%' OR rca1 <> '53')
    ),
    last_update_agg AS (
        -- Obtém a data da última atualização dos metadados.
        SELECT value AS last_update FROM data_metadata WHERE key = 'last_update' LIMIT 1
    ),
    passed_days_agg AS (
        -- Obtém o número de dias úteis passados no mês.
        SELECT value::INTEGER AS passed_working_days FROM data_metadata WHERE key = 'passed_working_days' LIMIT 1
    ),
    -- Novas CTEs para agregar dados para os gráficos
    sales_by_supervisor_agg AS (
        SELECT superv, SUM(vlvenda) as total_faturamento
        FROM filtered_sales
        GROUP BY superv
        ORDER BY total_faturamento DESC
    ),
    sales_by_pasta_agg AS (
        SELECT observacaofor, SUM(vlvenda) as total_faturamento
        FROM filtered_sales
        GROUP BY observacaofor
        ORDER BY total_faturamento ASC
    ),
    trend_agg AS (
        SELECT
            -- Calcula a média de faturamento dos últimos 3 meses do histórico.
            (SELECT AVG(monthly_revenue)
             FROM (
                SELECT SUM(vlvenda) as monthly_revenue
                FROM data_history
                WHERE dtped >= date_trunc('month', now()) - interval '3 months'
                  AND dtped < date_trunc('month', now())
                GROUP BY date_trunc('month', dtped)
             ) as monthly_data) as avg_revenue,
            -- Calcula a tendência de faturamento para o mês atual.
            (SELECT SUM(vlvenda) FROM filtered_sales) / COALESCE(NULLIF((SELECT passed_working_days FROM passed_days_agg), 0), 1) * 22 AS trend_revenue
    ),
    top_10_products_faturamento_agg AS (
        SELECT produto, descricao, SUM(vlvenda) as faturamento
        FROM filtered_sales
        GROUP BY produto, descricao
        ORDER BY faturamento DESC
        LIMIT 10
    ),
    top_10_products_peso_agg AS (
        SELECT produto, descricao, SUM(totpesoliq) as peso
        FROM filtered_sales
        GROUP BY produto, descricao
        ORDER BY peso DESC
        LIMIT 10
    )
    -- Monta o objeto JSON de KPIs.
    SELECT jsonb_build_object(
        'total_faturamento', COALESCE(k.total_faturamento, 0),
        'total_peso', COALESCE(k.total_peso, 0),
        'positivacao_count', COALESCE(k.positivacao_count, 0),
        'total_skus', COALESCE(k.total_skus, 0),
        'total_clients_for_coverage', COALESCE(c.total_clients, 0)
    ) INTO kpis
    FROM kpi_agg k, clients_for_coverage_agg c;

    -- Monta o objeto JSON de Filtros (dados para popular os dropdowns).
    SELECT jsonb_build_object(
        'supervisors', (SELECT jsonb_agg(DISTINCT superv) FROM data_detailed WHERE superv IS NOT NULL),
        'sellers', (SELECT jsonb_agg(DISTINCT nome) FROM data_detailed WHERE nome IS NOT NULL),
        'suppliers', (SELECT jsonb_agg(DISTINCT jsonb_build_object('codfor', codfor, 'fornecedor', fornecedor)) FROM (SELECT DISTINCT codfor, fornecedor FROM data_detailed WHERE codfor IS NOT NULL AND fornecedor IS NOT NULL) as distinct_suppliers)
    ) INTO filters;

    -- Monta o objeto JSON dos Gráficos.
    SELECT jsonb_build_object(
        'sales_by_supervisor', (SELECT jsonb_agg(s) FROM sales_by_supervisor_agg s),
        'sales_by_pasta', (SELECT jsonb_agg(p) FROM sales_by_pasta_agg p),
        'trend', (SELECT to_jsonb(t) FROM trend_agg t),
        'top_10_products_faturamento', (SELECT jsonb_agg(f) FROM top_10_products_faturamento_agg f),
        'top_10_products_peso', (SELECT jsonb_agg(w) FROM top_10_products_peso_agg w)
    ) INTO charts;

    -- Constrói o resultado final combinando todos os objetos JSON.
    SELECT jsonb_build_object(
        'kpis', kpis,
        'filters', filters,
        'charts', charts,
        'last_update', (SELECT last_update FROM last_update_agg),
        'passed_working_days', (SELECT passed_working_days FROM passed_days_agg)
    ) INTO result;

    RETURN result;
END;
$$ LANGUAGE plpgsql;
