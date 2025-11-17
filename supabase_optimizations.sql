-- =================================================================
-- SCRIPT DE OTIMIZAÇÃO DO DASHBOARD - V2 (Modularizado)
-- Contém funções RPC dedicadas para cada componente do dashboard,
-- garantindo chamadas mais rápidas e evitando timeouts.
-- =================================================================

-- Função 1: Buscar os dados agregados para os KPIs principais.
-- Esta é a chamada mais leve para o carregamento inicial.
CREATE OR REPLACE FUNCTION get_dashboard_kpis()
RETURNS jsonb AS $$
BEGIN
    -- Usa uma CTE para pré-calcular a base de clientes a ser considerada
    -- para a cobertura, aplicando a regra de negócio de exclusão.
    RETURN (
        WITH client_base_for_coverage AS (
            SELECT codigo_cliente
            FROM public.data_clients
            WHERE (COALESCE(rca1, '') <> '53') OR (razaosocial ILIKE '%AMERICANAS%')
        )
        SELECT jsonb_build_object(
            'total_faturamento', SUM(CASE WHEN dd.tipovenda IN ('1', '9') THEN dd.vlvenda ELSE 0 END),
            'total_peso', SUM(dd.totpesoliq),
            'positivacao_count', COUNT(DISTINCT dd.codcli),
            'total_skus', COUNT(*),
            'total_clients_for_coverage', (SELECT COUNT(*) FROM client_base_for_coverage)
        )
        FROM public.data_detailed dd
        WHERE dd.codcli IN (SELECT codigo_cliente FROM client_base_for_coverage)
          AND (dd.vlvenda > 0 OR dd.vlbonific > 0)
    );
END;
$$ LANGUAGE plpgsql;

-- Função 6: Obter dados para os gráficos do dashboard principal, com filtros.
CREATE OR REPLACE FUNCTION get_main_charts_data(
    p_supervisor TEXT DEFAULT NULL,
    p_sellers TEXT[] DEFAULT NULL
    -- Adicionar mais parâmetros de filtro conforme necessário
)
RETURNS jsonb AS $$
DECLARE
    sales_by_person jsonb;
    sales_by_pasta jsonb;
    trend_data jsonb;
    top_products_fat jsonb;
    top_products_peso jsonb;
BEGIN
    -- Lógica de Vendas por Pessoa
    IF p_supervisor IS NOT NULL THEN
        SELECT jsonb_agg(jsonb_build_object('person', nome, 'total', total)) INTO sales_by_person FROM (
            SELECT nome, SUM(vlvenda) AS total FROM public.data_detailed WHERE superv = p_supervisor GROUP BY nome ORDER BY total DESC
        ) AS sub;
    ELSE
        SELECT jsonb_agg(jsonb_build_object('person', superv, 'total', total)) INTO sales_by_person FROM (
            SELECT superv, SUM(vlvenda) AS total FROM public.data_detailed WHERE superv IS NOT NULL GROUP BY superv ORDER BY total DESC
        ) AS sub;
    END IF;

    -- Lógica de Vendas por Categoria (Pasta)
    SELECT jsonb_agg(jsonb_build_object('pasta', observacaofor, 'total', total)) INTO sales_by_pasta FROM (
        SELECT observacaofor, SUM(vlvenda) AS total FROM public.data_detailed WHERE observacaofor IS NOT NULL GROUP BY observacaofor ORDER BY total DESC LIMIT 10
    ) AS sub;

    -- Lógica de Tendência (Placeholder, a lógica real é mais complexa e pode precisar de dados históricos)
    SELECT jsonb_build_object('avg_revenue', 150000, 'trend_revenue', 180000) INTO trend_data;

    -- Lógica Top 10 Produtos por Faturamento
    SELECT jsonb_agg(jsonb_build_object('produto', produto, 'descricao', descricao, 'faturamento', total)) INTO top_products_fat FROM (
        SELECT produto, MAX(descricao) as descricao, SUM(vlvenda) as total FROM public.data_detailed GROUP BY produto ORDER BY total DESC LIMIT 10
    ) AS sub;

    -- Lógica Top 10 Produtos por Peso
    SELECT jsonb_agg(jsonb_build_object('produto', produto, 'descricao', descricao, 'peso', total)) INTO top_products_peso FROM (
        SELECT produto, MAX(descricao) as descricao, SUM(totpesoliq) as total FROM public.data_detailed GROUP BY produto ORDER BY total DESC LIMIT 10
    ) AS sub;

    -- Combina todos os resultados
    RETURN jsonb_build_object(
        'sales_by_supervisor', sales_by_person,
        'sales_by_pasta', sales_by_pasta,
        'trend', trend_data,
        'top_10_products_faturamento', top_products_fat,
        'top_10_products_peso', top_products_peso
    );
END;
$$ LANGUAGE plpgsql;

-- Função 2: Obter dados para popular os filtros da UI.
CREATE OR REPLACE FUNCTION get_filter_data()
RETURNS jsonb AS $$
BEGIN
    RETURN (
        SELECT jsonb_build_object(
            'supervisors', (SELECT jsonb_agg(DISTINCT superv ORDER BY superv) FROM public.data_detailed WHERE superv IS NOT NULL),
            'sellers', (SELECT jsonb_agg(DISTINCT nome ORDER BY nome) FROM public.data_detailed WHERE nome IS NOT NULL),
            'suppliers', (SELECT jsonb_agg(jsonb_build_object('codfor', codfor, 'fornecedor', fornecedor)) FROM (SELECT DISTINCT codfor, fornecedor FROM public.data_detailed WHERE codfor IS NOT NULL AND fornecedor IS NOT NULL ORDER BY fornecedor) AS s),
            'sale_types', (SELECT jsonb_agg(DISTINCT tipovenda ORDER BY tipovenda) FROM public.data_detailed WHERE tipovenda IS NOT NULL),
            'positions', (SELECT jsonb_agg(DISTINCT posicao ORDER BY posicao) FROM public.data_detailed WHERE posicao IS NOT NULL AND posicao <> ''),
            'redes', (SELECT jsonb_agg(DISTINCT ramo ORDER BY ramo) FROM public.data_clients WHERE ramo IS NOT NULL AND ramo <> 'N/A')
        )
    );
END;
$$ LANGUAGE plpgsql;

-- Função 3: Obter dados para a tabela de "Pedidos Detalhados", com filtros.
CREATE OR REPLACE FUNCTION get_detailed_orders_data(
    p_supervisor TEXT DEFAULT NULL,
    p_seller TEXT DEFAULT NULL,
    p_sale_type TEXT DEFAULT NULL,
    p_position TEXT DEFAULT NULL
)
RETURNS jsonb AS $$
BEGIN
    RETURN (
        SELECT jsonb_agg(t) FROM (
            SELECT *
            FROM public.data_detailed
            WHERE
                (p_supervisor IS NULL OR superv = p_supervisor) AND
                (p_seller IS NULL OR nome = p_seller) AND
                (p_sale_type IS NULL OR tipovenda = p_sale_type) AND
                (p_position IS NULL OR posicao = p_position)
            ORDER BY dtped DESC
            LIMIT 200 -- Limita a quantidade de dados para evitar sobrecarga
        ) t
    );
END;
$$ LANGUAGE plpgsql;

-- Função 4: Obter dados para a view "Comparativo".
CREATE OR REPLACE FUNCTION get_comparison_data()
RETURNS jsonb AS $$
BEGIN
    -- Esta função pode ser complexa. Por enquanto, retorna um placeholder.
    -- A lógica real de comparação de dados históricos seria implementada aqui.
    RETURN jsonb_build_object(
        'historical_sales', (SELECT jsonb_agg(t) FROM public.data_history t LIMIT 100),
        'current_sales', (SELECT jsonb_agg(t) FROM public.data_detailed t LIMIT 100)
    );
END;
$$ LANGUAGE plpgsql;

-- Função 5: Obter dados para a view "Desempenho por Cidade".
CREATE OR REPLACE FUNCTION get_city_performance_data()
RETURNS jsonb AS $$
BEGIN
    RETURN (
        SELECT jsonb_agg(jsonb_build_object('cidade', cidade, 'total_vendas', total_vendas))
        FROM (
            SELECT
                c.cidade,
                SUM(d.vlvenda) AS total_vendas
            FROM public.data_detailed d
            JOIN public.data_clients c ON d.codcli = c.codigo_cliente
            WHERE c.cidade IS NOT NULL
            GROUP BY c.cidade
            ORDER BY total_vendas DESC
            LIMIT 50
        ) AS sales_by_city
    );
END;
$$ LANGUAGE plpgsql;
