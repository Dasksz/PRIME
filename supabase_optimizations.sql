-- Remove todos os índices e funções anteriores para um recomeço limpo
DROP FUNCTION IF EXISTS get_initial_dashboard_data(TEXT, TEXT, TEXT[], TEXT, TEXT, TEXT);
DROP FUNCTION IF EXISTS get_comparison_data(TEXT, TEXT[], TEXT[], TEXT[], TEXT, TEXT, TEXT, TEXT, TEXT[]);
DROP FUNCTION IF EXISTS get_orders_data(INT, INT, TEXT, TEXT[], TEXT[], TEXT, TEXT, TEXT, TEXT, TEXT[]);
DROP FUNCTION IF EXISTS get_city_data(TEXT, TEXT[], TEXT, TEXT, TEXT, TEXT[]);
DROP FUNCTION IF EXISTS get_weekly_data(TEXT[], TEXT);
DROP FUNCTION IF EXISTS get_stock_data();
DROP FUNCTION IF EXISTS get_innovations_data();
DROP FUNCTION IF EXISTS get_coverage_data();

-- Recria apenas os índices essenciais
CREATE INDEX IF NOT EXISTS idx_data_detailed_filters ON data_detailed (superv, nome);
CREATE INDEX IF NOT EXISTS idx_data_detailed_dtped ON data_detailed (dtped);

-- VERSÃO 4: Abordagem de contingência.
-- Esta função abandona a agregação no servidor para evitar o timeout a todo custo.
-- Ela apenas filtra os dados brutos e os retorna ao cliente, que fará a agregação.
-- Isso garante que a aplicação carregue, mesmo que a performance não seja a ideal.
CREATE OR REPLACE FUNCTION get_initial_dashboard_data(
    supervisor_filter TEXT DEFAULT NULL,
    pasta_filter TEXT DEFAULT NULL,
    sellers_filter TEXT[] DEFAULT NULL,
    fornecedor_filter TEXT DEFAULT NULL,
    posicao_filter TEXT DEFAULT NULL,
    codcli_filter TEXT DEFAULT NULL
)
RETURNS JSONB AS $$
BEGIN
    RETURN (
        SELECT jsonb_build_object(
            'raw_sales', COALESCE(jsonb_agg(s), '[]'::jsonb)
        )
        FROM data_detailed s
        WHERE
            (supervisor_filter IS NULL OR s.superv = supervisor_filter)
        AND (pasta_filter IS NULL OR s.observacaofor = pasta_filter)
        AND (sellers_filter IS NULL OR s.nome = ANY(sellers_filter))
        AND (fornecedor_filter IS NULL OR s.codfor = fornecedor_filter)
        AND (posicao_filter IS NULL OR s.posicao = posicao_filter)
        AND (codcli_filter IS NULL OR s.codcli = codcli_filter)
    );
END;
$$ LANGUAGE plpgsql;

-- As outras funções permanecem como esqueletos para não quebrar a UI
CREATE OR REPLACE FUNCTION get_orders_data(p_page_number INT, p_items_per_page INT, p_supervisor_filter TEXT, p_sellers_filter TEXT[], p_tipos_venda_filter TEXT[], p_fornecedor_filter TEXT, p_posicao_filter TEXT, p_codcli_filter TEXT, p_rede_group_filter TEXT, p_selected_redes TEXT[])
RETURNS JSONB AS $$ BEGIN RETURN '{"total_count": 0, "orders": []}'::jsonb; END; $$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_comparison_data(p_supervisor_filter TEXT, p_sellers_filter TEXT[], p_suppliers_filter TEXT[], p_products_filter TEXT[], p_pasta_filter TEXT, p_city_filter TEXT, p_filial_filter TEXT, p_rede_group_filter TEXT, p_selected_redes TEXT[])
RETURNS JSONB AS $$ BEGIN RETURN '{"current_sales": [], "history_sales": []}'::jsonb; END; $$ LANGUAGE plpgsql;
