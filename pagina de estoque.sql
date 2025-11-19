-- 1. REMOVER TODAS AS VERSÕES ANTIGAS DA FUNÇÃO DE COBERTURA
-- Estamos usando variações de assinatura para garantir que o PostgreSQL encontre e apague qualquer versão que exista.

DROP FUNCTION IF EXISTS get_coverage_view_data(text, text[], text[], text[], text, text);
DROP FUNCTION IF EXISTS get_coverage_view_data(text, text[], text, text, text[], text[], boolean);
DROP FUNCTION IF EXISTS get_coverage_view_data(text, text[], text[], text[], text, text, text[]); -- Versão mais recente se existir

-- 2. RECRIAR A FUNÇÃO CORRETA (V3 - FINAL)
-- Esta é a versão que está alinhada com o seu novo index.html
CREATE OR REPLACE FUNCTION get_coverage_view_data(
  p_supervisor_filter TEXT default '',
  p_sellers_filter text[] default null,
  p_suppliers_filter text[] default null,
  p_products_filter text[] default null,
  p_filial_filter TEXT default 'ambas',
  p_city_filter TEXT default ''
) RETURNS JSONB as $$
DECLARE
    result JSONB;
    current_month_start DATE;
    previous_month_start DATE;
BEGIN
    -- Define datas
    SELECT date_trunc('month', MAX(dtped))::DATE INTO current_month_start FROM data_detailed;
    previous_month_start := current_month_start - INTERVAL '1 month';

    WITH
    -- 1. Dados de Produtos e Estoque (somado)
    stock_sum AS (
        SELECT product_code, SUM(stock_qty) as total_stock 
        FROM data_stock 
        WHERE (p_filial_filter = 'ambas' OR filial = p_filial_filter)
        GROUP BY product_code
    ),
    base_products AS (
        SELECT pd.code, pd.descricao, COALESCE(s.total_stock, 0) as stock_qty
        FROM data_product_details pd
        LEFT JOIN stock_sum s ON pd.code = s.product_code
        WHERE (p_suppliers_filter IS NULL OR pd.codfor = ANY(p_suppliers_filter))
          AND (p_products_filter IS NULL OR pd.code = ANY(p_products_filter))
    ),
    -- 2. Vendas Recentes (Mês Atual + Anterior) para cálculo de cobertura
    sales_data AS (
        SELECT 
            s.produto, 
            s.codcli, 
            CASE WHEN s.dtped >= current_month_start THEN 1 ELSE 0 END as is_current
        FROM (
            SELECT produto, codcli, dtped, superv, nome, filial, codfor, observacaofor FROM data_detailed
            UNION ALL
            SELECT produto, codcli, dtped, superv, nome, filial, codfor, observacaofor FROM data_history
        ) s
        JOIN data_clients c ON s.codcli = c.codigo_cliente
        WHERE s.dtped >= previous_month_start
          AND (p_supervisor_filter = '' OR s.superv = p_supervisor_filter)
          AND (p_sellers_filter IS NULL OR s.nome = ANY(p_sellers_filter))
          AND (p_filial_filter = 'ambas' OR s.filial = p_filial_filter)
          AND (p_suppliers_filter IS NULL OR s.codfor = ANY(p_suppliers_filter))
          AND (p_city_filter = '' OR c.cidade ILIKE p_city_filter)
    ),
    -- 3. Agregação Final
    final_agg AS (
        SELECT 
            bp.code,
            bp.descricao,
            bp.stock_qty,
            COUNT(DISTINCT CASE WHEN sd.is_current = 0 THEN sd.codcli END) as clients_prev,
            COUNT(DISTINCT CASE WHEN sd.is_current = 1 THEN sd.codcli END) as clients_curr
        FROM base_products bp
        LEFT JOIN sales_data sd ON bp.code = sd.produto
        GROUP BY 1, 2, 3
    )
    SELECT jsonb_build_object(
        'coverage_table', (SELECT COALESCE(jsonb_agg(fa ORDER BY clients_curr DESC), '[]'::jsonb) FROM final_agg fa),
        'active_clients_count', (SELECT COUNT(*) FROM data_clients WHERE bloqueio <> 'S')
    ) INTO result;

    RETURN result;
END;
$$ LANGUAGE plpgsql;
