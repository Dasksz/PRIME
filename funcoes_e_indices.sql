-- 1. Remover versões anteriores para garantir limpeza
DROP FUNCTION IF EXISTS get_stock_view_data(text, text[], text[], text[], text, text[], text);
DROP FUNCTION IF EXISTS get_stock_view_data(text, text[], text[], text[], text, text[], text, integer);

-- 2. Criar a Função Otimizada (V5 - Dias Úteis Dinâmicos)
CREATE OR REPLACE FUNCTION get_stock_view_data (
  p_supervisor_filter TEXT default '',
  p_sellers_filter text[] default null,
  p_suppliers_filter text[] default null,
  p_products_filter text[] default null,
  p_rede_group_filter TEXT default '',
  p_redes_filter text[] default null,
  p_filial_filter TEXT default 'ambas',
  p_custom_days INTEGER default 0 -- Novo parâmetro para dias manuais
) RETURNS JSONB as $$
DECLARE
    result JSONB;
    current_month_start DATE;
    last_sale_date DATE;
    history_start_date DATE;
    history_end_date DATE;

    -- Variáveis para cálculo de dias
    calc_start DATE;
    calc_end DATE;
    working_days_count INTEGER;
    days_divisor NUMERIC;
BEGIN
    -- Define datas base
    SELECT date_trunc('month', MAX(dtped))::DATE, MAX(dtped)::DATE
    INTO current_month_start, last_sale_date
    FROM data_detailed;

    -- Histórico: 3 meses antes do mês atual
    history_end_date := current_month_start - INTERVAL '1 day';
    history_start_date := current_month_start - INTERVAL '3 months';

    -- Lógica de Dias Úteis
    -- Período total: do inicio do histórico até a última venda registrada (hoje)
    calc_start := history_start_date;
    calc_end := last_sale_date;

    IF p_custom_days > 0 THEN
        -- Se o usuário digitou algo no input, usamos o valor dele
        working_days_count := p_custom_days;
    ELSE
        -- Senão, calculamos os dias úteis (Seg-Sex) no intervalo total
        -- extract(isodow) retorna 1=Seg, 6=Sab, 7=Dom
        SELECT COUNT(*)
        INTO working_days_count
        FROM generate_series(calc_start, calc_end, '1 day') d
        WHERE extract(isodow from d) < 6;
    END IF;

    -- Evita divisão por zero
    days_divisor := GREATEST(working_days_count, 1)::NUMERIC;

    WITH
    -- 1. Filtra Clientes
    sales_clients AS (
        SELECT DISTINCT codcli FROM data_detailed
        WHERE (p_supervisor_filter = '' OR superv = p_supervisor_filter)
          AND (p_sellers_filter IS NULL OR nome = ANY(p_sellers_filter))
    ),
    final_clients AS (
        SELECT c.codigo_cliente 
        FROM data_clients c
        JOIN sales_clients sc ON c.codigo_cliente = sc.codcli
        WHERE (p_rede_group_filter = '' OR 
              (p_rede_group_filter = 'sem_rede' AND (c.ramo IS NULL OR c.ramo = 'N/A')) OR
              (p_rede_group_filter = 'com_rede' AND (p_redes_filter IS NULL OR c.ramo = ANY(p_redes_filter))))
    ),
    
    -- 2. Estoque
    stock_aggregated AS (
        SELECT product_code, SUM(stock_qty) as total_stock
        FROM data_stock
        WHERE (p_filial_filter IS NULL OR p_filial_filter = 'ambas' OR filial = p_filial_filter)
        GROUP BY product_code
    ),

    -- 3. Vendas Atuais (Mês Corrente)
    current_sales AS (
        SELECT s.produto, s.qtvenda_embalagem_master
        FROM data_detailed s
        WHERE s.codcli IN (SELECT codigo_cliente FROM final_clients)
          AND (p_filial_filter = 'ambas' OR s.filial = p_filial_filter)
    ),
    current_sales_agg AS (
        SELECT produto, SUM(qtvenda_embalagem_master) as total_qty
        FROM current_sales GROUP BY produto
    ),

    -- 4. Vendas Históricas (3 Meses Anteriores)
    history_sales AS (
        SELECT s.produto, s.qtvenda_embalagem_master
        FROM data_history s
        WHERE s.codcli IN (SELECT codigo_cliente FROM final_clients)
          AND s.dtped BETWEEN history_start_date AND history_end_date
          AND (p_filial_filter = 'ambas' OR s.filial = p_filial_filter)
    ),
    history_sales_agg AS (
        SELECT produto, SUM(qtvenda_embalagem_master) as total_qty_history
        FROM history_sales GROUP BY produto
    ),
    
    -- 5. Cruzamento e Cálculos
    product_analysis AS (
        SELECT
            pd.code AS product_code,
            pd.descricao AS product_description,
            pd.fornecedor AS supplier_name,
            COALESCE(sa.total_stock, 0) as stock_qty,
            COALESCE(csa.total_qty, 0) as current_month_sales_qty,

            -- Média Mensal (Apenas para referência visual nas colunas de comparação)
            COALESCE(hsa.total_qty_history, 0) / 3.0 as history_avg_monthly_qty,

            -- CÁLCULO NOVO: Média Diária Real (Histórico Total + Atual Total) / Dias Totais
            (COALESCE(csa.total_qty, 0) + COALESCE(hsa.total_qty_history, 0)) / days_divisor as daily_avg_qty

        FROM data_product_details pd
        LEFT JOIN stock_aggregated sa ON pd.code = sa.product_code
        LEFT JOIN current_sales_agg csa ON pd.code = csa.produto
        LEFT JOIN history_sales_agg hsa ON pd.code = hsa.produto
        WHERE
            (p_suppliers_filter IS NULL OR pd.codfor = ANY(p_suppliers_filter))
            AND (p_products_filter IS NULL OR pd.code = ANY(p_products_filter))
            AND (COALESCE(sa.total_stock, 0) > 0 OR COALESCE(csa.total_qty, 0) > 0 OR COALESCE(hsa.total_qty_history, 0) > 0)
    ),
    
    categorized_products AS (
        SELECT *,
            CASE
                WHEN current_month_sales_qty > 0 AND history_avg_monthly_qty > 0 AND current_month_sales_qty >= history_avg_monthly_qty THEN 'growth'
                WHEN current_month_sales_qty > 0 AND history_avg_monthly_qty > 0 AND current_month_sales_qty < history_avg_monthly_qty THEN 'decline'
                WHEN current_month_sales_qty > 0 AND history_avg_monthly_qty = 0 THEN 'new'
                WHEN current_month_sales_qty = 0 AND history_avg_monthly_qty > 0 THEN 'lost'
                ELSE NULL
            END as category
        FROM product_analysis
    )
    
    SELECT jsonb_build_object(
        'working_days_used', working_days_count, -- Retorna quantos dias usou
        'stock_table', (
            SELECT COALESCE(jsonb_agg(jsonb_build_object(
                'product_code', pa.product_code,
                'product_description', pa.product_description,
                'supplier_name', pa.supplier_name,
                'stock_qty', pa.stock_qty,
                'avg_monthly_qty', pa.history_avg_monthly_qty,
                'daily_avg_qty', pa.daily_avg_qty,
                -- Tendência em dias = Estoque / Média Diária Real
                'trend_days', CASE WHEN pa.daily_avg_qty > 0 THEN (pa.stock_qty / pa.daily_avg_qty) ELSE 999 END
            ) ORDER BY (CASE WHEN pa.daily_avg_qty > 0 THEN (pa.stock_qty / pa.daily_avg_qty) ELSE 999 END) ASC), '[]'::jsonb)
            FROM product_analysis pa
        ),
        'growth_table', (SELECT COALESCE(jsonb_agg(cp ORDER BY (current_month_sales_qty - history_avg_monthly_qty) DESC), '[]'::jsonb) FROM categorized_products cp WHERE category = 'growth'),
        'decline_table', (SELECT COALESCE(jsonb_agg(cp ORDER BY (current_month_sales_qty - history_avg_monthly_qty) ASC), '[]'::jsonb) FROM categorized_products cp WHERE category = 'decline'),
        'new_table', (SELECT COALESCE(jsonb_agg(cp ORDER BY current_month_sales_qty DESC), '[]'::jsonb) FROM categorized_products cp WHERE category = 'new'),
        'lost_table', (SELECT COALESCE(jsonb_agg(cp ORDER BY history_avg_monthly_qty DESC), '[]'::jsonb) FROM categorized_products cp WHERE category = 'lost')
    ) INTO result;

    RETURN result;
END;
$$ LANGUAGE plpgsql;