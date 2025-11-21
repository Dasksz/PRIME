-- Função para Análise de Inovações (Filtrada para PEPSICO)
-- Substitui/Cria get_innovations_data_v2
-- Garante que apenas produtos Pepsico sejam retornados e otimiza a query.

DROP FUNCTION IF EXISTS get_innovations_data_v2(text, text[], text[], boolean, text, text, text[]);

CREATE OR REPLACE FUNCTION get_innovations_data_v2(
    p_supervisor_filter text DEFAULT ''::text,
    p_sellers_filter text[] DEFAULT NULL::text[],
    p_product_codes text[] DEFAULT NULL::text[],
    p_include_bonus boolean DEFAULT true,
    p_city_filter text DEFAULT ''::text,
    p_filial_filter text DEFAULT 'ambas'::text,
    p_redes_filter text[] DEFAULT NULL::text[]
)
 RETURNS jsonb
 LANGUAGE plpgsql
AS $function$
DECLARE
    result JSONB;
    current_month_start DATE;
    previous_month_start DATE;
    previous_month_end DATE;
BEGIN
    -- Define datas baseadas na última venda registrada
    SELECT date_trunc('month', MAX(dtped))::DATE INTO current_month_start FROM data_detailed;
    previous_month_start := current_month_start - INTERVAL '1 month';
    previous_month_end := current_month_start - INTERVAL '1 day';

    WITH
    -- 1. Identificar Clientes Ativos (Base para % de Cobertura)
    -- Aplica filtros de Supervisor, Vendedor, Cidade, Filial
    active_clients AS (
        SELECT DISTINCT c.codigo_cliente
        FROM data_clients c
        WHERE c.bloqueio <> 'S' -- Ignora bloqueados
          AND (p_city_filter = '' OR c.cidade ILIKE p_city_filter)
          -- Filtro de Supervisor (Olhando histórico recente para garantir vínculo)
          AND (p_supervisor_filter = '' OR EXISTS (
              SELECT 1 FROM data_detailed d WHERE d.codcli = c.codigo_cliente AND d.superv = p_supervisor_filter
              UNION ALL
              SELECT 1 FROM data_history h WHERE h.codcli = c.codigo_cliente AND h.superv = p_supervisor_filter
              LIMIT 1
          ))
          -- Filtro de Vendedor
          AND (p_sellers_filter IS NULL OR EXISTS (
              SELECT 1 FROM data_detailed d WHERE d.codcli = c.codigo_cliente AND d.nome = ANY(p_sellers_filter)
              UNION ALL
              SELECT 1 FROM data_history h WHERE h.codcli = c.codigo_cliente AND h.nome = ANY(p_sellers_filter)
              LIMIT 1
          ))
          -- Filtro de Filial (via tabela de vendas ou cadastro se disponível)
          AND (p_filial_filter = 'ambas' OR EXISTS (
              SELECT 1 FROM data_detailed d WHERE d.codcli = c.codigo_cliente AND d.filial = p_filial_filter
              UNION ALL
              SELECT 1 FROM data_history h WHERE h.codcli = c.codigo_cliente AND h.filial = p_filial_filter
              LIMIT 1
          ))
    ),

    -- 2. Estoque Atual
    stock_data AS (
        SELECT product_code, SUM(stock_qty) as total_stock
        FROM data_stock
        WHERE (p_filial_filter = 'ambas' OR filial = p_filial_filter)
        GROUP BY product_code
    ),

    -- 3. Vendas do Mês Atual (Agrupado por Produto/Cliente)
    current_sales AS (
        SELECT d.produto, d.codcli
        FROM data_detailed d
        WHERE d.dtped >= current_month_start
          AND (p_filial_filter = 'ambas' OR d.filial = p_filial_filter)
          AND (p_supervisor_filter = '' OR d.superv = p_supervisor_filter)
          AND (p_sellers_filter IS NULL OR d.nome = ANY(p_sellers_filter))
          -- Lógica de Venda vs Bonificação
          AND (
              (d.vlvenda > 0) OR
              (p_include_bonus AND d.vlbonific > 0)
          )
        GROUP BY d.produto, d.codcli
    ),

    -- 4. Vendas do Mês Anterior
    previous_sales AS (
        SELECT h.produto, h.codcli
        FROM data_history h
        WHERE h.dtped BETWEEN previous_month_start AND previous_month_end
          AND (p_filial_filter = 'ambas' OR h.filial = p_filial_filter)
          AND (p_supervisor_filter = '' OR h.superv = p_supervisor_filter)
          AND (p_sellers_filter IS NULL OR h.nome = ANY(p_sellers_filter))
          AND (
              (h.vlvenda > 0) OR
              (p_include_bonus AND h.vlbonific > 0)
          )
        GROUP BY h.produto, h.codcli
    ),

    -- 5. Lista de Produtos a Analisar (Apenas PEPSICO e Filtros)
    target_products AS (
        SELECT pd.code, pd.descricao
        FROM data_product_details pd
        WHERE
          -- ** HARDCODED PEPSICO FILTER **
          (pd.fornecedor ILIKE '%PEPSICO%' OR pd.fornecedor ILIKE '%ELMA CHIPS%' OR pd.fornecedor ILIKE '%MABEL%')

          -- Filtro opcional de produtos selecionados no front
          AND (p_product_codes IS NULL OR pd.code = ANY(p_product_codes))
    ),

    -- 6. Combinação Final
    final_metrics AS (
        SELECT
            tp.code,
            tp.descricao,
            COALESCE(s.total_stock, 0) as stock_qty,
            -- Conta quantos clientes ativos compraram este produto no mês anterior
            COUNT(DISTINCT ps.codcli) FILTER (WHERE ps.codcli IN (SELECT codigo_cliente FROM active_clients)) as previous_clients,
            -- Conta quantos clientes ativos compraram este produto no mês atual
            COUNT(DISTINCT cs.codcli) FILTER (WHERE cs.codcli IN (SELECT codigo_cliente FROM active_clients)) as current_clients
        FROM target_products tp
        LEFT JOIN stock_data s ON tp.code = s.product_code
        LEFT JOIN previous_sales ps ON tp.code = ps.produto
        LEFT JOIN current_sales cs ON tp.code = cs.produto
        GROUP BY tp.code, tp.descricao, s.total_stock
    )

    SELECT jsonb_build_object(
        'active_clients_count', (SELECT COUNT(*) FROM active_clients),
        'coverage_table', (
            SELECT COALESCE(jsonb_agg(
                jsonb_build_object(
                    'product_code', fm.code,
                    'descricao', fm.descricao,
                    'stock_qty', fm.stock_qty,
                    'previous_clients', fm.previous_clients,
                    'current_clients', fm.current_clients
                ) ORDER BY fm.current_clients DESC
            ), '[]'::jsonb)
            FROM final_metrics fm
            WHERE fm.stock_qty > 0 OR fm.previous_clients > 0 OR fm.current_clients > 0
        )
    ) INTO result;

    RETURN result;
END;
$function$;
