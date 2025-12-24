-- Índices para data_detailed
CREATE INDEX IF NOT EXISTS idx_detailed_codcli ON public.data_detailed (codcli);
CREATE INDEX IF NOT EXISTS idx_detailed_produto ON public.data_detailed (produto);
CREATE INDEX IF NOT EXISTS idx_detailed_dtped ON public.data_detailed (dtped);
CREATE INDEX IF NOT EXISTS idx_detailed_filial ON public.data_detailed (filial);
CREATE INDEX IF NOT EXISTS idx_detailed_fornecedor ON public.data_detailed (fornecedor);

-- Índices para data_history
CREATE INDEX IF NOT EXISTS idx_history_codcli ON public.data_history (codcli);
CREATE INDEX IF NOT EXISTS idx_history_codusur ON public.data_history (codusur);
CREATE INDEX IF NOT EXISTS idx_history_produto ON public.data_history (produto);
CREATE INDEX IF NOT EXISTS idx_history_dtped ON public.data_history (dtped);
CREATE INDEX IF NOT EXISTS idx_history_filial ON public.data_history (filial);

-- Índices para data_clients
CREATE INDEX IF NOT EXISTS idx_clients_codigo ON public.data_clients (codigo_cliente);
CREATE INDEX IF NOT EXISTS idx_clients_rca1 ON public.data_clients (rca1);
CREATE INDEX IF NOT EXISTS idx_clients_cidade ON public.data_clients (cidade);

-- Índices para data_orders
CREATE INDEX IF NOT EXISTS idx_orders_codcli ON public.data_orders (codcli);
CREATE INDEX IF NOT EXISTS idx_orders_dtped ON public.data_orders (dtped);

-- Índices para data_stock
CREATE INDEX IF NOT EXISTS idx_stock_product ON public.data_stock (product_code);
CREATE INDEX IF NOT EXISTS idx_stock_filial ON public.data_stock (filial);

-- Índices para data_product_details
CREATE INDEX IF NOT EXISTS idx_product_details_fornecedor ON public.data_product_details (fornecedor);
