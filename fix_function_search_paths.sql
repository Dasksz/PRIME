-- Fix Function Search Path Mutable Warnings

-- This file contains SQL commands to resolve the "Function Search Path Mutable" warnings
-- by setting a fixed search_path for the affected functions.

-- Note: Some functions might have arguments. If the simple `()` syntax fails,
-- you may need to specify the arguments (e.g., `get_data(text)`).
-- However, for simple void/trigger functions or standard getters, this usually works.

ALTER FUNCTION public.get_initial_dashboard_data() SET search_path = public;
ALTER FUNCTION public.get_comparison_data() SET search_path = public;
ALTER FUNCTION public.get_filtered_client_base() SET search_path = public;
ALTER FUNCTION public.get_city_view_data() SET search_path = public;
ALTER FUNCTION public.get_comparison_view_data() SET search_path = public;
ALTER FUNCTION public.get_orders_view_data() SET search_path = public;
ALTER FUNCTION public.get_main_charts_data() SET search_path = public;
ALTER FUNCTION public.get_detailed_orders_data() SET search_path = public;
ALTER FUNCTION public.get_innovations_data_v2() SET search_path = public;
ALTER FUNCTION public.get_weekly_view_data() SET search_path = public;
ALTER FUNCTION public.get_innovations_view_data() SET search_path = public;
ALTER FUNCTION public.get_detailed_orders() SET search_path = public;
ALTER FUNCTION public.get_coverage_view_data() SET search_path = public;
ALTER FUNCTION public.get_filtered_client_base_json() SET search_path = public;
ALTER FUNCTION public.get_stock_view_data() SET search_path = public;
ALTER FUNCTION public.handle_new_user() SET search_path = public;
