-- Fix Function Search Path Mutable Warnings

-- This file contains SQL commands to resolve the "Function Search Path Mutable" warnings
-- by setting a fixed search_path for the affected functions.

-- 1. get_initial_dashboard_data
ALTER FUNCTION public.get_initial_dashboard_data() SET search_path = public;

-- 2. get_comparison_data
ALTER FUNCTION public.get_comparison_data() SET search_path = public;

-- 3. get_filtered_client_base
ALTER FUNCTION public.get_filtered_client_base() SET search_path = public;

-- 4. get_city_view_data
ALTER FUNCTION public.get_city_view_data() SET search_path = public;

-- 5. get_comparison_view_data
ALTER FUNCTION public.get_comparison_view_data() SET search_path = public;

-- 6. get_orders_view_data
ALTER FUNCTION public.get_orders_view_data() SET search_path = public;

-- 7. get_main_charts_data
ALTER FUNCTION public.get_main_charts_data() SET search_path = public;

-- 8. get_detailed_orders_data
ALTER FUNCTION public.get_detailed_orders_data() SET search_path = public;

-- 9. get_innovations_data_v2
ALTER FUNCTION public.get_innovations_data_v2() SET search_path = public;

-- 10. get_weekly_view_data
ALTER FUNCTION public.get_weekly_view_data() SET search_path = public;

-- 11. get_innovations_view_data
ALTER FUNCTION public.get_innovations_view_data() SET search_path = public;

-- 12. get_detailed_orders
ALTER FUNCTION public.get_detailed_orders() SET search_path = public;

-- 13. get_coverage_view_data
ALTER FUNCTION public.get_coverage_view_data() SET search_path = public;

-- 14. get_filtered_client_base_json
ALTER FUNCTION public.get_filtered_client_base_json() SET search_path = public;

-- 15. get_stock_view_data
ALTER FUNCTION public.get_stock_view_data() SET search_path = public;

-- 16. handle_new_user (Ensure it is also updated here for completeness, though updated in codigo_sql_supabase.sql)
ALTER FUNCTION public.handle_new_user() SET search_path = public;
