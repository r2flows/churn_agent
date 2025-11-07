#!/usr/bin/env python3
"""
Script para verificar las fechas de última compra de los POS en el selector
"""
import pandas as pd
import sys

def check_pos_last_order_dates():
    try:
        # Cargar datos (usando la misma lógica que en churn_behavior.py)
        encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
        df_orders = None

        for encoding in encodings:
            try:
                df_orders = pd.read_csv('data/orders_delivered_pos_vendor_geozone.csv',
                                       encoding=encoding,
                                       low_memory=False)
                break
            except UnicodeDecodeError:
                continue

        if df_orders is None:
            print("❌ No se pudo leer el archivo con ninguna codificación conocida")
            return

        # Limpiar nombres de columnas
        df_orders.columns = df_orders.columns.str.strip()

        # Verificar y procesar fechas
        if 'order_date' in df_orders.columns:
            df_orders['order_date'] = pd.to_datetime(df_orders['order_date'], errors='coerce')
        
        # Identificar columna de valor total
        if 'total_compra' not in df_orders.columns and 'valor_vendedor' in df_orders.columns:
            df_orders['total_compra'] = df_orders['valor_vendedor']

        # Filtrar datos válidos
        df_clean = df_orders.dropna(subset=['order_date', 'point_of_sale_id', 'total_compra'])
        
        print(f"📊 Total de registros cargados: {len(df_orders):,}")
        print(f"✅ Registros válidos después de limpieza: {len(df_clean):,}")
        print()

        # Calcular pos_vendor_totals (igual que en churn_behavior.py)
        pos_vendor_totals = df_clean.groupby(['point_of_sale_id', 'vendor_id'])['total_compra'].sum().reset_index()
        
        # Lista de POS que aparecen en el selector (igual que línea 1269)
        pos_list = sorted(pos_vendor_totals['point_of_sale_id'].unique())
        
        print(f"🎯 Total de POS en el selector: {len(pos_list)}")
        print()

        # Calcular fecha de última compra por POS
        last_order_by_pos = df_clean.groupby('point_of_sale_id')['order_date'].max().reset_index()
        last_order_by_pos.columns = ['point_of_sale_id', 'ultima_compra']
        
        # Filtrar solo POS que están en el selector
        selector_pos_dates = last_order_by_pos[
            last_order_by_pos['point_of_sale_id'].isin(pos_list)
        ].copy()
        
        # Ordenar por fecha de última compra
        selector_pos_dates = selector_pos_dates.sort_values('ultima_compra')
        
        # Estadísticas
        fecha_mas_antigua = selector_pos_dates['ultima_compra'].min()
        fecha_mas_reciente = selector_pos_dates['ultima_compra'].max()
        
        print("📅 RESULTADOS - Fechas de Última Compra de POS en el Selector:")
        print("="*60)
        print(f"📅 Fecha más antigua: {fecha_mas_antigua.strftime('%Y-%m-%d')}")
        print(f"📅 Fecha más reciente: {fecha_mas_reciente.strftime('%Y-%m-%d')}")
        print(f"📊 Rango total: {(fecha_mas_reciente - fecha_mas_antigua).days} días")
        print()
        
        # Mostrar POS con fechas más antiguas
        print("🔍 TOP 10 POS CON ÚLTIMA COMPRA MÁS ANTIGUA:")
        print("-" * 50)
        for i, row in selector_pos_dates.head(10).iterrows():
            dias_desde_ultima = (fecha_mas_reciente - row['ultima_compra']).days
            print(f"{row['point_of_sale_id']:>8} | {row['ultima_compra'].strftime('%Y-%m-%d')} | hace {dias_desde_ultima:>3} días")
        
        print()
        print("🔍 TOP 10 POS CON ÚLTIMA COMPRA MÁS RECIENTE:")
        print("-" * 50)
        for i, row in selector_pos_dates.tail(10).iterrows():
            dias_desde_ultima = (fecha_mas_reciente - row['ultima_compra']).days
            print(f"{row['point_of_sale_id']:>8} | {row['ultima_compra'].strftime('%Y-%m-%d')} | hace {dias_desde_ultima:>3} días")
            
        print()
        print("📈 RESPUESTA A TU PREGUNTA:")
        print("="*60)
        dias_minimos = (fecha_mas_reciente - fecha_mas_antigua).days
        pos_mas_antiguo = selector_pos_dates.iloc[0]
        print(f"⏰ El POS con MÍNIMO tiempo desde su última compra hasta la fecha más reciente en datos:")
        print(f"   POS ID: {pos_mas_antiguo['point_of_sale_id']}")
        print(f"   Última compra: {pos_mas_antiguo['ultima_compra'].strftime('%Y-%m-%d')}")
        print(f"   Días desde su última compra: {(fecha_mas_reciente - pos_mas_antiguo['ultima_compra']).days} días")
        
        return selector_pos_dates

    except Exception as e:
        print(f"❌ Error al procesar datos: {str(e)}")
        return None

if __name__ == "__main__":
    check_pos_last_order_dates()