import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime
import matplotlib

# Configurar pandas para manejar tablas grandes y optimizar rendimiento
pd.set_option("styler.render.max_elements", 1000000)
pd.set_option('mode.chained_assignment', None)  # Suprimir warnings de asignación

# Configuración para optimizar rendimiento
import warnings
warnings.filterwarnings('ignore')

# Cache para DataFrames procesados
from functools import lru_cache

# Cache global para datos procesados
@st.cache_data(ttl=3600)  # Cache por 1 hora
def load_cached_csv(filepath, dtype_dict=None):
    """Carga CSV con cache de Streamlit"""
    if dtype_dict:
        return pd.read_csv(filepath, dtype=dtype_dict, low_memory=False)
    return pd.read_csv(filepath, low_memory=False)

# Configuración de la página
st.set_page_config(page_title="Análisis de Compras y Productos POS", layout="wide")
st.title("Análisis de Compras Reales vs Potenciales por Punto de Venta")

# Funciones de utilidad
def get_status_description(status):
    """
    Convierte un código de status numérico en su descripción correspondiente
    """
    if pd.isna(status): 
        return "Sin Status"
    
    status_map = {
        0: "Rechazado", 
        1: "Activo", 
        2: "Pendiente",
        -1: "Sin conectar"
    }
    
    return status_map.get(status, f"Status {status}")

def safe_get_status_description(status):
    """
    Función segura para obtener descripción del status,
    manejando valores None, NaN o no válidos
    """
    if pd.isna(status) or status is None:
        return "Sin conectar"
    
    try:
        status = int(status)
        return get_status_description(status)
    except (ValueError, TypeError):
        return "Sin definir"

def obtener_status_vendor(vendor_id, pos_id, df_vendors_pos):
    """
    Obtiene el status de un vendor para un punto de venta específico
    """
    if df_vendors_pos.empty:
        return np.nan
        
    vendor_id = pd.to_numeric(vendor_id, errors='coerce')
    pos_id = pd.to_numeric(pos_id, errors='coerce')
    
    if 'vendor_id' in df_vendors_pos.columns and 'point_of_sale_id' in df_vendors_pos.columns and 'status' in df_vendors_pos.columns:
        df_vendors_pos_copy = df_vendors_pos.copy()
        df_vendors_pos_copy['vendor_id'] = pd.to_numeric(df_vendors_pos_copy['vendor_id'], errors='coerce')
        df_vendors_pos_copy['point_of_sale_id'] = pd.to_numeric(df_vendors_pos_copy['point_of_sale_id'], errors='coerce')
        
        relacion = df_vendors_pos_copy[
            (df_vendors_pos_copy['vendor_id'] == vendor_id) & 
            (df_vendors_pos_copy['point_of_sale_id'] == pos_id)
        ]
        
        if not relacion.empty:
            return relacion['status'].iloc[0]
    
    return np.nan

def construir_analisis_productos(df_pos_clasificado, productos_con_vendors, vendor_col, drogueria_col, pos_id, df_vendors_pos):
    """
    Genera el análisis base producto por producto reutilizado en ambas pestañas utilizando operaciones vectorizadas.
    """
    if productos_con_vendors.empty:
        return pd.DataFrame()

    required_base_cols = {'super_catalog_id', 'order_id', 'valor_vendedor', 'precio_minimo', 'unidades_pedidas'}
    required_vendor_cols = {'super_catalog_id', 'order_id', vendor_col, 'precio_total_vendedor', 'precio_vendedor'}

    if not required_base_cols.issubset(df_pos_clasificado.columns) or not required_vendor_cols.issubset(productos_con_vendors.columns):
        print("⚠️ columnas insuficientes para construir análisis de productos")
        return pd.DataFrame()

    # Filtrar y limpiar datos de vendors una sola vez
    vendors_df = productos_con_vendors.copy()
    vendors_df[vendor_col] = pd.to_numeric(vendors_df[vendor_col], errors='coerce')
    vendors_df['precio_total_vendedor'] = pd.to_numeric(vendors_df['precio_total_vendedor'], errors='coerce')
    vendors_df['precio_vendedor'] = pd.to_numeric(vendors_df['precio_vendedor'], errors='coerce')

    vendors_df = vendors_df[
        vendors_df[vendor_col].notna() &
        vendors_df['precio_total_vendedor'].notna() &
        (vendors_df['precio_total_vendedor'] > 0) &
        (vendors_df[vendor_col] != 1156)
    ].copy()

    if vendors_df.empty:
        return pd.DataFrame()

    if 'point_of_sale_id' not in vendors_df.columns and 'point_of_sale_id' in df_pos_clasificado.columns:
        vendors_df['point_of_sale_id'] = df_pos_clasificado['point_of_sale_id'].iloc[0]

    # Calcular número de opciones disponibles por producto
    vendor_counts = (
        vendors_df.groupby(['super_catalog_id', 'order_id'], dropna=False)
        .size()
        .rename('Opciones Vendors')
        .reset_index()
    )

    # Seleccionar el vendor con mejor precio por producto/orden
    idx_min = vendors_df.groupby(['super_catalog_id', 'order_id'])['precio_total_vendedor'].idxmin()
    best_vendors = vendors_df.loc[idx_min].copy()

    # Preparar información de droguería
    drogueria_df = df_pos_clasificado[df_pos_clasificado['clasificacion'] == 'Precio droguería minimo'].copy()
    if drogueria_df.empty:
        return pd.DataFrame()

    drogueria_keep_cols = ['super_catalog_id', 'order_id', 'valor_vendedor', 'precio_minimo', 'unidades_pedidas']
    optional_cols = ['order_date', 'descripción', drogueria_col, 'point_of_sale_id']
    drogueria_keep_cols += [col for col in optional_cols if col in drogueria_df.columns]

    drogueria_df = (
        drogueria_df[drogueria_keep_cols]
        .drop_duplicates(subset=['super_catalog_id', 'order_id'], keep='first')
    )

    # Unir información de vendors con droguería
    merged = (
        best_vendors.merge(
            drogueria_df,
            on=['super_catalog_id', 'order_id'],
            how='inner',
            suffixes=('', '_drog')
        )
        .merge(
            vendor_counts,
            on=['super_catalog_id', 'order_id'],
            how='left'
        )
    )

    if merged.empty:
        return pd.DataFrame()

    merged['Ahorro con Mejor Vendor'] = merged['valor_vendedor'] - merged['precio_total_vendedor']
    merged = merged[merged['Ahorro con Mejor Vendor'] > 0]
    if merged.empty:
        return pd.DataFrame()

    merged['Porcentaje Ahorro'] = np.where(
        merged['valor_vendedor'] > 0,
        merged['Ahorro con Mejor Vendor'] / merged['valor_vendedor'] * 100,
        0
    )

    merged['Tipo Ahorro'] = pd.cut(
        merged['Porcentaje Ahorro'],
        bins=[-np.inf, 10, 20, np.inf],
        labels=['Bajo', 'Medio', 'Alto']
    )

    # Obtener status del vendor mediante merge para evitar iteraciones
    status_col = 'Status Mejor Vendor'
    if not df_vendors_pos.empty and {'vendor_id', 'point_of_sale_id', 'status'}.issubset(df_vendors_pos.columns):
        status_lookup = (
            df_vendors_pos[['vendor_id', 'point_of_sale_id', 'status']]
            .drop_duplicates()
            .copy()
        )
        status_lookup['vendor_id'] = pd.to_numeric(status_lookup['vendor_id'], errors='coerce')

        if 'point_of_sale_id' not in merged.columns and 'point_of_sale_id' in df_pos_clasificado.columns:
            merged['point_of_sale_id'] = df_pos_clasificado['point_of_sale_id'].iloc[0]

        merged = merged.merge(
            status_lookup,
            how='left',
            left_on=['point_of_sale_id', vendor_col],
            right_on=['point_of_sale_id', 'vendor_id'],
            suffixes=('', '_status')
        )
        merged[status_col] = merged['status'].apply(get_status_description)
        merged.drop(columns=['vendor_id', 'status'], inplace=True)
    else:
        merged[status_col] = 'Sin Status'

    descripcion_cols = [col for col in ['descripción', 'descripción_drog'] if col in merged.columns]
    if descripcion_cols:
        merged['Descripción'] = merged[descripcion_cols[0]].fillna(
            merged[descripcion_cols[-1]] if len(descripcion_cols) > 1 else ""
        )
    else:
        merged['Descripción'] = ""

    if 'point_of_sale_id' not in merged.columns and 'point_of_sale_id' in df_pos_clasificado.columns:
        merged['point_of_sale_id'] = df_pos_clasificado['point_of_sale_id'].iloc[0]

    fecha_orden = merged['order_date'] if 'order_date' in merged.columns else pd.Series(pd.NaT, index=merged.index)
    drogueria_id = merged[drogueria_col] if drogueria_col in merged.columns else pd.Series(np.nan, index=merged.index)
    tipo_ahorro = merged['Tipo Ahorro'].astype(str).replace('nan', 'Bajo')

    final_df = pd.DataFrame({
        'Producto ID': merged['super_catalog_id'],
        'Descripción': merged['Descripción'],
        'Orden ID': merged['order_id'],
        'Fecha Orden': fecha_orden,
        'Unidades': merged['unidades_pedidas'],
        'Droguería ID': drogueria_id,
        'Precio Unit. Droguería': merged['precio_minimo'],
        'Precio Total Droguería': merged['valor_vendedor'],
        'Opciones Vendors': merged['Opciones Vendors'].fillna(0).astype(int),
        'Mejor Vendor ID': merged[vendor_col].astype('Int64').astype(str),
        'Status Mejor Vendor': merged[status_col],
        'Precio Unit. Mejor Vendor': merged['precio_vendedor'],
        'Precio Total Mejor Vendor': merged['precio_total_vendedor'],
        'Ahorro con Mejor Vendor': merged['Ahorro con Mejor Vendor'],
        'Porcentaje Ahorro': merged['Porcentaje Ahorro'],
        'Tipo Ahorro': tipo_ahorro,
        'point_of_sale_id': merged['point_of_sale_id']
    })

    final_df = final_df.sort_values('Ahorro con Mejor Vendor', ascending=False)
    return final_df

def obtener_geo_zone(address):
    """
    Extrae la zona geográfica de una dirección
    """
    partes = address.split(', ')
    return ', '.join(partes[-2:-1])

def filtrar_registros_validos(df_clasificado, df_precios_comparativa):
    """
    Filtra df_clasificado para excluir el registro específico problemático
    order_id=346579, barcode=7501027800060, vendor_id=1164, precio=467
    """
    if df_clasificado.empty:
        return df_clasificado
    
    registros_antes = len(df_clasificado)
    
    # Filtrar el registro específico problemático con múltiples condiciones
    # Buscar por diferentes posibles nombres de columnas para vendor_id y barcode
    mask = True  # Inicializar como True para ir aplicando condiciones AND
    
    # Condición order_id
    if 'order_id' in df_clasificado.columns:
        mask = mask & (df_clasificado['order_id'] == 346579)
    
    # Condición barcode (puede estar en diferentes columnas)
    barcode_found = False
    for barcode_col in ['barcode', 'super_catalog_id']:
        if barcode_col in df_clasificado.columns:
            mask = mask & (df_clasificado[barcode_col].astype(str) == '7501027800060')
            barcode_found = True
            break
    
    # Condición vendor_id (puede estar en diferentes columnas)
    vendor_found = False
    for vendor_col in ['vendor_id', 'vendor_id_y', 'mejor_vendor_id']:
        if vendor_col in df_clasificado.columns:
            mask = mask & (df_clasificado[vendor_col] == 1164)
            vendor_found = True
            break
    
    # Solo aplicar el filtro si encontramos las columnas necesarias y hay registros que coincidan
    if barcode_found and vendor_found and mask.any():
        df_filtrado = df_clasificado[~mask].copy()
        registros_despues = len(df_filtrado)
    else:
        df_filtrado = df_clasificado.copy()
        registros_despues = len(df_filtrado)
    
    return df_filtrado

def load_vendors_dm():
    """
    Carga y procesa el archivo vendors_dm.csv
    """
    try:
        df_vendor_dm = pd.read_csv('data/vendors_dm.csv')
        if 'client_id' in df_vendor_dm.columns and 'vendor_id' not in df_vendor_dm.columns:
            df_vendor_dm.rename(columns={'client_id': 'vendor_id'}, inplace=True)
        return df_vendor_dm
    except Exception as e:
        print(f"Error al procesar vendors_dm.csv: {e}")
        return pd.DataFrame(columns=['vendor_id', 'name', 'drug_manufacturer_id'])

def procesar_datos_api_json():
    """
    Procesa el archivo datos_api.json y extrae los datos de precios de vendors 1275 y 1350
    """
    try:
        import json
        import os
        
        # Verificar si el archivo existe
        file_path = 'data/datos_api.json'
        if not os.path.exists(file_path):
            print(f"WARNING: Archivo {file_path} no encontrado en el entorno de despliegue")
            return pd.DataFrame()
        
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        records = []
        
        # Procesar cada vendor en vendors_data
        for vendor_data in data.get('vendors_data', []):
            vendor_id = vendor_data.get('vendor_id')
            vendor_name = vendor_data.get('vendor_name')
            client_id = vendor_data.get('client_id')
            
            # Solo procesar vendors 1275 y 1350
            if vendor_id not in ['1275', '1350']:
                continue
            
            # Procesar respuestas por lote
            for response_batch in vendor_data.get('responses', []):
                batch_number = response_batch.get('batch')
                response = response_batch.get('response', {})
                
                # Procesar items dentro de cada respuesta
                for item in response.get('items', []):
                    # Solo procesar items con status 'ok' y precio válido
                    if item.get('status') == 'ok' and item.get('habitual', {}).get('precio') is not None:
                        records.append({
                            'vendor_id': int(vendor_id),
                            'vendor_name': vendor_name,
                            'client_id': client_id,
                            'batch': batch_number,
                            'super_catalog_id': item.get('codbar'),
                            'barcode': item.get('codbar'),
                            'precio_vendedor': float(item.get('habitual', {}).get('precio', 0)),
                            'precio_publico': item.get('habitual', {}).get('publico'),
                            'nombre': item.get('nombre'),
                            'laboratorio': item.get('laboratorio'),
                            'monodroga': item.get('monodroga'),
                            'iva': item.get('iva'),
                            'stock': item.get('stock'),
                            'stock_quantity': item.get('stock_quantity'),
                            'mincant': item.get('mincant'),
                            'maxcant': item.get('maxcant'),
                            'troquel': item.get('troquel')
                        })
        
        df_api = pd.DataFrame(records)
        
        if not df_api.empty:
            # Convertir tipos de datos
            df_api['super_catalog_id'] = pd.to_numeric(df_api['super_catalog_id'], errors='coerce')
            df_api['barcode'] = pd.to_numeric(df_api['barcode'], errors='coerce')
            df_api['precio_vendedor'] = pd.to_numeric(df_api['precio_vendedor'], errors='coerce')
            df_api['vendor_id'] = pd.to_numeric(df_api['vendor_id'], errors='coerce')
            df_api['iva'] = pd.to_numeric(df_api['iva'], errors='coerce')
            
            # Filtrar registros con precios válidos
            df_api = df_api[df_api['precio_vendedor'] > 0].copy()
            
            print(f"Datos API procesados: {len(df_api)} registros de vendors {df_api['vendor_id'].unique()}")
        
        return df_api
        
    except Exception as e:
        print(f"Error al procesar datos_api.json: {e}")
        return pd.DataFrame()

def agregar_columna_clasificacion(df):
    """Agrega una columna de clasificación identificando droguería y vendors."""
    if df.empty:
        return df

    result_df = df.copy()
    if 'clasificacion' not in result_df.columns:
        result_df['clasificacion'] = ""

    required_cols = ['order_id', 'super_catalog_id', 'precio_minimo']
    for col in required_cols:
        if col not in result_df.columns:
            st.warning(f"Columnas faltantes para clasificación: {required_cols}")
            return result_df

    grupos = result_df.groupby(['order_id', 'super_catalog_id'], dropna=False)

    for (_, _), group in grupos:
        group_indices = group.index

        # Detectar registros de droguería
        drogueria_mask = pd.Series(False, index=group_indices)

        if 'vendor_id_y' in group.columns:
            actual_vendor = pd.to_numeric(group['vendor_id_x'], errors='coerce').iloc[0] if 'vendor_id_x' in group.columns else np.nan
            vendor_catalog_ids = pd.to_numeric(group['vendor_id_y'], errors='coerce')
            drogueria_mask = vendor_catalog_ids.isna()
            if not drogueria_mask.any() and not pd.isna(actual_vendor):
                drogueria_mask = vendor_catalog_ids == actual_vendor
        elif 'origen' in group.columns:
            drogueria_mask = group['origen'].astype(str).str.lower() == 'drogueria'

        if not drogueria_mask.any():
            drogueria_mask = group['precio_vendedor'].isna()

        if not drogueria_mask.any():
            drogueria_mask.iloc[0] = True

        drogueria_indices = drogueria_mask.index[drogueria_mask]
        result_df.loc[drogueria_indices, 'clasificacion'] = "Precio droguería minimo"

        vendor_indices = group_indices.difference(drogueria_indices)
        if len(vendor_indices) == 0:
            continue

        precios_vendor = pd.to_numeric(group.loc[vendor_indices, 'precio_vendedor'], errors='coerce')
        if precios_vendor.isna().all():
            # No hay precios válidos de vendors
            continue

        min_precio_vendor = precios_vendor.min(skipna=True)
        min_vendor_idx = precios_vendor[precios_vendor == min_precio_vendor].index
        result_df.loc[min_vendor_idx, 'clasificacion'] = "Precio vendor minimo"

        otros_idx = vendor_indices.difference(min_vendor_idx)
        if len(otros_idx) > 0:
            result_df.loc[otros_idx, 'clasificacion'] = "Precio vendor no minimo"

    return result_df

def crear_dashboard_ejecutivo_ahorro(df_clasificado, selected_pos):
    """
    Crea un dashboard ejecutivo con KPIs principales de ahorro
    """
    if df_clasificado.empty:
        st.warning("No hay datos para crear el dashboard ejecutivo")
        return
        
    df_pos = df_clasificado[df_clasificado['point_of_sale_id'] == selected_pos].copy()
    
    if df_pos.empty:
        st.warning("No hay datos para el POS seleccionado")
        return
    
    st.subheader("🎯 Dashboard Ejecutivo de Oportunidades de Ahorro")
    
    # Calcular KPIs principales
    total_comprado = df_pos['valor_vendedor'].sum() if 'valor_vendedor' in df_pos.columns else 0
    total_optimo = df_pos.groupby(['order_id', 'super_catalog_id'])['precio_total_vendedor'].min().sum() if 'precio_total_vendedor' in df_pos.columns else 0
    ahorro_maximo = total_comprado - total_optimo
    ahorro_pct = (ahorro_maximo / total_comprado * 100) if total_comprado > 0 else 0
    
    # Vendors con oportunidad
    vendors_con_ahorro = 0
    if 'vendor_id' in df_pos.columns and 'clasificacion' in df_pos.columns:
        vendors_con_ahorro = df_pos[
            df_pos['clasificacion'].isin(['Precio vendor minimo', 'Precio droguería minimo'])
        ]['vendor_id'].nunique()
    
    # Productos optimizables
    productos_optimizables = 0
    if 'valor_vendedor' in df_pos.columns and 'precio_total_vendedor' in df_pos.columns:
        productos_optimizables = df_pos[
            df_pos['valor_vendedor'] > df_pos['precio_total_vendedor']
        ]['super_catalog_id'].nunique()
    
    # Primera fila de métricas
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "💰 Ahorro Potencial Máximo",
            f"${ahorro_maximo:,.0f}",
            f"{ahorro_pct:.1f}% del total"
        )
    
    with col2:
        st.metric(
            "📊 Gasto Actual Total",
            f"${total_comprado:,.0f}",
            "Baseline actual"
        )
    
    with col3:
        st.metric(
            "🎯 Gasto Óptimo Posible",
            f"${total_optimo:,.0f}",
            f"-${ahorro_maximo:,.0f}"
        )
    
    with col4:
        roi = (ahorro_maximo / total_comprado * 100) if total_comprado > 0 else 0
        st.metric(
            "📈 ROI Potencial",
            f"{roi:.1f}%",
            "Retorno por optimización"
        )
    
    # Segunda fila de métricas
    st.write("")
    col5, col6, col7, col8 = st.columns(4)
    
    with col5:
        st.metric(
            "🏭 Vendors con Oportunidad",
            f"{vendors_con_ahorro}",
            "Con mejores precios"
        )
    
    with col6:
        st.metric(
            "📦 Productos Optimizables",
            f"{productos_optimizables}",
            "Con alternativas más baratas"
        )
    
    with col7:
        ordenes_afectadas = df_pos['order_id'].nunique() if 'order_id' in df_pos.columns else 0
        st.metric(
            "🛒 Órdenes Analizadas",
            f"{ordenes_afectadas}",
            "En el período"
        )
    
    with col8:
        vendors_activos = 0
        vendors_no_activos = 0
        if 'status' in df_pos.columns and 'vendor_id' in df_pos.columns:
            vendors_activos = df_pos[df_pos['status'] == 1]['vendor_id'].nunique()
            vendors_no_activos = df_pos[df_pos['status'].isin([0, 2])]['vendor_id'].nunique()
        
        st.metric(
            "✅ Vendors Activos",
            f"{vendors_activos}",
            f"{vendors_no_activos} por activar"
        )

def generar_recomendaciones_cambio_vendor(df_clasificado, selected_pos, umbral_ahorro=0.1):
    """
    Genera recomendaciones de cambio de vendor basadas en ahorro potencial
    """
    df_pos = df_clasificado[df_clasificado['point_of_sale_id'] == selected_pos].copy()
    
    if df_pos.empty:
        return pd.DataFrame()
    
    recomendaciones = []
    
    required_cols = ['super_catalog_id', 'order_id', 'valor_vendedor', 'vendor_id_x', 
                    'unidades_pedidas', 'precio_total_vendedor', 'vendor_id', 'status', 
                    'precio_minimo', 'precio_vendedor']
    
    missing_cols = [col for col in required_cols if col not in df_pos.columns]
    if missing_cols:
        st.warning(f"Columnas faltantes para recomendaciones: {missing_cols}")
        return pd.DataFrame()
    
    # Agrupar por producto y orden
    for (producto, orden), grupo in df_pos.groupby(['super_catalog_id', 'order_id']):
        if grupo.empty:
            continue
            
        # Encontrar precio actual
        precio_actual = grupo['valor_vendedor'].iloc[0]
        vendor_actual = grupo['vendor_id_x'].iloc[0]
        unidades = grupo['unidades_pedidas'].iloc[0]
        
        # Encontrar mejor alternativa
        mejor_idx = grupo['precio_total_vendedor'].idxmin()
        mejor_alternativa = grupo.loc[mejor_idx]
        
        ahorro = precio_actual - mejor_alternativa['precio_total_vendedor']
        ahorro_pct = (ahorro / precio_actual) if precio_actual > 0 else 0
        
        if ahorro_pct >= umbral_ahorro:
            recomendaciones.append({
                'producto_id': producto,
                'orden_id': orden,
                'unidades': unidades,
                'drogueria_actual': vendor_actual,
                'vendor_recomendado': mejor_alternativa['vendor_id'],
                'status_vendor': get_status_description(mejor_alternativa['status']),
                'precio_actual_unitario': mejor_alternativa['precio_minimo'],
                'precio_recomendado_unitario': mejor_alternativa['precio_vendedor'],
                'ahorro_total': ahorro,
                'ahorro_porcentaje': ahorro_pct * 100,
                'prioridad': 'Alta' if ahorro > 1000 else ('Media' if ahorro > 500 else 'Baja')
            })
    
    df_recomendaciones = pd.DataFrame(recomendaciones)
    
    if not df_recomendaciones.empty:
        df_recomendaciones = df_recomendaciones.sort_values('ahorro_total', ascending=False)
    
    return df_recomendaciones

def calcular_impacto_activacion_vendors(df_clasificado, df_vendors_pos, selected_pos):
    """
    Calcula el impacto potencial de activar vendors pendientes o rechazados
    """
    df_pos = df_clasificado[df_clasificado['point_of_sale_id'] == selected_pos].copy()
    
    if df_pos.empty:
        return pd.DataFrame()
    
    # Identificar la columna correcta de vendor
    vendor_col = None
    if 'vendor_id_y' in df_pos.columns:
        vendor_col = 'vendor_id_y'
    elif 'vendor_id' in df_pos.columns:
        vendor_col = 'vendor_id'
    
    if vendor_col is None or 'status' not in df_pos.columns:
        return pd.DataFrame()
    
    # Identificar vendors no activos con potencial
    vendors_no_activos = df_pos[df_pos['status'].isin([0, 2])][vendor_col].unique()
    
    impacto = []
    
    for vendor in vendors_no_activos:
        df_vendor = df_pos[df_pos[vendor_col] == vendor]
        
        if df_vendor.empty:
            continue
        
        # Calcular productos donde este vendor tiene mejor precio
        productos_ganadores = 0
        if 'clasificacion' in df_vendor.columns:
            productos_ganadores = df_vendor[
                df_vendor['clasificacion'] == 'Precio vendor minimo'
            ]['super_catalog_id'].nunique()
        
        # Calcular ahorro potencial
        ahorro_potencial = 0
        for _, row in df_vendor.iterrows():
            # Comparar con precio actual de la droguería
            productos_mismo = df_pos[
                (df_pos['super_catalog_id'] == row['super_catalog_id']) &
                (df_pos['order_id'] == row['order_id'])
            ]
            if not productos_mismo.empty and 'valor_vendedor' in productos_mismo.columns and 'precio_total_vendedor' in row:
                precio_actual = productos_mismo['valor_vendedor'].iloc[0]
                ahorro = precio_actual - row['precio_total_vendedor']
                if ahorro > 0:
                    ahorro_potencial += ahorro
        
        status = df_vendor['status'].iloc[0] if not df_vendor.empty else None
        
        impacto.append({
            'vendor_id': vendor,
            'status_actual': get_status_description(status),
            'productos_con_mejor_precio': productos_ganadores,
            'ahorro_potencial_total': ahorro_potencial,
            'productos_totales': df_vendor['super_catalog_id'].nunique(),
            'ordenes_afectadas': df_vendor['order_id'].nunique()
        })
    
    df_impacto = pd.DataFrame(impacto)
    
    if not df_impacto.empty:
        df_impacto = df_impacto.sort_values('ahorro_potencial_total', ascending=False)
    
    return df_impacto

def integrar_precios_vendors_activos(df_base, df_precios_csv, df_vendors_pos):
    """
    Integra precios reales de vendors activos desde el CSV de precios comparativos
    """
    if df_base.empty or df_precios_csv.empty:
        return df_base

    if 'vendor_id' not in df_base.columns:
        print(f"ADVERTENCIA: No se encontró columna vendor_id en {list(df_base.columns)}")
        return df_base

    if df_vendors_pos.empty or not {'vendor_id', 'status'}.issubset(df_vendors_pos.columns):
        return df_base

    vendors_activos = df_vendors_pos[df_vendors_pos['status'] == 1]['vendor_id'].dropna().unique()
    if len(vendors_activos) == 0:
        return df_base

    columnas_csv = {'order_id', 'barcode', 'drug_manufacturer_id', 'price_with_discount'}
    if not columnas_csv.issubset(df_precios_csv.columns):
        print("ADVERTENCIA: columnas insuficientes en df_precios_csv")
        return df_base

    df_resultado = df_base.copy()
    df_resultado['vendor_id'] = pd.to_numeric(df_resultado['vendor_id'], errors='coerce')
    df_resultado['super_catalog_id'] = pd.to_numeric(df_resultado['super_catalog_id'], errors='coerce')

    df_precios_preparado = (
        df_precios_csv[list(columnas_csv)]
        .copy()
        .rename(columns={
            'barcode': 'super_catalog_id',
            'drug_manufacturer_id': 'vendor_id',
            'price_with_discount': 'precio_vendedor_csv'
        })
    )
    df_precios_preparado['vendor_id'] = pd.to_numeric(df_precios_preparado['vendor_id'], errors='coerce')
    df_precios_preparado['super_catalog_id'] = pd.to_numeric(df_precios_preparado['super_catalog_id'], errors='coerce')
    df_precios_preparado = df_precios_preparado[
        df_precios_preparado['vendor_id'].isin(vendors_activos)
    ].dropna(subset=['vendor_id', 'super_catalog_id', 'order_id'])

    if df_precios_preparado.empty:
        return df_base

    df_precios_preparado = (
        df_precios_preparado.sort_values('precio_vendedor_csv')
        .drop_duplicates(subset=['order_id', 'super_catalog_id', 'vendor_id'], keep='first')
    )

    df_resultado = df_resultado.merge(
        df_precios_preparado,
        on=['order_id', 'super_catalog_id', 'vendor_id'],
        how='left'
    )

    mask_csv = df_resultado['precio_vendedor_csv'].notna()
    if mask_csv.any():
        df_resultado.loc[mask_csv, 'precio_vendedor'] = df_resultado.loc[mask_csv, 'precio_vendedor_csv']
        if 'unidades_pedidas' in df_resultado.columns:
            df_resultado.loc[mask_csv, 'precio_total_vendedor'] = (
                df_resultado.loc[mask_csv, 'precio_vendedor_csv'] * df_resultado.loc[mask_csv, 'unidades_pedidas']
            )

    df_resultado = df_resultado.drop(columns=['precio_vendedor_csv'])
    return df_resultado

def integrar_datos_api_vendors(df_base, df_api_vendors, df_vendors_pos):
    """
    Integra los datos de precios de vendors 1275 y 1350 desde la API
    """
    if df_base.empty or df_api_vendors.empty:
        return df_base

    if 'vendor_id' not in df_base.columns:
        print(f"ADVERTENCIA: No se encontró columna vendor_id en {list(df_base.columns)}")
        return df_base

    df_resultado = df_base.copy()
    df_resultado['vendor_id'] = pd.to_numeric(df_resultado['vendor_id'], errors='coerce')
    df_resultado['super_catalog_id'] = pd.to_numeric(df_resultado['super_catalog_id'], errors='coerce')

    vendors_api_activos = set()
    if not df_vendors_pos.empty and {'vendor_id', 'status'}.issubset(df_vendors_pos.columns):
        vendors_api_activos = set(
            df_vendors_pos[
                (df_vendors_pos['status'] == 1) &
                (df_vendors_pos['vendor_id'].isin([1275, 1350]))
            ]['vendor_id'].dropna().unique()
        )

    if not vendors_api_activos:
        return df_base

    columnas_api = {'super_catalog_id', 'vendor_id', 'precio_vendedor', 'nombre', 'laboratorio', 'stock'}
    columnas_disponibles = columnas_api.intersection(df_api_vendors.columns)
    api_preparado = df_api_vendors[list(columnas_disponibles)].copy()
    api_preparado['vendor_id'] = pd.to_numeric(api_preparado['vendor_id'], errors='coerce')
    api_preparado['super_catalog_id'] = pd.to_numeric(api_preparado['super_catalog_id'], errors='coerce')
    api_preparado = api_preparado[api_preparado['vendor_id'].isin(vendors_api_activos)]

    if api_preparado.empty:
        return df_base

    rename_map = {}
    if 'nombre' in api_preparado.columns:
        rename_map['nombre'] = 'nombre_api'
    if 'laboratorio' in api_preparado.columns:
        rename_map['laboratorio'] = 'laboratorio_api'
    if 'stock' in api_preparado.columns:
        rename_map['stock'] = 'stock_api'
    api_preparado = api_preparado.rename(columns=rename_map)

    api_preparado = (
        api_preparado.sort_values('precio_vendedor')
        .drop_duplicates(subset=['super_catalog_id', 'vendor_id'], keep='first')
    )

    df_resultado = df_resultado.merge(
        api_preparado,
        on=['super_catalog_id', 'vendor_id'],
        how='left',
        suffixes=('', '_api')
    )

    mask_api = df_resultado['precio_vendedor_api'].notna()
    if mask_api.any():
        df_resultado.loc[mask_api, 'precio_vendedor'] = df_resultado.loc[mask_api, 'precio_vendedor_api']
        if 'unidades_pedidas' in df_resultado.columns:
            df_resultado.loc[mask_api, 'precio_total_vendedor'] = (
                df_resultado.loc[mask_api, 'precio_vendedor_api'] * df_resultado.loc[mask_api, 'unidades_pedidas']
            )

    if 'precio_vendedor_api' in df_resultado.columns:
        df_resultado.drop(columns=['precio_vendedor_api'], inplace=True)
    return df_resultado

@st.cache_data
def load_and_process_data():
    """Función principal que procesa todos los datos necesarios"""
    try:
        import os
        
        # Verificar archivos críticos
        required_files = [
            'data/pos_address.csv',
            'data/orders_delivered_pos_vendor_geozone.csv', 
            'data/vendors_catalog.csv',
            'data/vendor_pos_relations.csv'
        ]
        
        missing_files = [f for f in required_files if not os.path.exists(f)]
        if missing_files:
            print(f"ERROR: Archivos críticos faltantes: {missing_files}")
            raise FileNotFoundError(f"Archivos críticos faltantes: {missing_files}")
        
        # Cargar archivos básicos con cache y optimizaciones
        print("Loading pos_address.csv...")
        df_pos_address = load_cached_csv('data/pos_address.csv')
        print(f"pos_address.csv cargado: {len(df_pos_address)} registros")
        
        print("Loading orders_delivered_pos_vendor_geozone.csv...")
        # Optimización: especificar tipos de datos para lectura más rápida
        dtype_orders = {
            'point_of_sale_id': 'int32',
            'order_id': 'int32', 
            'super_catalog_id': 'int64',
            'unidades_pedidas': 'int32',
            'precio_minimo': 'float32'
        }
        df_pedidos = load_cached_csv('data/orders_delivered_pos_vendor_geozone.csv', dtype_orders)
        print(f"orders_delivered_pos_vendor_geozone.csv cargado: {len(df_pedidos)} registros")
        
        print("Loading vendors_catalog.csv...")
        dtype_vendors = {
            'super_catalog_id': 'int64',
            'vendor_id': 'int32',
            'base_price': 'float32',
            'percentage': 'float32'
        }
        df_proveedores = load_cached_csv('data/vendors_catalog.csv', dtype_vendors)
        print(f"vendors_catalog.csv cargado: {len(df_proveedores)} registros")
        
        print("Loading vendor_pos_relations.csv...")
        dtype_relations = {
            'point_of_sale_id': 'int32',
            'vendor_id': 'int32',
            'status': 'int8'
        }
        df_vendors_pos = load_cached_csv('data/vendor_pos_relations.csv', dtype_relations)
        print(f"vendor_pos_relations.csv cargado: {len(df_vendors_pos)} registros")
        
        df_vendor_dm = load_vendors_dm()
        
        # Cargar datos de API JSON para vendors 1275 y 1350
        df_api_vendors = procesar_datos_api_json()
        
        # Cargar CSV de precios comparativos para validación
        try:
            df_precios_comparativa = load_cached_csv('precios_comparativa_vendors.csv')
        except FileNotFoundError:
            df_precios_comparativa = pd.DataFrame()
        
        try:
            df_min_purchase = load_cached_csv('minimum_purchase.csv')
        except FileNotFoundError:
            df_min_purchase = pd.DataFrame(columns=['vendor_id', 'name', 'min_purchase'])
        
        # Procesar dirección y geo_zone - optimizado
        # Usar vectorización en lugar de apply para mejor rendimiento
        df_pos_address['geo_zone'] = df_pos_address['address'].str.split(', ').str[-2]
        
        # Limpiar columnas duplicadas
        if 'geo_zone' in df_pedidos.columns:
            df_pedidos = df_pedidos.drop(columns=['geo_zone'])
            
        # Normalizar datos
        df_proveedores['percentage'] = df_proveedores['percentage'].fillna(0)
        pos_geo_zones = df_pos_address[['point_of_sale_id', 'geo_zone']].copy()
        
        # Reemplazar abreviaturas
        abreviaturas = {
            'B.C.S.': 'Baja California Sur', 'Qro.': 'Querétaro', 'Jal.': 'Jalisco',
            'Pue.': 'Puebla', 'Méx.': 'CDMX', 'Oax.': 'Oaxaca', 'Chih.': 'Chihuahua',
            'Coah.': 'Coahuila de Zaragoza', 'Mich.': 'Michoacán de Ocampo',
            'Ver.': 'Veracruz de Ignacio de la Llave', 'Chis.': 'Chiapas',
            'N.L.': 'Nuevo León', 'Hgo.': 'Hidalgo', 'Tlax.': 'Tlaxcala',
            'Tamps.': 'Tamaulipas', 'Yuc.': 'Yucatan', 'Mor.': 'Morelos',
            'Sin.': 'Sinaloa', 'S.L.P.': 'San Luis Potosí', 'Q.R.': 'Quintana Roo',
            'Dgo.': 'Durango', 'B.C.': 'Baja California', 'Gto.': 'Guanajuato',
            'Camp.': 'Campeche', 'Tab.': 'Tabasco', 'Son.': 'Sonora',
            'Gro.': 'Guerrero', 'Zac.': 'Zacatecas', 'Ags.': 'Aguascalientes',
            'Nay.': 'Nayarit'
        }
        pos_geo_zones['geo_zone'] = pos_geo_zones['geo_zone'].replace(abreviaturas)
        
        # Extender catálogo de proveedores con productos de API (vendors 1275 y 1350)
        if not df_api_vendors.empty:
            # Crear registros de catálogo para productos de API
            api_catalog_records = []
            for _, api_row in df_api_vendors.iterrows():
                api_catalog_records.append({
                    'super_catalog_id': api_row['super_catalog_id'],
                    'vendor_id': api_row['vendor_id'],
                    'descripción': api_row.get('nombre', ''),  # Usar nombre de API como descripción
                    'base_price': api_row['precio_vendedor'],
                    'percentage': 0.0,  # Ya incluido en el precio
                    'name': 'México',  # Asumimos nacional
                    'stock': api_row.get('stock', 'unknown'),
                    'nombre': api_row.get('nombre', ''),
                    'laboratorio': api_row.get('laboratorio', ''),
                    'monodroga': api_row.get('monodroga', '')
                })
            
            df_api_catalog = pd.DataFrame(api_catalog_records)
            
            # Evitar duplicados al concatenar
            df_proveedores_extendido = pd.concat([df_proveedores, df_api_catalog], axis=0, ignore_index=True)
            df_proveedores_extendido = df_proveedores_extendido.drop_duplicates(subset=['super_catalog_id', 'vendor_id'], keep='last')
        else:
            df_proveedores_extendido = df_proveedores.copy()
        
        # Separar proveedores nacionales y regionales del catálogo extendido
        df_proveedores_nacional = df_proveedores_extendido[df_proveedores_extendido['name'] == 'México'].copy()
        df_proveedores_regional = df_proveedores_extendido[df_proveedores_extendido['name'] != 'México'].copy()
        
        # Unir pedidos con zonas geográficas - PRESERVAR DESCRIPCIÓN DE PEDIDOS - OPTIMIZADO
        # Pre-filtrar antes del merge para reducir datos
        df_pedidos_filtered = df_pedidos[df_pedidos['unidades_pedidas'] > 0].copy()
        df_pedidos_zonas = pd.merge(df_pedidos_filtered, pos_geo_zones, on='point_of_sale_id', how='left')
        
        # Debug: Verificar descripción en pedidos después del merge con geo_zones
        if 'descripción' in df_pedidos_zonas.columns:
            print(f"DEBUG: Descripción preservada en df_pedidos_zonas: {df_pedidos_zonas['descripción'].notna().sum()} registros")
        
        # Procesar con proveedores nacionales y regionales - OPTIMIZADO
        # No necesitamos filtrar otra vez unidades_pedidas ya que se hizo antes
        df_pedidos_proveedores_nacional = pd.merge(
            df_pedidos_zonas, df_proveedores_nacional, on='super_catalog_id', how='inner', suffixes=('_pedidos', '_catalogo')
        )
        
        df_pedidos_proveedores_regional = pd.merge(
            df_pedidos_zonas, df_proveedores_regional, 
            left_on=['super_catalog_id', 'geo_zone'], right_on=['super_catalog_id', 'name'], 
            how='inner', suffixes=('_pedidos', '_catalogo')
        )
        
        # Convertir tipos de datos para cálculos correctos - optimizado
        for df in [df_pedidos_proveedores_nacional, df_pedidos_proveedores_regional]:
            # Usar operaciones vectorizadas más eficientes
            df.loc[:, 'base_price'] = pd.to_numeric(df['base_price'], errors='coerce')
            df.loc[:, 'percentage'] = pd.to_numeric(df['percentage'], errors='coerce')
            df.loc[:, 'precio_vendedor'] = df['base_price'] * (1 + df['percentage'] / 100)
        
        # Unir dataframes
        df_pedidos_proveedores = pd.concat([
            df_pedidos_proveedores_regional, df_pedidos_proveedores_nacional
        ], axis=0, ignore_index=True)
        
        # CONSOLIDAR DESCRIPCIONES Y VENDOR_ID: Priorizar descripción de pedidos, usar catálogo como fallback
        if 'descripción_pedidos' in df_pedidos_proveedores.columns and 'descripción_catalogo' in df_pedidos_proveedores.columns:
            df_pedidos_proveedores['descripción'] = df_pedidos_proveedores['descripción_pedidos'].fillna(df_pedidos_proveedores['descripción_catalogo'])
            # Eliminar columnas duplicadas
            df_pedidos_proveedores = df_pedidos_proveedores.drop(columns=['descripción_pedidos', 'descripción_catalogo'])
        elif 'descripción_pedidos' in df_pedidos_proveedores.columns:
            df_pedidos_proveedores['descripción'] = df_pedidos_proveedores['descripción_pedidos']
            df_pedidos_proveedores = df_pedidos_proveedores.drop(columns=['descripción_pedidos'])
        elif 'descripción_catalogo' in df_pedidos_proveedores.columns:
            df_pedidos_proveedores['descripción'] = df_pedidos_proveedores['descripción_catalogo']
            df_pedidos_proveedores = df_pedidos_proveedores.drop(columns=['descripción_catalogo'])
        
        # CORREGIR NOMBRES DE COLUMNAS VENDOR_ID: Mantener nombres estándar
        if 'vendor_id_catalogo' in df_pedidos_proveedores.columns:
            df_pedidos_proveedores['vendor_id'] = df_pedidos_proveedores['vendor_id_catalogo']
            df_pedidos_proveedores = df_pedidos_proveedores.drop(columns=['vendor_id_catalogo'])
        if 'vendor_id_pedidos' in df_pedidos_proveedores.columns:
            # La columna vendor_id_pedidos se renombrará más adelante para evitar confusión
            pass
        
        # Debug: Verificar si descripción está presente
        print(f"DEBUG: Columnas en df_pedidos_proveedores: {list(df_pedidos_proveedores.columns)}")
        if 'descripción' in df_pedidos_proveedores.columns:
            descripcion_no_vacias = df_pedidos_proveedores['descripción'].dropna()
            descripcion_no_vacias = descripcion_no_vacias[descripcion_no_vacias != '']
            print(f"DEBUG: Registros con descripción no vacía: {len(descripcion_no_vacias)}")
            if len(descripcion_no_vacias) > 0:
                print(f"DEBUG: Primeras 3 descripciones: {descripcion_no_vacias.head(3).tolist()}")
        else:
            print("DEBUG: Columna 'descripción' NO encontrada en df_pedidos_proveedores")
        
        # Calcular precio_total_vendedor
        if 'precio_vendedor' in df_pedidos_proveedores.columns and 'unidades_pedidas' in df_pedidos_proveedores.columns:
            df_pedidos_proveedores['precio_total_vendedor'] = (
                df_pedidos_proveedores['unidades_pedidas'].astype(float) * 
                df_pedidos_proveedores['precio_vendedor'].astype(float)
            )
        
        # CORECCIÓN CRÍTICA: Manejo correcto del merge con vendor_pos_relations
        if 'vendor_id' in df_pedidos_proveedores.columns and 'point_of_sale_id' in df_pedidos_proveedores.columns:
            # Primero, renombrar la columna vendor_id del catálogo para evitar conflictos
            df_pedidos_proveedores = df_pedidos_proveedores.rename(columns={'vendor_id': 'vendor_id_y'})
            
        # Renombrar vendor_id_pedidos a vendor_id_x (vendor de la droguería)
        if 'vendor_id_pedidos' in df_pedidos_proveedores.columns:
            df_pedidos_proveedores = df_pedidos_proveedores.rename(columns={'vendor_id_pedidos': 'vendor_id_x'})
            
        # Hacer el merge con vendor_pos_relations asegurando coincidencia por vendor y POS
        if 'point_of_sale_id' in df_pedidos_proveedores.columns:
            required_cols = {'point_of_sale_id', 'vendor_id', 'status'}
            if df_vendors_pos.empty or not required_cols.issubset(df_vendors_pos.columns):
                df_pedidos_proveedores['status'] = np.nan
            else:
                status_lookup = (
                    df_vendors_pos[list(required_cols)]
                    .drop_duplicates(subset=['point_of_sale_id', 'vendor_id'])
                    .copy()
                )
                status_lookup['vendor_id'] = pd.to_numeric(status_lookup['vendor_id'], errors='coerce')

                vendor_lookup_col = None
                for candidate in ['vendor_id_y', 'vendor_id', 'vendor_id_catalogo']:
                    if candidate in df_pedidos_proveedores.columns:
                        vendor_lookup_col = candidate
                        break

                if vendor_lookup_col:
                    df_pedidos_proveedores[vendor_lookup_col] = pd.to_numeric(
                        df_pedidos_proveedores[vendor_lookup_col],
                        errors='coerce'
                    )
                    df_pedidos_proveedores = df_pedidos_proveedores.merge(
                        status_lookup,
                        how='left',
                        left_on=['point_of_sale_id', vendor_lookup_col],
                        right_on=['point_of_sale_id', 'vendor_id']
                    )
                    df_pedidos_proveedores.drop(columns=['vendor_id'], inplace=True, errors='ignore')
                else:
                    df_pedidos_proveedores['status'] = np.nan
        
        # Calcular precios mínimos locales - optimizado
        cols_needed = ['point_of_sale_id', 'super_catalog_id', 'precio_minimo', 'order_id']
        if all(col in df_pedidos_proveedores.columns for col in cols_needed):
            # Usar transform para mejor rendimiento en cálculos de grupo
            min_prices = (df_pedidos_proveedores
                         .groupby(['point_of_sale_id', 'order_id', 'super_catalog_id'], sort=False)['precio_minimo']
                         .min()
                         .reset_index())
            min_prices.columns = ['point_of_sale_id', 'order_id', 'super_catalog_id', 'precio_minimo_orders']
            
            # Unir para comparar precios
            df_con_precios_minimos_local = pd.merge(
                df_pedidos_proveedores, min_prices,
                on=['point_of_sale_id', 'super_catalog_id', 'order_id'], how='left'
            )
            
            # Integrar precios reales de vendors activos desde CSV
            if not df_precios_comparativa.empty:
                df_con_precios_reales = integrar_precios_vendors_activos(df_con_precios_minimos_local, df_precios_comparativa, df_vendors_pos)
            else:
                df_con_precios_reales = df_con_precios_minimos_local
            
            # Integrar precios de vendors 1275 y 1350 desde datos de API
            if not df_api_vendors.empty:
                df_con_precios_api = integrar_datos_api_vendors(df_con_precios_reales, df_api_vendors, df_vendors_pos)
            else:
                df_con_precios_api = df_con_precios_reales
            
            # Clasificar productos
            df_clasificado = agregar_columna_clasificacion(df_con_precios_api)
            
            # Debug: Verificar si descripción y columnas vendor llegan hasta df_clasificado
            print(f"DEBUG: Columnas en df_clasificado: {list(df_clasificado.columns)}")
            
            # Verificar descripción
            if 'descripción' in df_clasificado.columns:
                descripcion_no_vacias = df_clasificado['descripción'].dropna()
                descripcion_no_vacias = descripcion_no_vacias[descripcion_no_vacias != '']
                print(f"DEBUG: Registros en df_clasificado con descripción no vacía: {len(descripcion_no_vacias)}")
                if len(descripcion_no_vacias) > 0:
                    print(f"DEBUG: Primeras 3 descripciones en df_clasificado: {descripcion_no_vacias.head(3).tolist()}")
            else:
                print("DEBUG: Columna 'descripción' NO encontrada en df_clasificado")
                
            # Verificar columnas de vendor
            vendor_columns = [col for col in df_clasificado.columns if 'vendor' in col.lower()]
            print(f"DEBUG: Columnas con 'vendor' en df_clasificado: {vendor_columns}")
            
            drug_columns = [col for col in df_clasificado.columns if 'drug' in col.lower()]
            print(f"DEBUG: Columnas con 'drug' en df_clasificado: {drug_columns}")
        else:
            df_clasificado = pd.DataFrame()
        
        # Calcular métricas para visualización
        df_orders = df_pedidos.copy()
        
        # Debug: Mostrar información del DataFrame
        print(f"DEBUG: df_orders shape: {df_orders.shape}")
        print(f"DEBUG: df_orders columns: {list(df_orders.columns)}")
        print(f"DEBUG: df_orders primeras 3 filas:")
        print(df_orders.head(3) if not df_orders.empty else "DataFrame vacío")
        
        # Agregar total_compra si no existe
        if 'total_compra' not in df_orders.columns and 'unidades_pedidas' in df_orders.columns and 'precio_minimo' in df_orders.columns:
            df_orders['total_compra'] = df_orders['unidades_pedidas'] * df_orders['precio_minimo']
            print(f"DEBUG: total_compra calculado para {len(df_orders)} registros")
        elif 'total_compra' in df_orders.columns:
            print(f"DEBUG: total_compra ya existe en el DataFrame")
        else:
            print(f"DEBUG: No se pudo calcular total_compra - columnas faltantes")
        
        # Verificar datos no vacíos
        if df_orders.empty:
            print("DEBUG: df_orders está vacío!")
            pos_order_stats = pd.DataFrame(columns=['point_of_sale_id', 'promedio_por_orden', 'numero_ordenes'])
            pos_vendor_totals = pd.DataFrame(columns=['point_of_sale_id', 'vendor_id', 'total_compra'])
        else:
            # Calcular estadísticas por POS
            required_cols_stats = ['point_of_sale_id', 'order_id', 'total_compra']
            if all(col in df_orders.columns for col in required_cols_stats):
                order_totals = df_orders.groupby(['point_of_sale_id', 'order_id'])['total_compra'].sum().reset_index()
                pos_order_stats = order_totals.groupby('point_of_sale_id').agg({
                    'total_compra': ['mean', 'count']
                }).reset_index()
                pos_order_stats.columns = ['point_of_sale_id', 'promedio_por_orden', 'numero_ordenes']
                print(f"DEBUG: pos_order_stats calculado para {len(pos_order_stats)} POS")
            else:
                missing_cols = [col for col in required_cols_stats if col not in df_orders.columns]
                print(f"DEBUG: Columnas faltantes para pos_order_stats: {missing_cols}")
                pos_order_stats = pd.DataFrame(columns=['point_of_sale_id', 'promedio_por_orden', 'numero_ordenes'])
            
            # Calcular totales por vendor - usar columna correcta de vendor
            vendor_col = 'vendor_id'
            if 'vendor_id_x' in df_orders.columns:
                vendor_col = 'vendor_id_x'  # Usar vendor de la droguería
            
            required_cols_vendor = ['point_of_sale_id', vendor_col, 'total_compra']
            if all(col in df_orders.columns for col in required_cols_vendor):
                pos_vendor_totals = df_orders.groupby(['point_of_sale_id', vendor_col])['total_compra'].sum().reset_index()
                pos_vendor_totals.columns = ['point_of_sale_id', 'vendor_id', 'total_compra']
                print(f"DEBUG: pos_vendor_totals calculado para {len(pos_vendor_totals)} combinaciones POS-Vendor")
                print(f"DEBUG: POS únicos en pos_vendor_totals: {pos_vendor_totals['point_of_sale_id'].nunique()}")
            else:
                missing_cols = [col for col in required_cols_vendor if col not in df_orders.columns]
                print(f"DEBUG: Columnas faltantes para pos_vendor_totals: {missing_cols}")
                print(f"DEBUG: Usando columnas disponibles: {list(df_orders.columns)}")
                pos_vendor_totals = pd.DataFrame(columns=['point_of_sale_id', 'vendor_id', 'total_compra'])
        
        return pos_vendor_totals, df_pedidos, pos_order_stats, df_min_purchase, df_vendor_dm, pos_geo_zones, df_clasificado, df_vendors_pos, df_precios_comparativa
    
    except Exception as e:
        import traceback
        print("ERROR en load_and_process_data:", traceback.format_exc())
        print(f"ERROR específico: {str(e)}")
        
        # Crear DataFrames vacíos con estructura correcta
        empty_pos_vendor_totals = pd.DataFrame(columns=['point_of_sale_id', 'vendor_id', 'total_compra'])
        empty_df_pedidos = pd.DataFrame()
        empty_pos_order_stats = pd.DataFrame(columns=['point_of_sale_id', 'promedio_por_orden', 'numero_ordenes'])
        empty_df_min_purchase = pd.DataFrame(columns=['vendor_id', 'name', 'min_purchase'])
        empty_df_vendor_dm = pd.DataFrame(columns=['vendor_id', 'name', 'drug_manufacturer_id'])
        empty_pos_geo_zones = pd.DataFrame(columns=['point_of_sale_id', 'geo_zone'])
        empty_df_clasificado = pd.DataFrame()
        empty_df_vendors_pos = pd.DataFrame(columns=['point_of_sale_id', 'vendor_id', 'status'])
        empty_df_precios_comparativa = pd.DataFrame()
        
        return (empty_pos_vendor_totals, empty_df_pedidos, empty_pos_order_stats, 
                empty_df_min_purchase, empty_df_vendor_dm, empty_pos_geo_zones, 
                empty_df_clasificado, empty_df_vendors_pos, empty_df_precios_comparativa)

# Código principal
try:
    
    pos_vendor_totals, df_original, pos_order_stats, df_min_purchase, df_vendor_dm, pos_geo_zones, df_clasificado, df_vendors_pos, df_precios_comparativa = load_and_process_data()
    
    # Filtro de punto de venta
    st.header("Análisis Individual de POS")
    
    # Debug info para el usuario
    with st.expander("🔍 Información de Debug (Datos Cargados)", expanded=False):
        st.write(f"**pos_vendor_totals:** {len(pos_vendor_totals)} registros")
        st.write(f"**df_original:** {len(df_original)} registros") 
        st.write(f"**df_clasificado:** {len(df_clasificado)} registros")
        st.write(f"**df_vendors_pos:** {len(df_vendors_pos)} registros")
        
        if not pos_vendor_totals.empty:
            st.write(f"**POS únicos en pos_vendor_totals:** {pos_vendor_totals['point_of_sale_id'].nunique()}")
            st.write("**Primeras 5 filas de pos_vendor_totals:**")
            st.dataframe(pos_vendor_totals.head())
        else:
            st.error("❌ pos_vendor_totals está vacío")
            
        if not df_original.empty:
            st.write("**Columnas en df_original:**")
            st.write(list(df_original.columns))
        else:
            st.error("❌ df_original está vacío")
    
    pos_list = sorted(list(set(pos_vendor_totals['point_of_sale_id']))) if not pos_vendor_totals.empty else []
    
    if not pos_list:
        st.error("❌ No hay puntos de venta disponibles para analizar")
        st.info("💡 **Posibles causas:**")
        st.write("- Archivos de datos no encontrados en el entorno de despliegue")
        st.write("- Error en el procesamiento de datos")
        st.write("- Datos vacíos en los archivos fuente")
        st.write("- Problema con las columnas requeridas")
        
        # Mostrar estado de archivos
        import os
        st.write("**📁 Estado de archivos:**")
        files_to_check = [
            'data/pos_address.csv',
            'data/orders_delivered_pos_vendor_geozone.csv',
            'data/vendors_catalog.csv', 
            'data/vendor_pos_relations.csv',
            'data/datos_api.json'
        ]
        
        for file_path in files_to_check:
            exists = os.path.exists(file_path)
            status = "✅" if exists else "❌"
            st.write(f"{status} {file_path}")
            
    else:
        selected_pos = st.selectbox("Seleccionar Punto de Venta", options=pos_list)

        # Mostrar información del POS seleccionado
        if selected_pos:
            # Filtrar datos para el POS seleccionado
            pos_data = pos_vendor_totals[pos_vendor_totals['point_of_sale_id'] == selected_pos]
            pos_data = pos_data.sort_values('total_compra', ascending=False) if not pos_data.empty else pd.DataFrame()

            # Obtener estadísticas
            pos_stats = pos_order_stats[pos_order_stats['point_of_sale_id'] == selected_pos]
            promedio_por_orden = pos_stats.iloc[0]['promedio_por_orden'] if not pos_stats.empty else 0
            numero_ordenes = int(pos_stats.iloc[0]['numero_ordenes']) if not pos_stats.empty else 0
                
            st.subheader("Información del Punto de Venta")

            # Métricas principales
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric(f"Total de Compras - POS {selected_pos}", 
                          f"${pos_data['total_compra'].sum():,.2f}" if not pos_data.empty else "$0.00")
            with col2:
                st.metric("Promedio por Orden", f"${promedio_por_orden:,.2f}")
            with col3:
                st.metric("Número de Órdenes", f"{numero_ordenes:,}")

            # Información adicional
            pos_info = pos_geo_zones[pos_geo_zones['point_of_sale_id'] == selected_pos]
            pos_country = df_original[df_original['point_of_sale_id'] == selected_pos]

            country = pos_country['country'].iloc[0] if not pos_country.empty and 'country' in pos_country.columns else 'No disponible'
            geo_zone = pos_info['geo_zone'].iloc[0] if not pos_info.empty and 'geo_zone' in pos_info.columns else 'No disponible'

            info_col1, info_col2, info_col3 = st.columns(3)
            
            with info_col1:
                st.metric("País", country)
            with info_col2:
                st.metric("Zona Geográfica", geo_zone)
            with info_col3:
                st.metric("Total Vendors", len(pos_data) if not pos_data.empty else 0)

            # Detalle de compras
            st.subheader("Detalle de Compras por Droguería/Vendor")
            if not pos_data.empty:
                pos_data['porcentaje'] = (pos_data['total_compra'] / pos_data['total_compra'].sum()) * 100    
                detail_table = pos_data.copy()
                detail_table.columns = ['POS ID', 'Droguería/Vendor ID', 'Total Comprado', 'Porcentaje']
                detail_table = detail_table.round({'Porcentaje': 2})
                
                st.dataframe(
                    detail_table.style.format({
                        'Total Comprado': '${:,.2f}',
                        'Porcentaje': '{:.2f}%'
                    })
                )

                st.header("📊 Análisis Detallado de Oportunidades de Ahorro por Producto")

                # Verificar si tenemos los datos necesarios
                if not df_clasificado.empty and selected_pos:
                    # Filtrar registros que no existen en el CSV de precios comparativos
                    df_clasificado_validado = filtrar_registros_validos(df_clasificado, df_precios_comparativa)
                    
                    # Filtrar datos para el POS seleccionado
                    df_pos_clasificado = df_clasificado_validado[df_clasificado_validado['point_of_sale_id'] == selected_pos].copy()
                    
                    if not df_pos_clasificado.empty:
                        
                        # Dashboard ejecutivo
                        #crear_dashboard_ejecutivo_ahorro(df_clasificado, selected_pos)

                        # Tabs para diferentes vistas
                        vendor_col = None
                        if 'vendor_id_y' in df_pos_clasificado.columns:
                            vendor_col = 'vendor_id_y'  # Esta debería ser la columna del vendor del catálogo
                        elif 'vendor_id' in df_pos_clasificado.columns:
                            vendor_col = 'vendor_id'

                        drogueria_col = None
                        if 'vendor_id_x' in df_pos_clasificado.columns:
                            drogueria_col = 'vendor_id_x'  # Esta debería ser la columna de la droguería
                        elif 'drug_manufacturer_id' in df_pos_clasificado.columns:
                            drogueria_col = 'drug_manufacturer_id'

                        if vendor_col is None or drogueria_col is None:
                            st.error(f"No se encontraron las columnas necesarias. Vendor: {vendor_col}, Droguería: {drogueria_col}")
                            st.write("Columnas disponibles:")
                            st.write(list(df_pos_clasificado.columns))
                        else:
                            productos_con_vendors = df_pos_clasificado[
                                df_pos_clasificado['clasificacion'].isin(['Precio vendor minimo', 'Precio vendor no minimo'])
                            ].copy()

                            if not productos_con_vendors.empty:
                                df_producto_analysis = construir_analisis_productos(
                                    df_pos_clasificado,
                                    productos_con_vendors,
                                    vendor_col,
                                    drogueria_col,
                                    selected_pos,
                                    df_vendors_pos
                                )

                                if df_producto_analysis.empty:
                                    st.warning("No se pudieron generar análisis de productos.")
                                else:
                                    tab1, tab2 = st.tabs(["🏭 Análisis por Vendor", "📊 Análisis Detallado por Producto"])

                                    with tab1:
                                        def status_mode(series):
                                            non_null = series.dropna()
                                            if not non_null.empty:
                                                return non_null.mode().iloc[0]
                                            return "Sin Status"

                                        def clasificar_oportunidad(porcentaje):
                                            if porcentaje > 15:
                                                return "Oportunidad Alta"
                                            if porcentaje > 5:
                                                return "Oportunidad Media"
                                            return "Oportunidad Baja"

                                        def contar_mejores(series):
                                            return int((series > 0).sum())

                                        df_vendor_analysis = (
                                            df_producto_analysis.groupby('Mejor Vendor ID').agg(
                                                Status=('Status Mejor Vendor', status_mode),
                                                Productos_Unicos=('Producto ID', pd.Series.nunique),
                                                Ordenes_Afectadas=('Orden ID', pd.Series.nunique),
                                                Registros_con_Mejor_Precio=('Ahorro con Mejor Vendor', contar_mejores),
                                                Valor_Actual=('Precio Total Droguería', 'sum'),
                                                Valor_con_Vendor=('Precio Total Mejor Vendor', 'sum'),
                                                Ahorro_Potencial=('Ahorro con Mejor Vendor', 'sum')
                                            )
                                            .reset_index()
                                        )

                                        if df_vendor_analysis.empty:
                                            st.warning("No se encontraron vendors con datos válidos para el análisis.")
                                        else:
                                            df_vendor_analysis.rename(columns={
                                                'Mejor Vendor ID': 'Vendor ID',
                                                'Productos_Unicos': 'Productos Únicos',
                                                'Ordenes_Afectadas': 'Órdenes Afectadas',
                                                'Registros_con_Mejor_Precio': 'Registros con Mejor Precio',
                                                'Valor_Actual': 'Valor Actual (Droguería)',
                                                'Valor_con_Vendor': 'Valor con Vendor',
                                                'Ahorro_Potencial': 'Ahorro Potencial'
                                            }, inplace=True)

                                            df_vendor_analysis['Vendor ID'] = df_vendor_analysis['Vendor ID'].astype(str)
                                            df_vendor_analysis['Status'] = df_vendor_analysis['Status'].fillna('Sin Status')
                                            df_vendor_analysis['Porcentaje Ahorro'] = np.where(
                                                df_vendor_analysis['Valor Actual (Droguería)'] > 0,
                                                (df_vendor_analysis['Ahorro Potencial'] / df_vendor_analysis['Valor Actual (Droguería)']) * 100,
                                                0
                                            )
                                            df_vendor_analysis['Clasificación'] = df_vendor_analysis['Porcentaje Ahorro'].apply(clasificar_oportunidad)
                                            df_vendor_analysis = df_vendor_analysis.sort_values('Ahorro Potencial', ascending=False)

                                            col1, col2, col3, col4 = st.columns(4)
                                            with col1:
                                                vendors_analizados_placeholder = st.empty()
                                            with col2:
                                                total_ahorro_placeholder = st.empty()
                                            with col3:
                                                vendors_positivos_placeholder = st.empty()
                                            with col4:
                                                vendors_activos_placeholder = st.empty()

                                            col1, col2, col3 = st.columns(3)
                                            with col1:
                                                status_options = df_vendor_analysis['Status'].unique().tolist()
                                                status_filter = st.multiselect(
                                                    "Filtrar por Status:",
                                                    options=status_options,
                                                    default=status_options
                                                )
                                            with col2:
                                                ahorro_minimo = st.number_input(
                                                    "Ahorro mínimo ($):",
                                                    min_value=0.0,
                                                    value=0.0,
                                                    step=500.0
                                                )
                                            with col3:
                                                clasificacion_options = df_vendor_analysis['Clasificación'].unique().tolist()
                                                clasificacion_filter = st.multiselect(
                                                    "Filtrar por Clasificación:",
                                                    options=clasificacion_options,
                                                    default=clasificacion_options
                                                )

                                            df_filtrado = df_vendor_analysis[
                                                (df_vendor_analysis['Status'].isin(status_filter)) &
                                                (df_vendor_analysis['Ahorro Potencial'] >= ahorro_minimo) &
                                                (df_vendor_analysis['Clasificación'].isin(clasificacion_filter))
                                            ]

                                            vendors_analizados_placeholder.metric("Vendors Analizados", len(df_filtrado))
                                            total_ahorro = df_filtrado['Ahorro Potencial'].sum()
                                            total_ahorro_placeholder.metric("Ahorro Total Potencial", f"${total_ahorro:,.2f}")

                                            vendors_positivos = len(df_filtrado[df_filtrado['Ahorro Potencial'] > 0])
                                            vendors_positivos_placeholder.metric("Vendors con Ahorro Positivo", vendors_positivos)
                                            vendors_activos = len(df_filtrado[df_filtrado['Status'] == 'Activo'])
                                            vendors_activos_placeholder.metric("Vendors Activos", vendors_activos)

                                            def color_status(val):
                                                if val == "Activo":
                                                    return 'background-color: #90EE90'
                                                elif val == "Pendiente":
                                                    return 'background-color: #FFD700'
                                                elif val == "Rechazado":
                                                    return 'background-color: #ffcccb'
                                                else:
                                                    return 'background-color: #e6f3ff'

                                            styled_df = df_filtrado.style.format({
                                                'Valor Actual (Droguería)': '${:,.2f}',
                                                'Valor con Vendor': '${:,.2f}',
                                                'Ahorro Potencial': '${:,.2f}',
                                                'Porcentaje Ahorro': '{:.1f}%'
                                            }).applymap(color_status, subset=['Status']).background_gradient(subset=['Ahorro Potencial'], cmap='RdYlGn')

                                            st.dataframe(styled_df)

                                            if len(df_filtrado) > 0:
                                                df_chart = df_filtrado.head(15).copy()
                                                df_chart['Vendor ID'] = df_chart['Vendor ID'].astype(str)

                                                fig_vendors = px.bar(
                                                    df_chart,
                                                    x='Vendor ID',
                                                    y='Ahorro Potencial',
                                                    color='Status',
                                                    title='Top 15 Vendors por Ahorro Potencial',
                                                    labels={'Ahorro Potencial': 'Ahorro Potencial ($)'},
                                                    color_discrete_map={
                                                        'Activo': '#51cf66',
                                                        'Pendiente': '#ffd43b',
                                                        'Rechazado': '#ff6b6b',
                                                        'Sin Status': '#87ceeb'
                                                    }
                                                )
                                                fig_vendors.update_xaxes(type='category')
                                                fig_vendors.update_layout(xaxis_tickangle=-45, bargap=0.2)
                                                st.plotly_chart(fig_vendors, use_container_width=True)

                                                st.subheader("Análisis por Status de Vendor")
                                                status_summary = df_filtrado.groupby('Status').agg({
                                                    'Ahorro Potencial': ['sum', 'mean', 'count'],
                                                    'Vendor ID': 'count'
                                                }).round(2)

                                                status_summary.columns = ['Ahorro Total', 'Ahorro Promedio', 'Count1', 'Número de Vendors']
                                                status_summary = status_summary.drop('Count1', axis=1)

                                                st.dataframe(
                                                    status_summary.style.format({
                                                        'Ahorro Total': '${:,.2f}',
                                                        'Ahorro Promedio': '${:,.2f}'
                                                    })
                                                )

                                    with tab2:
                                        st.subheader("📊 Análisis Detallado Producto por Producto")

                                        col1, col2, col3 = st.columns(3)
                                        with col1:
                                            ahorro_min_producto = st.number_input(
                                                "Ahorro mínimo por producto ($):",
                                                min_value=0.0,
                                                value=0.0,
                                                step=100.0,
                                                key="ahorro_min_producto"
                                            )
                                        with col2:
                                            tipo_ahorro_filter = st.multiselect(
                                                "Filtrar por Tipo de Ahorro:",
                                                options=['Alto', 'Medio', 'Bajo'],
                                                default=['Alto', 'Medio', 'Bajo'],
                                                key="tipo_ahorro_filter"
                                            )
                                        with col3:
                                            status_vendor_filter = st.multiselect(
                                                "Status del Mejor Vendor:",
                                                options=df_producto_analysis['Status Mejor Vendor'].unique(),
                                                default=df_producto_analysis['Status Mejor Vendor'].unique(),
                                                key="status_vendor_filter"
                                            )

                                        df_productos_filtrado = df_producto_analysis[
                                            (df_producto_analysis['Ahorro con Mejor Vendor'] >= ahorro_min_producto) &
                                            (df_producto_analysis['Tipo Ahorro'].isin(tipo_ahorro_filter)) &
                                            (df_producto_analysis['Status Mejor Vendor'].isin(status_vendor_filter))
                                        ]

                                        col1, col2, col3, col4 = st.columns(4)
                                        with col1:
                                            st.metric("Productos Analizados", len(df_productos_filtrado))
                                        with col2:
                                            total_ahorro_productos = df_productos_filtrado['Ahorro con Mejor Vendor'].sum()
                                            st.metric("Ahorro Total Productos", f"${total_ahorro_productos:,.2f}")

                                        with col3:
                                            productos_con_ahorro = len(df_productos_filtrado[df_productos_filtrado['Ahorro con Mejor Vendor'] > 0])
                                            st.metric("Productos con Ahorro", productos_con_ahorro)
                                        with col4:
                                            promedio_opciones = df_productos_filtrado['Opciones Vendors'].mean() if len(df_productos_filtrado) > 0 else 0
                                            st.metric("Promedio Opciones/Producto", f"{promedio_opciones:.1f}")

                                        st.write(f"**Mostrando {len(df_productos_filtrado)} productos de {len(df_producto_analysis)} totales**")

                                        styled_productos = df_productos_filtrado.style.format({
                                            'Precio Unit. Droguería': '${:,.2f}',
                                            'Precio Total Droguería': '${:,.2f}',
                                            'Precio Unit. Mejor Vendor': '${:,.2f}',
                                            'Precio Total Mejor Vendor': '${:,.2f}',
                                            'Ahorro con Mejor Vendor': '${:,.2f}',
                                            'Porcentaje Ahorro': '{:.1f}%',
                                            'Unidades': '{:,.0f}'
                                        }).background_gradient(subset=['Ahorro con Mejor Vendor'], cmap='RdYlGn')

                                        st.dataframe(styled_productos, height=400)

                                        if len(df_productos_filtrado) > 0:
                                            st.subheader("Vendors que Aparecen Más Frecuentemente como Mejor Opción")
                                            vendor_frecuencia = df_productos_filtrado.groupby(['Mejor Vendor ID', 'Status Mejor Vendor']).agg({
                                                'Producto ID': 'count',
                                                'Ahorro con Mejor Vendor': ['sum', 'mean']
                                            }).round(2)

                                            vendor_frecuencia.columns = ['Productos Como Mejor Opción', 'Ahorro Total', 'Ahorro Promedio']
                                            vendor_frecuencia = vendor_frecuencia.reset_index().sort_values('Ahorro Total', ascending=False)

                                            st.dataframe(
                                                vendor_frecuencia.style.format({
                                                    'Ahorro Total': '${:,.2f}',
                                                    'Ahorro Promedio': '${:,.2f}'
                                                })
                                            )
                            else:
                                st.warning("No se encontraron productos con opciones de vendors disponibles.")
                    else:
                        st.warning("No hay datos clasificados disponibles para el punto de venta seleccionado.")
                else:
                    st.warning("No hay datos de clasificación disponibles.")

            else:
                st.info("No hay datos de compras para este punto de venta.")

    # NUEVA SECCIÓN: Consolidado de Oportunidades de Ahorro - Todos los POS (FUERA de la sección individual)
    st.header("🗂️ Consolidado de Oportunidades de Ahorro - Todos los POS")
    st.write("Esta tabla muestra las oportunidades de ahorro **superiores al 30%** para todos los puntos de venta, ordenados por el potencial de ahorro.")
    st.info("ℹ️ Solo se incluyen productos donde el ahorro potencial es mayor al 30% del precio actual.")

    if not df_clasificado.empty:
        # Filtrar registros válidos para todo el dataset
        df_clasificado_global = filtrar_registros_validos(df_clasificado, df_precios_comparativa)
        
        # Identificar columnas necesarias
        vendor_col_global = None
        if 'vendor_id_y' in df_clasificado_global.columns:
            vendor_col_global = 'vendor_id_y'
        elif 'vendor_id' in df_clasificado_global.columns:
            vendor_col_global = 'vendor_id'

        drogueria_col_global = None
        if 'vendor_id_x' in df_clasificado_global.columns:
            drogueria_col_global = 'vendor_id_x'
        elif 'drug_manufacturer_id' in df_clasificado_global.columns:
            drogueria_col_global = 'drug_manufacturer_id'

        if vendor_col_global and drogueria_col_global:
            # Obtener todos los POS únicos
            pos_list_global = df_clasificado_global['point_of_sale_id'].unique()
        
            # Procesar todos los POS
            all_pos_analysis = []
            
            with st.spinner("Procesando datos para todos los POS..."):
                for pos_id in pos_list_global:
                    df_pos_temp = df_clasificado_global[df_clasificado_global['point_of_sale_id'] == pos_id].copy()
                    
                    if not df_pos_temp.empty:
                        productos_con_vendors_temp = df_pos_temp[
                            df_pos_temp['clasificacion'].isin(['Precio vendor minimo', 'Precio vendor no minimo'])
                        ].copy()

                        if not productos_con_vendors_temp.empty:
                            df_analisis_temp = construir_analisis_productos(
                                df_pos_temp,
                                productos_con_vendors_temp,
                                vendor_col_global,
                                drogueria_col_global,
                                pos_id,
                                df_vendors_pos
                            )
                            
                            if not df_analisis_temp.empty:
                                # FILTRO: Solo incluir oportunidades con ahorro mayor al 30%
                                df_analisis_temp = df_analisis_temp[df_analisis_temp['Porcentaje Ahorro'] > 30]
                                
                            if not df_analisis_temp.empty:
                                # Agregar información del POS
                                df_analisis_temp['POS ID'] = pos_id
                                
                                # Obtener geo_zone del POS
                                pos_geo_info = pos_geo_zones[pos_geo_zones['point_of_sale_id'] == pos_id]
                                if not pos_geo_info.empty:
                                    df_analisis_temp['Zona Geográfica'] = pos_geo_info['geo_zone'].iloc[0]
                                else:
                                    df_analisis_temp['Zona Geográfica'] = 'No disponible'
                                
                                all_pos_analysis.append(df_analisis_temp)
            
            if all_pos_analysis:
                # Concatenar todos los análisis
                df_consolidado = pd.concat(all_pos_analysis, ignore_index=True)
                
                # Ordenar por ahorro descendente
                df_consolidado = df_consolidado.sort_values('Ahorro con Mejor Vendor', ascending=False)
                
                # Reorganizar columnas para mejor visualización
                columnas_orden = [
                    'POS ID', 'Zona Geográfica', 'Producto ID', 'Descripción', 'Orden ID', 'Fecha Orden', 'Unidades',
                    'Droguería ID', 'Precio Unit. Droguería', 'Precio Total Droguería',
                    'Mejor Vendor ID', 'Status Mejor Vendor', 'Precio Unit. Mejor Vendor', 
                    'Precio Total Mejor Vendor', 'Ahorro con Mejor Vendor', 'Porcentaje Ahorro',
                    'Tipo Ahorro', 'Opciones Vendors'
                ]
                
                columnas_disponibles = [col for col in columnas_orden if col in df_consolidado.columns]
                df_consolidado = df_consolidado[columnas_disponibles]
                
                # Métricas principales del consolidado
                metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
                with metric_col1:
                    st.metric("Total Oportunidades", len(df_consolidado))
                with metric_col2:
                    total_ahorro_consolidado = df_consolidado['Ahorro con Mejor Vendor'].sum()
                    st.metric("Ahorro Total Potencial", f"${total_ahorro_consolidado:,.2f}")
                with metric_col3:
                    pos_unicos = df_consolidado['POS ID'].nunique()
                    st.metric("POS con Oportunidades", pos_unicos)
                with metric_col4:
                    promedio_ahorro = df_consolidado['Ahorro con Mejor Vendor'].mean()
                    st.metric("Ahorro Promedio", f"${promedio_ahorro:,.2f}")
                
                # Filtros para el consolidado
                st.subheader("🔍 Filtros")
                filter_col1, filter_col2, filter_col3, filter_col4 = st.columns(4)
                with filter_col1:
                    pos_filter = st.multiselect(
                        "Filtrar por POS:",
                        options=sorted(df_consolidado['POS ID'].unique()),
                        default=[],
                        key="consolidado_pos_filter"
                    )
                with filter_col2:
                    zona_filter = st.multiselect(
                        "Filtrar por Zona:",
                        options=sorted(df_consolidado['Zona Geográfica'].unique()),
                        default=[],
                        key="consolidado_zona_filter"
                    )
                with filter_col3:
                    ahorro_min_consolidado = st.number_input(
                        "Ahorro mínimo ($):",
                        min_value=0.0,
                        value=0.0,
                        step=100.0,
                        key="consolidado_ahorro_min"
                    )
                with filter_col4:
                    tipo_ahorro_consolidado = st.multiselect(
                        "Tipo de Ahorro:",
                        options=['Alto', 'Medio', 'Bajo'],
                        default=['Alto'],
                        key="consolidado_tipo_ahorro",
                        help="Solo oportunidades >30% ya están incluidas por defecto"
                    )
                
                # Aplicar filtros
                df_consolidado_filtrado = df_consolidado.copy()
                if pos_filter:
                    df_consolidado_filtrado = df_consolidado_filtrado[
                        df_consolidado_filtrado['POS ID'].isin(pos_filter)
                    ]
                if zona_filter:
                    df_consolidado_filtrado = df_consolidado_filtrado[
                        df_consolidado_filtrado['Zona Geográfica'].isin(zona_filter)
                    ]
                df_consolidado_filtrado = df_consolidado_filtrado[
                    df_consolidado_filtrado['Ahorro con Mejor Vendor'] >= ahorro_min_consolidado
                ]
                df_consolidado_filtrado = df_consolidado_filtrado[
                    df_consolidado_filtrado['Tipo Ahorro'].isin(tipo_ahorro_consolidado)
                ]
                
                # Botón de descarga
                if not df_consolidado_filtrado.empty:
                    df_download = df_consolidado_filtrado.copy()
                    if 'Precio Unit. Droguería' in df_download.columns:
                        df_download['Precio Unit. Droguería'] = df_download['Precio Unit. Droguería'].round(2)
                    if 'Precio Total Droguería' in df_download.columns:
                        df_download['Precio Total Droguería'] = df_download['Precio Total Droguería'].round(2)
                    if 'Precio Unit. Mejor Vendor' in df_download.columns:
                        df_download['Precio Unit. Mejor Vendor'] = df_download['Precio Unit. Mejor Vendor'].round(2)
                    if 'Precio Total Mejor Vendor' in df_download.columns:
                        df_download['Precio Total Mejor Vendor'] = df_download['Precio Total Mejor Vendor'].round(2)
                    if 'Ahorro con Mejor Vendor' in df_download.columns:
                        df_download['Ahorro con Mejor Vendor'] = df_download['Ahorro con Mejor Vendor'].round(2)
                    if 'Porcentaje Ahorro' in df_download.columns:
                        df_download['Porcentaje Ahorro'] = df_download['Porcentaje Ahorro'].round(2)
                    
                    csv_data = df_download.to_csv(index=False, encoding='utf-8')
                    st.download_button(
                        label="📥 Descargar Lista Completa de Oportunidades (CSV)",
                        data=csv_data,
                        file_name=f"oportunidades_ahorro_consolidado_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                        help="Descarga la lista completa de productos ordenados por oportunidad de ahorro"
                    )
                
                st.subheader(f"📊 Tabla Consolidada ({len(df_consolidado_filtrado)} registros)")
                if not df_consolidado_filtrado.empty:
                    total_records = len(df_consolidado_filtrado)
                    if total_records > 5000:
                        st.warning(f"⚠️ Dataset grande ({total_records:,} registros). Mostrando los primeros 5,000 por rendimiento.")
                        st.info("💡 Usa los filtros para reducir el conjunto de datos o descarga el archivo completo.")
                        df_display = df_consolidado_filtrado.head(5000)
                    else:
                        df_display = df_consolidado_filtrado
                    
                    if total_records > 10000:
                        st.dataframe(df_display, height=500, use_container_width=True)
                    else:
                        try:
                            styled_consolidado = df_display.style.format({
                                'Precio Unit. Droguería': '${:,.2f}',
                                'Precio Total Droguería': '${:,.2f}',
                                'Precio Unit. Mejor Vendor': '${:,.2f}',
                                'Precio Total Mejor Vendor': '${:,.2f}',
                                'Ahorro con Mejor Vendor': '${:,.2f}',
                                'Porcentaje Ahorro': '{:.1f}%',
                                'Unidades': '{:,.0f}'
                            }).background_gradient(subset=['Ahorro con Mejor Vendor'], cmap='RdYlGn')
                            st.dataframe(styled_consolidado, height=500, use_container_width=True)
                        except Exception:
                            st.warning("⚠️ No se pudo aplicar formato debido al tamaño del dataset. Mostrando tabla simple.")
                            st.dataframe(df_display, height=500, use_container_width=True)
                    
                    st.subheader("📈 Resumen por POS: Top 5 Productos por Vendor")
                    resumen_detallado = []
                    for pos_id in df_consolidado_filtrado['POS ID'].unique():
                        df_pos = df_consolidado_filtrado[df_consolidado_filtrado['POS ID'] == pos_id]
                        zona = df_pos['Zona Geográfica'].iloc[0] if not df_pos.empty else 'No disponible'
                        for vendor_id in df_pos['Mejor Vendor ID'].unique():
                            df_vendor = df_pos[df_pos['Mejor Vendor ID'] == vendor_id].head(5)
                            for _, row in df_vendor.iterrows():
                                resumen_detallado.append({
                                    'POS ID': pos_id,
                                    'Zona Geográfica': zona,
                                    'Vendor ID': vendor_id,
                                    'Status Vendor': row['Status Mejor Vendor'],
                                    'Producto ID': row['Producto ID'],
                                    'Ahorro Individual': row['Ahorro con Mejor Vendor'],
                                    'Porcentaje Ahorro': row['Porcentaje Ahorro'],
                                    'Precio Droguería': row['Precio Total Droguería'],
                                    'Precio Vendor': row['Precio Total Mejor Vendor']
                                })
                    
                    if resumen_detallado:
                        df_resumen_detallado = pd.DataFrame(resumen_detallado)
                        df_resumen_detallado = df_resumen_detallado.sort_values(
                            ['POS ID', 'Vendor ID', 'Ahorro Individual'],
                            ascending=[True, True, False]
                        )
                        st.dataframe(
                            df_resumen_detallado.style.format({
                                'Ahorro Individual': '${:,.2f}',
                                'Porcentaje Ahorro': '{:.1f}%',
                                'Precio Droguería': '${:,.2f}',
                                'Precio Vendor': '${:,.2f}'
                            }).background_gradient(subset=['Ahorro Individual'], cmap='RdYlGn'),
                            use_container_width=True,
                            height=400
                        )
                else:
                    st.info("No hay datos que coincidan con los filtros seleccionados.")
            else:
                st.warning("No se encontraron oportunidades de ahorro superiores al 30% en ningún POS.")
        
        else:
            st.error("No se pueden procesar los datos. Faltan columnas necesarias en el dataset.")

    else:
        st.warning("No hay datos clasificados disponibles para generar el consolidado.")

except Exception as e:
    st.error(f"Error al procesar los datos: {str(e)}")
    st.error("Información de debug:")
    
    # Mostrar información de debug
    if 'df_clasificado' in locals():
        st.write("**Columnas disponibles en df_clasificado:**")
        st.write(list(df_clasificado.columns))
        st.write("**Primeras 5 filas:**")
        st.dataframe(df_clasificado.head())
    
    import traceback
    st.expander("Ver detalles del error", expanded=False).code(traceback.format_exc())
    st.info("Asegúrate de que todos los archivos CSV estén en el directorio correcto y tengan el formato esperado.")
