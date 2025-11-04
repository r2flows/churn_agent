import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime, timedelta

# Configuracion de la pagina
st.set_page_config(
    page_title="Dashboard de Comportamiento de Compras por Drogueria",
    page_icon="📊",
    layout="wide"
)

@st.cache_data
def load_data():
    """Carga y procesa los datos de ordenes"""
    try:
        # Intentar cargar con diferentes codificaciones
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
            st.error("No se pudo leer el archivo con ninguna codificacion conocida")
            return pd.DataFrame()

        # Limpiar nombres de columnas (eliminar espacios en blanco)
        df_orders.columns = df_orders.columns.str.strip()

        # Verificar columnas necesarias
        if 'order_date' in df_orders.columns:
            df_orders['order_date'] = pd.to_datetime(df_orders['order_date'], errors='coerce')

        # Identificar la columna de valor total
        if 'total_compra' not in df_orders.columns and 'valor_vendedor' in df_orders.columns:
            df_orders['total_compra'] = df_orders['valor_vendedor']

        return df_orders
    except Exception as e:
        st.error(f"Error al cargar los datos: {str(e)}")
        return pd.DataFrame()

def calculate_pos_vendor_totals(df_orders):
    """Calcula los totales de compra por POS y vendor"""
    if df_orders.empty:
        return pd.DataFrame()

    # Agrupar por POS y vendor
    pos_vendor_totals = df_orders.groupby(['point_of_sale_id', 'vendor_id'])['total_compra'].sum().reset_index()
    pos_vendor_totals.columns = ['point_of_sale_id', 'vendor_id', 'total_compra']

    # Calcular porcentajes por POS
    total_by_pos = pos_vendor_totals.groupby('point_of_sale_id')['total_compra'].sum().reset_index()
    total_by_pos.columns = ['point_of_sale_id', 'total_pos']

    pos_vendor_totals = pos_vendor_totals.merge(total_by_pos, on='point_of_sale_id')
    pos_vendor_totals['porcentaje'] = (pos_vendor_totals['total_compra'] / pos_vendor_totals['total_pos']) * 100

    return pos_vendor_totals

def calculate_weekly_distribution(df_orders):
    """Calcula la distribucion semanal de compras por drogueria"""
    if df_orders.empty or 'order_date' not in df_orders.columns:
        return pd.DataFrame()

    # Crear columna de semana
    df_orders['week'] = df_orders['order_date'].dt.to_period('W').astype(str)

    # Agrupar por semana, POS y vendor
    weekly_totals = df_orders.groupby(['week', 'point_of_sale_id', 'vendor_id'])['total_compra'].sum().reset_index()

    # Calcular porcentajes semanales por POS
    weekly_pos_totals = weekly_totals.groupby(['week', 'point_of_sale_id'])['total_compra'].sum().reset_index()
    weekly_pos_totals.columns = ['week', 'point_of_sale_id', 'total_week_pos']

    weekly_distribution = weekly_totals.merge(weekly_pos_totals, on=['week', 'point_of_sale_id'])
    weekly_distribution['porcentaje'] = (weekly_distribution['total_compra'] / weekly_distribution['total_week_pos']) * 100

    return weekly_distribution

def detect_monopolization_trend(weekly_distribution, threshold_increase=10):
    """
    Detecta POS donde una drogueria esta monopolizando las compras
    threshold_increase: incremento minimo en porcentaje entre primera y ultima semana
    """
    if weekly_distribution.empty:
        return pd.DataFrame()

    # Ordenar por semana
    weekly_distribution = weekly_distribution.sort_values('week')

    # Obtener primera y ultima semana para cada POS-vendor
    first_week = weekly_distribution.groupby(['point_of_sale_id', 'vendor_id']).first().reset_index()
    last_week = weekly_distribution.groupby(['point_of_sale_id', 'vendor_id']).last().reset_index()

    # Comparar porcentajes
    comparison = first_week[['point_of_sale_id', 'vendor_id']].copy()
    comparison['porcentaje_inicial'] = first_week['porcentaje'].values
    comparison['porcentaje_final'] = last_week['porcentaje'].values
    comparison['cambio_porcentual'] = comparison['porcentaje_final'] - comparison['porcentaje_inicial']

    # Filtrar POS donde hay monopolizacion (aumento significativo)
    monopolization = comparison[comparison['cambio_porcentual'] >= threshold_increase]

    # Agregar informacion adicional
    monopolization = monopolization.merge(
        last_week[['point_of_sale_id', 'vendor_id', 'porcentaje']],
        on=['point_of_sale_id', 'vendor_id'],
        how='left'
    )

    return monopolization.sort_values('cambio_porcentual', ascending=False)

def analyze_vendor_risk(weekly_distribution, pos_id):
    """
    Analiza el riesgo de dependencia de proveedores basado en criterios especificos:
    1. Alerta si en la ultima semana tiene solo 1 drogueria
    2. Alerta si paso de 3+ proveedores a 2 y la diferencia entre vendors es >50%
    """
    if weekly_distribution.empty:
        return None

    # Filtrar datos del POS
    pos_data = weekly_distribution[weekly_distribution['point_of_sale_id'] == pos_id].copy()

    if pos_data.empty:
        return None

    # Ordenar por semana
    pos_data = pos_data.sort_values('week')

    # Obtener primera y ultima semana
    first_week = pos_data['week'].min()
    last_week = pos_data['week'].max()

    # Datos de primera semana
    first_week_data = pos_data[pos_data['week'] == first_week]
    num_vendors_first = first_week_data['vendor_id'].nunique()

    # Datos de ultima semana
    last_week_data = pos_data[pos_data['week'] == last_week]
    num_vendors_last = last_week_data['vendor_id'].nunique()

    # Calcular porcentajes en ultima semana
    last_week_vendors = last_week_data[['vendor_id', 'porcentaje']].sort_values('porcentaje', ascending=False)

    alert_type = None
    alert_description = None
    risk_level = '🟢 BAJO'

    # CRITERIO 1: Solo 1 drogueria en ultima semana
    if num_vendors_last == 1:
        alert_type = 'MONOPOLIO'
        alert_description = f'El POS tiene dependencia total de 1 solo proveedor (Vendor {last_week_vendors.iloc[0]["vendor_id"]})'
        risk_level = '🔴 CRITICO'

    # CRITERIO 2: Paso de 3+ a 2 proveedores y diferencia >50%
    elif num_vendors_first >= 3 and num_vendors_last == 2:
        top_vendor_pct = last_week_vendors.iloc[0]['porcentaje']
        second_vendor_pct = last_week_vendors.iloc[1]['porcentaje']
        difference = top_vendor_pct - second_vendor_pct

        if difference > 50:
            alert_type = 'CONCENTRACION'
            alert_description = f'Reduccion de {num_vendors_first} a {num_vendors_last} proveedores con concentracion del {top_vendor_pct:.1f}% en Vendor {last_week_vendors.iloc[0]["vendor_id"]}'
            risk_level = '🟠 ALTO'

    # Sin alerta pero evaluar situacion
    elif num_vendors_last == 2:
        top_vendor_pct = last_week_vendors.iloc[0]['porcentaje']
        if top_vendor_pct > 70:
            alert_type = 'MONITOREAR'
            alert_description = f'{num_vendors_last} proveedores con alta concentracion ({top_vendor_pct:.1f}%) en Vendor {last_week_vendors.iloc[0]["vendor_id"]}'
            risk_level = '🟡 MODERADO'

    return {
        'pos_id': pos_id,
        'num_vendors_first': num_vendors_first,
        'num_vendors_last': num_vendors_last,
        'vendors_last_week': last_week_vendors,
        'alert_type': alert_type,
        'alert_description': alert_description,
        'risk_level': risk_level,
        'first_week': first_week,
        'last_week': last_week
    }

def create_pie_chart(pos_data, pos_id):
    """Crea un grafico de torta para un POS especifico"""
    fig = px.pie(
        pos_data,
        values='total_compra',
        names='vendor_id',
        title=f'Distribucion de Compras - POS {pos_id}',
        hover_data=['porcentaje'],
        labels={'vendor_id': 'Drogueria/Vendor', 'total_compra': 'Total Comprado'}
    )

    fig.update_traces(
        textposition='inside',
        textinfo='percent+label',
        hovertemplate='<b>Vendor %{label}</b><br>Total: $%{value:,.2f}<br>Porcentaje: %{customdata[0]:.2f}%<extra></extra>'
    )

    return fig

def create_weekly_evolution_chart(weekly_dist, selected_pos):
    """Crea un grafico de evolucion semanal para un POS"""
    pos_weekly = weekly_dist[weekly_dist['point_of_sale_id'] == selected_pos].copy()

    if pos_weekly.empty:
        return None

    # Ordenar por semana
    pos_weekly = pos_weekly.sort_values('week')

    fig = px.line(
        pos_weekly,
        x='week',
        y='porcentaje',
        color='vendor_id',
        title=f'Evolucion Semanal de Distribucion de Compras - POS {selected_pos}',
        labels={'week': 'Semana', 'porcentaje': 'Porcentaje de Compras (%)', 'vendor_id': 'Drogueria/Vendor'},
        markers=True
    )

    fig.update_layout(
        xaxis_title='Semana',
        yaxis_title='Porcentaje de Compras (%)',
        hovermode='x unified',
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.3,
            xanchor="center",
            x=0.5
        )
    )

    return fig

def create_overall_weekly_evolution(weekly_dist):
    """Crea visualizacion general de evolucion semanal para todos los POS"""
    if weekly_dist.empty:
        return None

    # Calcular el promedio de concentracion por semana
    # (que tan monopolizada esta la distribucion)
    concentration = weekly_dist.groupby(['week', 'point_of_sale_id'])['porcentaje'].max().reset_index()
    concentration_avg = concentration.groupby('week')['porcentaje'].agg(['mean', 'std']).reset_index()

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=concentration_avg['week'],
        y=concentration_avg['mean'],
        mode='lines+markers',
        name='Promedio de Concentracion',
        line=dict(color='blue', width=2),
        hovertemplate='Semana: %{x}<br>Promedio: %{y:.2f}%<extra></extra>'
    ))

    # Agregar banda de desviacion estandar
    fig.add_trace(go.Scatter(
        x=concentration_avg['week'],
        y=concentration_avg['mean'] + concentration_avg['std'],
        mode='lines',
        name='+ 1 Desv. Estandar',
        line=dict(width=0),
        showlegend=False,
        hoverinfo='skip'
    ))

    fig.add_trace(go.Scatter(
        x=concentration_avg['week'],
        y=concentration_avg['mean'] - concentration_avg['std'],
        mode='lines',
        name='- 1 Desv. Estandar',
        line=dict(width=0),
        fillcolor='rgba(68, 68, 68, 0.2)',
        fill='tonexty',
        showlegend=False,
        hoverinfo='skip'
    ))

    fig.update_layout(
        title='Evolucion de la Concentracion de Compras (Promedio General)',
        xaxis_title='Semana',
        yaxis_title='Porcentaje Maximo de Compra a una Drogueria (%)',
        hovermode='x unified'
    )

    return fig

# ============================================
# MAIN APP
# ============================================

st.title("📊 Dashboard de Comportamiento de Compras por Drogueria")
st.markdown("""
Este dashboard analiza el comportamiento de compras de cada POS (Point of Sale) a diferentes droguerias,
mostrando la distribucion actual, evolucion temporal y tendencias de monopolizacion.
""")

# Cargar datos
with st.spinner('Cargando datos...'):
    df_orders = load_data()

if df_orders.empty:
    st.error("No se pudieron cargar los datos. Verifica que el archivo 'data/orders_delivered_pos_vendor_geozone.csv' este disponible.")
    st.stop()

# Calcular metricas
pos_vendor_totals = calculate_pos_vendor_totals(df_orders)
weekly_distribution = calculate_weekly_distribution(df_orders)

# ============================================
# SELECTOR PRINCIPAL DE POS Y FILTROS
# ============================================
st.header("🎯 Seleccion de POS y Filtros")

col1, col2 = st.columns([2, 3])

with col1:
    # Selector principal de POS
    pos_list = sorted(pos_vendor_totals['point_of_sale_id'].unique())
    selected_pos = st.selectbox(
        "Selecciona un POS para analizar:",
        pos_list,
        key='main_pos_selector',
        help="Este POS sera analizado en todas las secciones del dashboard"
    )

with col2:
    # Filtro de fechas
    if 'order_date' in df_orders.columns and not df_orders['order_date'].isna().all():
        min_date = df_orders['order_date'].min().date()
        max_date = df_orders['order_date'].max().date()

        date_range = st.date_input(
            "Rango de fechas:",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
            key='date_range_filter',
            help="Selecciona el rango de fechas para filtrar los datos"
        )

        # Validar que se seleccionaron dos fechas
        if len(date_range) == 2:
            start_date, end_date = date_range
            # Filtrar df_orders por rango de fechas
            df_orders_filtered = df_orders[
                (df_orders['order_date'].dt.date >= start_date) &
                (df_orders['order_date'].dt.date <= end_date)
            ]
        else:
            df_orders_filtered = df_orders
            st.warning("Selecciona un rango completo (fecha inicio y fin)")
    else:
        st.info("No hay datos de fechas disponibles para filtrar")
        df_orders_filtered = df_orders

# Recalcular metricas con datos filtrados
pos_vendor_totals_filtered = calculate_pos_vendor_totals(df_orders_filtered)
weekly_distribution_filtered = calculate_weekly_distribution(df_orders_filtered)

st.markdown("---")

# ============================================
# SECCION 1: GRAFICOS DE TORTA POR POS
# ============================================
st.header(f"🥧 Distribucion de Compras por Drogueria - POS {selected_pos}")

# Obtener datos del POS seleccionado con filtro de fechas
pos_data = pos_vendor_totals_filtered[pos_vendor_totals_filtered['point_of_sale_id'] == selected_pos].copy()

if not pos_data.empty:
    pos_data = pos_data.sort_values('total_compra', ascending=False)

    col1, col2 = st.columns([1, 1])

    with col1:
        # Grafico de torta
        fig_pie = create_pie_chart(pos_data, selected_pos)
        st.plotly_chart(fig_pie, use_container_width=True)

    with col2:
        # Tabla de detalles
        st.subheader(f"Detalle de Compras")

        detail_table = pos_data[['vendor_id', 'total_compra', 'porcentaje']].copy()
        detail_table.columns = ['Drogueria/Vendor ID', 'Total Comprado', 'Porcentaje']
        detail_table['Porcentaje'] = detail_table['Porcentaje'].round(2)

        st.dataframe(
            detail_table.style.format({
                'Total Comprado': '${:,.2f}',
                'Porcentaje': '{:.2f}%'
            }),
            use_container_width=True,
            hide_index=True
        )

        # Metricas adicionales
        num_vendors = len(pos_data)
        concentration = pos_data.iloc[0]['porcentaje'] if len(pos_data) > 0 else 0
        total_compras_pos = pos_data['total_compra'].sum()

        st.markdown("**Metricas del POS:**")
        metric_col1, metric_col2, metric_col3 = st.columns(3)
        with metric_col1:
            st.metric("Droguerias", num_vendors)
        with metric_col2:
            st.metric("Concentracion", f"{concentration:.1f}%")
        with metric_col3:
            st.metric("Total", f"${total_compras_pos:,.0f}")
else:
    st.warning(f"No hay datos disponibles para el POS {selected_pos} en el rango de fechas seleccionado.")

st.markdown("---")

# ============================================
# SECCION 2: EVOLUCION SEMANAL
# ============================================
st.header(f"📅 Evolucion Semanal de la Distribucion de Compras - POS {selected_pos}")
st.caption("📊 Mostrando todo el periodo historico para mejor visualizacion de tendencias")

if not weekly_distribution.empty:
    # Evolucion por POS seleccionado usando TODOS los datos (sin filtro de fechas)
    fig_evolution = create_weekly_evolution_chart(weekly_distribution, selected_pos)
    if fig_evolution:
        st.plotly_chart(fig_evolution, use_container_width=True)

        st.info("""
        **💡 Interpretacion:** Este grafico muestra como evoluciona el porcentaje de compras a cada drogueria
        semana a semana en todo el periodo historico. Lineas ascendentes indican que el POS esta aumentando
        compras a esa drogueria, mientras que lineas descendentes indican reduccion.

        **Nota:** Esta visualizacion muestra el periodo completo (no afectado por el filtro de fechas)
        para facilitar la comparacion de tendencias a largo plazo.
        """)
    else:
        st.warning(f"No hay datos de evolucion semanal para el POS {selected_pos}.")
else:
    st.warning("No hay datos de fechas disponibles para mostrar la evolucion semanal.")

st.markdown("---")

# ============================================
# SECCION 3: ANALISIS DETALLADO DEL POS SELECCIONADO
# ============================================
st.header(f"🔍 Analisis Detallado - POS {selected_pos}")

risk_analysis = analyze_vendor_risk(weekly_distribution, selected_pos)

if risk_analysis:
    # Banner de alerta principal
    if risk_analysis['alert_type']:
        risk_level = risk_analysis['risk_level']
        risk_color = '#ffebee' if 'CRITICO' in risk_level else '#fff3e0' if 'ALTO' in risk_level else '#fffde7'
        border_color = '#d32f2f' if 'CRITICO' in risk_level else '#f57c00' if 'ALTO' in risk_level else '#fbc02d'

        st.markdown(f"""
        <div style="background-color: {risk_color}; padding: 20px; border-radius: 10px; border-left: 5px solid {border_color};">
            <h3 style="margin: 0; color: #333;">{risk_level} - {risk_analysis['alert_type']}</h3>
            <p style="margin: 10px 0 0 0; color: #555; font-size: 16px;">{risk_analysis['alert_description']}</p>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown(f"""
        <div style="background-color: #e8f5e9; padding: 20px; border-radius: 10px; border-left: 5px solid #388e3c;">
            <h3 style="margin: 0; color: #333;">🟢 BAJO - SIN ALERTA</h3>
            <p style="margin: 10px 0 0 0; color: #555; font-size: 16px;">El POS mantiene una diversificacion adecuada de proveedores</p>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("###")

    # Metricas comparativas
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            "Proveedores Inicial",
            risk_analysis['num_vendors_first'],
            help=f"Semana {risk_analysis['first_week']}"
        )
    with col2:
        st.metric(
            "Proveedores Final",
            risk_analysis['num_vendors_last'],
            delta=int(risk_analysis['num_vendors_last'] - risk_analysis['num_vendors_first']),
            delta_color="normal" if risk_analysis['num_vendors_last'] >= risk_analysis['num_vendors_first'] else "inverse",
            help=f"Semana {risk_analysis['last_week']}"
        )
    with col3:
        max_concentration = risk_analysis['vendors_last_week'].iloc[0]['porcentaje']
        st.metric(
            "Concentracion Max",
            f"{max_concentration:.1f}%",
            delta="Riesgo" if max_concentration > 70 else "Normal",
            delta_color="inverse" if max_concentration > 70 else "off",
            help="En ultima semana"
        )
    with col4:
        if len(risk_analysis['vendors_last_week']) >= 2:
            diff = risk_analysis['vendors_last_week'].iloc[0]['porcentaje'] - risk_analysis['vendors_last_week'].iloc[1]['porcentaje']
            st.metric(
                "Diferencia 1° vs 2°",
                f"{diff:.1f}%",
                delta="Alerta" if diff > 50 else "Normal",
                delta_color="inverse" if diff > 50 else "off",
                help="Diferencia entre top 2 proveedores"
            )
        else:
            st.metric(
                "Diferencia 1° vs 2°",
                "N/A",
                help="Solo hay 1 proveedor"
            )

    st.markdown("---")

    # Tabla de proveedores en ultima semana
    st.markdown(f"**📋 Distribucion de Proveedores en Ultima Semana ({risk_analysis['last_week']}):**")

    vendors_display = risk_analysis['vendors_last_week'].copy()
    vendors_display.columns = ['Drogueria ID', 'Porcentaje']

    st.dataframe(
        vendors_display.style.format({
            'Porcentaje': '{:.2f}%'
        }).background_gradient(subset=['Porcentaje'], cmap='Reds'),
        use_container_width=True,
        hide_index=True
    )

    # Gauge de concentracion
    st.markdown("---")
    st.markdown("**📊 Indicador de Concentracion Actual**")

    fig_gauge = go.Figure(go.Indicator(
        mode="gauge+number",
        value=max_concentration,
        domain={'x': [0, 1], 'y': [0, 1]},
        title={'text': "Concentracion Maxima (%)", 'font': {'size': 18}},
        number={'suffix': '%'},
        gauge={
            'axis': {'range': [None, 100], 'tickwidth': 1},
            'bar': {'color': "darkred" if max_concentration > 70 else "orange" if max_concentration > 50 else "gold"},
            'bgcolor': "white",
            'borderwidth': 2,
            'bordercolor': "gray",
            'steps': [
                {'range': [0, 50], 'color': '#c8e6c9'},
                {'range': [50, 70], 'color': '#fff9c4'},
                {'range': [70, 100], 'color': '#ffcdd2'}
            ],
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': 70
            }
        }
    ))
    fig_gauge.update_layout(height=300)
    st.plotly_chart(fig_gauge, use_container_width=True)

    # Recomendaciones
    if risk_analysis['alert_type'] == 'MONOPOLIO':
        st.error("""
        **⚠️ ACCION INMEDIATA REQUERIDA:**
        - Diversificar urgentemente - el POS depende 100% de un solo proveedor
        - Identificar proveedores alternativos inmediatamente
        - Establecer planes de contingencia
        """)
    elif risk_analysis['alert_type'] == 'CONCENTRACION':
        st.warning("""
        **⚠️ ACCION RECOMENDADA:**
        - El POS ha reducido sus proveedores y muestra alta concentracion
        - Considerar reactivar proveedores anteriores
        - Negociar condiciones con proveedores secundarios
        """)
    elif risk_analysis['alert_type'] == 'MONITOREAR':
        st.info("""
        **💡 MONITOREAR:**
        - Situacion estable pero con concentracion moderada
        - Revisar periodicamente la distribucion
        - Mantener relaciones con proveedores secundarios
        """)
    else:
        st.success("""
        **✅ SITUACION SALUDABLE:**
        - Buena diversificacion de proveedores
        - Continuar monitoreando tendencias
        """)

else:
    st.warning(f"No hay suficientes datos historicos para evaluar el riesgo del POS {selected_pos}.")

st.markdown("---")

# ============================================
# SECCION 4: ANALISIS DE RIESGO
# ============================================
st.header("⚠️ Analisis de Riesgo y Alertas - Todos los POS")
st.markdown("""
Esta seccion evalua el riesgo de dependencia basado en la evolucion temporal de todos los POS,
comparando el inicio y fin del periodo para detectar concentracion de proveedores.
""")

# Descripcion del analisis
with st.expander("📖 Criterios de Alerta", expanded=False):
    st.markdown("""
    **Sistema de Alertas Basado en Evolucion Temporal**

    Las alertas se activan segun estos criterios:

    **🔴 CRITICO - MONOPOLIO:**
    - El POS tiene solo **1 drogueria** en la ultima semana del periodo

    **🟠 ALTO - CONCENTRACION:**
    - El POS paso de **3+ proveedores a 2** proveedores
    - Y la diferencia entre el proveedor principal y el secundario es **mayor al 50%**

    **🟡 MODERADO - MONITOREAR:**
    - El POS tiene 2 proveedores con concentracion mayor al 70% en uno

    **🟢 BAJO - SIN RIESGO:**
    - Cualquier otra situacion indica diversificacion saludable
    """)

st.markdown("---")

# Analizar riesgo para TODOS los POS
all_pos_list = sorted(weekly_distribution['point_of_sale_id'].unique())
all_alerts = []

with st.spinner('Analizando riesgo en todos los POS...'):
    for pos_id in all_pos_list:
        risk_analysis = analyze_vendor_risk(weekly_distribution, pos_id)
        if risk_analysis:
            all_alerts.append(risk_analysis)

# Filtrar solo POS con alertas
pos_with_alerts = [alert for alert in all_alerts if alert['alert_type'] is not None]

# Metricas generales de alertas
st.subheader("📊 Resumen General de Alertas")
col1, col2, col3, col4 = st.columns(4)

total_pos_analyzed = len(all_alerts)
total_with_alerts = len(pos_with_alerts)
critico_count = len([a for a in pos_with_alerts if a['risk_level'] == '🔴 CRITICO'])
alto_count = len([a for a in pos_with_alerts if a['risk_level'] == '🟠 ALTO'])
moderado_count = len([a for a in pos_with_alerts if a['risk_level'] == '🟡 MODERADO'])

with col1:
    st.metric("Total POS Analizados", total_pos_analyzed)
with col2:
    st.metric(
        "🔴 Critico",
        critico_count,
        delta="Monopolio" if critico_count > 0 else None,
        delta_color="inverse" if critico_count > 0 else "off"
    )
with col3:
    st.metric(
        "🟠 Alto",
        alto_count,
        delta="Concentracion" if alto_count > 0 else None,
        delta_color="inverse" if alto_count > 0 else "off"
    )
with col4:
    st.metric(
        "🟡 Moderado",
        moderado_count,
        delta="Monitorear" if moderado_count > 0 else None,
        delta_color="off"
    )

st.markdown("---")

# Tabla de todas las alertas
if pos_with_alerts:
    st.subheader(f"🚨 Lista de POS con Alertas Detectadas ({len(pos_with_alerts)} POS)")

    # Crear DataFrame para mostrar
    alerts_data = []
    for alert in pos_with_alerts:
        max_concentration = alert['vendors_last_week'].iloc[0]['porcentaje']

        if len(alert['vendors_last_week']) >= 2:
            diff = alert['vendors_last_week'].iloc[0]['porcentaje'] - alert['vendors_last_week'].iloc[1]['porcentaje']
        else:
            diff = 100  # Si solo hay 1 proveedor, diferencia es 100%

        alerts_data.append({
            'POS ID': alert['pos_id'],
            'Nivel Riesgo': alert['risk_level'],
            'Tipo Alerta': alert['alert_type'],
            'Proveedores Inicial': alert['num_vendors_first'],
            'Proveedores Final': alert['num_vendors_last'],
            'Concentracion Max (%)': max_concentration,
            'Diferencia 1°-2° (%)': diff,
            'Descripcion': alert['alert_description']
        })

    alerts_df = pd.DataFrame(alerts_data)

    # Ordenar por nivel de riesgo (Critico primero)
    risk_order = {'🔴 CRITICO': 0, '🟠 ALTO': 1, '🟡 MODERADO': 2}
    alerts_df['risk_order'] = alerts_df['Nivel Riesgo'].map(risk_order)
    alerts_df = alerts_df.sort_values('risk_order').drop('risk_order', axis=1)

    # Mostrar tabla con formato
    st.dataframe(
        alerts_df.style.format({
            'Concentracion Max (%)': '{:.1f}%',
            'Diferencia 1°-2° (%)': '{:.1f}%'
        }).apply(lambda x: ['background-color: #ffebee' if v == '🔴 CRITICO'
                            else 'background-color: #fff3e0' if v == '🟠 ALTO'
                            else 'background-color: #fffde7' if v == '🟡 MODERADO'
                            else '' for v in x], subset=['Nivel Riesgo']),
        use_container_width=True,
        hide_index=True,
        height=400
    )

    # Opcion de exportar
    csv = alerts_df.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="📥 Descargar Alertas (CSV)",
        data=csv,
        file_name=f"alertas_riesgo_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv"
    )

else:
    st.success("✅ No se detectaron alertas en ninguno de los POS. Todos mantienen diversificacion saludable.")

st.markdown("**Dashboard creado con Streamlit y Plotly**")
