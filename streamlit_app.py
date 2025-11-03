#!/usr/bin/env python3
"""
Churn Alert Dashboard - Streamlit App
====================================

Aplicación web interactiva para ejecutar el análisis de riesgo de churn
y visualizar los resultados de forma gráfica en tiempo real.
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from datetime import datetime, timezone
import time

# Configuración base para todos los charts Plotly
PLOTLY_CONFIG = {"displaylogo": False, "responsive": True}

# Importar nuestro agente de análisis
from behavioral_alert_agent import ChurnAlertFlow, Config


def configure_page():
    """Configuración inicial de la página Streamlit"""
    st.set_page_config(
        page_title="Churn Alert Dashboard",
        page_icon="⚠️",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    st.title("⚠️ Churn Alert Dashboard")
    st.markdown("""
    **Sistema de Análisis de Riesgo de Churn** - Detección temprana de puntos de venta en riesgo
    """)


def create_sidebar():
    """Crear barra lateral con controles"""
    st.sidebar.header("🔧 Configuración")
    
    # Configuración de reportes
    st.sidebar.subheader("Reportes")
    html_name = st.sidebar.text_input("Nombre reporte HTML", "behavioral_alerts.html")
    md_name = st.sidebar.text_input("Nombre reporte Markdown", "behavioral_alerts.md")
    
    # Botón para ejecutar análisis
    st.sidebar.markdown("---")
    run_analysis = st.sidebar.button("🚀 Ejecutar Análisis", type="primary")
    
    return {
        "html_name": html_name,
        "md_name": md_name,
        "run_analysis": run_analysis
    }


def run_churn_analysis(config_params):
    """Ejecutar el análisis de churn"""
    config = Config(
        html_report_name=config_params["html_name"],
        markdown_report_name=config_params["md_name"]
    )
    
    with st.spinner("🔍 Ejecutando análisis de riesgo de churn..."):
        flow = ChurnAlertFlow(config)
        results = flow.run()
    
    return results


def create_risk_distribution_chart(assessments):
    """Crear gráfico de distribución de riesgos"""
    if not assessments:
        return None
    
    # Contar riesgos por nivel y crear etiquetas legibles
    risk_counts = {}
    level_labels = {
        "extreme": "💀 Extremo",
        "urgent": "🔴 Urgente", 
        "moderate": "🟡 Moderado",
        "low": "🟢 Bajo"
    }
    
    for assessment in assessments:
        level = assessment.risk_level  # Ya está en minúsculas
        label = level_labels.get(level, level)
        risk_counts[label] = risk_counts.get(label, 0) + 1
    
    # Colores para cada nivel de riesgo (mapeo con etiquetas legibles)
    colors = {
        "💀 Extremo": "#B71C1C",  # Rojo muy oscuro
        "🔴 Urgente": "#F44336",   # Rojo
        "🟡 Moderado": "#FF9800", # Naranja
        "🟢 Bajo": "#4CAF50"       # Verde
    }
    
    fig = px.pie(
        values=list(risk_counts.values()),
        names=list(risk_counts.keys()),
        title="📊 Distribución de Niveles de Riesgo",
        color=list(risk_counts.keys()),
        color_discrete_map=colors
    )
    
    fig.update_traces(textposition='inside', textinfo='percent+label')
    fig.update_layout(height=400)
    
    return fig


def create_risk_score_chart(assessments):
    """Crear gráfico de scores de riesgo por POS"""
    if not assessments:
        return None
    
    # Preparar datos
    pos_ids = [f"POS {a.point_of_sale_id}" for a in assessments]
    risk_scores = [a.risk_score for a in assessments]
    risk_levels = [a.risk_level for a in assessments]  # Ya están en minúsculas
    
    # Colores basados en nivel de riesgo (corregido para consistencia)
    color_map = {
        "extreme": "#B71C1C",  # Rojo muy oscuro
        "urgent": "#F44336",   # Rojo
        "moderate": "#FF9800", # Naranja
        "low": "#4CAF50"       # Verde
    }
    colors = [color_map.get(level, "#9E9E9E") for level in risk_levels]
    
    fig = go.Figure(data=[
        go.Bar(
            x=pos_ids,
            y=risk_scores,
            marker_color=colors,
            text=[f"{score:.2f}" for score in risk_scores],
            textposition='auto',
        )
    ])
    
    fig.update_layout(
        title="📈 Risk Score por Punto de Venta",
        xaxis_title="Punto de Venta",
        yaxis_title="Risk Score",
        height=400,
        yaxis=dict(range=[0, 1])
    )
    
    # Líneas de referencia para nuevos niveles
    fig.add_hline(y=1.0, line_dash="dash", line_color="#B71C1C", 
                  annotation_text="Urgencia Extrema (3 criterios)")
    fig.add_hline(y=0.75, line_dash="dash", line_color="#F44336", 
                  annotation_text="Urgente (2 criterios)")
    fig.add_hline(y=0.5, line_dash="dash", line_color="#FF9800", 
                  annotation_text="Moderado (1 criterio)")
    
    return fig


def create_delivery_rates_chart(assessments):
    """Crear gráfico de tasas de entrega"""
    # Filtrar solo los que tienen datos de delivery
    valid_data = []
    for assessment in assessments:
        data_points = assessment.data_points_used
        rate_4w = data_points.get("orders_delivery_rate_4w")
        rate_2w = data_points.get("orders_delivery_rate_2w")
        
        if rate_4w is not None or rate_2w is not None:
            valid_data.append({
                "pos_id": f"POS {assessment.point_of_sale_id}",
                "rate_4w": rate_4w or 0,
                "rate_2w": rate_2w or 0,
                "risk_level": assessment.risk_level.lower()
            })
    
    if not valid_data:
        return None
    
    df = pd.DataFrame(valid_data)
    
    fig = go.Figure()
    
    # Barras para 4 semanas
    fig.add_trace(go.Bar(
        name="4 Semanas",
        x=df["pos_id"],
        y=df["rate_4w"],
        marker_color="lightblue",
        opacity=0.7
    ))
    
    # Barras para 2 semanas
    fig.add_trace(go.Bar(
        name="2 Semanas",
        x=df["pos_id"],
        y=df["rate_2w"],
        marker_color="darkblue",
        opacity=0.8
    ))
    
    fig.update_layout(
        title="📦 Tasas de Entrega por Período",
        xaxis_title="Punto de Venta",
        yaxis_title="Porcentaje de Entrega (%)",
        height=400,
        barmode='group'
    )
    
    return fig


def create_time_saved_distribution_chart(assessments):
    """Crear gráfico de distribución de time_saved por número de proveedores"""
    if not assessments:
        return None
    
    # Preparar datos con mapeo de time_saved a número de proveedores
    time_saved_data = []
    vendor_mapping = {
        "minimum": {"vendors": 1, "coefficient": 0.5, "color": "#F44336"},
        "medium": {"vendors": 2, "coefficient": 0.75, "color": "#FF9800"}, 
        "high": {"vendors": 3, "coefficient": 0.83, "color": "#4CAF50"}
    }
    
    for assessment in assessments:
        data_points = assessment.data_points_used
        time_saved = data_points.get("time_saved", "minimum")
        
        # Obtener información del mapeo
        mapping_info = vendor_mapping.get(time_saved, vendor_mapping["minimum"])
        
        time_saved_data.append({
            "pos_id": f"POS {assessment.point_of_sale_id}",
            "time_saved": time_saved,
            "vendors": mapping_info["vendors"],
            "coefficient": mapping_info["coefficient"],
            "color": mapping_info["color"],
            "risk_score": assessment.risk_score,
            "risk_level": assessment.risk_level
        })
    
    # Contar distribución por tipo de time_saved
    time_saved_counts = {}
    for item in time_saved_data:
        ts = item["time_saved"]
        if ts not in time_saved_counts:
            time_saved_counts[ts] = []
        time_saved_counts[ts].append(item)
    
    # Crear gráfico de barras agrupadas
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=("Distribución por Número de Proveedores", "Coeficientes de Tiempo Ahorrado"),
        specs=[[{"type": "bar"}, {"type": "bar"}]]
    )
    
    # Gráfico 1: Distribución por número de proveedores
    categories = []
    counts = []
    colors = []
    hover_texts = []
    
    for time_saved, items in time_saved_counts.items():
        mapping_info = vendor_mapping[time_saved]
        vendor_count = mapping_info["vendors"]
        coefficient = mapping_info["coefficient"]
        
        categories.append(f"{vendor_count} Proveedor{'es' if vendor_count > 1 else ''}<br>({time_saved.title()})")
        counts.append(len(items))
        colors.append(mapping_info["color"])
        
        # Crear hover text con lista de POS
        pos_list = [item["pos_id"] for item in items[:10]]  # Máximo 10 para no saturar
        hover_text = f"Tiempo: {time_saved.title()}<br>Coeficiente: {coefficient}<br>POS ({len(items)}): {', '.join(pos_list)}"
        if len(items) > 10:
            hover_text += f"<br>... y {len(items) - 10} más"
        hover_texts.append(hover_text)
    
    fig.add_trace(
        go.Bar(
            x=categories,
            y=counts,
            marker_color=colors,
            name="Distribución",
            showlegend=False,
            hovertemplate="%{hovertext}<extra></extra>",
            hovertext=hover_texts
        ),
        row=1, col=1
    )
    
    # Gráfico 2: Coeficientes de tiempo ahorrado
    coefficients = [vendor_mapping[ts]["coefficient"] for ts in time_saved_counts.keys()]
    fig.add_trace(
        go.Bar(
            x=categories,
            y=coefficients,
            marker_color=colors,
            name="Coeficientes",
            showlegend=False,
            text=[f"{coeff:.2f}" for coeff in coefficients],
            textposition='auto'
        ),
        row=1, col=2
    )
    
    fig.update_layout(
        title="⏰ Análisis de Tiempo Ahorrado por Número de Proveedores",
        height=400,
        showlegend=False
    )
    
    # Agregar líneas de referencia en coeficientes
    fig.add_hline(y=0.5, line_dash="dash", line_color="#F44336", 
                  annotation_text="Mínimo (1 proveedor)", row=1, col=2)
    fig.add_hline(y=0.75, line_dash="dash", line_color="#FF9800", 
                  annotation_text="Medio (2 proveedores)", row=1, col=2)
    fig.add_hline(y=0.83, line_dash="dash", line_color="#4CAF50", 
                  annotation_text="Alto (3+ proveedores)", row=1, col=2)
    
    return fig


def create_platform_use_distribution_chart(assessments):
    """Crear gráfico de distribución de platform_use (criterio de riesgo)"""
    if not assessments:
        return None
    
    # Preparar datos con mapeo de platform_use
    platform_use_data = []
    usage_mapping = {
        "low": {"risk_factor": "🔴 Alto Riesgo", "color": "#F44336", "orders_week": "≤1 orden/semana"},
        "medium": {"risk_factor": "🟡 Riesgo Medio", "color": "#FF9800", "orders_week": "2-4 órdenes/semana"},
        "high": {"risk_factor": "🟢 Bajo Riesgo", "color": "#4CAF50", "orders_week": "5+ órdenes/semana"}
    }
    
    for assessment in assessments:
        data_points = assessment.data_points_used
        platform_use = data_points.get("platform_use", "medium")
        
        # Obtener información del mapeo
        mapping_info = usage_mapping.get(platform_use, usage_mapping["medium"])
        
        platform_use_data.append({
            "pos_id": f"POS {assessment.point_of_sale_id}",
            "platform_use": platform_use,
            "risk_factor": mapping_info["risk_factor"],
            "color": mapping_info["color"],
            "orders_week": mapping_info["orders_week"],
            "risk_score": assessment.risk_score,
            "risk_level": assessment.risk_level
        })
    
    # Contar distribución por tipo de platform_use
    platform_use_counts = {}
    for item in platform_use_data:
        pu = item["platform_use"]
        if pu not in platform_use_counts:
            platform_use_counts[pu] = []
        platform_use_counts[pu].append(item)
    
    # Crear gráfico de barras con información detallada
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=("Distribución de Uso de Plataforma", "Impacto en Risk Score"),
        specs=[[{"type": "bar"}, {"type": "box"}]]
    )
    
    # Gráfico 1: Distribución por nivel de uso
    categories = []
    counts = []
    colors = []
    hover_texts = []
    
    # Ordenar por nivel de riesgo (low primero porque es más crítico)
    order = ["low", "medium", "high"]
    
    for platform_use in order:
        if platform_use in platform_use_counts:
            items = platform_use_counts[platform_use]
            mapping_info = usage_mapping[platform_use]
            
            categories.append(f"{platform_use.title()}<br>({mapping_info['orders_week']})")
            counts.append(len(items))
            colors.append(mapping_info["color"])
            
            # Crear hover text con lista de POS
            pos_list = [item["pos_id"] for item in items[:10]]  # Máximo 10 para no saturar
            hover_text = f"Uso: {platform_use.title()}<br>Factor: {mapping_info['risk_factor']}<br>POS ({len(items)}): {', '.join(pos_list)}"
            if len(items) > 10:
                hover_text += f"<br>... y {len(items) - 10} más"
            hover_texts.append(hover_text)
    
    fig.add_trace(
        go.Bar(
            x=categories,
            y=counts,
            marker_color=colors,
            name="Distribución",
            showlegend=False,
            hovertemplate="%{hovertext}<extra></extra>",
            hovertext=hover_texts
        ),
        row=1, col=1
    )
    
    # Gráfico 2: Box plot de Risk Score por platform_use
    for platform_use in order:
        if platform_use in platform_use_counts:
            items = platform_use_counts[platform_use]
            mapping_info = usage_mapping[platform_use]
            risk_scores = [item["risk_score"] for item in items]
            
            fig.add_trace(
                go.Box(
                    y=risk_scores,
                    name=f"{platform_use.title()}",
                    marker_color=mapping_info["color"],
                    showlegend=False
                ),
                row=1, col=2
            )
    
    fig.update_layout(
        title="📱 Análisis de Uso de Plataforma (Criterio de Riesgo)",
        height=400,
        showlegend=False
    )
    
    # Agregar líneas de referencia en risk scores
    fig.add_hline(y=1.0, line_dash="dash", line_color="#B71C1C", 
                  annotation_text="Extremo (≥1.0)", row=1, col=2)
    fig.add_hline(y=0.75, line_dash="dash", line_color="#F44336", 
                  annotation_text="Urgente (≥0.75)", row=1, col=2)
    fig.add_hline(y=0.5, line_dash="dash", line_color="#FF9800", 
                  annotation_text="Moderado (≥0.5)", row=1, col=2)
    
    return fig


def display_alerts_table(assessments):
    """Mostrar tabla detallada de alertas"""
    if not assessments:
        st.info("No se detectaron alertas de riesgo.")
        return
    
    # Preparar datos para la tabla
    table_data = []
    for assessment in assessments:
        data_points = assessment.data_points_used
        
        table_data.append({
            "POS ID": assessment.point_of_sale_id,
            "Nivel de Riesgo": assessment.risk_level.lower(),
            "Score": f"{assessment.risk_score:.2f}",
            "Confianza": f"{assessment.confidence:.2f}",
            "Platform Use": data_points.get('platform_use', 'N/A').title(),
            "Time Saved": data_points.get('time_saved', 'N/A').title(),
            "Ahorros Diarios": f"${data_points.get('average_daily_savings', 0):.2f}",
            "Tendencia": data_points.get('purchase_trend', 'N/A').title(),
            "Entrega 4w": f"{data_points.get('orders_delivery_rate_4w', 0):.1f}%" 
                          if data_points.get('orders_delivery_rate_4w') is not None else "N/A",
            "Entrega 2w": f"{data_points.get('orders_delivery_rate_2w', 0):.1f}%" 
                          if data_points.get('orders_delivery_rate_2w') is not None else "N/A",
            "Acción Recomendada": assessment.recommended_action
        })
    
    df = pd.DataFrame(table_data)
    
    # Aplicar estilos basados en el nivel de riesgo (corregido para nuevos niveles)
    def style_risk_level(val):
        if val == "extreme":
            return "background-color: #ffebee; color: #b71c1c"
        elif val == "urgent":
            return "background-color: #ffebee; color: #f44336"
        elif val == "moderate":
            return "background-color: #fff3e0; color: #ef6c00"
        else:
            return "background-color: #e8f5e8; color: #2e7d32"
    
    styled_df = df.style.map(style_risk_level, subset=['Nivel de Riesgo'])

    st.subheader("📋 Detalle de Alertas")
    st.dataframe(styled_df, width="stretch")


def display_summary_metrics(results):
    """Mostrar métricas resumen"""
    assessments = results.get("assessments", [])
    alerts_count = results.get("alerts_count", 0)
    
    # Contar por nivel de riesgo (nuevos niveles) - ya están en minúsculas
    extreme_risk = sum(1 for a in assessments if a.risk_level == "extreme")
    urgent_risk = sum(1 for a in assessments if a.risk_level == "urgent")
    moderate_risk = sum(1 for a in assessments if a.risk_level == "moderate")
    low_risk = sum(1 for a in assessments if a.risk_level == "low")
    
    # Mostrar métricas basadas en criterios cumplidos
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("🚨 Total Alertas", alerts_count)
    
    with col2:
        st.metric("💀 3 Criterios (Extremo)", extreme_risk, 
                 delta=f"{extreme_risk/max(alerts_count,1)*100:.1f}%" if extreme_risk > 0 else "Sin casos")
    
    with col3:
        st.metric("🔴 2 Criterios (Urgente)", urgent_risk, 
                 delta=f"{urgent_risk/max(alerts_count,1)*100:.1f}%" if urgent_risk > 0 else "Sin casos")
    
    with col4:
        st.metric("🟡 1 Criterio (Moderado)", moderate_risk, 
                 delta=f"{moderate_risk/max(alerts_count,1)*100:.1f}%" if moderate_risk > 0 else "Sin casos")
    
    # Desglose detallado de criterios
    display_criteria_breakdown(assessments)


def display_criteria_breakdown(assessments):
    """Mostrar desglose detallado de criterios cumplidos"""
    if not assessments:
        return
    
    # Tabla resumen de combinaciones de criterios
    with st.expander("🔍 Ver detalle de combinaciones de criterios"):
        criteria_combinations = {}
        
        for assessment in assessments:
            reasons = assessment.data_points_used.get("reasons", [])
            combo_key = []
            
            if "Tiempo de ahorro mínimo (opera con 1 proveedor)" in reasons:
                combo_key.append("⏰ Tiempo")
            if "Bajo uso de plataforma" in reasons:
                combo_key.append("📱 Platform")
            if "Tendencia de compra riesgosa (inactive/risky)" in reasons:
                combo_key.append("📉 Tendencia")
            
            combo_str = " + ".join(combo_key) if combo_key else "Sin criterios"
            
            if combo_str not in criteria_combinations:
                criteria_combinations[combo_str] = []
            criteria_combinations[combo_str].append(assessment.point_of_sale_id)
        
        st.markdown("**Combinaciones de criterios encontradas:**")
        for combo, pos_list in sorted(criteria_combinations.items(), key=lambda x: len(x[1]), reverse=True):
            st.write(f"- **{combo}** ({len(pos_list)} POS): {', '.join(map(str, pos_list[:10]))}")
            if len(pos_list) > 10:
                st.write(f"  ... y {len(pos_list) - 10} POS más")


def display_owner_summary_metrics(owner_assessments, all_assessments=None):
    """Mostrar métricas resumen agrupadas por owner"""
    if not owner_assessments:
        return
    
    total_owners = len(owner_assessments)
    total_pos = sum(owner.pos_count for owner in owner_assessments)
    
    # Calcular POS sin owner si se proporciona el total
    pos_sin_owner = 0
    if all_assessments:
        total_all_pos = len(all_assessments)
        pos_sin_owner = total_all_pos - total_pos
    
    # Contar owners por presencia de POS críticos (no por score promedio)
    high_risk_owners = sum(1 for owner in owner_assessments if len(owner.high_risk_pos) > 0)
    moderate_risk_owners = sum(1 for owner in owner_assessments if len(owner.high_risk_pos) == 0 and len(owner.moderate_risk_pos) > 0)
    low_risk_owners = sum(1 for owner in owner_assessments if len(owner.high_risk_pos) == 0 and len(owner.moderate_risk_pos) == 0)
    
    # Total POS por nivel de riesgo
    total_high_risk_pos = sum(len(owner.high_risk_pos) for owner in owner_assessments)
    total_moderate_risk_pos = sum(len(owner.moderate_risk_pos) for owner in owner_assessments)
    total_low_risk_pos = sum(len(owner.low_risk_pos) for owner in owner_assessments)
    
    # Mostrar métricas en columnas
    if pos_sin_owner > 0:
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.metric("👥 Total Owners", total_owners)
        with col2:
            st.metric("🏪 Total POS", total_pos)
        with col3:
            st.metric("❓ POS Sin Owner", pos_sin_owner)
        with col4:
            st.metric("🔴 Owners c/ POS Alto Riesgo", high_risk_owners)
        with col5:
            st.metric("🟡 Owners c/ POS Riesgo Moderado", moderate_risk_owners)
    else:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("👥 Total Owners", total_owners)
        with col2:
            st.metric("🏪 Total POS", total_pos)
        with col3:
            st.metric("🔴 Owners c/ POS Alto Riesgo", high_risk_owners)
        with col4:
            st.metric("🟡 Owners c/ POS Riesgo Moderado", moderate_risk_owners)
    
    # Validación de consistencia
    total_categorized_owners = high_risk_owners + moderate_risk_owners + low_risk_owners
    if total_categorized_owners != total_owners:
        st.error(f"⚠️ Inconsistencia detectada: Total owners {total_owners} ≠ Categorizados {total_categorized_owners}")
    
    # Explicación de las métricas
    with st.expander("ℹ️ Explicación de Métricas de Owners"):
        st.markdown("""
        **📊 Distribución de Owners según el riesgo de sus POS:**
        
        - **🔴 Owners c/ POS Alto Riesgo**: Owners que tienen al menos 1 POS con alto riesgo de churn
        - **🟡 Owners c/ POS Riesgo Moderado**: Owners que tienen POS de riesgo moderado pero ninguno de alto riesgo  
        - **🟢 Owners Solo POS Bajo Riesgo**: Owners donde todos sus POS tienen bajo riesgo
        - **📊 Total POS Alto Riesgo**: Cantidad total de POS individuales clasificados como alto riesgo
        
        **Nota:** Un owner puede aparecer en "Alto Riesgo" aunque tenga múltiples POS, solo necesita tener 1 POS crítico.
        """)
    
    # Debug: Mostrar información de consistencia
    if st.checkbox("🔍 Mostrar información de debug"):
        st.write("**Información de Debug:**")
        st.write(f"- Total owners: {total_owners} | Categorizados: {total_categorized_owners}")
        st.write(f"- Alto riesgo: {high_risk_owners} + Moderado: {moderate_risk_owners} + Bajo: {low_risk_owners}")
        st.write(f"- Total POS: {total_pos}")
        st.write(f"- POS Alto riesgo: {total_high_risk_pos} | Moderado: {total_moderate_risk_pos} | Bajo: {total_low_risk_pos}")
        
        for owner in owner_assessments[:3]:  # Solo primeros 3 para no saturar
            st.write(f"- Owner {owner.owner_id}: {owner.pos_count} POS")
            st.write(f"  Alto: {len(owner.high_risk_pos)} | Moderado: {len(owner.moderate_risk_pos)} | Bajo: {len(owner.low_risk_pos)}")
            assessments_count = len(owner.individual_assessments) if hasattr(owner, 'individual_assessments') and owner.individual_assessments else 0
            st.write(f"  Assessments individuales: {assessments_count}")


def create_owner_individual_charts(owner_assessments):
    """Crear charts individuales para cada owner mostrando sus POS"""
    if not owner_assessments:
        st.info("No se detectaron owners con alertas de riesgo.")
        return
    
    st.subheader("📊 Charts Individuales por Owner")
    
    for owner in owner_assessments:
        # Determinar color del owner basado en riesgo promedio (nuevos umbrales)
        if owner.avg_risk_score >= 1.0:
            owner_color = "#B71C1C"
            risk_label = "💀 Urgencia Extrema"
        elif owner.avg_risk_score >= 0.75:
            owner_color = "#F44336"
            risk_label = "🔴 Urgente"
        elif owner.avg_risk_score >= 0.5:
            owner_color = "#FF9800" 
            risk_label = "🟡 Moderado"
        else:
            owner_color = "#4CAF50"
            risk_label = "🟢 Bajo Riesgo"
        
        # Crear expander para cada owner
        high_risk_count = len(owner.high_risk_pos)
        moderate_risk_count = len(owner.moderate_risk_pos)
        
        # Agregar contadores en el título
        alert_info = ""
        if high_risk_count > 0:
            alert_info += f" | 🔴 {high_risk_count} ALERTAS ALTO RIESGO"
        if moderate_risk_count > 0:
            alert_info += f" | 🟡 {moderate_risk_count} ALERTAS MODERADAS"
        if high_risk_count == 0 and moderate_risk_count == 0:
            alert_info += " | ✅ SIN ALERTAS CRÍTICAS"
            
        with st.expander(f"🧑‍💼 {owner.owner_name} - {risk_label} (Score: {owner.avg_risk_score:.2f}){alert_info}"):
            # Métricas del owner en columnas
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("🏪 Total POS", owner.pos_count)
            with col2:
                st.metric("🔴 Alto Riesgo", high_risk_count, 
                         delta=f"POS: {owner.high_risk_pos}" if high_risk_count > 0 else "Sin POS críticos")
            with col3:
                st.metric("🟡 Moderado", moderate_risk_count,
                         delta=f"POS: {owner.moderate_risk_pos}" if moderate_risk_count > 0 else "Sin POS moderados")
            with col4:
                st.metric("🟢 Bajo Riesgo", len(owner.low_risk_pos))
            
            # Preparar datos de los POS del owner
            pos_data = []
            if hasattr(owner, 'individual_assessments') and owner.individual_assessments:
                for assessment in owner.individual_assessments:
                    try:
                        data_points = assessment.data_points_used if hasattr(assessment, 'data_points_used') else {}
                        pos_data.append({
                            "pos_id": assessment.point_of_sale_id,
                            "risk_score": assessment.risk_score,
                            "risk_level": assessment.risk_level,
                            "avg_daily_savings": data_points.get("average_daily_savings", 0),
                            "orders_4w": data_points.get("orders_delivery_rate_4w", 0) or 0,
                            "purchase_trend": data_points.get("purchase_trend", "N/A"),
                            "time_saved": data_points.get("time_saved", "minimum"),
                            "platform_use": data_points.get("platform_use", "medium"),
                            "reasons": data_points.get("reasons", [])
                        })
                    except Exception as e:
                        st.error(f"Error procesando POS {assessment.point_of_sale_id}: {str(e)}")
                        continue
            
            if pos_data:
                # Chart principal con múltiples métricas
                fig = make_subplots(
                    rows=2, cols=3,
                    subplot_titles=(
                        f"Risk Score por POS ({owner.owner_name})",
                        f"Ahorros Diarios por POS",
                        f"Platform Use por POS",
                        f"Tendencia de Compra por POS", 
                        f"Time Saved (Proveedores)",
                        f"Risk vs Platform Use"
                    ),
                    specs=[[{"type": "bar"}, {"type": "bar"}, {"type": "bar"}],
                           [{"type": "bar"}, {"type": "bar"}, {"type": "scatter"}]]
                )
                
                pos_ids = [f"POS {pos['pos_id']}" for pos in pos_data]
                risk_scores = [pos["risk_score"] for pos in pos_data]
                savings = [pos["avg_daily_savings"] for pos in pos_data]
                trends = [pos["purchase_trend"] for pos in pos_data]
                time_saved_values = [pos["time_saved"] for pos in pos_data]
                platform_use_values = [pos["platform_use"] for pos in pos_data]
                
                # Colores basados en nivel de riesgo individual (nuevos niveles)
                # Mapa de colores corregido
                risk_color_map = {
                    "extreme": "#B71C1C",  # Rojo muy oscuro
                    "urgent": "#F44336",   # Rojo
                    "moderate": "#FF9800", # Naranja
                    "low": "#4CAF50"       # Verde
                }
                
                colors = []
                for pos in pos_data:
                    risk_level = pos["risk_level"]  # Ya está en minúsculas
                    color = risk_color_map.get(risk_level, "#9E9E9E")  # Gris por defecto
                    colors.append(color)
                
                # Risk Score
                fig.add_trace(
                    go.Bar(x=pos_ids, y=risk_scores, name="Risk Score",
                           marker_color=colors, showlegend=False),
                    row=1, col=1
                )
                
                # Ahorros Diarios
                fig.add_trace(
                    go.Bar(x=pos_ids, y=savings, name="Ahorros ($)",
                           marker_color="#2196F3", showlegend=False),
                    row=1, col=2
                )
                
                # Platform Use
                platform_use_mapping = {
                    "low": {"color": "#F44336", "order": 1},
                    "medium": {"color": "#FF9800", "order": 2},
                    "high": {"color": "#4CAF50", "order": 3}
                }
                
                platform_use_orders = [platform_use_mapping.get(pu, platform_use_mapping["medium"])["order"] for pu in platform_use_values]
                platform_use_colors = [platform_use_mapping.get(pu, platform_use_mapping["medium"])["color"] for pu in platform_use_values]
                
                fig.add_trace(
                    go.Bar(x=pos_ids, y=platform_use_orders, name="Platform Use",
                           marker_color=platform_use_colors, showlegend=False,
                           text=[f"{pu.title()}" for pu in platform_use_values],
                           textposition='auto'),
                    row=1, col=3
                )
                
                # Purchase Trend - Contar risky e inactive y mostrar POS
                risky_pos = [pos_ids[i] for i, trend in enumerate(trends) if trend == "risky"]
                inactive_pos = [pos_ids[i] for i, trend in enumerate(trends) if trend == "inactive"]
                stable_pos = [pos_ids[i] for i, trend in enumerate(trends) if trend not in ["risky", "inactive"]]
                
                risky_count = len(risky_pos)
                inactive_count = len(inactive_pos)
                stable_count = len(stable_pos)
                
                trend_categories = ["Risky", "Inactive", "Stable"]
                trend_counts = [risky_count, inactive_count, stable_count]
                trend_colors = ["#F44336", "#FF9800", "#4CAF50"]
                
                # Crear hover text con los POS
                hover_text = [
                    f"Risky ({risky_count})<br>POS: {', '.join(risky_pos) if risky_pos else 'Ninguno'}",
                    f"Inactive ({inactive_count})<br>POS: {', '.join(inactive_pos) if inactive_pos else 'Ninguno'}",
                    f"Stable ({stable_count})<br>POS: {', '.join(stable_pos) if stable_pos else 'Ninguno'}"
                ]
                
                fig.add_trace(
                    go.Bar(x=trend_categories, y=trend_counts, name="Tendencia",
                           marker_color=trend_colors, showlegend=False,
                           hovertemplate="%{hovertext}<extra></extra>",
                           hovertext=hover_text),
                    row=2, col=1
                )
                
                # Gráfico de Time Saved (convertir a número de proveedores)
                vendor_mapping = {
                    "minimum": {"vendors": 1, "color": "#F44336"},
                    "medium": {"vendors": 2, "color": "#FF9800"}, 
                    "high": {"vendors": 3, "color": "#4CAF50"}
                }
                
                vendor_counts = [vendor_mapping.get(ts, vendor_mapping["minimum"])["vendors"] for ts in time_saved_values]
                time_saved_colors = [vendor_mapping.get(ts, vendor_mapping["minimum"])["color"] for ts in time_saved_values]
                
                fig.add_trace(
                    go.Bar(x=pos_ids, y=vendor_counts, name="Proveedores",
                           marker_color=time_saved_colors, showlegend=False,
                           text=[f"{ts.title()}<br>{vc}P" for ts, vc in zip(time_saved_values, vendor_counts)],
                           textposition='auto'),
                    row=2, col=2
                )
                
                # Scatter plot Risk vs Platform Use
                platform_use_numeric = [platform_use_mapping.get(pu, platform_use_mapping["medium"])["order"] for pu in platform_use_values]
                fig.add_trace(
                    go.Scatter(x=platform_use_numeric, y=risk_scores, mode='markers+text',
                               text=pos_ids, textposition="top center",
                               marker=dict(size=10, color=platform_use_colors),
                               name="Risk vs Platform", showlegend=False),
                    row=2, col=3
                )
                
                # Título con información de alertas
                alert_summary = ""
                if high_risk_count > 0:
                    alert_summary += f"🔴 {high_risk_count} ALTO RIESGO "
                if moderate_risk_count > 0:
                    alert_summary += f"🟡 {moderate_risk_count} MODERADO "
                if alert_summary:
                    alert_summary = f"ALERTAS: {alert_summary.strip()}"
                else:
                    alert_summary = "✅ SIN ALERTAS CRÍTICAS"
                
                fig.update_layout(
                    title=f"Análisis Detallado - {owner.owner_name} ({owner.pos_count} POS) | {alert_summary}",
                    height=500,
                    showlegend=False
                )
                
                # Agregar líneas de referencia para nuevos niveles de riesgo
                fig.add_hline(y=1.0, line_dash="dash", line_color="#B71C1C", 
                              annotation_text="Urgencia Extrema (≥1.0)", row=1, col=1)
                fig.add_hline(y=0.75, line_dash="dash", line_color="#F44336", 
                              annotation_text="Urgente (≥0.75)", row=1, col=1)
                fig.add_hline(y=0.5, line_dash="dash", line_color="#FF9800", 
                              annotation_text="Moderado (≥0.5)", row=1, col=1)
                
                # Mostrar el chart con key único y ayuda
                col_chart, col_help = st.columns([10, 1])
                with col_chart:
                    st.plotly_chart(fig, key=f"owner_chart_{owner.owner_id}", config=PLOTLY_CONFIG)
                with col_help:
                    st.markdown("❓", help=f"Dashboard completo del owner {owner.owner_name}: Risk Score, Ahorros, Platform Use, Tendencias, Time Saved y Risk vs Platform Use de todos sus POS")
                
                # Resumen y acción recomendada con contadores
                st.info(f"**Resumen:** {owner.summary}")
                
                # Debug info para verificar consistencia
                total_assessments = len(owner.individual_assessments) if hasattr(owner, 'individual_assessments') and owner.individual_assessments else 0
                if total_assessments != owner.pos_count:
                    st.warning(f"⚠️ **Advertencia:** Inconsistencia detectada - Total POS: {owner.pos_count}, Assessments: {total_assessments}")
                
                if high_risk_count > 0:
                    st.error(f"🚨 **ACCIÓN URGENTE:** {owner.recommended_action}")
                    st.error(f"📍 **POS Críticos ({high_risk_count}):** {', '.join(map(str, owner.high_risk_pos))}")
                elif moderate_risk_count > 0:
                    st.warning(f"⚠️ **ACCIÓN RECOMENDADA:** {owner.recommended_action}")
                    st.warning(f"📍 **POS en Seguimiento ({moderate_risk_count}):** {', '.join(map(str, owner.moderate_risk_pos))}")
                else:
                    st.success(f"✅ **MONITOREO RUTINARIO:** {owner.recommended_action}")
                    st.success(f"👍 **Todos los POS ({len(owner.low_risk_pos)}) en estado estable**")
            
            else:
                st.warning("No hay datos detallados disponibles para este owner.")
            
            st.markdown("---")


def create_owner_risk_distribution_chart(owner_assessments):
    """Crear gráfico de distribución de riesgos por owner"""
    if not owner_assessments:
        return None
    
    # Contar owners por nivel de riesgo (nuevos umbrales)
    risk_counts = {"Extremo": 0, "Urgente": 0, "Moderado": 0, "Bajo": 0}
    for owner in owner_assessments:
        if owner.avg_risk_score >= 1.0:
            risk_counts["Extremo"] += 1
        elif owner.avg_risk_score >= 0.75:
            risk_counts["Urgente"] += 1
        elif owner.avg_risk_score >= 0.5:
            risk_counts["Moderado"] += 1
        else:
            risk_counts["Bajo"] += 1
    
    # Colores para cada nivel de riesgo (consistente con el resto del sistema)
    colors = {
        "Extremo": "#B71C1C",  # Rojo muy oscuro
        "Urgente": "#F44336",  # Rojo
        "Moderado": "#FF9800", # Naranja
        "Bajo": "#4CAF50"      # Verde
    }
    
    fig = px.pie(
        values=list(risk_counts.values()),
        names=list(risk_counts.keys()),
        title="📊 Owners según Riesgo Promedio de sus POS",
        color=list(risk_counts.keys()),
        color_discrete_map=colors
    )
    
    fig.update_traces(textposition='inside', textinfo='percent+label')
    fig.update_layout(height=400)
    
    return fig


def create_owner_pos_distribution_chart(owner_assessments):
    """Crear gráfico de barras de POS por owner"""
    if not owner_assessments:
        return None
    
    # Preparar datos
    owner_names = [f"{owner.owner_name}" for owner in owner_assessments[:10]]  # Top 10
    high_risk_counts = [len(owner.high_risk_pos) for owner in owner_assessments[:10]]
    moderate_risk_counts = [len(owner.moderate_risk_pos) for owner in owner_assessments[:10]]
    low_risk_counts = [len(owner.low_risk_pos) for owner in owner_assessments[:10]]
    
    fig = go.Figure()
    
    # Barras apiladas
    fig.add_trace(go.Bar(name="Alto Riesgo", x=owner_names, y=high_risk_counts, marker_color="#F44336"))
    fig.add_trace(go.Bar(name="Riesgo Moderado", x=owner_names, y=moderate_risk_counts, marker_color="#FF9800"))
    fig.add_trace(go.Bar(name="Bajo Riesgo", x=owner_names, y=low_risk_counts, marker_color="#4CAF50"))
    
    fig.update_layout(
        title="📈 Distribución de POS por Owner (Top 10)",
        xaxis_title="Owner",
        yaxis_title="Número de POS",
        height=400,
        barmode='stack'
    )
    
    return fig


def create_critical_pos_infographic(assessments):
    """Crear infografía para POS críticos (low platform_use + time_saved = minimum)"""
    if not assessments:
        return None
    
    # Filtrar POS críticos
    critical_pos = []
    for assessment in assessments:
        data_points = assessment.data_points_used
        reasons = data_points.get("reasons", [])
        
        # Verificar si tiene tanto bajo uso como tiempo mínimo
        has_low_platform = "Bajo uso de plataforma" in " ".join(reasons)
        has_min_time = "Tiempo de ahorro mínimo" in " ".join(reasons)
        
        if has_low_platform and has_min_time:
            critical_pos.append({
                "pos_id": assessment.point_of_sale_id,
                "risk_score": assessment.risk_score,
                "avg_daily_savings": data_points.get("average_daily_savings", 0),
                "purchase_trend": data_points.get("purchase_trend", "N/A"),
                "time_saved": data_points.get("time_saved", "minimum"),
                "platform_use": data_points.get("platform_use", "medium"),
                "reasons": reasons
            })
    
    if not critical_pos:
        return None
    
    # Preparar datos para la infografía
    pos_ids = [f"POS {pos['pos_id']}" for pos in critical_pos]
    risk_scores = [pos["risk_score"] for pos in critical_pos]
    savings = [pos["avg_daily_savings"] for pos in critical_pos]
    trends = [pos["purchase_trend"] for pos in critical_pos]
    time_saved_values = [pos["time_saved"] for pos in critical_pos]
    
    # Crear subplot con solo gráficos de barras
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=("Risk Score", "Ahorros Diarios ($)", "Tendencia de Compra", "Time Saved (Proveedores)"),
        specs=[[{"type": "bar"}, {"type": "bar"}],
               [{"type": "bar"}, {"type": "bar"}]]
    )
    
    # Risk Score
    fig.add_trace(
        go.Bar(x=pos_ids, y=risk_scores, name="Risk Score", 
               marker_color="#F44336", showlegend=False),
        row=1, col=1
    )
    
    # Ahorros Diarios
    fig.add_trace(
        go.Bar(x=pos_ids, y=savings, name="Ahorros ($)", 
               marker_color="#FF9800", showlegend=False),
        row=1, col=2
    )
    
    # Purchase Trend - Contar risky e inactive y mostrar POS
    risky_pos = [pos_ids[i] for i, trend in enumerate(trends) if trend == "risky"]
    inactive_pos = [pos_ids[i] for i, trend in enumerate(trends) if trend == "inactive"]
    stable_pos = [pos_ids[i] for i, trend in enumerate(trends) if trend not in ["risky", "inactive"]]
    
    risky_count = len(risky_pos)
    inactive_count = len(inactive_pos)
    stable_count = len(stable_pos)
    
    trend_categories = ["Risky", "Inactive", "Stable"]
    trend_counts = [risky_count, inactive_count, stable_count]
    trend_colors = ["#F44336", "#FF9800", "#4CAF50"]
    
    # Crear hover text con los POS
    hover_text = [
        f"Risky ({risky_count})<br>POS: {', '.join(risky_pos) if risky_pos else 'Ninguno'}",
        f"Inactive ({inactive_count})<br>POS: {', '.join(inactive_pos) if inactive_pos else 'Ninguno'}",
        f"Stable ({stable_count})<br>POS: {', '.join(stable_pos) if stable_pos else 'Ninguno'}"
    ]
    
    fig.add_trace(
        go.Bar(x=trend_categories, y=trend_counts, name="Tendencia", 
               marker_color=trend_colors, showlegend=False,
               hovertemplate="%{hovertext}<extra></extra>",
               hovertext=hover_text),
        row=2, col=1
    )
    
    # Gráfico de Time Saved (convertir a número de proveedores)
    vendor_mapping = {
        "minimum": {"vendors": 1, "color": "#F44336"},
        "medium": {"vendors": 2, "color": "#FF9800"}, 
        "high": {"vendors": 3, "color": "#4CAF50"}
    }
    
    vendor_counts = [vendor_mapping.get(ts, vendor_mapping["minimum"])["vendors"] for ts in time_saved_values]
    time_saved_colors = [vendor_mapping.get(ts, vendor_mapping["minimum"])["color"] for ts in time_saved_values]
    
    fig.add_trace(
        go.Bar(x=pos_ids, y=vendor_counts, name="Proveedores",
               marker_color=time_saved_colors, showlegend=False,
               text=[f"{ts.title()}<br>{vc}P" for ts, vc in zip(time_saved_values, vendor_counts)],
               textposition='auto'),
        row=2, col=2
    )
    
    fig.update_layout(
        title=f"🚨 INFOGRAFÍA POS CRÍTICOS - {len(critical_pos)} Farmacias en Riesgo Extremo",
        height=600,
        showlegend=False
    )
    
    return fig


def main():
    """Función principal de la aplicación"""
    configure_page()
    
    # Crear sidebar
    config_params = create_sidebar()
    
    # Toggle para vista por owner
    st.sidebar.markdown("---")
    view_mode = st.sidebar.radio(
        "📊 Modo de Vista",
        ["Por POS Individual", "Por Owner (Agrupado)"],
        index=1  # Default a vista por owner
    )
    
    # Información sobre el sistema
    with st.expander("ℹ️ Información del Sistema"):
        st.markdown("""
        **Churn Alert Dashboard** utiliza análisis cuantitativo para identificar puntos de venta en riesgo:
        
        ### 📊 Nuevos Niveles de Urgencia:
        - **🚨 Urgencia Extrema (3 criterios)**: EMERGENCIA - Intervención en 24 horas
        - **🔴 Urgente (2 criterios)**: URGENTE - Contacto en 48 horas  
        - **🟡 Moderado (1 criterio)**: Seguimiento en 1 semana
        - **🟢 Bajo Riesgo (0 criterios)**: Monitoreo rutinario
        
        ### 🧮 Nuevo Sistema de Clasificación:
        
        **Basado en 3 criterios específicos:**
        
        1. **⏰ Tiempo de ahorro mínimo** (`time_saved == "minimum"`)
           - POS que operan con un solo proveedor
           
        2. **📱 Bajo uso de plataforma** (`platform_use == "low"`)
           - POS con ≤1 orden por semana
           
        3. **📉 Tendencia de compra riesgosa** (`purchase_trend == "inactive" o "risky"`)
           - POS con patrones de compra problemáticos
        
        **🎯 Clasificación por criterios cumplidos:**
        - **3 criterios** → **Urgencia Extrema** (95% confianza)
        - **2 criterios** → **Urgente** (85% confianza)
        - **1 criterio** → **Moderado** (70% confianza)
        - **0 criterios** → **Bajo riesgo** (60% confianza)
        
        ### ⚡ **Eliminaciones del sistema anterior:**
        - ❌ **Análisis predictivo de churn**: Ya no se considera (reemplazado por platform_use)
        - ❌ **Ahorros diarios bajos**: Ya no se considera
        - ❌ **Sistema de puntajes numéricos**: Reemplazado por clasificación categórica
        """)
    
    # Ejecutar análisis si se presiona el botón
    if config_params["run_analysis"]:
        try:
            # Ejecutar análisis
            results = run_churn_analysis(config_params)
            
            # Guardar resultados en session state
            st.session_state.results = results
            st.session_state.timestamp = datetime.now(timezone.utc)
            
            st.success(f"✅ Análisis completado! Se detectaron {results['alerts_count']} alertas.")
            
        except Exception as e:
            st.error(f"❌ Error durante el análisis: {str(e)}")
            return
    
    # Mostrar resultados si están disponibles
    if "results" in st.session_state:
        results = st.session_state.results
        assessments = results.get("assessments", [])
        
        # Timestamp del último análisis
        if "timestamp" in st.session_state:
            st.info(f"📅 Último análisis: {st.session_state.timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        
        if view_mode == "Por Owner (Agrupado)":
            try:
                from behavioral_alert_agent import (
                    Config,
                    DataExtractor,
                    OwnerDirectory,
                    OwnerGrouper,
                )

                owner_assessments = results.get("owner_assessments")

                if owner_assessments is None:
                    temp_config = Config()
                    directory = OwnerDirectory(temp_config)
                    extractor = DataExtractor(temp_config)
                    grouper = OwnerGrouper(extractor.pos_owner_map, directory)
                    owner_assessments = grouper.group_by_owner(assessments)

                # Métricas resumen por owner
                display_owner_summary_metrics(owner_assessments, assessments)

                st.markdown("---")

                # Gráficos para vista por owner
                col1, col2 = st.columns(2)

                with col1:
                    # Gráfico de distribución de riesgos por owner
                    col_chart, col_help = st.columns([10, 1])
                    with col_chart:
                        owner_risk_fig = create_owner_risk_distribution_chart(owner_assessments)
                        if owner_risk_fig:
                            st.plotly_chart(owner_risk_fig, key="owner_risk_distribution", config=PLOTLY_CONFIG)
                    with col_help:
                        st.markdown("❓", help="Distribución de owners según el riesgo promedio de todos sus POS. Útil para priorizar atención comercial")

                with col2:
                    # Gráfico de distribución de POS por owner
                    col_chart, col_help = st.columns([10, 1])
                    with col_chart:
                        owner_pos_fig = create_owner_pos_distribution_chart(owner_assessments)
                        if owner_pos_fig:
                            st.plotly_chart(owner_pos_fig, key="owner_pos_distribution", config=PLOTLY_CONFIG)
                    with col_help:
                        st.markdown("❓", help="Barras apiladas mostrando cuántos POS de cada nivel de riesgo tiene cada owner (Top 10). Rojo=Alto, Naranja=Moderado, Verde=Bajo")

                st.markdown("---")

                # Infografía de POS críticos global
                st.markdown("---")
                col_chart, col_help = st.columns([10, 1])
                with col_chart:
                    critical_fig = create_critical_pos_infographic(assessments)
                    if critical_fig:
                        st.plotly_chart(critical_fig, config=PLOTLY_CONFIG)
                    else:
                        st.success(
                            "✅ No se detectaron POS con criterios críticos extremos (bajo uso + tiempo mínimo)"
                        )
                with col_help:
                    st.markdown(
                        "❓",
                        help="Dashboard de POS críticos: bajo uso + tiempo mínimo. Muestra Risk Score, Ahorros, Tendencias y Time Saved (proveedores)",
                    )
                
                # Expander explicativo para la infografía
                with st.expander("ℹ️ Explicación de la Infografía de POS Críticos"):
                    st.markdown("""
                    ### 🚨 **Infografía de POS Críticos**
                    
                    Esta infografía muestra **POS en riesgo extremo** que cumplen **ambas condiciones críticas**:
                    
                    #### 📊 **Criterios de Selección:**
                    - **🔴 Bajo uso de plataforma**: POS con ≤ 1 orden/semana
                    - **⏰ Tiempo de ahorro mínimo**: POS que operan con un solo proveedor
                    
                    #### 📈 **Gráficos Mostrados:**
                    
                    1. **Risk Score**: Puntuación de riesgo calculada (0-1)
                    2. **Ahorros Diarios ($)**: Promedio de ahorro diario generado
                    3. **Tendencia de Compra**: 
                       - **🔴 Risky**: POS con patrones de compra riesgosos
                       - **🟡 Inactive**: POS con actividad reducida
                       - **🟢 Stable**: POS con comportamiento estable
                       - *Hover sobre las barras para ver qué POS pertenecen a cada categoría*
                    4. **Time Saved**: Número de proveedores (1=mínimo, 2=medio, 3+=alto)
                    
                    #### 🎯 **Interpretación:**
                    - **POS con pocos ahorros + alto riesgo**: Intervención inmediata
                    - **Tendencia "Risky" o "Inactive"**: Seguimiento prioritario
                    - **Múltiples indicadores críticos**: Asignación de ejecutivo urgente
                    
                    #### 🔍 **Acción Recomendada:**
                    Estos POS requieren **atención comercial inmediata** para prevenir churn.
                    """)
                    

                # Charts individuales por owner
                st.markdown("---")
                create_owner_individual_charts(owner_assessments)

            except Exception as exc:
                st.error(f"Error al agrupar por owner: {exc}")
                st.info("Mostrando vista por POS individual como fallback")
                view_mode = "Por POS Individual"
        
        if view_mode == "Por POS Individual":
            # Métricas resumen por POS
            display_summary_metrics(results)
            
            st.markdown("---")
            
            # Gráficos en dos columnas
            col1, col2 = st.columns(2)
            
            with col1:
                # Gráfico de distribución de riesgos
                col_chart, col_help = st.columns([10, 1])
                with col_chart:
                    risk_dist_fig = create_risk_distribution_chart(assessments)
                    if risk_dist_fig:
                        st.plotly_chart(risk_dist_fig, config=PLOTLY_CONFIG)
                with col_help:
                    st.markdown("❓", help="Muestra la proporción de POS en cada nivel de riesgo: Alto (rojo), Moderado (naranja) y Bajo (verde)")
            
            with col2:
                # Gráfico de risk scores
                col_chart, col_help = st.columns([10, 1])
                with col_chart:
                    risk_score_fig = create_risk_score_chart(assessments)
                    if risk_score_fig:
                        st.plotly_chart(risk_score_fig, config=PLOTLY_CONFIG)
                with col_help:
                    st.markdown("❓", help="Muestra el puntaje de riesgo de cada POS (0-1). Las líneas indican umbrales: >0.75 Alto Riesgo, >0.55 Riesgo Moderado")
            
            # Segunda fila de gráficos
            col3, col4 = st.columns(2)
            
            with col3:
                # Gráfico de tasas de entrega
                col_chart, col_help = st.columns([10, 1])
                with col_chart:
                    delivery_fig = create_delivery_rates_chart(assessments)
                    if delivery_fig:
                        st.plotly_chart(delivery_fig, config=PLOTLY_CONFIG)
                    else:
                        st.info("📦 No hay datos de entrega disponibles")
                with col_help:
                    st.markdown("❓", help="Compara las tasas de entrega exitosa de los últimos 4 vs 2 semanas por POS. Útil para detectar deterioro en el servicio")
            
            with col4:
                # Gráfico de distribución de time_saved
                col_chart, col_help = st.columns([10, 1])
                with col_chart:
                    time_saved_fig = create_time_saved_distribution_chart(assessments)
                    if time_saved_fig:
                        st.plotly_chart(time_saved_fig, config=PLOTLY_CONFIG)
                with col_help:
                    st.markdown("❓", help="Distribución de POS por número de proveedores y coeficientes de tiempo ahorrado. 1 proveedor=mínimo (0.5), 2=medio (0.75), 3+=alto (0.83)")
            
            # Tercera fila: Gráfico de platform_use
            st.markdown("---")
            col_chart, col_help = st.columns([10, 1])
            with col_chart:
                platform_use_fig = create_platform_use_distribution_chart(assessments)
                if platform_use_fig:
                    st.plotly_chart(platform_use_fig, config=PLOTLY_CONFIG)
                else:
                    st.info("📱 No hay datos de uso de plataforma disponibles")
            with col_help:
                st.markdown("❓", help="Distribución de uso de plataforma (criterio de riesgo). Low=≤1 orden/semana (alto riesgo), Medium=2-4 órdenes/semana, High=5+ órdenes/semana")
            
            # Infografía de POS críticos
            st.markdown("---")
            col_chart, col_help = st.columns([10, 1])
            with col_chart:
                critical_fig = create_critical_pos_infographic(assessments)
                if critical_fig:
                    st.plotly_chart(critical_fig, config=PLOTLY_CONFIG)
                else:
                    st.success("✅ No se detectaron POS con criterios críticos extremos (bajo uso + tiempo mínimo)")
            with col_help:
                st.markdown("❓", help="POS críticos con bajo uso + tiempo mínimo. Muestra Risk Score, Ahorros, Tendencias y Time Saved (número de proveedores)")
            
            # Expander explicativo para la infografía
            with st.expander("ℹ️ Explicación de la Infografía de POS Críticos"):
                st.markdown("""
                ### 🚨 **Infografía de POS Críticos**
                
                Esta infografía muestra **POS en riesgo extremo** que cumplen **ambas condiciones críticas**:
                
                #### 📊 **Criterios de Selección:**
                - **🔴 Bajo uso de plataforma**: POS con ≤ 1 orden/semana
                - **⏰ Tiempo de ahorro mínimo**: POS que operan con un solo proveedor
                
                #### 📈 **Gráficos Mostrados:**
                
                1. **Risk Score**: Puntuación de riesgo calculada (0-1)
                2. **Ahorros Diarios ($)**: Promedio de ahorro diario generado
                3. **Tendencia de Compra**: 
                   - **🔴 Risky**: POS con patrones de compra riesgosos
                   - **🟡 Inactive**: POS con actividad reducida
                   - **🟢 Stable**: POS con comportamiento estable
                   - *Hover sobre las barras para ver qué POS pertenecen a cada categoría*
                4. **Time Saved**: Número de proveedores (1=mínimo, 2=medio, 3+=alto)
                
                #### 🎯 **Interpretación:**
                - **POS con pocos ahorros + alto riesgo**: Intervención inmediata
                - **Tendencia "Risky" o "Inactive"**: Seguimiento prioritario
                - **Múltiples indicadores críticos**: Asignación de ejecutivo urgente
                
                #### 🔍 **Acción Recomendada:**
                Estos POS requieren **atención comercial inmediata** para prevenir churn.
                """)
            
            st.markdown("---")
            
            # Tabla detallada de alertas por POS
            display_alerts_table(assessments)
        
    
    else:
        # Mostrar instrucciones iniciales
        st.info("👈 Presiona **'Ejecutar Análisis'** en la barra lateral para comenzar")
        
        # Mostrar ejemplo de datos
        st.subheader("📊 Fuentes de Datos Analizadas")
        st.markdown("""
        El sistema analiza cinco fuentes de datos:
        
        1. **Trial Data**: Métricas de uso de la plataforma (`data/trial_data.json`)
        2. **Orders Data**: Datos de entregas y riesgo malicioso (`data/orders_delivered.json`)  
        3. **Purchase Trend**: Clasificación de tendencias de compra (`data/purchase_trend.json`)
        4. **⚠️ Churn Risk Data**: POS con características de abandono potencial (`data/zombies.json`)
        5. **👥 POS Owner**: Mapeo de POS a vendedores Hubspot (`data/pos_owner.csv`)
        
        ### 🎯 Fórmula de Tiempo Ahorrado:
        ```
        tiempo_ahorrado = 1 - Numero de ordenes/2(Numero de deliveries)
        ```
        - **Más distribuidores** → **Mayor tiempo ahorrado** (tendencia a 1)
        - **Factor 2**: Ahorro doble (catálogo + compra)
        - **Facilita adopción** de nuevos distribuidores
        """)


if __name__ == "__main__":
    main()
