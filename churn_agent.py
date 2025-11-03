#!/usr/bin/env python3
"""
Churn Agent - Vendor Mix Overview
=================================

Primera iteración del nuevo modelo de alertas. Replica la lógica utilizada en
`app_scoring.py` para obtener el detalle de compras por droguería/vendor y lo
expone como gráfico de torta dentro de Streamlit usando Plotly.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from datetime import datetime, timedelta


BASE_DIR = Path(__file__).resolve().parent
ORDERS_PATH = BASE_DIR / "data" / "orders_delivered_pos_vendor_geozone.csv"


def get_week_number(date_str: str) -> str:
    """Convierte una fecha en formato YYYY-MM-DD HH:MM:SS a número de semana YYYY-WW."""
    try:
        date_obj = pd.to_datetime(date_str)
        year = date_obj.year
        week = date_obj.isocalendar().week
        return f"{year}-W{week:02d}"
    except:
        return "Unknown"


@st.cache_data(ttl=3600)
def load_pos_vendor_totals(path: Path = ORDERS_PATH) -> pd.DataFrame:
    """Carga el CSV original y sintetiza el total comprado por POS/Vendor."""
    if not path.exists():
        return pd.DataFrame(columns=["point_of_sale_id", "vendor_id", "total_compra"])

    dtype_map = {
        "point_of_sale_id": "Int64",
        "vendor_id": "Int64",
        "vendor_id_x": "Int64",
    }
    df = pd.read_csv(path, dtype=dtype_map, low_memory=False)

    numeric_cols = ["unidades_pedidas", "precio_minimo", "valor_vendedor"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "total_compra" not in df.columns:
        if "valor_vendedor" in df.columns:
            df["total_compra"] = df["valor_vendedor"]
        else:
            # `valor_vendedor` debería existir, pero calculamos el fallback por seguridad.
            df["total_compra"] = df["unidades_pedidas"] * df["precio_minimo"]

    vendor_col = "vendor_id_x" if "vendor_id_x" in df.columns else "vendor_id"
    if vendor_col not in df.columns:
        return pd.DataFrame(columns=["point_of_sale_id", "vendor_id", "total_compra"])

    grouped = (
        df.groupby(["point_of_sale_id", vendor_col], dropna=False)["total_compra"]
        .sum()
        .reset_index()
    )
    grouped.rename(
        columns={"point_of_sale_id": "POS ID", vendor_col: "Droguería/Vendor ID"},
        inplace=True,
    )
    grouped["Total Comprado"] = grouped["total_compra"]
    grouped.drop(columns=["total_compra"], inplace=True)
    for col in ["POS ID", "Droguería/Vendor ID"]:
        grouped[col] = pd.to_numeric(grouped[col], errors="coerce").astype("Int64")

    return grouped


@st.cache_data(ttl=3600)
def load_pos_vendor_weekly_data(path: Path = ORDERS_PATH) -> pd.DataFrame:
    """Carga el CSV y agrupa por POS, Vendor y semana para análisis temporal."""
    if not path.exists():
        return pd.DataFrame(columns=["point_of_sale_id", "vendor_id", "week", "total_compra"])

    dtype_map = {
        "point_of_sale_id": "Int64",
        "vendor_id": "Int64",
        "vendor_id_x": "Int64",
    }
    df = pd.read_csv(path, dtype=dtype_map, low_memory=False)

    numeric_cols = ["unidades_pedidas", "precio_minimo", "valor_vendedor"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "total_compra" not in df.columns:
        if "valor_vendedor" in df.columns:
            df["total_compra"] = df["valor_vendedor"]
        else:
            df["total_compra"] = df["unidades_pedidas"] * df["precio_minimo"]

    vendor_col = "vendor_id_x" if "vendor_id_x" in df.columns else "vendor_id"
    if vendor_col not in df.columns or "order_date" not in df.columns:
        return pd.DataFrame(columns=["point_of_sale_id", "vendor_id", "week", "total_compra"])

    df["week"] = df["order_date"].apply(get_week_number)
    
    grouped = (
        df.groupby(["point_of_sale_id", vendor_col, "week"], dropna=False)["total_compra"]
        .sum()
        .reset_index()
    )
    
    grouped.rename(
        columns={
            "point_of_sale_id": "POS ID", 
            vendor_col: "Droguería/Vendor ID",
            "week": "Semana"
        },
        inplace=True,
    )
    grouped["Total Comprado"] = grouped["total_compra"]
    grouped.drop(columns=["total_compra"], inplace=True)
    
    for col in ["POS ID", "Droguería/Vendor ID"]:
        grouped[col] = pd.to_numeric(grouped[col], errors="coerce").astype("Int64")

    return grouped


def build_detail_table(grouped: pd.DataFrame, pos_id: int) -> pd.DataFrame:
    """Filtra y enriquece la tabla de detalle para un POS concreto."""
    pos_data = grouped[grouped["POS ID"] == pos_id].copy()
    if pos_data.empty:
        return pos_data

    total = pos_data["Total Comprado"].sum()
    if total > 0:
        pos_data["Porcentaje"] = (pos_data["Total Comprado"] / total) * 100
    else:
        pos_data["Porcentaje"] = 0.0

    pos_data.sort_values("Total Comprado", ascending=False, inplace=True)
    pos_data["Porcentaje"] = pos_data["Porcentaje"].round(2)

    return pos_data


def create_vendor_mix_chart(detail_table: pd.DataFrame) -> go.Figure | None:
    """Genera el gráfico de torta del mix de compras por vendor."""
    if detail_table.empty:
        return None

    pos_id = detail_table["POS ID"].iloc[0]
    fig = px.pie(
        detail_table,
        values="Total Comprado",
        names="Droguería/Vendor ID",
        title=f"Distribución de Compras por Vendor - POS {pos_id}",
        hole=0.25,
    )
    fig.update_traces(textposition="inside", textinfo="percent+label")
    fig.update_layout(height=420)
    return fig


def create_weekly_comparison_chart(weekly_data: pd.DataFrame, pos_id: int) -> go.Figure | None:
    """Genera gráfico de comparación semanal de distribuciones por vendor."""
    pos_data = weekly_data[weekly_data["POS ID"] == pos_id].copy()
    if pos_data.empty:
        return None

    weekly_totals = pos_data.groupby("Semana")["Total Comprado"].sum().reset_index()
    weekly_totals = weekly_totals.sort_values("Semana")
    
    pos_data = pos_data.merge(weekly_totals, on="Semana", suffixes=("", "_week_total"))
    pos_data["Porcentaje"] = (pos_data["Total Comprado"] / pos_data["Total Comprado_week_total"]) * 100
    
    pivot_data = pos_data.pivot_table(
        index="Semana", 
        columns="Droguería/Vendor ID", 
        values="Porcentaje", 
        fill_value=0
    ).reset_index()
    
    fig = go.Figure()
    
    colors = px.colors.qualitative.Set3
    for i, vendor_id in enumerate(pivot_data.columns[1:]):
        color = colors[i % len(colors)]
        fig.add_trace(go.Scatter(
            x=pivot_data["Semana"],
            y=pivot_data[vendor_id],
            mode='lines+markers',
            name=f'Vendor {vendor_id}',
            line=dict(color=color, width=2),
            marker=dict(color=color, size=6)
        ))
    
    fig.update_layout(
        title=f"Evolución Semanal - Distribución % por Vendor (POS {pos_id})",
        xaxis_title="Semana",
        yaxis_title="Porcentaje de Compras (%)",
        height=500,
        hovermode='x unified',
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    
    fig.update_xaxes(tickangle=45)
    
    return fig


def create_weekly_heatmap(weekly_data: pd.DataFrame, pos_id: int) -> go.Figure | None:
    """Genera un heatmap de la distribución semanal por vendor."""
    pos_data = weekly_data[weekly_data["POS ID"] == pos_id].copy()
    if pos_data.empty:
        return None

    weekly_totals = pos_data.groupby("Semana")["Total Comprado"].sum().reset_index()
    pos_data = pos_data.merge(weekly_totals, on="Semana", suffixes=("", "_week_total"))
    pos_data["Porcentaje"] = (pos_data["Total Comprado"] / pos_data["Total Comprado_week_total"]) * 100
    
    pivot_data = pos_data.pivot_table(
        index="Droguería/Vendor ID", 
        columns="Semana", 
        values="Porcentaje", 
        fill_value=0
    )
    
    fig = go.Figure(data=go.Heatmap(
        z=pivot_data.values,
        x=pivot_data.columns,
        y=[f"Vendor {vendor}" for vendor in pivot_data.index],
        colorscale='RdYlBu_r',
        hoverongaps=False,
        colorbar=dict(title="% Compras")
    ))
    
    fig.update_layout(
        title=f"Mapa de Calor - Distribución Semanal por Vendor (POS {pos_id})",
        xaxis_title="Semana",
        yaxis_title="Vendor ID",
        height=400
    )
    
    fig.update_xaxes(tickangle=45)
    
    return fig


def main() -> None:
    st.set_page_config(
        page_title="Churn Agent - Mix de Compras",
        page_icon="🧠",
        layout="wide",
    )

    st.title("🧠 Churn Agent - Distribución de Compras por Vendor")
    st.markdown(
        "Análisis completo del modelo de alertas: distribución de compras por vendor y evolución semanal para detectar cambios de comportamiento."
    )

    pos_vendor_totals = load_pos_vendor_totals()
    weekly_data = load_pos_vendor_weekly_data()
    
    if pos_vendor_totals.empty:
        st.error(
            "No fue posible cargar `orders_delivered_pos_vendor_geozone.csv`. "
            "Verifica que el archivo exista y tenga las columnas esperadas."
        )
        return

    pos_ids = sorted(pos_vendor_totals["POS ID"].dropna().unique())
    if not pos_ids:
        st.warning("No se encontraron puntos de venta en los datos cargados.")
        return

    selected_pos = st.selectbox("Selecciona un Punto de Venta", options=pos_ids)
    if selected_pos is None:
        st.info("Selecciona un POS para ver su distribución de compras.")
        return

    detail_table = build_detail_table(pos_vendor_totals, selected_pos)
    if detail_table.empty:
        st.warning(f"No se encontraron compras registradas para el POS {selected_pos}.")
        return

    tab1, tab2, tab3 = st.tabs(["📊 Distribución General", "📈 Evolución Semanal", "🗓️ Mapa de Calor Semanal"])
    
    with tab1:
        fig = create_vendor_mix_chart(detail_table)
        
        st.subheader("Detalle de Compras por Droguería/Vendor")
        st.dataframe(
            detail_table.style.format(
                {"Total Comprado": "${:,.2f}", "Porcentaje": "{:.2f}%"}
            ),
            use_container_width=True,
        )

        if fig is not None:
            st.subheader("Mix de Compras")
            st.plotly_chart(fig, use_container_width=True, config={"displaylogo": False})
    
    with tab2:
        if not weekly_data.empty:
            weekly_chart = create_weekly_comparison_chart(weekly_data, selected_pos)
            if weekly_chart is not None:
                st.plotly_chart(weekly_chart, use_container_width=True, config={"displaylogo": False})
                
                pos_weekly = weekly_data[weekly_data["POS ID"] == selected_pos]
                if not pos_weekly.empty:
                    st.subheader("Datos Semanales Detallados")
                    weekly_pivot = pos_weekly.pivot_table(
                        index="Semana", 
                        columns="Droguería/Vendor ID", 
                        values="Total Comprado", 
                        fill_value=0
                    ).reset_index()
                    st.dataframe(weekly_pivot, use_container_width=True)
            else:
                st.warning("No hay datos semanales suficientes para generar el gráfico de evolución.")
        else:
            st.warning("No se pudieron cargar los datos semanales.")
    
    with tab3:
        if not weekly_data.empty:
            heatmap_chart = create_weekly_heatmap(weekly_data, selected_pos)
            if heatmap_chart is not None:
                st.plotly_chart(heatmap_chart, use_container_width=True, config={"displaylogo": False})
                st.markdown("**Interpretación del Mapa de Calor:**")
                st.markdown("- Colores más rojos indican mayor concentración de compras en esa semana/vendor")
                st.markdown("- Colores más azules indican menor actividad")
                st.markdown("- Patrones verticales muestran vendors consistentes")
                st.markdown("- Cambios horizontales revelan variaciones semanales")
            else:
                st.warning("No hay datos semanales suficientes para generar el mapa de calor.")
        else:
            st.warning("No se pudieron cargar los datos semanales.")


if __name__ == "__main__":
    main()
