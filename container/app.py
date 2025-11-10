import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime, timedelta
import pymysql
from contextlib import contextmanager
from sqlalchemy import create_engine
import sqlalchemy
import logging
import select
import boto3
import io
import os
import requests
#from sshtunnel import SSHTunnelForwarder
from contextlib import contextmanager
from requests_oauthlib import OAuth2Session
import logging

EMAILS_APROBADOS = [
    "enric.castillo@extendeal.com", 
    "marcos.nasillo@extendeal.com",
    "pedro.sabatte@extendeal.com",
    "arturo.gomez@extendeal.com",
    "agostina.stefani@extendeal.com",
]

#==================== GOOGLE AUTH FUNCTIONS ====================
def get_google_auth_url():
    """Genera URL de autenticación de Google"""
    google = OAuth2Session(
        st.secrets["auth"]["client_id"],
        scope=["email", "profile"],
        redirect_uri=st.secrets["auth"]["redirect_uri"]
    )
    auth_url, _ = google.authorization_url(
        "https://accounts.google.com/o/oauth2/v2/auth",
        access_type="offline"
    )
    return auth_url

def get_user_info(code):
    """Obtiene información del usuario desde Google"""
    try:
        google = OAuth2Session(
            st.secrets["auth"]["client_id"], 
            redirect_uri=st.secrets["auth"]["redirect_uri"]
        )
        
        # Add timeout and better error handling for token fetch
        token = google.fetch_token(
            "https://oauth2.googleapis.com/token",
            code=code,
            client_secret=st.secrets["auth"]["client_secret"],
            timeout=30  # Add timeout
        )
        
        response = requests.get(
            "https://www.googleapis.com/oauth2/v2/userinfo",
            headers={"Authorization": f"Bearer {token['access_token']}"},
            timeout=30  # Add timeout
        )
        
        if response.status_code != 200:
            raise Exception(f"Google API error: {response.status_code} - {response.text}")
            
        return response.json()
        
    except Exception as e:
        error_msg = str(e)
        if "invalid_grant" in error_msg.lower():
            raise Exception(f"(invalid_grant) Bad Request - El código de autorización ha expirado o ya fue usado. Por favor, inicia sesión nuevamente.")
        elif "timeout" in error_msg.lower():
            raise Exception(f"Timeout en conexión con Google. Verifica tu conexión a internet.")
        else:
            raise Exception(f"Error OAuth: {error_msg}")

def check_email_approved(email):
    """Verifica si el email está en la lista de aprobados"""
    if not email:
        return False
    return email.lower().strip() in [e.lower().strip() for e in EMAILS_APROBADOS]

#==================== INICIALIZAR SESSION STATE ====================
if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False
if 'user_data' not in st.session_state:
    st.session_state.user_data = None
if 'show_denied' not in st.session_state:
    st.session_state.show_denied = False

#==================== MANEJAR CALLBACK DE GOOGLE ====================
query_params = st.experimental_get_query_params()
if 'code' in query_params and not st.session_state.authenticated:
    try:
        with st.spinner("Verificando credenciales..."):
            # Handle both string and list formats for query params
            code = query_params['code'][0] if isinstance(query_params['code'], list) else query_params['code']
                
            user_info = get_user_info(code)
            email = user_info.get('email', '')
            
            if check_email_approved(email):
                # LOGIN EXITOSO
                st.session_state.authenticated = True
                st.session_state.user_data = user_info
                st.session_state.show_denied = False
                st.experimental_set_query_params()  # Limpiar query params
                st.rerun()
            else:
                # EMAIL NO APROBADO
                st.session_state.authenticated = False
                st.session_state.show_denied = True
                st.session_state.denied_email = email
                st.experimental_set_query_params()  # Limpiar query params
                st.rerun()
                
    except Exception as e:
        st.error(f"Error en autenticación: {str(e)}")
        st.session_state.authenticated = False
        st.experimental_set_query_params()  # Limpiar query params en caso de error

#==================== PANTALLAS ====================

def show_login():
    """Pantalla de login"""
    st.markdown("<div style='padding: 50px 0;'></div>", unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown("""
        <h1 style='text-align: center; font-family: "Trebuchet MS", sans-serif;'>
            CHURN Alerts & Analysis
        </h1>
        """, unsafe_allow_html=True)

        # CSS personalizado para el botón
        st.markdown("""
            <style>
            div.stButton > button:first-child {
                background-color: #4285F4;
                color: white;
                border: none;
                border-radius: 5px;
                font-weight: bold;
                transition: background-color 0.3s ease;
            }
            div.stButton > button:first-child:hover {
                background-color: #3367D6;
            }
            </style>
        """, unsafe_allow_html=True)

        # Usar el botón de Streamlit pero con estilo personalizado
        if st.button("🔒 Iniciar sesión con Google", type="primary", use_container_width=True):
            auth_url = get_google_auth_url()
            st.markdown(f'<meta http-equiv="refresh" content="0; url={auth_url}">', unsafe_allow_html=True)
        
        st.markdown("<p style='text-align: center; color: gray; margin-top: 20px;'>Acceso restringido a usuarios autorizados</p>", unsafe_allow_html=True)

def show_access_denied():
    """Pantalla de acceso denegado"""
    st.markdown("<div style='padding: 50px 0;'></div>", unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.error("🚫 **ACCESO DENEGADO**")
        st.warning(f"El email **{st.session_state.denied_email}** no está autorizado.")
        st.info("Contacta al administrador para solicitar acceso.")
        
        if st.button("🔙 Volver al login", use_container_width=True):
            st.session_state.show_denied = False
            if 'denied_email' in st.session_state:
                del st.session_state.denied_email
            st.rerun()

#==================== LÓGICA PRINCIPAL ====================
# Verificar autenticación antes de continuar con el dashboard
if not st.session_state.authenticated:
    if st.session_state.show_denied:
        show_access_denied()
    else:
        show_login()
    st.stop()  # Detener ejecución si no está autenticado

# Configuración de la página
st.set_page_config(
    page_title="🎯 Churn Alert Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS personalizado
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 2rem;
        border-radius: 15px;
        text-align: center;
        margin-bottom: 2rem;
    }
    .section-header {
        background: linear-gradient(90deg, #4facfe 0%, #00f2fe 100%);
        color: white;
        padding: 1rem;
        border-radius: 10px;
        margin: 1.5rem 0 1rem 0;
    }
    .metric-card {
        background: white;
        padding: 1.5rem;
        border-radius: 10px;
        border-left: 4px solid #667eea;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        text-align: center;
        min-height: 230px;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
    }
    .conversion-card {
        background: linear-gradient(135deg, #2d3748 0%, #1a202c 100%);
        color: white;
        padding: 1.5rem;
        border-radius: 12px;
        text-align: center;
        box-shadow: 0 6px 12px rgba(0,0,0,0.15);
    }
    .big-number {
        font-size: 2.5rem;
        font-weight: bold;
        margin: 0;
    }
    .small-text {
        font-size: 0.9rem;
        opacity: 0.8;
    }
    .metric-card .small-text {
        font-size: 1.05rem;
    }
    .metric-card .big-number {
        font-size: 2.7rem;
    }
    .help-icon {
        font-size: 1rem;
        margin-left: 0.4rem;
        cursor: help;
    }
</style>
""", unsafe_allow_html=True)


# Constante para período de análisis
#days = 100

# Variables de entorno requeridas para AWS S3:
# AWS_ACCESS_KEY_ID: Clave de acceso de AWS
# AWS_SECRET_ACCESS_KEY: Clave secreta de AWS  
# AWS_DEFAULT_REGION: Región de AWS (opcional, por defecto us-east-1)
days = 100
def load_geo_data_from_s3():
    """Carga datos geográficos desde S3"""
    try:
        # Configurar cliente S3 - usará las credenciales configuradas en AWS CLI
        s3_client = boto3.client('s3', region_name='us-west-2')
        
        # Descargar archivo desde S3
        bucket_name = 'etl-vendors-bi'
        object_key = 'Vendor_oportunities/pos_geo_country.csv'
        
        
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        
        # Leer el contenido del archivo
        encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
        df_geo = None
        
        # Leer el contenido una sola vez
        csv_content = response['Body'].read()
        
        for encoding in encodings:
            try:
                # Usar BytesIO con el contenido ya leído
                df_geo = pd.read_csv(
                    io.BytesIO(csv_content), 
                    encoding=encoding, 
                    low_memory=False
                )
                break
            except UnicodeDecodeError:
                continue
        
        if df_geo is None:
            st.error("No se pudo leer el archivo pos_geo_country.csv desde S3")
            return pd.DataFrame()
        
        return df_geo
        
    except Exception as e:
        st.error(f"❌ Error al acceder a S3: {str(e)}")
        st.error(f"❌ Tipo de error: {type(e).__name__}")
        
        # Verificar si es un error de permisos específico
        if "AccessDenied" in str(e) or "Forbidden" in str(e):
            st.error("🔒 Error de permisos S3 - Verificar IAM role del ECS task")
        elif "NoCredentialsError" in str(e):
            st.error("🔑 No se encontraron credenciales AWS")
        elif "EndpointConnectionError" in str(e):
            st.error("🌐 Error de conexión a S3")
        
        # Fallback a archivo local
        try:
            encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
            df_geo = None
            
            for encoding in encodings:
                try:
                    df_geo = pd.read_csv('data/pos_geo_country.csv', 
                                       encoding=encoding, 
                                       low_memory=False)
                    break
                except (UnicodeDecodeError, FileNotFoundError):
                    continue
                    
            if df_geo is None:
                st.error("No se pudo leer el archivo pos_geo_country.csv ni desde S3 ni localmente")
                return pd.DataFrame()
            
            return df_geo
            
        except Exception as fallback_error:
            st.error(f"❌ Error en fallback local: {fallback_error}")
            return pd.DataFrame()

class DatabaseConnection:
    def __init__(self):
        """Inicializa la conexión a la base de datos optimizada"""
        self.db_host = st.secrets["database"]["host"]
        self.db_port = int(st.secrets["database"]["port"])
        self.db_user = st.secrets["database"]["username"]
        self.db_password = st.secrets["database"]["password"]
        self.db_name = st.secrets["database"]["database"]
        
        # Create SQLAlchemy engine for better performance
        self.engine = create_engine(
            f"mysql+pymysql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}",
            pool_pre_ping=True,
            pool_recycle=3600,
            pool_size=5,
            max_overflow=10
        )
        
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    @contextmanager
    def get_connection(self):
        """Context manager para obtener una conexión optimizada SQLAlchemy"""
        connection = None
        try:    
            connection = self.engine.connect()
            yield connection
        except Exception as e:
            st.error(f"Error en conexión a base de datos: {e}")
            raise
        finally:
            if connection:
                connection.close()

    def get_orders_100(self, days: int = 100) -> pd.DataFrame:
        """Obtiene datos de órdenes de los últimos N días"""
        query = f"""
 SELECT 
    o.point_of_sale_id AS point_of_sale_id,
    pos.name AS pos_name,
    op.barcode AS super_catalog_id, 
    o.id AS order_id,
    o.created_at AS order_date,
 	op.quantity AS unidades_pedidas,
    ap.price_with_discount AS precio_minimo, 
    ap.drug_manufacturer_id AS vendor_id,
    op.quantity * ap.price_with_discount AS valor_vendedor
FROM orders o 
JOIN points_of_sale pos ON o.point_of_sale_id = pos.id 
JOIN clients c ON pos.client_id = c.id 
JOIN order_products op ON o.id = op.order_id 
JOIN api_products ap ON op.id = ap.order_product_id 
JOIN delivery_products dp ON dp.api_product_id = ap.id 
JOIN deliveries d ON d.id = dp.delivery_id 
WHERE o.created_at >= NOW() - INTERVAL {days} DAY 
  AND op.deleted_at IS NULL 
  AND c.is_demo = 0 
  AND d.status_id != 1 
 
        """
        
        try:
            # Use engine directly with pandas for SQLAlchemy compatibility
            df = pd.read_sql(query, self.engine)
            
            if 'order_date' in df.columns:
                df['order_date'] = pd.to_datetime(df['order_date'])
            
            return df
            
        except Exception as e:
            st.error(f"Error obteniendo datos de órdenes: {e}")
            return pd.DataFrame()

    def get_currency_rates(self):
        """Obtiene tasas de cambio para convertir MXN y ARS a USD"""
        import requests
        import datetime
        
        try:
            # API gratuita para tasas de cambio
            response = requests.get("https://api.exchangerate-api.com/v4/latest/USD", timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                rates = {
                    'USD_to_MXN': data['rates'].get('MXN', 20.0),  # Fallback por si falla
                    'USD_to_ARS': data['rates'].get('ARS', 350.0),  # Fallback por si falla
                    'date': datetime.datetime.now().strftime('%Y-%m-%d')
                }
                
                # Convertir a tasas desde las monedas locales a USD
                rates['MXN_to_USD'] = 1 / rates['USD_to_MXN']
                rates['ARS_to_USD'] = 1 / rates['USD_to_ARS']
                
                return rates
            else:
                # Tasas de fallback si la API no funciona
                return {
                    'MXN_to_USD': 0.05,  # ~20 MXN por USD
                    'ARS_to_USD': 0.003,  # ~350 ARS por USD
                    'USD_to_MXN': 20.0,
                    'USD_to_ARS': 350.0,
                    'date': datetime.datetime.now().strftime('%Y-%m-%d'),
                    'source': 'fallback'
                }
                
        except Exception as e:
            st.warning(f"No se pudieron obtener tasas de cambio actuales: {e}. Usando tasas predeterminadas.")
            return {
                'MXN_to_USD': 0.05,
                'ARS_to_USD': 0.003,
                'USD_to_MXN': 20.0,
                'USD_to_ARS': 350.0,
                'date': datetime.datetime.now().strftime('%Y-%m-%d'),
                'source': 'fallback'
            }





@st.cache_data
def load_and_process_data():
    """Carga datos de ETL y los procesa con información geográfica"""
    df_orders = pd.DataFrame()
    
    # Cargar exclusivamente desde base de datos
    try:
        # Crear instancia de DatabaseConnection
        etl_instance = DatabaseConnection()
        
        # Obtener datos de la ETL
        st.info("📡 Cargando datos desde la base de datos...")
        df_orders = etl_instance.get_orders_100(days)
        
        if df_orders.empty:
            st.error("❌ No se pudieron obtener datos de la base de datos")
            return pd.DataFrame()
        
        
    except Exception as e:
        st.error(f"❌ Error en conexión a base de datos: {e}")
        return pd.DataFrame()
    
    # Cargar datos geográficos desde S3
    try:
        df_geo = load_geo_data_from_s3()
        
        if df_geo.empty:
            st.error("No se pudieron cargar datos geográficos")
            return pd.DataFrame()
            
        # Limpiar nombres de columnas
        df_orders.columns = df_orders.columns.str.strip()
        df_geo.columns = df_geo.columns.str.strip()
        
        # Procesar fechas
        if 'order_date' in df_orders.columns:
            df_orders['order_date'] = pd.to_datetime(df_orders['order_date'], errors='coerce')
            
        # Asegurar que tenemos la columna total_compra
        if 'total_compra' not in df_orders.columns and 'valor_vendedor' in df_orders.columns:
            df_orders['total_compra'] = df_orders['valor_vendedor']
            
        # Hacer merge con datos geográficos
        df_final = df_orders.merge(
            df_geo, 
            on='point_of_sale_id', 
            how='left'
        )
        
        # Convertir monedas a USD
        if 'total_compra' in df_final.columns and 'country_code' in df_final.columns:
            try:
                st.info("💱 Obteniendo tasas de cambio...")
                rates = etl_instance.get_currency_rates()
                
                # Crear columna total_compra_usd con conversión basada en país
                def convert_to_usd(row):
                    if pd.isna(row['total_compra']) or pd.isna(row['country_code']):
                        return row['total_compra']
                    
                    country_code = str(row['country_code']).upper()
                    amount = float(row['total_compra'])
                    
                    if country_code == 'MX':  # México
                        return amount * rates['MXN_to_USD']
                    elif country_code == 'AR':  # Argentina  
                        return amount * rates['ARS_to_USD']
                    else:  # Asumir USD por defecto
                        return amount
                
                df_final['total_compra_usd'] = df_final.apply(convert_to_usd, axis=1)
                
                # Mantener también la columna original por compatibilidad
                if 'total_compra' not in df_final.columns:
                    df_final['total_compra'] = df_final['total_compra_usd']
                    
                st.success(f"✅ Monedas convertidas a USD (Fecha: {rates.get('date', 'N/A')})")
                
            except Exception as e:
                st.warning(f"⚠️ No se pudo convertir monedas: {e}. Usando valores originales.")
                df_final['total_compra_usd'] = df_final.get('total_compra', 0)
        
        # Eliminar duplicados
        initial_count = len(df_final)
        df_final = df_final.drop_duplicates()
        final_count = len(df_final)
        
        if initial_count != final_count:
            st.info(f"🔧 Eliminados {initial_count - final_count} registros duplicados")
            
        
        return df_final
        
    except Exception as e:
        st.error(f"Error al cargar y procesar los datos: {str(e)}")
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

def analyze_spending_trends(df_orders, pos_id):
    """
    Analiza las tendencias de gasto de un POS basado en:
    1. Gasto cero en ultimos 7 dias
    2. Disminucion mayor al 50% respecto al promedio historico
    """
    if df_orders.empty or 'order_date' not in df_orders.columns:
        return None
    
    # Filtrar datos del POS
    pos_data = df_orders[df_orders['point_of_sale_id'] == pos_id].copy()
    
    if pos_data.empty:
        return None
    
    # Asegurar que order_date es datetime
    pos_data['order_date'] = pd.to_datetime(pos_data['order_date'], errors='coerce')
    pos_data = pos_data.dropna(subset=['order_date'])
    
    if pos_data.empty:
        return None
    
    # Obtener fecha maxima en los datos
    max_date = pos_data['order_date'].max()
    
    # Calcular fecha de inicio de ultimos 7 dias
    last_7_days_start = max_date - pd.Timedelta(days=6)  # Incluye el dia actual
    
    # Filtrar datos de ultimos 7 dias
    last_7_days_data = pos_data[pos_data['order_date'] >= last_7_days_start]
    last_7_days_spending = last_7_days_data['total_compra'].sum()
    
    # Calcular promedio historico por periodo de 7 dias (excluyendo ultimos 7 dias)
    historical_data = pos_data[pos_data['order_date'] < last_7_days_start]
    
    if historical_data.empty:
        return None
    
    # Agrupar datos historicos en periodos de 7 dias
    historical_data = historical_data.sort_values('order_date')
    min_historical_date = historical_data['order_date'].min()
    
    # Crear periodos de 7 dias
    periods = []
    current_date = min_historical_date
    
    while current_date < last_7_days_start:
        period_end = current_date + pd.Timedelta(days=6)
        period_data = historical_data[
            (historical_data['order_date'] >= current_date) & 
            (historical_data['order_date'] <= period_end)
        ]
        
        if not period_data.empty:
            periods.append({
                'start_date': current_date,
                'end_date': period_end,
                'total_spending': period_data['total_compra'].sum()
            })
        
        current_date += pd.Timedelta(days=7)
    
    if not periods:
        return None
    
    # Calcular promedio de periodos de 7 dias
    avg_spending = sum(p['total_spending'] for p in periods) / len(periods)
    
    # Determinar nivel de alerta
    alert_type = None
    alert_description = None
    risk_level = '🟢 NORMAL'
    
    # CRITERIO 1: Gasto cero en ultimos 7 dias
    if last_7_days_spending == 0:
        alert_type = 'GASTO_CERO'
        alert_description = f'POS sin actividad de compra en los ultimos 7 dias ({last_7_days_start.strftime("%Y-%m-%d")} - {max_date.strftime("%Y-%m-%d")})'
        risk_level = '🔴 CRITICO'
    
    # CRITERIO 2: Disminucion mayor al 50% respecto al promedio
    elif avg_spending > 0:
        decrease_percentage = ((avg_spending - last_7_days_spending) / avg_spending) * 100
        
        if decrease_percentage >= 50:
            alert_type = 'DISMINUCION_CRITICA'
            alert_description = f'Disminucion del {decrease_percentage:.1f}% en gasto respecto al promedio historico (ultimos 7 dias)'
            risk_level = '🟠 ALTO'
        elif decrease_percentage >= 30:
            alert_type = 'DISMINUCION_MODERADA'
            alert_description = f'Disminucion del {decrease_percentage:.1f}% en gasto respecto al promedio historico (ultimos 7 dias)'
            risk_level = '🟡 MODERADO'
    
    return {
        'pos_id': pos_id,
        'last_period': f'{last_7_days_start.strftime("%Y-%m-%d")} - {max_date.strftime("%Y-%m-%d")}',
        'last_period_spending': last_7_days_spending,
        'avg_spending': avg_spending,
        'decrease_percentage': ((avg_spending - last_7_days_spending) / avg_spending) * 100 if avg_spending > 0 else 0,
        'alert_type': alert_type,
        'alert_description': alert_description,
        'risk_level': risk_level,
        'total_periods': len(periods)
    }

def consolidate_all_alerts(pos_with_alerts, pos_with_spending_alerts, pos_with_orders_alerts):
    """
    Consolida todas las alertas y calcula ranking de gravedad basado en alertas críticas
    """
    # Crear diccionario para consolidar alertas por POS
    consolidated = {}
    
    # Procesar alertas de proveedores
    for alert in pos_with_alerts:
        pos_id = alert['pos_id']
        if pos_id not in consolidated:
            consolidated[pos_id] = {
                'pos_id': pos_id,
                'alertas_criticas': 0,
                'total_alertas': 0,
                'alerta_proveedores': None,
                'alerta_gasto': None,
                'alerta_ordenes': None
            }
        
        consolidated[pos_id]['alerta_proveedores'] = alert
        consolidated[pos_id]['total_alertas'] += 1
        if 'CRITICO' in alert['risk_level']:
            consolidated[pos_id]['alertas_criticas'] += 1
    
    # Procesar alertas de gasto
    for alert in pos_with_spending_alerts:
        pos_id = alert['pos_id']
        if pos_id not in consolidated:
            consolidated[pos_id] = {
                'pos_id': pos_id,
                'alertas_criticas': 0,
                'total_alertas': 0,
                'alerta_proveedores': None,
                'alerta_gasto': None,
                'alerta_ordenes': None
            }
        
        consolidated[pos_id]['alerta_gasto'] = alert
        consolidated[pos_id]['total_alertas'] += 1
        if 'CRITICO' in alert['risk_level']:
            consolidated[pos_id]['alertas_criticas'] += 1
    
    # Procesar alertas de órdenes
    for alert in pos_with_orders_alerts:
        pos_id = alert['pos_id']
        if pos_id not in consolidated:
            consolidated[pos_id] = {
                'pos_id': pos_id,
                'alertas_criticas': 0,
                'total_alertas': 0,
                'alerta_proveedores': None,
                'alerta_gasto': None,
                'alerta_ordenes': None
            }
        
        consolidated[pos_id]['alerta_ordenes'] = alert
        consolidated[pos_id]['total_alertas'] += 1
        if 'CRITICO' in alert['risk_level']:
            consolidated[pos_id]['alertas_criticas'] += 1
    
    # Convertir a lista y ordenar por gravedad (alertas críticas primero, luego total de alertas)
    result = list(consolidated.values())
    result.sort(key=lambda x: (-x['alertas_criticas'], -x['total_alertas']))
    
    return result


def create_unified_alerts_dataframe(consolidated_alerts, pos_name_mapping=None):
    """
    Crea un DataFrame unificado para descarga con todas las alertas
    """
    unified_data = []
    
    for pos_alert in consolidated_alerts:
        pos_id = pos_alert['pos_id']
        alertas_criticas = pos_alert['alertas_criticas']
        total_alertas = pos_alert['total_alertas']
        
        # Obtener nombre del POS si está disponible
        pos_name = pos_name_mapping.get(pos_id, f"POS {pos_id}") if pos_name_mapping else f"POS {pos_id}"
        
        # Determinar nivel de prioridad
        if alertas_criticas >= 3:
            prioridad = "🚨 MÁXIMA"
        elif alertas_criticas == 2:
            prioridad = "🔴 ALTA"
        elif alertas_criticas == 1:
            prioridad = "🟠 MEDIA"
        else:
            prioridad = "🟡 BAJA"
        
        # Información de alertas de proveedores
        prov_tipo = prov_nivel = prov_desc = ""
        if pos_alert['alerta_proveedores']:
            prov_alert = pos_alert['alerta_proveedores']
            prov_tipo = prov_alert['alert_type']
            prov_nivel = prov_alert['risk_level']
            prov_desc = prov_alert['alert_description']
        
        # Información de alertas de gasto
        gasto_tipo = gasto_nivel = gasto_desc = gasto_valor = gasto_dismin = ""
        if pos_alert['alerta_gasto']:
            gasto_alert = pos_alert['alerta_gasto']
            gasto_tipo = gasto_alert['alert_type']
            gasto_nivel = gasto_alert['risk_level']
            gasto_desc = gasto_alert['alert_description']
            gasto_valor = f"${gasto_alert['last_period_spending']:,.0f}"
            gasto_dismin = f"{gasto_alert['decrease_percentage']:.1f}%"
        
        # Información de alertas de órdenes
        ord_tipo = ord_nivel = ord_desc = ord_valor = ord_dismin = ""
        if pos_alert['alerta_ordenes']:
            ord_alert = pos_alert['alerta_ordenes']
            ord_tipo = ord_alert['alert_type']
            ord_nivel = ord_alert['risk_level']
            ord_desc = ord_alert['alert_description']
            ord_valor = f"{ord_alert['last_period_orders']:,.0f}"
            ord_dismin = f"{ord_alert['decrease_percentage']:.1f}%"
        
        unified_data.append({
            'POS_ID': pos_id,
            'POS_NAME': pos_name,
            'PRIORIDAD': prioridad,
            'ALERTAS_CRITICAS': alertas_criticas,
            'TOTAL_ALERTAS': total_alertas,
            'PROV_TIPO_ALERTA': prov_tipo,
            'PROV_NIVEL_RIESGO': prov_nivel,
            'PROV_DESCRIPCION': prov_desc,
            'GASTO_TIPO_ALERTA': gasto_tipo,
            'GASTO_NIVEL_RIESGO': gasto_nivel,
            'GASTO_ULTIMO_PERIODO': gasto_valor,
            'GASTO_DISMINUCION': gasto_dismin,
            'GASTO_DESCRIPCION': gasto_desc,
            'ORD_TIPO_ALERTA': ord_tipo,
            'ORD_NIVEL_RIESGO': ord_nivel,
            'ORD_ULTIMO_PERIODO': ord_valor,
            'ORD_DISMINUCION': ord_dismin,
            'ORD_DESCRIPCION': ord_desc
        })
    
    return pd.DataFrame(unified_data)

def analyze_orders_trends(df_orders, pos_id):
    """
    Analiza las tendencias de número de órdenes de un POS basado en:
    1. Cero órdenes en últimos 7 días
    2. Disminución mayor al 50% respecto al promedio histórico
    3. Disminución mayor al 30% respecto al promedio histórico
    """
    if df_orders.empty or 'order_date' not in df_orders.columns:
        return None
    
    # Filtrar datos del POS
    pos_data = df_orders[df_orders['point_of_sale_id'] == pos_id].copy()
    
    if pos_data.empty:
        return None
    
    # Asegurar que order_date es datetime
    pos_data['order_date'] = pd.to_datetime(pos_data['order_date'], errors='coerce')
    pos_data = pos_data.dropna(subset=['order_date'])
    
    if pos_data.empty:
        return None
    
    # Obtener fecha máxima en los datos
    max_date = pos_data['order_date'].max()
    
    # Calcular fecha de inicio de últimos 7 días
    last_7_days_start = max_date - pd.Timedelta(days=6)  # Incluye el día actual
    
    # Filtrar datos de últimos 7 días y contar órdenes
    last_7_days_data = pos_data[pos_data['order_date'] >= last_7_days_start]
    last_7_days_orders = len(last_7_days_data)
    
    # Calcular promedio histórico de órdenes por período de 7 días (excluyendo últimos 7 días)
    historical_data = pos_data[pos_data['order_date'] < last_7_days_start]
    
    if historical_data.empty:
        return None
    
    # Agrupar datos históricos en períodos de 7 días
    historical_data = historical_data.sort_values('order_date')
    min_historical_date = historical_data['order_date'].min()
    
    # Crear períodos de 7 días
    periods = []
    current_date = min_historical_date
    
    while current_date < last_7_days_start:
        period_end = current_date + pd.Timedelta(days=6)
        period_data = historical_data[
            (historical_data['order_date'] >= current_date) & 
            (historical_data['order_date'] <= period_end)
        ]
        
        periods.append({
            'start_date': current_date,
            'end_date': period_end,
            'total_orders': len(period_data)
        })
        
        current_date += pd.Timedelta(days=7)
    
    if not periods:
        return None
    
    # Calcular promedio de órdenes por período de 7 días
    avg_orders = sum(p['total_orders'] for p in periods) / len(periods)
    
    # Determinar nivel de alerta
    alert_type = None
    alert_description = None
    risk_level = '🟢 NORMAL'
    
    # CRITERIO 1: Cero órdenes en últimos 7 días
    if last_7_days_orders == 0:
        alert_type = 'ORDENES_CERO'
        alert_description = f'POS sin órdenes en los últimos 7 días ({last_7_days_start.strftime("%Y-%m-%d")} - {max_date.strftime("%Y-%m-%d")})'
        risk_level = '🔴 CRITICO'
    
    # CRITERIO 2: Disminución mayor al 50% respecto al promedio
    elif avg_orders > 0:
        decrease_percentage = ((avg_orders - last_7_days_orders) / avg_orders) * 100
        
        if decrease_percentage >= 50:
            alert_type = 'ORDENES_DISMINUCION_CRITICA'
            alert_description = f'Disminución del {decrease_percentage:.1f}% en número de órdenes respecto al promedio histórico'
            risk_level = '🟠 ALTO'
        elif decrease_percentage >= 30:
            alert_type = 'ORDENES_DISMINUCION_MODERADA'
            alert_description = f'Disminución del {decrease_percentage:.1f}% en número de órdenes respecto al promedio histórico'
            risk_level = '🟡 MODERADO'
    
    return {
        'pos_id': pos_id,
        'last_period': f'{last_7_days_start.strftime("%Y-%m-%d")} - {max_date.strftime("%Y-%m-%d")}',
        'last_period_orders': last_7_days_orders,
        'avg_orders': avg_orders,
        'decrease_percentage': ((avg_orders - last_7_days_orders) / avg_orders) * 100 if avg_orders > 0 else 0,
        'alert_type': alert_type,
        'alert_description': alert_description,
        'risk_level': risk_level,
        'total_periods': len(periods)
    }

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
    """Crea un grafico de evolucion semanal de distribucion de proveedores"""
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

def create_weekly_orders_money_chart(df_orders, selected_pos):
    """Crea un grafico de evolucion semanal con doble eje: órdenes y dinero"""
    # Filtrar datos del POS seleccionado
    pos_orders = df_orders[df_orders['point_of_sale_id'] == selected_pos].copy()
    
    if pos_orders.empty or 'order_date' not in pos_orders.columns:
        return None

    # Crear columna de semana en los datos de órdenes
    pos_orders['week'] = pos_orders['order_date'].dt.to_period('W').astype(str)
    
    # Calcular totales semanales
    weekly_summary = pos_orders.groupby('week').agg({
        'total_compra': 'sum',
        'point_of_sale_id': 'count'  # contar órdenes
    }).reset_index()
    weekly_summary.columns = ['week', 'dinero_total', 'numero_ordenes']

    # Ordenar por semana
    weekly_summary = weekly_summary.sort_values('week')

    if weekly_summary.empty:
        return None

    # Crear figura con subplots y doble eje Y
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Agregar línea de dinero (eje izquierdo)
    fig.add_trace(
        go.Scatter(
            x=weekly_summary['week'],
            y=weekly_summary['dinero_total'],
            mode='lines+markers',
            name='Dinero Total ($)',
            line=dict(color='#1f77b4', width=3),
            marker=dict(size=8),
            hovertemplate='<b>Semana:</b> %{x}<br><b>Dinero:</b> $%{y:,.0f}<extra></extra>'
        ),
        secondary_y=False,
    )

    # Agregar línea de órdenes (eje derecho)
    fig.add_trace(
        go.Scatter(
            x=weekly_summary['week'],
            y=weekly_summary['numero_ordenes'],
            mode='lines+markers',
            name='Número de Órdenes',
            line=dict(color='#ff7f0e', width=3),
            marker=dict(size=8),
            hovertemplate='<b>Semana:</b> %{x}<br><b>Órdenes:</b> %{y}<extra></extra>'
        ),
        secondary_y=True,
    )

    # Configurar títulos de ejes
    fig.update_xaxes(title_text="Semana")
    fig.update_yaxes(title_text="<b>Dinero Total ($)</b>", secondary_y=False)
    fig.update_yaxes(title_text="<b>Número de Órdenes</b>", secondary_y=True)

    # Configurar layout
    fig.update_layout(
        title=f'Evolución Semanal: Órdenes y Dinero - POS {selected_pos}',
        hovermode='x unified',
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.2,
            xanchor="center",
            x=0.5
        ),
        height=500
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

# Cargar datos
with st.spinner('Cargando datos...'):
    df_orders = load_and_process_data()

if df_orders.empty:
    st.error("No se pudieron cargar los datos. Verifica la conexión a la base de datos.")
    st.stop()

# Calcular metricas
pos_vendor_totals = calculate_pos_vendor_totals(df_orders)
weekly_distribution = calculate_weekly_distribution(df_orders)

st.markdown("---")

# ============================================
# SECCION 1: ANALISIS DE RIESGO Y ALERTAS - SOLO POS ACTIVOS
# ============================================
st.header("⚠️ Analisis de Riesgo y Alertas - POS Activos (últimos 10 días)")

# Obtener lista de POS activos en los últimos 10 días
if not df_orders.empty and 'order_date' in df_orders.columns:
    fecha_maxima = df_orders['order_date'].max()
    fecha_limite = fecha_maxima - pd.Timedelta(days=10)
    ordenes_recientes = df_orders[df_orders['order_date'] >= fecha_limite]
    pos_activos_recientes = sorted(ordenes_recientes['point_of_sale_id'].unique())
else:
    pos_activos_recientes = sorted(weekly_distribution['point_of_sale_id'].unique())

all_alerts = []
all_spending_alerts = []
all_orders_alerts = []

with st.spinner(f'Analizando riesgo en {len(pos_activos_recientes)} POS activos...'):
    for pos_id in pos_activos_recientes:
        # Analisis de riesgo de proveedores
        risk_analysis = analyze_vendor_risk(weekly_distribution, pos_id)
        if risk_analysis:
            all_alerts.append(risk_analysis)
        
        # Analisis de tendencias de gasto
        spending_analysis = analyze_spending_trends(df_orders, pos_id)
        if spending_analysis:
            all_spending_alerts.append(spending_analysis)
        
        # Analisis de tendencias de órdenes
        orders_analysis = analyze_orders_trends(df_orders, pos_id)
        if orders_analysis:
            all_orders_alerts.append(orders_analysis)

# Filtrar solo POS con alertas
pos_with_alerts = [alert for alert in all_alerts if alert['alert_type'] is not None]
pos_with_spending_alerts = [alert for alert in all_spending_alerts if alert['alert_type'] is not None]
pos_with_orders_alerts = [alert for alert in all_orders_alerts if alert['alert_type'] is not None]

# Consolidar todas las alertas y calcular ranking de gravedad
consolidated_alerts = consolidate_all_alerts(pos_with_alerts, pos_with_spending_alerts, pos_with_orders_alerts)


# ============================================
# SECCIÓN PRIORITARIA: POS CON MÚLTIPLES ALERTAS CRÍTICAS
# ============================================

# Filtrar POS con alertas críticas para sección prioritaria
critical_pos = [alert for alert in consolidated_alerts if alert['alertas_criticas'] > 0]

if critical_pos:
    st.header("🚨 ALERTA PRIORITARIA - POS EN RIESGO CRÍTICO")
    
    # Mostrar estadísticas principales de POS críticos
    max_critical = critical_pos[0]['alertas_criticas'] if critical_pos else 0
    pos_3_critical = len([p for p in critical_pos if p['alertas_criticas'] >= 3])
    pos_2_critical = len([p for p in critical_pos if p['alertas_criticas'] == 2])
    pos_1_critical = len([p for p in critical_pos if p['alertas_criticas'] == 1])
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(
            "🚨 Máxima Prioridad",
            pos_3_critical,
            delta="3 alertas críticas" if pos_3_critical > 0 else None,
            delta_color="inverse" if pos_3_critical > 0 else "off",
            help="POS que tienen las 3 alertas críticas simultáneamente: monopolio de proveedores + gasto cero + órdenes cero. Requieren intervención inmediata."
        )
    with col2:
        st.metric(
            "🔴 Alta Prioridad", 
            pos_2_critical,
            delta="2 alertas críticas" if pos_2_critical > 0 else None,
            delta_color="inverse" if pos_2_critical > 0 else "off",
            help="POS que tienen exactamente 2 alertas críticas de las 3 posibles (proveedores, gasto, órdenes). Alto riesgo de churn."
        )
    with col3:
        st.metric(
            "🟠 Media Prioridad",
            pos_1_critical,
            delta="1 alerta crítica" if pos_1_critical > 0 else None,
            delta_color="inverse" if pos_1_critical > 0 else "off",
            help="POS que tienen exactamente 1 alerta crítica. Situación preocupante que requiere monitoreo estrecho."
        )
    with col4:
        st.metric(
            "Total POS en Riesgo",
            len(critical_pos),
            help="Número total de POS que tienen al menos 1 alerta crítica activa. Universo de POS que requieren atención."
        )
    
    # Tabla de POS críticos ordenados por gravedad
    st.markdown("### 🎯 Ranking de POS por Gravedad (Acción Inmediata Requerida)")
    
    # Crear tabla resumida de POS críticos
    critical_summary = []
    for pos_alert in critical_pos[:10]:  # Mostrar top 10 más críticos
        pos_id = pos_alert['pos_id']
        alertas_criticas = pos_alert['alertas_criticas']
        total_alertas = pos_alert['total_alertas']
        
        # Determinar prioridad
        if alertas_criticas >= 3:
            prioridad = "🚨 MÁXIMA"
        elif alertas_criticas == 2:
            prioridad = "🔴 ALTA"
        else:
            prioridad = "🟠 MEDIA"
        
        # Recopilar tipos de alertas críticas
        alertas_criticas_tipos = []
        if pos_alert['alerta_proveedores'] and 'CRITICO' in pos_alert['alerta_proveedores']['risk_level']:
            alertas_criticas_tipos.append("🏪 Proveedores")
        if pos_alert['alerta_gasto'] and 'CRITICO' in pos_alert['alerta_gasto']['risk_level']:
            alertas_criticas_tipos.append("💰 Gasto")
        if pos_alert['alerta_ordenes'] and 'CRITICO' in pos_alert['alerta_ordenes']['risk_level']:
            alertas_criticas_tipos.append("📦 Órdenes")
        
        # Recopilar tipos de alertas no críticas
        alertas_no_criticas_tipos = []
        if pos_alert['alerta_proveedores'] and 'CRITICO' not in pos_alert['alerta_proveedores']['risk_level']:
            if 'ALTO' in pos_alert['alerta_proveedores']['risk_level']:
                alertas_no_criticas_tipos.append("🟠 Proveedores")
            elif 'MODERADO' in pos_alert['alerta_proveedores']['risk_level']:
                alertas_no_criticas_tipos.append("🟡 Proveedores")
        if pos_alert['alerta_gasto'] and 'CRITICO' not in pos_alert['alerta_gasto']['risk_level']:
            if 'ALTO' in pos_alert['alerta_gasto']['risk_level']:
                alertas_no_criticas_tipos.append("🟠 Gasto")
            elif 'MODERADO' in pos_alert['alerta_gasto']['risk_level']:
                alertas_no_criticas_tipos.append("🟡 Gasto")
        if pos_alert['alerta_ordenes'] and 'CRITICO' not in pos_alert['alerta_ordenes']['risk_level']:
            if 'ALTO' in pos_alert['alerta_ordenes']['risk_level']:
                alertas_no_criticas_tipos.append("🟠 Órdenes")
            elif 'MODERADO' in pos_alert['alerta_ordenes']['risk_level']:
                alertas_no_criticas_tipos.append("🟡 Órdenes")
        
        critical_summary.append({
            'Ranking': len(critical_summary) + 1,
            'POS ID': pos_id,
            'POS Name': pos_name_mapping.get(pos_id, f"POS {pos_id}"),
            'Prioridad': prioridad,
            'Alertas Críticas': alertas_criticas,
            'Total Alertas': total_alertas,
            'Tipos de Alertas Críticas': " | ".join(alertas_criticas_tipos) if alertas_criticas_tipos else "Sin alertas críticas",
            'Alertas No Críticas': " | ".join(alertas_no_criticas_tipos) if alertas_no_criticas_tipos else "Sin alertas no críticas"
        })
    
    critical_df = pd.DataFrame(critical_summary)
    
    # Mostrar tabla con formato especial para POS críticos
    st.dataframe(
        critical_df.style.apply(lambda x: [
            'background-color: #ffcdd2; font-weight: bold' if v == '🚨 MÁXIMA' 
            else 'background-color: #ffebee; font-weight: bold' if v == '🔴 ALTA'
            else 'background-color: #fff3e0' if v == '🟠 MEDIA'
            else '' for v in x
        ], subset=['Prioridad']),
        use_container_width=True,
        hide_index=True,
        height=400
    )
    
    # Botón de descarga unificado - sección prioritaria
    if consolidated_alerts:
        unified_df = create_unified_alerts_dataframe(consolidated_alerts, pos_name_mapping)
        csv_unified = unified_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 DESCARGAR TODAS LAS ALERTAS (Ranking Completo)",
            data=csv_unified,
            file_name=f"alertas_consolidadas_ranking_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            help="Descarga un archivo CSV con todas las alertas consolidadas, ordenadas por prioridad y gravedad",
            use_container_width=True
        )
    
    st.markdown("---")

# Metricas generales de alertas
st.subheader("📊 Resumen General de Alertas")

# Crear tabs para separar tipos de alertas
tab1, tab2, tab3 = st.tabs(["🏪 Alertas de Proveedores", "💰 Alertas de Gasto", "📦 Alertas de Órdenes"])

with tab1:
    col1, col2, col3, col4 = st.columns(4)
    
    # Total de POS analizados (ya filtrados)
    total_pos_analyzed = len(pos_activos_recientes)
    total_with_alerts = len(pos_with_alerts)
    critico_count = len([a for a in pos_with_alerts if a['risk_level'] == '🔴 CRITICO'])
    alto_count = len([a for a in pos_with_alerts if a['risk_level'] == '🟠 ALTO'])
    moderado_count = len([a for a in pos_with_alerts if a['risk_level'] == '🟡 MODERADO'])

    with col1:
        st.metric(
            "Total POS Analizados", 
            total_pos_analyzed,
            help="Total de POS que tuvieron suficientes datos históricos para ser analizados en alertas de proveedores."
        )
    with col2:
        st.metric(
            "🔴 Critico",
            critico_count,
            delta="Monopolio" if critico_count > 0 else None,
            delta_color="inverse" if critico_count > 0 else "off",
            help="POS que dependen de un solo proveedor (monopolio). Riesgo crítico de desabastecimiento si el proveedor falla."
        )
    with col3:
        st.metric(
            "🟠 Alto",
            alto_count,
            delta="Concentracion" if alto_count > 0 else None,
            delta_color="inverse" if alto_count > 0 else "off",
            help="POS que redujeron de 3+ proveedores a 2, con concentración >50% en el principal. Riesgo de monopolización."
        )
    with col4:
        st.metric(
            "🟡 Moderado",
            moderado_count,
            delta="Monitorear" if moderado_count > 0 else None,
            delta_color="off",
            help="POS con 2 proveedores pero concentración >70% en uno. Situación que requiere monitoreo para evitar monopolio."
        )

with tab2:
    col1, col2, col3, col4 = st.columns(4)
    
    # Total de POS analizados para gasto (ya filtrados)
    total_spending_analyzed = len(pos_activos_recientes)
    total_with_spending_alerts = len(pos_with_spending_alerts)
    gasto_cero_count = len([a for a in pos_with_spending_alerts if a['alert_type'] == 'GASTO_CERO'])
    disminucion_critica_count = len([a for a in pos_with_spending_alerts if a['alert_type'] == 'DISMINUCION_CRITICA'])
    disminucion_moderada_count = len([a for a in pos_with_spending_alerts if a['alert_type'] == 'DISMINUCION_MODERADA'])
    
    with col1:
        st.metric(
            "Total POS Analizados", 
            total_spending_analyzed,
            help="Total de POS que tuvieron suficientes datos históricos para análisis de tendencias de gasto."
        )
    with col2:
        st.metric(
            "🔴 Gasto Cero",
            gasto_cero_count,
            delta="Sin actividad" if gasto_cero_count > 0 else None,
            delta_color="inverse" if gasto_cero_count > 0 else "off",
            help="POS que no registraron ningún gasto en los últimos 7 días. Indicador fuerte de churn o inactividad."
        )
    with col3:
        st.metric(
            "🟠 Disminución Crítica",
            disminucion_critica_count,
            delta=">50%" if disminucion_critica_count > 0 else None,
            delta_color="inverse" if disminucion_critica_count > 0 else "off",
            help="POS cuyo gasto en los últimos 7 días es menos del 50% de su promedio histórico. Señal de riesgo alto."
        )
    with col4:
        st.metric(
            "🟡 Disminución Moderada",
            disminucion_moderada_count,
            delta="30-50%" if disminucion_moderada_count > 0 else None,
            delta_color="off",
            help="POS con gasto 30-50% menor al promedio histórico. Requiere monitoreo para evitar deterioro mayor."
        )

# Tablas de alertas por tipo
if pos_with_alerts or pos_with_spending_alerts or pos_with_orders_alerts:
    st.subheader("🚨 Lista de POS con Alertas Detectadas")
    
    tab1_table, tab2_table, tab3_table = st.tabs(["🏪 Alertas de Proveedores", "💰 Alertas de Gasto", "📦 Alertas de Órdenes"])
    
    with tab1_table:
        if pos_with_alerts:
            st.write(f"**{len(pos_with_alerts)} POS con alertas de proveedores:**")
            
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
                    'POS Name': pos_name_mapping.get(alert['pos_id'], f"POS {alert['pos_id']}"),
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

        else:
            st.success("✅ No se detectaron alertas de proveedores.")
    
    with tab2_table:
        if pos_with_spending_alerts:
            st.write(f"**{len(pos_with_spending_alerts)} POS con alertas de gasto:**")
            
            # Crear DataFrame para alertas de gasto
            spending_alerts_data = []
            for alert in pos_with_spending_alerts:
                spending_alerts_data.append({
                    'POS ID': alert['pos_id'],
                    'POS Name': pos_name_mapping.get(alert['pos_id'], f"POS {alert['pos_id']}"),
                    'Nivel Riesgo': alert['risk_level'],
                    'Tipo Alerta': alert['alert_type'],
                    'Ultimo Periodo': alert['last_period'],
                    'Gasto Ultimos 7 Dias': alert['last_period_spending'],
                    'Promedio Historico': alert['avg_spending'],
                    'Disminucion (%)': alert['decrease_percentage'],
                    'Periodos Analizados': alert['total_periods'],
                    'Descripcion': alert['alert_description']
                })

            spending_alerts_df = pd.DataFrame(spending_alerts_data)

            # Ordenar por nivel de riesgo y disminucion
            risk_order = {'🔴 CRITICO': 0, '🟠 ALTO': 1, '🟡 MODERADO': 2}
            spending_alerts_df['risk_order'] = spending_alerts_df['Nivel Riesgo'].map(risk_order)
            spending_alerts_df = spending_alerts_df.sort_values(['risk_order', 'Disminucion (%)'], ascending=[True, False]).drop('risk_order', axis=1)

            # Mostrar tabla con formato
            st.dataframe(
                spending_alerts_df.style.format({
                    'Gasto Ultimos 7 Dias': '${:,.2f}',
                    'Promedio Historico': '${:,.2f}',
                    'Disminucion (%)': '{:.1f}%'
                }).apply(lambda x: ['background-color: #ffebee' if v == '🔴 CRITICO'
                                    else 'background-color: #fff3e0' if v == '🟠 ALTO'
                                    else 'background-color: #fffde7' if v == '🟡 MODERADO'
                                    else '' for v in x], subset=['Nivel Riesgo']),
                use_container_width=True,
                hide_index=True,
                height=400
            )

        else:
            st.success("✅ No se detectaron alertas de gasto.")
    
    with tab3_table:
        if pos_with_orders_alerts:
            st.write(f"**{len(pos_with_orders_alerts)} POS con alertas de órdenes:**")
            
            # Crear DataFrame para alertas de órdenes
            orders_alerts_data = []
            for alert in pos_with_orders_alerts:
                orders_alerts_data.append({
                    'POS ID': alert['pos_id'],
                    'POS Name': pos_name_mapping.get(alert['pos_id'], f"POS {alert['pos_id']}"),
                    'Nivel Riesgo': alert['risk_level'],
                    'Tipo Alerta': alert['alert_type'],
                    'Ultimo Periodo': alert['last_period'],
                    'Órdenes Últimos 7 Días': alert['last_period_orders'],
                    'Promedio Histórico': alert['avg_orders'],
                    'Disminución (%)': alert['decrease_percentage'],
                    'Períodos Analizados': alert['total_periods'],
                    'Descripción': alert['alert_description']
                })

            orders_alerts_df = pd.DataFrame(orders_alerts_data)

            # Ordenar por nivel de riesgo y disminución
            risk_order = {'🔴 CRITICO': 0, '🟠 ALTO': 1, '🟡 MODERADO': 2}
            orders_alerts_df['risk_order'] = orders_alerts_df['Nivel Riesgo'].map(risk_order)
            orders_alerts_df = orders_alerts_df.sort_values(['risk_order', 'Disminución (%)'], ascending=[True, False]).drop('risk_order', axis=1)

            # Mostrar tabla con formato
            st.dataframe(
                orders_alerts_df.style.format({
                    'Órdenes Últimos 7 Días': '{:,.0f}',
                    'Promedio Histórico': '{:.1f}',
                    'Disminución (%)': '{:.1f}%'
                }).apply(lambda x: ['background-color: #ffebee' if v == '🔴 CRITICO'
                                    else 'background-color: #fff3e0' if v == '🟠 ALTO'
                                    else 'background-color: #fffde7' if v == '🟡 MODERADO'
                                    else '' for v in x], subset=['Nivel Riesgo']),
                use_container_width=True,
                hide_index=True,
                height=400
            )

        else:
            st.success("✅ No se detectaron alertas de órdenes.")

# Botón de descarga unificado al final de las tablas
if pos_with_alerts or pos_with_spending_alerts or pos_with_orders_alerts:
    st.markdown("### 📥 Descarga Consolidada")
    st.markdown("**Descarga todas las alertas en un solo archivo con ranking de prioridad:**")
    
    unified_df = create_unified_alerts_dataframe(consolidated_alerts, pos_name_mapping)
    csv_unified = unified_df.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="📥 DESCARGAR REPORTE COMPLETO DE ALERTAS",
        data=csv_unified,
        file_name=f"reporte_alertas_completo_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
        help="Archivo CSV con todas las alertas consolidadas, incluyendo ranking de prioridad y detalles completos",
        use_container_width=True
    )

with tab3:
    col1, col2, col3, col4 = st.columns(4)
    
    # Total de POS analizados para órdenes (ya filtrados)
    total_orders_analyzed = len(pos_activos_recientes)
    total_with_orders_alerts = len(pos_with_orders_alerts)
    ordenes_cero_count = len([a for a in pos_with_orders_alerts if a['alert_type'] == 'ORDENES_CERO'])
    ordenes_disminucion_critica_count = len([a for a in pos_with_orders_alerts if a['alert_type'] == 'ORDENES_DISMINUCION_CRITICA'])
    ordenes_disminucion_moderada_count = len([a for a in pos_with_orders_alerts if a['alert_type'] == 'ORDENES_DISMINUCION_MODERADA'])
    
    with col1:
        st.metric(
            "Total POS Analizados", 
            total_orders_analyzed,
            help="Total de POS que tuvieron suficientes datos históricos para análisis de tendencias de órdenes."
        )
    with col2:
        st.metric(
            "🔴 Órdenes Cero",
            ordenes_cero_count,
            delta="Sin órdenes" if ordenes_cero_count > 0 else None,
            delta_color="inverse" if ordenes_cero_count > 0 else "off",
            help="POS que no registraron ninguna orden en los últimos 7 días. Indicador directo de inactividad comercial."
        )
    with col3:
        st.metric(
            "🟠 Disminución Crítica",
            ordenes_disminucion_critica_count,
            delta=">50%" if ordenes_disminucion_critica_count > 0 else None,
            delta_color="inverse" if ordenes_disminucion_critica_count > 0 else "off",
            help="POS cuyo número de órdenes en los últimos 7 días es menos del 50% de su promedio histórico."
        )
    with col4:
        st.metric(
            "🟡 Disminución Moderada",
            ordenes_disminucion_moderada_count,
            delta="30-50%" if ordenes_disminucion_moderada_count > 0 else None,
            delta_color="off",
            help="POS con 30-50% menos órdenes que su promedio histórico. Señal temprana de reducción en actividad."
        )

# Descripcion del analisis al final
with st.expander("📖 Criterios de Alerta", expanded=False):
    st.markdown("""
    **Sistema de Alertas Basado en Evolucion Temporal**

    ### 🏪 Alertas de Proveedores:
    **🔴 CRITICO - MONOPOLIO:**
    - El POS tiene solo **1 drogueria** en la ultima semana del periodo

    **🟠 ALTO - CONCENTRACION:**
    - El POS paso de **3+ proveedores a 2** proveedores
    - Y la diferencia entre el proveedor principal y el secundario es **mayor al 50%**

    **🟡 MODERADO - MONITOREAR:**
    - El POS tiene 2 proveedores con concentracion mayor al 70% en uno

    **🟢 BAJO - SIN RIESGO:**
    - Cualquier otra situacion indica diversificacion saludable
    
    ### 💰 Alertas de Gasto:
    **🔴 CRITICO - GASTO CERO:**
    - El POS **no tuvo actividad de compra** en los últimos 7 días

    **🟠 ALTO - DISMINUCION CRITICA:**
    - El gasto de los últimos 7 días es **menor al 50%** del promedio histórico

    **🟡 MODERADO - DISMINUCION MODERADA:**
    - El gasto de los últimos 7 días es **menor al 30%** del promedio histórico

    **🟢 NORMAL:**
    - El gasto se mantiene dentro de rangos normales
    
    ### 📦 Alertas de Órdenes:
    **🔴 CRITICO - ÓRDENES CERO:**
    - El POS **no tuvo órdenes** en los últimos 7 días

    **🟠 ALTO - DISMINUCIÓN CRÍTICA:**
    - El número de órdenes de los últimos 7 días es **menor al 50%** del promedio histórico

    **🟡 MODERADO - DISMINUCIÓN MODERADA:**
    - El número de órdenes de los últimos 7 días es **menor al 30%** del promedio histórico

    **🟢 NORMAL:**
    - El número de órdenes se mantiene dentro de rangos normales
    """)

st.markdown("---")

# ============================================
# SELECTOR PRINCIPAL DE POS Y FILTROS
# ============================================
st.header("🎯 Seleccion de POS y Filtros")

col1, col2 = st.columns([2, 3])

with col1:
    # Crear mapeo de POS ID a nombre para visualización
    if 'pos_name' in df_orders.columns:
        pos_name_mapping = df_orders[['point_of_sale_id', 'pos_name']].drop_duplicates().set_index('point_of_sale_id')['pos_name'].to_dict()
    else:
        pos_name_mapping = {}
    
    # Usar la misma lista de POS activos ya calculada
    pos_list_completa = sorted(pos_vendor_totals['point_of_sale_id'].unique())
    pos_list = pos_activos_recientes
    
    # Crear opciones para selectbox con formato "ID - Nombre"
    pos_options = []
    for pos_id in pos_list:
        pos_name = pos_name_mapping.get(pos_id, f"POS {pos_id}")
        pos_options.append(f"{pos_id} - {pos_name}")
    
    # Crear mapeo inverso para recuperar el ID desde la selección
    option_to_id = {option: pos_list[i] for i, option in enumerate(pos_options)}
    
    # Mostrar información del filtro
    st.caption(f"🔍 Filtro: Solo POS con órdenes en los últimos 10 días ({len(pos_list)} de {len(pos_list_completa)} POS)")
    
    # Selector principal de POS
    selected_option = st.selectbox(
        "Selecciona un POS para analizar:",
        pos_options,
        key='main_pos_selector',
        help="Este POS sera analizado en todas las secciones del dashboard"
    )
    
    # Recuperar el POS ID seleccionado
    selected_pos = option_to_id.get(selected_option, pos_list[0] if pos_list else None)
    
    # Mostrar fecha de última orden del POS seleccionado
    if selected_pos and not df_orders.empty:
        pos_orders = df_orders[df_orders['point_of_sale_id'] == selected_pos]
        if not pos_orders.empty and 'order_date' in pos_orders.columns:
            pos_orders_clean = pos_orders.dropna(subset=['order_date'])
            if not pos_orders_clean.empty:
                ultima_orden = pos_orders_clean['order_date'].max()
                st.caption(f"📅 Última orden: **{ultima_orden.strftime('%Y-%m-%d')}**")

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

# ============================================

# ============================================
# SECCION 3: GRAFICOS DE TORTA POR POS
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
# SECCION 4: EVOLUCION SEMANAL
# ============================================
st.header(f"📊 Análisis de Evolución Semanal - POS {selected_pos}")
st.caption("📈 Análisis temporal completo: distribución de proveedores, órdenes y montos")

if not weekly_distribution.empty:
    # ============================================
    # GRÁFICO 1: EVOLUCIÓN DE DISTRIBUCIÓN DE PROVEEDORES
    # ============================================
    st.subheader("📈 Evolución de Distribución por Proveedor")
    st.info("💡 **Explicación:** Muestra cómo cambia el porcentaje de compras a cada droguería semana a semana. Líneas ascendentes = aumento de compras a esa droguería; líneas descendentes = reducción. Útil para detectar monopolización o diversificación.")
    
    fig_providers = create_weekly_evolution_chart(weekly_distribution, selected_pos)
    if fig_providers:
        st.plotly_chart(fig_providers, use_container_width=True)
    
    st.markdown("---")
    
    # ============================================
    # GRÁFICO 2: EVOLUCIÓN DE ÓRDENES Y DINERO
    # ============================================
    st.subheader("📊 Evolución de Órdenes y Dinero")
    st.info("💡 **Explicación:** Gráfico de doble eje que muestra: **Línea azul** = dinero total gastado por semana (eje izquierdo), **Línea naranja** = número de órdenes por semana (eje derecho). Permite analizar correlación entre volumen y valor, detectar estacionalidades y cambios en patrones de compra.")
    
    fig_orders_money = create_weekly_orders_money_chart(df_orders, selected_pos)
    if fig_orders_money:
        st.plotly_chart(fig_orders_money, use_container_width=True)
    
    st.markdown("""
    **Nota:** Ambas visualizaciones muestran el período completo (no afectadas por el filtro de fechas)
    para facilitar la comparación de tendencias a largo plazo.
    """)
else:
    st.warning("No hay datos de fechas disponibles para mostrar la evolución semanal.")

st.markdown("---")

# ============================================
# SECCION 5: ANALISIS DETALLADO DEL POS SELECCIONADO
# ============================================
st.header(f"🔍 Analisis Detallado - POS {selected_pos}")

risk_analysis = analyze_vendor_risk(weekly_distribution, selected_pos)
spending_analysis = analyze_spending_trends(df_orders, selected_pos)
orders_analysis = analyze_orders_trends(df_orders, selected_pos)

if risk_analysis or spending_analysis or orders_analysis:
    # Crear tabs para separar los analisis
    tab_vendor, tab_spending, tab_orders = st.tabs(["🏪 Análisis de Proveedores", "💰 Análisis de Gasto", "📦 Análisis de Órdenes"])
    
    with tab_vendor:
        if risk_analysis and risk_analysis['alert_type']:
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
    
    with tab_spending:
        if spending_analysis and spending_analysis['alert_type']:
            spending_level = spending_analysis['risk_level']
            spending_color = '#ffebee' if 'CRITICO' in spending_level else '#fff3e0' if 'ALTO' in spending_level else '#fffde7'
            spending_border_color = '#d32f2f' if 'CRITICO' in spending_level else '#f57c00' if 'ALTO' in spending_level else '#fbc02d'

            st.markdown(f"""
            <div style="background-color: {spending_color}; padding: 20px; border-radius: 10px; border-left: 5px solid {spending_border_color};">
                <h3 style="margin: 0; color: #333;">{spending_level} - {spending_analysis['alert_type']}</h3>
                <p style="margin: 10px 0 0 0; color: #555; font-size: 16px;">{spending_analysis['alert_description']}</p>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
            <div style="background-color: #e8f5e9; padding: 20px; border-radius: 10px; border-left: 5px solid #388e3c;">
                <h3 style="margin: 0; color: #333;">🟢 NORMAL - SIN ALERTA</h3>
                <p style="margin: 10px 0 0 0; color: #555; font-size: 16px;">El POS mantiene un gasto normal respecto al promedio historico</p>
            </div>
            """, unsafe_allow_html=True)
    
    with tab_orders:
        if orders_analysis and orders_analysis['alert_type']:
            orders_level = orders_analysis['risk_level']
            orders_color = '#ffebee' if 'CRITICO' in orders_level else '#fff3e0' if 'ALTO' in orders_level else '#fffde7'
            orders_border_color = '#d32f2f' if 'CRITICO' in orders_level else '#f57c00' if 'ALTO' in orders_level else '#fbc02d'

            st.markdown(f"""
            <div style="background-color: {orders_color}; padding: 20px; border-radius: 10px; border-left: 5px solid {orders_border_color};">
                <h3 style="margin: 0; color: #333;">{orders_level} - {orders_analysis['alert_type']}</h3>
                <p style="margin: 10px 0 0 0; color: #555; font-size: 16px;">{orders_analysis['alert_description']}</p>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
            <div style="background-color: #e8f5e9; padding: 20px; border-radius: 10px; border-left: 5px solid #388e3c;">
                <h3 style="margin: 0; color: #333;">🟢 NORMAL - SIN ALERTA</h3>
                <p style="margin: 10px 0 0 0; color: #555; font-size: 16px;">El POS mantiene un número normal de órdenes respecto al promedio histórico</p>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("###")

    # Crear tabs para metricas detalladas
    tab_vendor_metrics, tab_spending_metrics, tab_orders_metrics = st.tabs(["🏪 Métricas de Proveedores", "💰 Métricas de Gasto", "📦 Métricas de Órdenes"])
    
    with tab_vendor_metrics:
        if risk_analysis:
            # Metricas comparativas de proveedores
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
        else:
            st.info("No hay datos suficientes para el análisis de proveedores")
    
    with tab_spending_metrics:
        if spending_analysis:
            # Metricas de gasto
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric(
                    "Gasto Últimos 7 Días",
                    f"${spending_analysis['last_period_spending']:,.0f}",
                    help=f"Periodo {spending_analysis['last_period']}"
                )
            with col2:
                st.metric(
                    "Promedio Histórico",
                    f"${spending_analysis['avg_spending']:,.0f}",
                    help="Promedio de períodos de 7 días históricos"
                )
            with col3:
                decrease_pct = spending_analysis['decrease_percentage']
                st.metric(
                    "Variación (%)",
                    f"{decrease_pct:.1f}%",
                    delta="Crítico" if decrease_pct >= 50 else "Moderado" if decrease_pct >= 30 else "Normal",
                    delta_color="inverse" if decrease_pct >= 30 else "off",
                    help="Disminución respecto al promedio"
                )
            with col4:
                st.metric(
                    "Períodos Analizados",
                    spending_analysis['total_periods'],
                    help="Total de períodos de 7 días analizados"
                )
        else:
            st.info("No hay datos suficientes para el análisis de gasto")
    
    with tab_orders_metrics:
        if orders_analysis:
            # Métricas de órdenes
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric(
                    "Órdenes Últimos 7 Días",
                    f"{orders_analysis['last_period_orders']:,.0f}",
                    help=f"Periodo {orders_analysis['last_period']}"
                )
            with col2:
                st.metric(
                    "Promedio Histórico",
                    f"{orders_analysis['avg_orders']:.1f}",
                    help="Promedio de períodos de 7 días históricos"
                )
            with col3:
                orders_decrease_pct = orders_analysis['decrease_percentage']
                st.metric(
                    "Variación (%)",
                    f"{orders_decrease_pct:.1f}%",
                    delta="Crítico" if orders_decrease_pct >= 50 else "Moderado" if orders_decrease_pct >= 30 else "Normal",
                    delta_color="inverse" if orders_decrease_pct >= 30 else "off",
                    help="Disminución respecto al promedio"
                )
            with col4:
                st.metric(
                    "Períodos Analizados",
                    orders_analysis['total_periods'],
                    help="Total de períodos de 7 días analizados"
                )
        else:
            st.info("No hay datos suficientes para el análisis de órdenes")

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

st.markdown("**Dashboard creado con Streamlit y Plotly**")
