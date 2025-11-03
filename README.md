# Churn Alert Behavior Agent

Sistema de análisis de comportamiento para detección de riesgo de churn en puntos de venta (POS), basado en análisis cuantitativo y heurística para procesamiento de datos y generación de alertas tempranas.

## 🎯 Objetivo

Identificar automáticamente puntos de venta con alto riesgo de abandono (churn) mediante análisis cuantitativo de patrones de uso, métricas de entrega y tendencias de compra.

## 🏗️ Arquitectura del Flujo

El sistema sigue un pipeline de 6 etapas:

1. **Extract & Validate** - Carga y validación de datos JSON
2. **Feature Engineering** - Cálculo de banderas de riesgo
3. **Churn Risk Scoring** - Evaluación cuantitativa de riesgo mediante análisis heurístico
4. **Generate Reports** - Generación de reportes HTML/Markdown
5. **Send Notifications** - Envío de alertas vía Gmail API
6. **Owner Intelligence & Allocation** - Cruce de POS con sellers/owners HubSpot para priorizar cuentas y generar reportes individuales por owner_company

## 📁 Estructura del Proyecto

```
churn_alert_behavior/
├── behavioral_alert_agent.py    # Código principal del agente
├── streamlit_app.py             # Dashboard web interactivo
├── requirements.txt             # Dependencias de Python
├── config/                      # Credenciales y contactos para notificaciones
│   ├── google-credentials.json  # Credenciales OAuth 2.0
│   ├── google-token.json        # Token autorizado/refrescable
│   └── owner_contacts.json      # Mapa owner_id → contacto/email
├── docs/
│   └── RevOps_Datadriven.docx   # Documento de contexto RevOps
├── data/                        # Datos de entrada
│   ├── trial_data.json         # Datos de pruebas/trial
│   ├── orders_delivered.json   # Datos de órdenes entregadas
│   ├── purchase_trend.json     # Clasificación de tendencias
│   ├── zombies.json            # 🧟‍♂️ POS con alto riesgo de churn
│   └── pos_owner.csv           # Mapa POS ↔ seller/owner HubSpot
├── reports/                     # Reportes generados
│   ├── behavioral_alerts.html
│   ├── behavioral_alerts.md
│   ├── behavioral_alerts_chart.txt
│   ├── owner_behavioral_alerts.html
│   └── owner_behavioral_alerts.md
└── venv/                        # Entorno virtual Python
```

## 🚀 Instalación y Configuración

### 1. Crear entorno virtual

```bash
python -m venv venv
source venv/bin/activate  # En Linux/Mac
# o
venv\Scripts\activate     # En Windows
```

### 2. Instalar dependencias

```bash
pip install -r requirements.txt
```

### 3. Verificar configuración

El sistema utiliza análisis heurístico cuantitativo basado en métricas objetivas, no requiere configuración adicional.

## 📊 Datos de Entrada

### trial_data.json
Datos de pruebas de los POS con métricas de uso:
- `point_of_sale_id`: ID único del POS
- `platform use`: Nivel de uso (low/medium/high)
- `time saved`: Tiempo ahorrado (minimum/medium/high)
- `average daily savings`: Ahorros promedio diarios
- `predicted subscription value`: Valor de suscripción predicho

### orders_delivered.json
Métricas de entrega de órdenes:
- `orders_delivered (4 weeks)`: Órdenes entregadas en 4 semanas
- `percentage_delivered (4 weeks)`: Porcentaje de entrega en 4 semanas
- `malicious_use_risk_4_weeks`: Riesgo de uso malicioso (high/moderate/low)

### purchase_trend.json
Clasificación de tendencias de compra:
- `point_of_sale_id`: ID del POS
- `trend_classification`: Clasificación (active/risky/inactive)

### zombies.json 🧟‍♂️
POS identificados con alto riesgo de churn (nueva funcionalidad):
- `point_of_sale_id`: ID del POS
- `days_since_first_purchase`: Días desde primera compra
- `platform_use`: Nivel de uso (low/medium/high)
- `time_saved`: Tiempo ahorrado (minimum/medium/high)
- `predicted_subscription_value`: Valor predicho de suscripción

> 📐 **Fórmula de Tiempo Ahorrado**
>
> En el sistema operativo del app, el tiempo ahorrado se modela como:
>
> ```
> tiempo_ahorrado = N° distribuidores / (2 * (1 - N° distribuidores))
> ```
>
> Donde un mayor número de distribuidores incrementa el ahorro (tendencia a 1) y el factor 2 captura el doble impacto (catálogo + compra).

### pos_owner.csv (Nuevo)
Mapa maestro que vincula cada POS con su seller y owner registrado en HubSpot. Permite consolidar métricas comerciales por `owner_company`.
- `id`: Equivale al `point_of_sale_id` utilizado en el resto de archivos.
- `client_id`: Seller / cliente asignado al POS dentro de HubSpot.
- `hs_company_id`: ID de la compañía en HubSpot para cruces con CRM.
- `company_owner_id`: Owner (ejecutivo / account manager) responsable del POS. Puede venir vacío si el POS aún no tiene owner definido.

## 👤 Integración con Sellers y Owners (HubSpot)

- El archivo `data/pos_owner.csv` se cruza automáticamente durante la etapa **Owner Intelligence & Allocation** para etiquetar cada POS con su seller (`client_id`) y el `company_owner_id` proveniente de HubSpot.
- El flujo construye resúmenes por owner_company (OwnerGrouper) con métricas clave: cantidad de POS por nivel de riesgo, promedio de score, lista de POS críticos y acciones recomendadas.
- Los reportes generados contemplan **dos niveles**: individual por POS y consolidado por owner. Los nuevos archivos `reports/owner_behavioral_alerts.html` y `reports/owner_behavioral_alerts.md` (prefijo `owner_`) permiten derivar tareas comerciales directamente a los ejecutivos responsables.
- En el dashboard (Streamlit) existe un toggle para cambiar entre la vista tradicional por POS y la vista por owner, donde se visualizan badges, métricas y gráficos exclusivos para cada ejecutivo.

## ✉️ Notificaciones por Email

El agente ahora puede enviar las alertas generadas directamente a los owners mediante Gmail API:

1. **Mapa de contactos**: edita `config/owner_contacts.json` y agrega entradas `owner_id → {"name": "…", "email": "…"}`. Si un owner no tiene email configurado se utilizará el fallback definido en `Config.default_owner_email` (por ahora `arturo.gomez.@extendeal.com`).
2. **Credenciales de Google**: coloca el `client_id/client_secret` en `config/google-credentials.json` y genera/actualiza `config/google-token.json` otorgando el scope `https://www.googleapis.com/auth/gmail.send`.
3. **Dependencias**: instala/actualiza las librerías con `pip install -r requirements.txt` (añade google-api-python-client, google-auth-oauthlib y kaleido para exportar gráficos).
4. **Scopes configurables**: si ya cuentas con un token emitido para otro scope (por ejemplo `https://mail.google.com/`), actualiza `Config.google_api_scopes` para que coincida con tus credenciales y evita volver a autorizar.
5. **Ejecución**: al correr `python behavioral_alert_agent.py` o el dashboard se agruparán las alertas por owner y se enviará un resumen por correo con cabecera **Reporte Farmacias Riesgosas** y un gráfico de Plotly embebido (PNG generado con Kaleido). Si la API no está correctamente configurada, el sistema continuará funcionando y mostrará un warning en consola.

> 🔁 **Reautorizar Gmail**
>
> Si cambias el scope (por ejemplo del viejo `calendar` a `https://mail.google.com/`) borra únicamente `config/google-token.json` y vuelve a ejecutar el agente. El flujo abrirá la ventana de consentimiento para el mismo `client_id/client_secret` y generará un token compatible con el alcance configurado en `Config.google_api_scopes`.

Para deshabilitar temporalmente los emails puedes instanciar `Config(enable_email_notifications=False)` o borrar el token.

## ⚙️ Prefect local

El flujo usa tareas de Prefect para orquestar cada etapa. El script ahora asume por defecto `PREFECT_API_URL=http://127.0.0.1:4200/api`, así que solo necesitas arrancar el servidor Orion en otra terminal:

1. Activa el entorno virtual y ejecuta `prefect server start` (mantén esa terminal abierta). Orion expondrá la API en `http://127.0.0.1:4200/api` y el dashboard en `http://127.0.0.1:4200`.
2. En la terminal donde correrás el agente o Streamlit, asegúrate de tener `PREFECT_API_URL=http://127.0.0.1:4200/api` (el script lo establece automáticamente si no estaba configurado). Si tienes otro endpoint, expórtalo antes de ejecutar.

Si prefieres omitir Prefect, puedes exportar `PREFECT_API_URL=""` manualmente antes de correr el script para degradar a modo sin orquestador.

## 🏃‍♂️ Uso

### 🌐 Dashboard Web (Recomendado)

Ejecutar la aplicación web interactiva:

```bash
streamlit run streamlit_app.py
```

Esto abrirá un dashboard en `http://localhost:8501` con:
- **Ejecución interactiva** del análisis
- **Visualizaciones gráficas** en tiempo real
- **Métricas resumidas** y KPIs
- **Tablas detalladas** de alertas
- **Configuración personalizable**
- **Vista dual POS / Owner_company** para accionar tanto por punto de venta como por ejecutivo HubSpot

### 📊 Funcionalidades del Dashboard:
- 📈 **Gráfico de distribución de riesgos** (pie chart)
- 📊 **Risk scores por POS** (bar chart)
- 📦 **Tasas de entrega** comparativas (grouped bars)
- 💰 **Ahorros vs Risk Score** (scatter plot)
- 📋 **Tabla detallada** con acciones recomendadas
- 👤 **Panel por owner** con métricas, badges por POS y alertas críticas asignables
- 🧭 **Gráficos de distribución por owner** (niveles de riesgo y cantidad de POS por ejecutivo)

### 🖥️ Ejecución por línea de comandos

```bash
python behavioral_alert_agent.py
```

La ejecución por CLI genera automáticamente ambos tipos de reportes: los tradicionales por POS (`behavioral_alerts.*`) y los nuevos consolidados por owner (`owner_behavioral_alerts.*`), además del ASCII chart comparativo.

### 🐍 Usando como módulo

```python
from behavioral_alert_agent import ChurnAlertFlow, Config

# Configuración personalizada
config = Config(
    html_report_name="mi_reporte.html",   # Nombre del reporte HTML
    markdown_report_name="mi_reporte.md", # Nombre del reporte Markdown
    google_api_scopes=("https://mail.google.com/",),  # Ajusta al scope que ya tengas autorizado
)

# Ejecutar análisis
flow = ChurnAlertFlow(config)
results = flow.run()

print(f"Alertas generadas: {results['alerts_count']}")
print(results["reports"])  # Incluye llaves: html, markdown, ascii_chart, owner_html, owner_markdown
```

## 📈 Criterios de Riesgo

### Banderas de Riesgo Detectadas:
- **Tiempo de ahorro mínimo**: `time_saved == "minimum"` (opera con 1 proveedor)
- **📱 Bajo uso de plataforma**: `platform_use == "low"` (≤1 orden/semana) - criterio principal
- **Tendencia de compra riesgosa**: Clasificada como "risky" o "inactive"
- **Ahorros bajos**: < $5 USD diarios

### Algoritmo de Scoring:
```
Score base: 0.3
+ 0.25 (Bajo uso de plataforma)
+ 0.20 (Tiempo de ahorro mínimo) 
+ 0.40 (🧟‍♂️ ZOMBIE - penalización alta)
+ 0.10 (Tendencia risky/inactive)
+ 0.05 (Ahorros < $5)
Máximo: 1.0
```

### Niveles de Riesgo (actualizados):
- **🔴 High** (≥ 0.8): URGENTE - Asignar ejecutivo inmediatamente
- **🟡 Moderate** (0.6-0.79): Seguimiento programado  
- **🟢 Low** (< 0.6): Monitoreo rutinario

Los owners (`owner_company`) heredan estos umbrales mediante el promedio de score de sus POS y la presencia de cuentas críticas, lo que permite priorizar ejecutivos a partir de la misma lógica cuantitativa.

### 🧟‍♂️ **Nueva Funcionalidad: Detección de Zombies**
- **Fuente**: `data/zombies.json` - POS con características de churn potencial
- **Impacto**: Score +0.4 (mayor penalización individual)
- **Confianza**: 85% (vs 60% en análisis normal)
- **Acción**: "URGENTE: Asignar ejecutivo inmediatamente para prevenir churn"

## 📋 Reportes Generados

1. **HTML Report (POS)** (`reports/behavioral_alerts.html`): Reporte visual interactivo por punto de venta.
2. **Markdown Report (POS)** (`reports/behavioral_alerts.md`): Reporte en texto plano para compartir por canales asincrónicos.
3. **ASCII Chart** (`reports/behavioral_alerts_chart.txt`): Gráfico de barras en texto con tasas de entrega 4w vs 2w.
4. **HTML Report (Owner)** (`reports/owner_behavioral_alerts.html`): Nueva vista consolidada por `owner_company` con badges de POS por nivel de riesgo.
5. **Markdown Report (Owner)** (`reports/owner_behavioral_alerts.md`): Resumen ejecutivo por owner con acciones sugeridas y listado de POS críticos.

## 🔧 Configuración Avanzada

### Modificar parámetros en Config:

```python
config = Config(
    html_report_name="mi_reporte.html",   # Nombre del reporte HTML
    markdown_report_name="mi_reporte.md"  # Nombre del reporte Markdown
)
```

## 🛠️ Dependencias

### Principales:
- `streamlit`: Framework para dashboard web interactivo
- `plotly`: Visualizaciones gráficas interactivas
- `kaleido`: Exportación de charts Plotly a PNG para los correos
- `pandas`: Manipulación y análisis de datos
- `prefect`: Framework de orquestación de flujos (opcional)

### Opcionales:
- `matplotlib`, `plotly`: Para visualizaciones avanzadas
- `pandas`, `numpy`: Para análisis de datos
- `pytest`, `black`, `flake8`, `mypy`: Para desarrollo

## 🚨 Tolerancia a Errores

- **Análisis cuantitativo**: Basado en métricas objetivas y reglas heurísticas
- **Sin Prefect**: Mantiene funcionalidad core sin orquestación
- **Datos faltantes**: Maneja graciosamente campos opcionales
- **Archivos inexistentes**: Proporciona mensajes de error claros

## 📝 Logs y Notificaciones

El sistema genera logs informativos durante la ejecución:
- `[INFO]`: Información general del proceso
- `[WARN]`: Advertencias sobre fallbacks o datos faltantes
- `[NOTIFY]`: Simulación de notificaciones (sin envío real)

## 🤝 Contribuciones

Para contribuir al proyecto:

1. Fork el repositorio
2. Crea una rama feature (`git checkout -b feature/nueva-funcionalidad`)
3. Commit tus cambios (`git commit -am 'Añadir nueva funcionalidad'`)
4. Push a la rama (`git push origin feature/nueva-funcionalidad`)
5. Crea un Pull Request

## 📄 Licencia

Este proyecto es de uso interno para análisis RevOps y detección de churn.

---

**Nota**: Este sistema está diseñado para propósitos defensivos de análisis de comportamiento y retención de clientes. No debe usarse para fines maliciosos.
