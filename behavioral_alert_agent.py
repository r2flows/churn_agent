#!/usr/bin/env python3
"""
Churn Alert Flow (Prefect-inspired)
-----------------------------------

Reestructura del agente original siguiendo el flujo sugerido en la arquitectura:

1. Extract & Validate            -> Carga datos JSON y valida esquema mínimo.
2. Feature Engineering           -> Calcula banderas de riesgo (bajo uso, proveedor único).
3. Churn Risk Scoring (OpenAI)   -> Enriquecimiento contextual mediante OpenAI API.
4. Generate HTML Report          -> Renderiza reporte HTML y Markdown con métricas y tendencias.
5. Send Notification (mock)      -> Prepara mensaje/email (solo logging local).

Se mantiene compatibilidad con los datos actuales y se añade un fallback heurístico
cuando no hay credenciales de OpenAI o librerías externas (Plotly/Matplotlib).
"""

from __future__ import annotations

import base64
import json
import os
import textwrap
import zipfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from xml.etree import ElementTree as ET

# Forzar Prefect a usar la URL local de Orion si no hay configuración válida
_prefect_api_url = os.environ.get("PREFECT_API_URL", "").strip()
if not _prefect_api_url or "0.0.0.0" in _prefect_api_url:
    os.environ["PREFECT_API_URL"] = "http://127.0.0.1:4200/api"
    print(
        "[INFO] PREFECT_API_URL configurado a http://127.0.0.1:4200/api. Ejecuta 'prefect server start' en otra terminal para habilitar la orquestación local."
    )

try:  # Prefect es opcional; el flujo sigue funcionando sin la librería.
    from prefect import task
except Exception:  # pragma: no cover - fallback cuando Prefect no está instalado
    def task(func=None, **_kwargs):
        if func is None:
            return task

        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        return wrapper

# OpenAI removido - usando solo análisis heurístico cuantitativo

try:  # Plotly para gráficos en email
    import plotly.graph_objects as go
    import plotly.io as pio
except Exception:  # pragma: no cover - el email seguirá funcionando sin gráfico
    go = None  # type: ignore
    pio = None  # type: ignore

BASE_DIR = Path(__file__).resolve().parent
DOC_PATH = BASE_DIR / "docs" / "RevOps_Datadriven.docx"
TRIAL_DATA_PATH = BASE_DIR / "data" / "trial_data.json"
ORDERS_DATA_PATH = BASE_DIR / "data" / "orders_delivered.json"
PURCHASE_TREND_PATH = BASE_DIR / "data" / "purchase_trend.json"
ZOMBIES_DATA_PATH = BASE_DIR / "data" / "zombies.json"
POS_OWNER_PATH = BASE_DIR / "data" / "pos_owner.csv"
REPORT_DIR = BASE_DIR / "reports"


@dataclass
class Config:
    doc_path: Path = DOC_PATH
    trial_path: Path = TRIAL_DATA_PATH
    orders_path: Path = ORDERS_DATA_PATH
    trend_path: Path = PURCHASE_TREND_PATH
    zombies_path: Path = ZOMBIES_DATA_PATH
    pos_owner_path: Path = POS_OWNER_PATH
    report_dir: Path = REPORT_DIR
    html_report_name: str = "behavioral_alerts.html"
    markdown_report_name: str = "behavioral_alerts.md"
    ascii_chart_name: str = "behavioral_alerts_chart.txt"
    google_credentials_path: Path = BASE_DIR / "config" / "google-credentials.json"
    google_token_path: Path = BASE_DIR / "config" / "google-token.json"
    owner_contacts_path: Path = BASE_DIR / "config" / "owner_contacts.json"
    default_owner_email: str = "arturo.gomez.@extendeal.com"
    enable_email_notifications: bool = True
    google_api_scopes: Tuple[str, ...] = ("https://mail.google.com/",)
    # Configuración simplificada sin OpenAI


class OwnerDirectory:
    """Carga los datos de contacto de owners y provee fallbacks."""

    def __init__(self, config: Config):
        self.config = config
        self._contacts = self._load_contacts()

    def _load_contacts(self) -> Dict[str, OwnerContact]:
        contacts: Dict[str, OwnerContact] = {}
        path = self.config.owner_contacts_path
        if not path.exists():
            return contacts

        try:
            with path.open("r", encoding="utf-8") as handle:
                raw_contacts = json.load(handle)
        except Exception as exc:  # pragma: no cover - protección ante archivos corruptos
            print(f"[WARN] No se pudieron cargar los contactos de owners: {exc}")
            return contacts

        for owner_id, payload in raw_contacts.items():
            name = payload.get("name") or f"Owner {owner_id}"
            email = payload.get("email") or None
            contacts[str(owner_id)] = OwnerContact(
                owner_id=str(owner_id),
                name=name,
                email=email,
            )
        return contacts

    def get_contact(self, owner_id: str) -> OwnerContact:
        contact = self._contacts.get(owner_id)
        if contact:
            return contact
        fallback_email = self.config.default_owner_email or None
        return OwnerContact(owner_id=owner_id, name=f"Owner {owner_id}", email=fallback_email)

class GmailClient:
    """Cliente minimalista para enviar emails mediante Gmail API."""

    SCOPES = ["https://mail.google.com/"]

    def __init__(
        self,
        credentials_path: Path,
        token_path: Path,
        scopes: Optional[Sequence[str]] = None,
    ):
        self.credentials_path = credentials_path
        self.token_path = token_path
        self.scopes = list(scopes) if scopes else list(self.SCOPES)
        self._service = None

    def _build_service(self):
        try:
            from google.oauth2.credentials import Credentials
            from google.auth.transport.requests import Request
            from googleapiclient.discovery import build
        except Exception as exc:  # pragma: no cover - dependencias externas
            raise RuntimeError(
                "Dependencias de Google API no instaladas o incompatibles."
            ) from exc

        if not self.token_path.exists():
            raise FileNotFoundError(
                f"No se encontró el token OAuth en {self.token_path}. Ejecuta la autorización primero."
            )

        scope_list = self.scopes if self.scopes else None
        creds = Credentials.from_authorized_user_file(
            str(self.token_path), scopes=scope_list
        )
        if not creds.valid:
            if creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                raise RuntimeError(
                    "Credenciales de Gmail inválidas. Vuelve a autorizar con el scope gmail.send."
                )

        self._service = build("gmail", "v1", credentials=creds)
        return self._service

    def send_message(self, mime_message) -> None:
        service = self._service or self._build_service()
        raw_message = base64.urlsafe_b64encode(mime_message.as_bytes()).decode("utf-8")
        service.users().messages().send(userId="me", body={"raw": raw_message}).execute()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


@dataclass
class ZombieEntry:
    point_of_sale_id: int
    days_since_first_purchase: int
    platform_use: str
    time_saved: str
    predicted_subscription_value: float
    raw: Dict[str, object]


@dataclass
class TrialEntry:
    point_of_sale_id: int
    platform_use: str
    time_saved: str
    average_daily_savings: float
    predicted_subscription_value: float
    raw: Dict[str, object]


@dataclass
class OrdersEntry:
    point_of_sale_id: int
    orders_delivered_4w: Optional[float]
    percentage_delivered_4w: Optional[float]
    malicious_risk_4w: Optional[str]
    orders_delivered_2w: Optional[float]
    percentage_delivered_2w: Optional[float]
    malicious_risk_2w: Optional[str]
    raw: Dict[str, object]


@dataclass
class FeatureAlert:
    point_of_sale_id: int
    reasons: List[str]
    platform_use: str
    time_saved: str
    average_daily_savings: float
    predicted_subscription_value: float
    purchase_trend: Optional[str]
    orders_percentage_4w: Optional[float]
    orders_percentage_2w: Optional[float]
    malicious_risk_4w: Optional[str]
    malicious_risk_2w: Optional[str]
    source_trial: Optional[TrialEntry]
    source_orders: Optional[OrdersEntry] = None


@dataclass
class RiskAssessment:
    point_of_sale_id: int
    risk_level: str
    risk_score: float
    confidence: float
    summary: str
    recommended_action: str
    data_points_used: Dict[str, object] = field(default_factory=dict)

@dataclass
class OwnerRiskAssessment:
    owner_id: str
    owner_name: str
    owner_email: Optional[str]
    pos_count: int
    high_risk_pos: List[int]
    moderate_risk_pos: List[int]
    low_risk_pos: List[int]
    avg_risk_score: float
    total_high_risk_pos: int
    summary: str
    recommended_action: str
    individual_assessments: List[RiskAssessment] = field(default_factory=list)


@dataclass
class OwnerContact:
    owner_id: str
    name: str
    email: Optional[str]


class DocumentContextLoader:
    @staticmethod
    def load(path: Path) -> str:
        # Ya no cargamos contexto externo
        return ""

    @staticmethod
    def excerpt(context: str, num_lines: int = 25) -> str:
        return ""


class DataExtractor:
    REQUIRED_TRIAL_FIELDS = {
        "point_of_sale_id",
        "platform use",
        "time saved",
        "average daily savings",
        "predicted subscription value",
    }
    REQUIRED_ORDERS_FIELDS = {
        "point_of_sale_id",
        "orders_delivered (4 weeks)",
        "percentage_delivered (4 weeks)",
        "malicious_use_risk_4_weeks",
        "orders_delivered (2 weeks)",
        "percentage_delivered (2 weeks)",
        "malicious_use_risk_2_weeks",
    }
    REQUIRED_ZOMBIE_FIELDS = {
        "point_of_sale_id",
        "days_since_first_purchase",
        "platform_use",
        "time_saved",
        "predicted_subscription_value",
    }

    def __init__(self, config: Config):
        self.config = config
        self.pos_owner_map = self._load_pos_owner_mapping()

    def _load_json(self, path: Path) -> List[Dict]:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)

    @task(name="Extract & Validate")
    def extract(self) -> Tuple[List[TrialEntry], Dict[int, OrdersEntry], Dict[int, str]]:
        trial_raw = self._load_json(self.config.trial_path)
        orders_raw = self._load_json(self.config.orders_path)
        trend_raw = self._load_json(self.config.trend_path)

        trial_entries = [self._validate_trial(entry) for entry in trial_raw]
        orders_entries = {
            entry["point_of_sale_id"]: self._validate_orders(entry) for entry in orders_raw
        }
        trend_index = {
            entry["point_of_sale_id"]: entry.get("trend_classification", "").lower()
            for entry in trend_raw
        }

        return trial_entries, orders_entries, trend_index

    def _validate_trial(self, entry: Dict) -> TrialEntry:
        missing = self.REQUIRED_TRIAL_FIELDS - set(entry)
        if missing:
            raise ValueError(f"Trial entry missing fields {missing}: {entry}")
        return TrialEntry(
            point_of_sale_id=int(entry["point_of_sale_id"]),
            platform_use=str(entry["platform use"]).lower(),
            time_saved=str(entry["time saved"]).lower(),
            average_daily_savings=float(entry["average daily savings"]),
            predicted_subscription_value=float(entry["predicted subscription value"]),
            raw=entry,
        )

    def _validate_orders(self, entry: Dict) -> OrdersEntry:
        missing = self.REQUIRED_ORDERS_FIELDS - set(entry)
        if missing:
            raise ValueError(f"Orders entry missing fields {missing}: {entry}")
        return OrdersEntry(
            point_of_sale_id=int(entry["point_of_sale_id"]),
            orders_delivered_4w=self._optional_float(entry.get("orders_delivered (4 weeks)")),
            percentage_delivered_4w=self._optional_float(
                entry.get("percentage_delivered (4 weeks)")
            ),
            malicious_risk_4w=entry.get("malicious_use_risk_4_weeks"),
            orders_delivered_2w=self._optional_float(entry.get("orders_delivered (2 weeks)")),
            percentage_delivered_2w=self._optional_float(
                entry.get("percentage_delivered (2 weeks)")
            ),
            malicious_risk_2w=entry.get("malicious_use_risk_2_weeks"),
            raw=entry,
        )

    def _validate_zombie(self, entry: Dict) -> ZombieEntry:
        missing = self.REQUIRED_ZOMBIE_FIELDS - set(entry)
        if missing:
            raise ValueError(f"Zombie entry missing fields {missing}: {entry}")
        return ZombieEntry(
            point_of_sale_id=int(entry["point_of_sale_id"]),
            days_since_first_purchase=int(entry["days_since_first_purchase"]),
            platform_use=str(entry["platform_use"]).lower(),
            time_saved=str(entry["time_saved"]).lower(),
            predicted_subscription_value=float(entry["predicted_subscription_value"]),
            raw=entry,
        )

    @staticmethod
    def _optional_float(value: Optional[object]) -> Optional[float]:
        if value in (None, "null", ""):
            return None
        return float(value)

    def _load_pos_owner_mapping(self) -> Dict[int, str]:
        """Cargar el mapeo de POS ID a owner_id desde CSV"""
        import csv
        pos_owner_map = {}
        
        try:
            with open(self.config.pos_owner_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    pos_id = int(row['id'])
                    owner_id = row.get('company_owner_id', '').strip()
                    if owner_id:  # Solo incluir POS con owner asignado
                        pos_owner_map[pos_id] = owner_id
        except Exception as e:
            return {}
        
        return pos_owner_map


class FeatureEngineer:
    MIN_TIME_LABEL = "Tiempo de ahorro mínimo (opera con 1 proveedor)"
    LOW_PLATFORM_USE_LABEL = "Bajo uso de plataforma (≤1 orden/semana)"
    RISKY_TREND_LABEL = "Tendencia de compra riesgosa (inactive/risky)"

    def __init__(
        self,
        trial_entries: Iterable[TrialEntry],
        orders_index: Dict[int, OrdersEntry],
        trend_index: Dict[int, str],
    ):
        self.trial_entries = trial_entries
        self.orders_index = orders_index
        self.trend_index = trend_index

    @task(name="Feature Engineering")
    def build_alerts(self) -> List[FeatureAlert]:
        alerts: List[FeatureAlert] = []
        
        # Procesar trial entries con nuevo sistema de 3 criterios
        for trial in self.trial_entries:
            reasons = []
            
            # Criterio 1: Tiempo de ahorro mínimo
            if trial.time_saved == "minimum":
                reasons.append(self.MIN_TIME_LABEL)
            
            # Criterio 2: Bajo uso de plataforma
            if trial.platform_use == "low":
                reasons.append(self.LOW_PLATFORM_USE_LABEL)
            
            # Criterio 3: Tendencia de compra riesgosa
            trend = self.trend_index.get(trial.point_of_sale_id, "").lower()
            if trend in ["inactive", "risky"]:
                reasons.append(self.RISKY_TREND_LABEL)
            
            # Solo crear alerta si cumple al menos 1 criterio
            if not reasons:
                continue

            orders = self.orders_index.get(trial.point_of_sale_id)

            alerts.append(
                FeatureAlert(
                    point_of_sale_id=trial.point_of_sale_id,
                    reasons=reasons,
                    platform_use=trial.platform_use,
                    time_saved=trial.time_saved,
                    average_daily_savings=trial.average_daily_savings,
                    predicted_subscription_value=trial.predicted_subscription_value,
                    purchase_trend=trend,
                    orders_percentage_4w=_round_optional(
                        orders.percentage_delivered_4w if orders else None
                    ),
                    orders_percentage_2w=_round_optional(
                        orders.percentage_delivered_2w if orders else None
                    ),
                    malicious_risk_4w=(orders.malicious_risk_4w if orders else None),
                    malicious_risk_2w=(orders.malicious_risk_2w if orders else None),
                    source_trial=trial,
                    source_orders=orders,
                )
            )
        
        return alerts


class ChurnScorer:
    # Análisis cuantitativo basado en métricas objetivas

    def __init__(self, config: Config, context: str):
        self.config = config
        self.context = context
        print("[INFO] Usando análisis heurístico cuantitativo para scoring de riesgo.")

    @task(name="Churn Risk Scoring")
    def score(self, alerts: List[FeatureAlert]) -> List[RiskAssessment]:
        assessments: List[RiskAssessment] = []
        for alert in alerts:
            payload = self._build_payload(alert)
            assessment = self._heuristic_assessment(payload)
            assessments.append(assessment)
        return assessments

    def _build_payload(self, alert: FeatureAlert) -> Dict:
        return {
            "point_of_sale_id": alert.point_of_sale_id,
            "reasons": alert.reasons,
            "platform_use": alert.platform_use,
            "time_saved": alert.time_saved,
            "average_daily_savings": alert.average_daily_savings,
            "predicted_subscription_value": alert.predicted_subscription_value,
            "purchase_trend": alert.purchase_trend or "unknown",
            "orders_delivery_rate_4w": alert.orders_percentage_4w,
            "orders_delivery_rate_2w": alert.orders_percentage_2w,
            "malicious_risk_4w": alert.malicious_risk_4w or "unknown",
            "malicious_risk_2w": alert.malicious_risk_2w or "unknown",
        }

    # Método removido - solo usamos análisis heurístico

    def _heuristic_assessment(self, payload: Dict) -> RiskAssessment:
        reasons = payload["reasons"]
        
        # Contar criterios cumplidos
        criteria_count = 0
        
        # Criterio 1: Tiempo de ahorro mínimo
        if FeatureEngineer.MIN_TIME_LABEL in reasons:
            criteria_count += 1
            
        # Criterio 2: Bajo uso de plataforma
        if FeatureEngineer.LOW_PLATFORM_USE_LABEL in reasons:
            criteria_count += 1
            
        # Criterio 3: Tendencia de compra riesgosa
        if FeatureEngineer.RISKY_TREND_LABEL in reasons:
            criteria_count += 1
        
        # Clasificación categórica basada en número de criterios
        if criteria_count == 3:
            risk_level = "extreme"
            risk_score = 1.0
            action = "EXTREMA URGENCIA: Intervención inmediata - Asignar ejecutivo senior en 24 horas."
            confidence = 0.95
        elif criteria_count == 2:
            risk_level = "urgent"
            risk_score = 0.75
            action = "URGENTE: Contacto inmediato - Asignar ejecutivo en 48 horas."
            confidence = 0.85
        elif criteria_count == 1:
            risk_level = "moderate"
            risk_score = 0.5
            action = "MODERADO: Programar llamada de seguimiento en 1 semana."
            confidence = 0.70
        else:
            # Esto no debería suceder ya que solo creamos alertas con al menos 1 criterio
            risk_level = "low"
            risk_score = 0.25
            action = "Monitoreo rutinario."
            confidence = 0.60

        # Summary basado en criterios cumplidos
        summary = f"POS cumple {criteria_count} de 3 criterios de riesgo: {', '.join(reasons)}."

        return RiskAssessment(
            point_of_sale_id=int(payload["point_of_sale_id"]),
            risk_level=risk_level,
            risk_score=round(risk_score, 2),
            confidence=confidence,
            summary=summary,
            recommended_action=action,
            data_points_used=payload,
        )


class OwnerGrouper:
    """Agrupa alertas por owner y genera assessments consolidados"""
    
    def __init__(self, pos_owner_map: Dict[int, str], owner_directory: OwnerDirectory):
        self.pos_owner_map = pos_owner_map
        self.owner_directory = owner_directory
    
    def group_by_owner(self, assessments: List[RiskAssessment]) -> List[OwnerRiskAssessment]:
        """Agrupa assessments individuales por owner"""
        owner_groups = {}
        
        for assessment in assessments:
            pos_id = assessment.point_of_sale_id
            owner_id = self.pos_owner_map.get(pos_id)
            
            # Solo incluir POS que tienen owner asignado
            if owner_id:
                if owner_id not in owner_groups:
                    owner_groups[owner_id] = []
                owner_groups[owner_id].append(assessment)
        
        owner_assessments = []
        for owner_id, group_assessments in owner_groups.items():
            owner_assessment = self._create_owner_assessment(owner_id, group_assessments)
            owner_assessments.append(owner_assessment)
        
        # Ordenar por avg_risk_score descendente
        owner_assessments.sort(key=lambda x: x.avg_risk_score, reverse=True)
        return owner_assessments
    
    def _create_owner_assessment(self, owner_id: str, assessments: List[RiskAssessment]) -> OwnerRiskAssessment:
        """Crear assessment consolidado para un owner"""
        extreme_risk_pos = []
        urgent_risk_pos = []
        moderate_risk_pos = []
        low_risk_pos = []
        
        for assessment in assessments:
            pos_id = assessment.point_of_sale_id
            risk_level = assessment.risk_level.lower()
            
            if risk_level == "extreme":
                extreme_risk_pos.append(pos_id)
            elif risk_level == "urgent":
                urgent_risk_pos.append(pos_id)
            elif risk_level == "moderate":
                moderate_risk_pos.append(pos_id)
            else:
                low_risk_pos.append(pos_id)
        
        # Los POS de alto riesgo ahora incluyen extreme + urgent
        high_risk_pos = extreme_risk_pos + urgent_risk_pos
        
        avg_risk_score = sum(a.risk_score for a in assessments) / len(assessments)
        pos_count = len(assessments)
        
        # Generar resumen y acción recomendada basado en los nuevos niveles
        if len(extreme_risk_pos) > 0:
            summary = f"🚨 EXTREMO: {len(extreme_risk_pos)} POS urgencia extrema de {pos_count} total"
            action = f"EMERGENCIA: Intervención inmediata en 24h - POS críticos: {extreme_risk_pos}"
        elif len(urgent_risk_pos) > 0:
            summary = f"🔴 URGENTE: {len(urgent_risk_pos)} POS urgentes de {pos_count} total"
            action = f"URGENTE: Contactar en 48h - POS prioritarios: {urgent_risk_pos}"
        elif len(moderate_risk_pos) > 0:
            summary = f"🟡 MODERADO: {len(moderate_risk_pos)} POS seguimiento de {pos_count} total"
            action = f"Programar llamadas en 1 semana para POS {moderate_risk_pos}"
        else:
            summary = f"✅ ESTABLE: Todos los {pos_count} POS bajo control"
            action = "Monitoreo rutinario semanal"
        
        contact = self.owner_directory.get_contact(owner_id)
        owner_name = contact.name
        owner_email = contact.email

        return OwnerRiskAssessment(
            owner_id=owner_id,
            owner_name=owner_name,
            owner_email=owner_email,
            pos_count=pos_count,
            high_risk_pos=high_risk_pos,  # Incluye extreme + urgent
            moderate_risk_pos=moderate_risk_pos,
            low_risk_pos=low_risk_pos,
            avg_risk_score=round(avg_risk_score, 2),
            total_high_risk_pos=len(high_risk_pos),
            summary=summary,
            recommended_action=action,
            individual_assessments=assessments
        )


class ReportGenerator:
    RISK_COLOR = {"extreme": "#B71C1C", "urgent": "#F44336", "moderate": "#FF9800", "low": "#4CAF50"}

    def __init__(self, config: Config, context_excerpt: str, pos_owner_map: Dict[int, str], owner_directory: OwnerDirectory):
        self.config = config
        self.context_excerpt = context_excerpt
        self.pos_owner_map = pos_owner_map
        self.owner_directory = owner_directory
        self.owner_grouper = OwnerGrouper(pos_owner_map, owner_directory)

    @task(name="Generate HTML Report")
    def generate(
        self,
        alerts: List[FeatureAlert],
        assessments: List[RiskAssessment],
    ) -> Tuple[Dict[str, Optional[Path]], List[OwnerRiskAssessment]]:
        ensure_dir(self.config.report_dir)
        
        # Generar reportes tradicionales por POS
        ordered = self._merge_and_sort(alerts, assessments)
        html_path = self._render_html(ordered)
        markdown_path = self._render_markdown(ordered)
        ascii_path = self._render_ascii_chart(ordered)
        
        # Generar reportes agrupados por owner
        owner_assessments = self.owner_grouper.group_by_owner(assessments)
        owner_html_path = self._render_owner_html(owner_assessments)
        owner_markdown_path = self._render_owner_markdown(owner_assessments)
        
        return {
            "html": html_path, 
            "markdown": markdown_path, 
            "ascii_chart": ascii_path,
            "owner_html": owner_html_path,
            "owner_markdown": owner_markdown_path
        }, owner_assessments

    def _merge_and_sort(
        self, alerts: List[FeatureAlert], assessments: List[RiskAssessment]
    ) -> List[Tuple[FeatureAlert, RiskAssessment]]:
        assessment_index = {item.point_of_sale_id: item for item in assessments}
        pairs: List[Tuple[FeatureAlert, RiskAssessment]] = []
        for alert in alerts:
            risk = assessment_index.get(alert.point_of_sale_id)
            if risk:
                pairs.append((alert, risk))
        pairs.sort(key=lambda item: item[1].risk_score, reverse=True)
        return pairs

    def _render_html(
        self, data: List[Tuple[FeatureAlert, RiskAssessment]]
    ) -> Optional[Path]:
        if not data:
            return None

        rows = []
        for alert, assessment in data:
            color = self.RISK_COLOR.get(assessment.risk_level.lower(), "#9E9E9E")
            badges = "".join(
                f'<span class="badge">{reason}</span>' for reason in alert.reasons
            )
            rows.append(
                f"""
                <tr>
                    <td>POS {alert.point_of_sale_id}</td>
                    <td><div class="risk" style="background:{color}">{assessment.risk_level.title()}</div>
                        <div class="score">Score: {assessment.risk_score:.2f} | Conf: {assessment.confidence:.2f}</div>
                    </td>
                    <td>{badges}</td>
                    <td>{alert.purchase_trend or 'Sin dato'}</td>
                    <td>{_fmt(alert.orders_percentage_4w, suffix='%')}</td>
                    <td>{_fmt(alert.orders_percentage_2w, suffix='%')}</td>
                    <td>{assessment.summary}</td>
                    <td>{assessment.recommended_action}</td>
                </tr>
                """
            )

        html_content = f"""
        <!DOCTYPE html>
        <html lang="es">
        <head>
          <meta charset="UTF-8" />
          <title>Alertas de Churn</title>
          <style>
            body {{ font-family: Arial, sans-serif; margin: 24px; }}
            h1 {{ color: #1F2937; }}
            section {{ margin-bottom: 32px; }}
            table {{ width: 100%; border-collapse: collapse; }}
            th, td {{ border: 1px solid #E5E7EB; padding: 8px; vertical-align: top; }}
            th {{ background: #F3F4F6; text-transform: uppercase; font-size: 12px; }}
            .risk {{ color: #fff; padding: 4px 8px; border-radius: 4px; font-weight: bold; display: inline-block; }}
            .badge {{ display: inline-block; background: #E0E7FF; color: #1E3A8A; padding: 2px 6px; margin: 2px; border-radius: 4px; font-size: 12px; }}
            .score {{ font-size: 12px; color: #4B5563; }}
            pre {{ background: #F9FAFB; padding: 12px; border-radius: 6px; }}
          </style>
        </head>
        <body>
          <h1>Alertas de comportamiento riesgoso</h1>
          <section>
            <h2>Resumen de alertas prioritarias</h2>
            <table>
              <thead>
                <tr>
                  <th>POS</th>
                  <th>Riesgo</th>
                  <th>Motivos</th>
                  <th>Purchase Trend</th>
                  <th>Delivery Rate 4w</th>
                  <th>Delivery Rate 2w</th>
                  <th>Resumen</th>
                  <th>Acción recomendada</th>
                </tr>
              </thead>
              <tbody>
                {''.join(rows)}
              </tbody>
            </table>
          </section>
        </body>
        </html>
        """.strip()

        html_path = self.config.report_dir / self.config.html_report_name
        html_path.write_text(html_content, encoding="utf-8")
        return html_path

    def _render_markdown(
        self, data: List[Tuple[FeatureAlert, RiskAssessment]]
    ) -> Path:
        lines = [
            "# Alertas de comportamiento riesgoso",
            "",
            "## Alertas priorizadas",
            "",
        ]

        if not data:
            lines.append(
                "No se detectaron puntos de venta con banderas de riesgo para la semana analizada."
            )
        else:
            for alert, assessment in data:
                reasons_str = "; ".join(alert.reasons)
                lines.extend(
                    [
                        f"- **POS {alert.point_of_sale_id}**",
                        f"  - Riesgo estimado: {assessment.risk_level.title()} (score {assessment.risk_score:.2f}, confianza {assessment.confidence:.2f})",
                        f"  - Motivos: {reasons_str}",
                        f"  - Purchase trend: {alert.purchase_trend or 'sin dato'}",
                        f"  - Orders delivery rate (4w / 2w): {_fmt(alert.orders_percentage_4w, '%')} / {_fmt(alert.orders_percentage_2w, '%')}",
                        f"  - Resumen IA: {assessment.summary}",
                        f"  - Acción recomendada: {assessment.recommended_action}",
                        "",
                    ]
                )

        markdown_path = self.config.report_dir / self.config.markdown_report_name
        markdown_path.write_text("\n".join(lines), encoding="utf-8")
        return markdown_path

    def _render_ascii_chart(
        self, data: List[Tuple[FeatureAlert, RiskAssessment]]
    ) -> Optional[Path]:
        if not data:
            return None

        max_value = max(
            [
                value
                for alert, _ in data
                for value in (alert.orders_percentage_4w, alert.orders_percentage_2w)
                if value is not None
            ],
            default=0,
        )
        if max_value == 0:
            return None

        scale = 40 / max_value
        lines = [
            "ASCII chart: Orders delivery rate (4w vs 2w) by POS ID",
            "Legend: #=4w, +=2w; riesgo por texto",
            "",
        ]

        for alert, assessment in data:
            perc4 = alert.orders_percentage_4w or 0
            perc2 = alert.orders_percentage_2w or 0
            lines.append(
                f"POS {alert.point_of_sale_id:<6} | Risk {assessment.risk_level.title():<8} | "
                f"4w {perc4:>6.2f}% {'#' * max(1, int(perc4 * scale)) if perc4 else ''}"
            )
            lines.append(
                f"{'':18} | {'':16} | 2w {perc2:>6.2f}% "
                f"{'+' * max(1, int(perc2 * scale)) if perc2 else ''}"
            )
            lines.append("")

        ascii_path = self.config.report_dir / self.config.ascii_chart_name
        ascii_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
        return ascii_path
    
    def _render_owner_html(self, owner_assessments: List[OwnerRiskAssessment]) -> Optional[Path]:
        """Generar reporte HTML agrupado por owner"""
        if not owner_assessments:
            return None

        rows = []
        for owner in owner_assessments:
            # Color basado en el riesgo promedio
            if owner.avg_risk_score >= 0.8:
                color = self.RISK_COLOR["high"]
                risk_label = "Alto"
            elif owner.avg_risk_score >= 0.6:
                color = self.RISK_COLOR["moderate"] 
                risk_label = "Moderado"
            else:
                color = self.RISK_COLOR["low"]
                risk_label = "Bajo"
            
            # Crear badges para POS por riesgo
            high_badges = " ".join(f'<span class="badge high-risk">POS {pos}</span>' for pos in owner.high_risk_pos)
            mod_badges = " ".join(f'<span class="badge mod-risk">POS {pos}</span>' for pos in owner.moderate_risk_pos)
            low_badges = " ".join(f'<span class="badge low-risk">POS {pos}</span>' for pos in owner.low_risk_pos)
            all_badges = f"{high_badges} {mod_badges} {low_badges}".strip()
            
            rows.append(f"""
                <tr>
                    <td><strong>{owner.owner_name}</strong><br><small>ID: {owner.owner_id}<br>Email: {owner.owner_email or 'N/D'}</small></td>
                    <td><div class="risk" style="background:{color}">{risk_label}</div>
                        <div class="score">Score: {owner.avg_risk_score:.2f}</div>
                    </td>
                    <td>{owner.pos_count}</td>
                    <td>{len(owner.high_risk_pos)}</td>
                    <td>{len(owner.moderate_risk_pos)}</td>
                    <td>{len(owner.low_risk_pos)}</td>
                    <td>{owner.total_high_risk_pos}</td>
                    <td>{all_badges}</td>
                    <td>{owner.summary}</td>
                    <td>{owner.recommended_action}</td>
                </tr>
            """)

        html_content = f"""
        <!DOCTYPE html>
        <html lang="es">
        <head>
          <meta charset="UTF-8" />
          <title>Reporte de Riesgo por Owner</title>
          <style>
            body {{ font-family: Arial, sans-serif; margin: 24px; }}
            h1 {{ color: #1F2937; }}
            section {{ margin-bottom: 32px; }}
            table {{ width: 100%; border-collapse: collapse; }}
            th, td {{ border: 1px solid #E5E7EB; padding: 8px; vertical-align: top; }}
            th {{ background: #F3F4F6; text-transform: uppercase; font-size: 12px; }}
            .risk {{ color: #fff; padding: 4px 8px; border-radius: 4px; font-weight: bold; display: inline-block; }}
            .badge {{ display: inline-block; padding: 2px 6px; margin: 2px; border-radius: 4px; font-size: 11px; }}
            .high-risk {{ background: #ffebee; color: #c62828; }}
            .mod-risk {{ background: #fff3e0; color: #ef6c00; }}
            .low-risk {{ background: #e8f5e8; color: #2e7d32; }}
            .score {{ font-size: 12px; color: #4B5563; }}
            pre {{ background: #F9FAFB; padding: 12px; border-radius: 6px; }}
          </style>
        </head>
        <body>
          <h1>📊 Reporte de Riesgo Comportamental por Owner</h1>
          <section>
            <h2>Resumen por Owner (ordenado por riesgo promedio)</h2>
            <table>
              <thead>
                <tr>
                  <th>Owner</th>
                  <th>Riesgo Promedio POS</th>
                  <th>Total POS</th>
                  <th>POS Alto Riesgo</th>
                  <th>POS Riesgo Moderado</th>
                  <th>POS Bajo Riesgo</th>
                  <th>Total POS Críticos</th>
                  <th>POS Detallados</th>
                  <th>Resumen</th>
                  <th>Acción Recomendada</th>
                </tr>
              </thead>
              <tbody>
                {''.join(rows)}
              </tbody>
            </table>
          </section>
        </body>
        </html>
        """.strip()

        owner_html_path = self.config.report_dir / f"owner_{self.config.html_report_name}"
        owner_html_path.write_text(html_content, encoding="utf-8")
        return owner_html_path
    
    def _render_owner_markdown(self, owner_assessments: List[OwnerRiskAssessment]) -> Path:
        """Generar reporte Markdown agrupado por owner"""
        lines = [
            "# Reporte de Riesgo Comportamental por Owner",
            "",
            "## Resumen por Owner (ordenado por riesgo individual)",
            "",
        ]

        if not owner_assessments:
            lines.append("No se detectaron owners con puntos de venta en riesgo.")
        else:
            for owner in owner_assessments:
                risk_label = "🔴 Alto" if owner.avg_risk_score >= 0.8 else "🟡 Moderado" if owner.avg_risk_score >= 0.6 else "🟢 Bajo"
                
                lines.extend([
                    f"### {owner.owner_name} (ID: {owner.owner_id})",
                    f"- **Email**: {owner.owner_email or 'N/D'}",
                    f"- **Riesgo promedio**: {risk_label} (score {owner.avg_risk_score:.2f})",
                    f"- **Total POS**: {owner.pos_count}",
                    f"- **POS Alto riesgo**: {len(owner.high_risk_pos)} → {owner.high_risk_pos}",
                    f"- **POS Riesgo moderado**: {len(owner.moderate_risk_pos)} → {owner.moderate_risk_pos}",
                    f"- **POS Bajo riesgo**: {len(owner.low_risk_pos)} → {owner.low_risk_pos}",
                    f"- **Total POS críticos**: {owner.total_high_risk_pos}",
                    f"- **Resumen**: {owner.summary}",
                    f"- **Acción recomendada**: {owner.recommended_action}",
                    "",
                ])

        owner_markdown_path = self.config.report_dir / f"owner_{self.config.markdown_report_name}"
        owner_markdown_path.write_text("\n".join(lines), encoding="utf-8")
        return owner_markdown_path


class NotificationService:
    def __init__(self, config: Config, owner_directory: OwnerDirectory):
        self.config = config
        self.owner_directory = owner_directory
        self._gmail_client: Optional[GmailClient] = None

    def _get_gmail_client(self) -> Optional[GmailClient]:
        if not self.config.enable_email_notifications:
            return None
        if self._gmail_client:
            return self._gmail_client
        try:
            self._gmail_client = GmailClient(
                self.config.google_credentials_path,
                self.config.google_token_path,
                scopes=self.config.google_api_scopes,
            )
            return self._gmail_client
        except Exception as exc:
            print(f"[WARN] Gmail API no disponible: {exc}")
            return None

    @task(name="Send Email Alerts")
    def dispatch(
        self,
        assessments: List[RiskAssessment],
        owner_assessments: List[OwnerRiskAssessment],
    ) -> None:
        if not assessments:
            print("[INFO] No hay alertas para notificar.")
            return

        self._log_top_assessment(assessments[0])
        self._send_owner_notifications(owner_assessments)

    def _log_top_assessment(self, top: RiskAssessment) -> None:
        timestamp = datetime.now(timezone.utc).isoformat()
        message = (
            f"[NOTIFY] {timestamp} - POS {top.point_of_sale_id} "
            f"riesgo {top.risk_level.upper()} (score {top.risk_score:.2f}). "
            f"Sugerencia: {top.recommended_action}"
        )
        print(message)

    def _send_owner_notifications(self, owner_assessments: List[OwnerRiskAssessment]) -> None:
        if not owner_assessments:
            print("[INFO] No hay owners con POS asignados para notificar.")
            return

        client = self._get_gmail_client()
        if client is None:
            print("[INFO] Notificaciones por email omitidas (cliente Gmail no disponible).")
            return

        for owner in owner_assessments:
            if not owner.owner_email:
                print(f"[WARN] Owner {owner.owner_id} sin email configurado. Se omite.")
                continue
            try:
                message = self._compose_owner_email(owner, owner.owner_email)
            except Exception as exc:
                print(
                    f"[WARN] No se pudo componer el email para owner {owner.owner_id}: {exc}"
                )
                continue

            try:
                client.send_message(message)
                print(
                    f"[NOTIFY] Email enviado a {owner.owner_email} para owner {owner.owner_id}."
                )
            except Exception as exc:
                if "invalid_scope" in str(exc):
                    print(
                        "[WARN] Gmail API rechazó el scope configurado. Ajusta Config.google_api_scopes o regenera el token con permiso de Gmail."
                    )
                else:
                    print(
                        f"[WARN] No se pudo enviar email a {owner.owner_email} (owner {owner.owner_id}): {exc}"
                    )
                break

    def _compose_owner_email(self, owner: OwnerRiskAssessment, to_email: str):
        subject = f"Reporte Farmacias Riesgosas - {owner.owner_name}"
        plain_lines = [
            "Reporte Farmacias Riesgosas",
            f"Owner: {owner.owner_name} ({owner.owner_id})",
            f"Resumen: {owner.summary}",
            f"Acción prioritaria: {owner.recommended_action}",
            "",
            "Detalle por POS:",
        ]

        for assessment in owner.individual_assessments:
            plain_lines.append(
                (
                    f"- POS {assessment.point_of_sale_id}: score {assessment.risk_score:.2f} "
                    f"({assessment.risk_level.upper()}) → {assessment.recommended_action}"
                )
            )

        plain_lines.append("\nEste mensaje se generó automáticamente desde el Churn Alert Agent.")

        html_cards = []
        for assessment in owner.individual_assessments:
            html_cards.append(
                f"""
                <div style="border:1px solid #e5e7eb;border-radius:8px;padding:12px;margin-bottom:8px">
                    <strong>POS {assessment.point_of_sale_id}</strong> · Score {assessment.risk_score:.2f} ({assessment.risk_level.title()})<br/>
                    Acción: {assessment.recommended_action}
                </div>
                """.strip()
            )

        chart_bytes = self._build_owner_chart_image(owner)
        chart_block = ""
        if chart_bytes:
            chart_block = (
                '<p style="text-align:center;margin:16px 0 8px 0;"><img src="cid:owner-chart" '
                'alt="Reporte Farmacias Riesgosas" style="max-width:100%;height:auto;border:1px solid #e5e7eb;border-radius:8px;" /></p>'
            )

        html_body = f"""
        <div style="font-family:Arial,sans-serif;color:#1f2937">
            <h1 style="color:#b91c1c;margin-bottom:4px;">Reporte Farmacias Riesgosas</h1>
            <p style="margin-top:0;color:#4b5563;">Owner: <strong>{owner.owner_name}</strong> (ID {owner.owner_id})</p>
            <p><strong>Resumen:</strong> {owner.summary}</p>
            <p><strong>Acción prioritaria:</strong> {owner.recommended_action}</p>
            {chart_block}
            <h3 style="margin-top:24px;">Detalle por POS</h3>
            {''.join(html_cards) or '<p>No hay POS asociados.</p>'}
            <p style="font-size:12px;color:#6b7280;margin-top:24px;">Mensaje generado automáticamente desde el Churn Alert Agent.</p>
        </div>
        """.strip()

        message = MIMEMultipart("related")
        message["to"] = to_email
        message["subject"] = subject

        alt_part = MIMEMultipart("alternative")
        alt_part.attach(MIMEText("\n".join(plain_lines), "plain", "utf-8"))
        alt_part.attach(MIMEText(html_body, "html", "utf-8"))
        message.attach(alt_part)

        if chart_bytes:
            image = MIMEImage(chart_bytes, _subtype="png")
            image.add_header("Content-ID", "<owner-chart>")
            image.add_header("Content-Disposition", "inline", filename="reporte_farmacias_riesgosas.png")
            message.attach(image)

        return message

    def _build_owner_chart_image(self, owner: OwnerRiskAssessment) -> Optional[bytes]:
        if go is None or pio is None:
            print("[WARN] Plotly/kaleido no disponible. El email se enviará sin gráfico.")
            return None

        assessments = sorted(
            owner.individual_assessments, key=lambda a: a.risk_score, reverse=True
        )
        if not assessments:
            return None

        labels = [f"POS {item.point_of_sale_id}" for item in assessments]
        scores = [item.risk_score for item in assessments]
        colors = [
            ReportGenerator.RISK_COLOR.get(item.risk_level.lower(), "#6b7280")
            for item in assessments
        ]

        fig = go.Figure(
            data=[
                go.Bar(
                    x=labels,
                    y=scores,
                    marker_color=colors,
                    text=[f"{score:.2f}" for score in scores],
                    textposition="outside",
                )
            ]
        )
        fig.update_layout(
            title="Reporte Farmacias Riesgosas",
            yaxis=dict(range=[0, 1], title="Risk Score"),
            xaxis=dict(title="Puntos de venta"),
            margin=dict(l=40, r=20, t=60, b=120),
            height=400,
            template="plotly_white",
        )

        try:
            return pio.to_image(fig, format="png", engine="kaleido", scale=2)
        except Exception as exc:
            print(f"[WARN] No se pudo renderizar el gráfico para emails: {exc}")
            return None


class ChurnAlertFlow:
    def __init__(self, config: Config):
        self.config = config
        self.context_full = DocumentContextLoader.load(config.doc_path)
        self.context_excerpt = DocumentContextLoader.excerpt(self.context_full)

        self.owner_directory = OwnerDirectory(config)
        self.extractor = DataExtractor(config)
        self.scorer = ChurnScorer(config, self.context_full)
        self.reporter = ReportGenerator(
            config, self.context_excerpt, self.extractor.pos_owner_map, self.owner_directory
        )
        self.notifier = NotificationService(config, self.owner_directory)

    def run(self) -> Dict[str, object]:
        trial_entries, orders_index, trend_index = self.extractor.extract()
        engineer = FeatureEngineer(trial_entries, orders_index, trend_index)
        alerts = engineer.build_alerts()
        assessments = self.scorer.score(alerts)
        report_paths, owner_assessments = self.reporter.generate(alerts, assessments)
        self.notifier.dispatch(assessments, owner_assessments)

        # Contar POS por nivel de urgencia
        extreme_count = sum(1 for a in assessments if a.risk_level == "extreme")
        urgent_count = sum(1 for a in assessments if a.risk_level == "urgent")
        moderate_count = sum(1 for a in assessments if a.risk_level == "moderate")

        return {
            "alerts_count": len(alerts),
            "reports": report_paths,
            "assessments": assessments,
            "extreme_urgency_count": extreme_count,
            "urgent_count": urgent_count,
            "moderate_count": moderate_count,
            "owner_assessments": owner_assessments,
        }


def _round_optional(value: Optional[float], digits: int = 2) -> Optional[float]:
    if value is None:
        return None
    return round(value, digits)


def _fmt(value: Optional[float], suffix: str = "") -> str:
    if value is None:
        return "N/D"
    return f"{value:.2f}{suffix}"


def main() -> None:
    config = Config()
    flow = ChurnAlertFlow(config)
    results = flow.run()
    print(f"Alertas generadas: {results['alerts_count']}")
    for kind, path in results["reports"].items():
        if path:
            print(f"{kind.title()} disponible en: {path}")
        else:
            print(f"{kind.title()} no generado.")


if __name__ == "__main__":
    main()
