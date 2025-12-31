"""
Almanach - AIS Message Models
==============================

Modèles Pydantic partagés pour tous les modules NOVA.

Ces modèles définissent les formats de données circulant sur les topics NATS :
- nova.parsed : ParsedAISMessage
- nova.enriched : EnrichedAISMessage  
- nova.analyzed : AnalysisResult
- nova.fused : FusionResult (à définir)
"""

from pydantic import BaseModel, Field, field_validator
from typing import Optional
from datetime import datetime


class BaseAISMessage(BaseModel):
    """
    Champs communs à tous les messages AIS.
    
    Utilisé comme classe de base pour tous les messages du pipeline NOVA.
    """
    
    data_id: str = Field(..., description="Unique message identifier")
    mmsi: int = Field(..., ge=100000000, le=999999999, description="Maritime Mobile Service Identity (9 digits)")
    timestamp: datetime = Field(..., description="Message timestamp (ISO 8601)")
    lon: float = Field(..., ge=-180.0, le=180.0, description="Longitude in degrees")
    lat: float = Field(..., ge=-90.0, le=90.0, description="Latitude in degrees")
    
    @field_validator('mmsi')
    @classmethod
    def validate_mmsi(cls, v: int) -> int:
        """Validate MMSI format (must be 9 digits)"""
        if not (100000000 <= v <= 999999999):
            raise ValueError(f"MMSI must be 9 digits, got {v}")
        return v


class ParsedAISMessage(BaseAISMessage):
    """
    Message AIS parsé (Topic: nova.parsed).
    
    Produit par : Parser
    Consommé par : Enricher
    
    Contient les données AIS brutes parsées depuis CSV/PKL.
    """
    
    sog: float = Field(..., ge=0.0, le=102.2, description="Speed over ground (knots)")
    cog: float = Field(..., ge=0.0, lt=360.0, description="Course over ground (degrees)")
    heading: Optional[int] = Field(None, ge=0, lt=360, description="True heading (degrees)")
    nav_status: Optional[int] = Field(None, description="Navigation status code")
    vessel_type: Optional[int] = Field(None, description="Vessel type code")
    imo: Optional[int] = Field(None, description="IMO number")
    callsign: Optional[str] = Field(None, description="Vessel callsign")
    vessel_name: Optional[str] = Field(None, description="Vessel name")
    
    @field_validator('cog')
    @classmethod
    def normalize_cog(cls, v: float) -> float:
        """Normalize COG to [0, 360)"""
        return v % 360.0


class EnrichedAISMessage(ParsedAISMessage):
    """
    Message AIS enrichi avec statistiques historiques (Topic: nova.enriched).
    
    Produit par : Enricher
    Consommé par : Analyzer
    
    Ajoute des statistiques calculées sur historique et contexte de navigation.
    """
    
    # Trajectory identification
    trajectory_id: str = Field(..., description="Trajectory/route identifier")
    
    # Deltas (changes from previous message)
    delta_sog: float = Field(..., description="Change in speed since last message (knots)")
    delta_cog: float = Field(..., description="Change in course since last message (degrees)")
    acceleration: float = Field(..., description="Acceleration (knots/minute)")
    
    # Historical statistics
    avg_speed_historical: float = Field(..., ge=0.0, description="Historical average speed (knots)")
    avg_heading_historical: float = Field(..., ge=0.0, lt=360.0, description="Historical average heading (degrees)")
    velocity_variance_30m: float = Field(..., ge=0.0, description="Velocity variance over 30min window (knots²)")
    heading_variance_30m: float = Field(..., ge=0.0, description="Heading variance over 30min window (degrees²)")
    
    # Zone and pattern information
    preferred_zone: Optional[str] = Field(None, description="Vessel's typical operating zone")
    time_in_zone: Optional[int] = Field(None, ge=0, description="Time spent in current zone (minutes)")
    recent_anomaly_count: int = Field(default=0, ge=0, description="Count of recent anomalies")
    
    @field_validator('avg_heading_historical')
    @classmethod
    def normalize_heading(cls, v: float) -> float:
        """Normalize heading to [0, 360)"""
        return v % 360.0


class AnalysisResult(BaseModel):
    """
    Résultat de détection d'anomalie (Topic: nova.analyzed).
    
    Produit par : Analyzer
    Consommé par : Fusioner
    
    Contient la décision binaire d'anomalie plus scores détaillés.
    """
    
    # Core identifiers (from input)
    data_id: str
    mmsi: int
    timestamp: datetime
    trajectory_id: str
    
    # Statistical scores
    zscore_velocity: float = Field(..., description="Z-score for velocity anomaly")
    zscore_heading: float = Field(..., description="Z-score for heading anomaly")
    
    # Individual anomaly flags
    outlier_velocity: bool = Field(..., description="Velocity is statistical outlier")
    outlier_heading: bool = Field(..., description="Heading is statistical outlier")
    outlier_acceleration: bool = Field(default=False, description="Acceleration is excessive")
    outlier_zone: bool = Field(default=False, description="In unusual zone")
    outlier_pattern: bool = Field(default=False, description="Abnormal pattern detected")
    
    # Final decision
    anomaly_detected: bool = Field(..., description="BINARY DECISION: Anomaly yes/no")
    anomaly_score: float = Field(..., ge=0.0, le=1.0, description="Overall anomaly score [0-1]")
    anomaly_reasons: list[str] = Field(default_factory=list, description="List of triggered anomaly types")
    
    # Confidence and metadata
    confidence: float = Field(..., ge=0.0, le=1.0, description="Confidence in detection [0-1]")
    analysis_time_ms: int = Field(..., ge=0, description="Processing time (milliseconds)")
    
    # Individual category scores for detailed analysis
    category_scores: Optional[dict[str, float]] = Field(
        default=None,
        description="Detailed scores per category: {speed: 0.8, heading: 0.3, ...}"
    )


class DetectionMetrics(BaseModel):
    """
    Métriques de performance du détecteur (usage interne, Redis).
    
    Utilisé pour : Monitoring et métriques
    Stocké dans : Redis (analyzer:metrics)
    """
    
    total_processed: int = Field(default=0, description="Total messages processed")
    anomalies_detected: int = Field(default=0, description="Total anomalies detected")
    avg_processing_time_ms: float = Field(default=0.0, description="Average processing time")
    detection_rate: float = Field(default=0.0, ge=0.0, le=1.0, description="Anomaly detection rate")
    
    def update(self, anomaly_detected: bool, processing_time_ms: int):
        """Update metrics with new analysis result"""
        self.total_processed += 1
        if anomaly_detected:
            self.anomalies_detected += 1
        
        # Running average of processing time
        n = self.total_processed
        self.avg_processing_time_ms = (
            self.avg_processing_time_ms * (n - 1) + processing_time_ms
        ) / n
        
        # Detection rate
        self.detection_rate = self.anomalies_detected / self.total_processed if self.total_processed > 0 else 0.0


# Export all models
__all__ = [
    'BaseAISMessage',
    'ParsedAISMessage',
    'EnrichedAISMessage',
    'AnalysisResult',
    'DetectionMetrics',
]
