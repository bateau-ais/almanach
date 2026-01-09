from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from uuid import UUID


class AnalysisResult(BaseModel):
    """
    Résultat de détection d'anomalie (Topic: nova.analyzed).
    
    Produit par : Analyzer
    Consommé par : Fusioner
    
    Contient la décision binaire d'anomalie plus scores détaillés.
    """
    
    # Core identifiers (from input)
    data_id: UUID
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

