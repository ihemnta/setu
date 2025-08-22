from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator
from django.utils import timezone
import logging

logger = logging.getLogger(__name__)


class Region(models.Model):
    """
    Model to store UK regions for weather data.
    """
    name = models.CharField(max_length=100, unique=True)
    code = models.CharField(max_length=10, unique=True)
    description = models.TextField(blank=True)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['name']
        verbose_name = 'Region'
        verbose_name_plural = 'Regions'

    def __str__(self):
        return f"{self.name} ({self.code})"


class WeatherParameter(models.Model):
    """
    Model to store weather parameters (e.g., Tmax, Tmin, Rainfall).
    """
    PARAMETER_CHOICES = [
        ('Tmax', 'Maximum Temperature'),
        ('Tmin', 'Minimum Temperature'),
        ('Tmean', 'Mean Temperature'),
        ('Rainfall', 'Rainfall'),
        ('Sunshine', 'Sunshine Hours'),
        ('AirFrost', 'Air Frost Days'),
        ('RainDays1mm', 'Rain Days ≥1mm'),
        ('RainDays10mm', 'Rain Days ≥10mm'),
    ]

    name = models.CharField(max_length=50, unique=True)
    display_name = models.CharField(max_length=100)
    unit = models.CharField(max_length=20)
    description = models.TextField(blank=True)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['name']
        verbose_name = 'Weather Parameter'
        verbose_name_plural = 'Weather Parameters'

    def __str__(self):
        return f"{self.display_name} ({self.unit})"


class WeatherRecord(models.Model):
    """
    Model to store individual weather data records.
    """
    region = models.ForeignKey(
        Region, 
        on_delete=models.CASCADE, 
        related_name='weather_records'
    )
    parameter = models.ForeignKey(
        WeatherParameter, 
        on_delete=models.CASCADE, 
        related_name='weather_records'
    )
    date = models.DateField()
    value = models.DecimalField(
        max_digits=8, 
        decimal_places=2,
        validators=[MinValueValidator(-100), MaxValueValidator(100)]
    )
    quality_flag = models.CharField(max_length=10, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['-date', 'region', 'parameter']
        unique_together = ['region', 'parameter', 'date']
        indexes = [
            models.Index(fields=['date']),
            models.Index(fields=['region', 'parameter']),
            models.Index(fields=['parameter', 'date']),
            models.Index(fields=['region', 'date']),
        ]
        verbose_name = 'Weather Record'
        verbose_name_plural = 'Weather Records'

    def __str__(self):
        return f"{self.region.name} - {self.parameter.name} - {self.date}: {self.value}"

    def save(self, *args, **kwargs):
        # Log data quality issues
        if self.value < -50 or self.value > 50:
            logger.warning(
                f"Unusual weather value detected: {self.value} for {self.parameter.name} "
                f"in {self.region.name} on {self.date}"
            )
        super().save(*args, **kwargs)


class DataIngestionLog(models.Model):
    """
    Model to track data ingestion processes.
    """
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('in_progress', 'In Progress'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
        ('partial', 'Partially Completed'),
    ]

    region = models.ForeignKey(
        Region, 
        on_delete=models.CASCADE, 
        related_name='ingestion_logs',
        null=True, 
        blank=True
    )
    parameter = models.ForeignKey(
        WeatherParameter, 
        on_delete=models.CASCADE, 
        related_name='ingestion_logs',
        null=True, 
        blank=True
    )
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    records_processed = models.IntegerField(default=0)
    records_created = models.IntegerField(default=0)
    records_updated = models.IntegerField(default=0)
    records_failed = models.IntegerField(default=0)
    start_time = models.DateTimeField(auto_now_add=True)
    end_time = models.DateTimeField(null=True, blank=True)
    error_message = models.TextField(blank=True)
    source_url = models.URLField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-start_time']
        verbose_name = 'Data Ingestion Log'
        verbose_name_plural = 'Data Ingestion Logs'

    def __str__(self):
        return f"Ingestion {self.id}: {self.status} - {self.start_time}"

    @property
    def duration(self):
        """Calculate the duration of the ingestion process."""
        if self.end_time:
            return self.end_time - self.start_time
        return timezone.now() - self.start_time

    def mark_completed(self, records_created=0, records_updated=0, records_failed=0):
        """Mark the ingestion as completed."""
        self.status = 'completed'
        self.records_created = records_created
        self.records_updated = records_updated
        self.records_failed = records_failed
        self.end_time = timezone.now()
        self.save()

    def mark_failed(self, error_message):
        """Mark the ingestion as failed."""
        self.status = 'failed'
        self.error_message = error_message
        self.end_time = timezone.now()
        self.save()


class WeatherAggregate(models.Model):
    """
    Model to store pre-calculated weather aggregates for performance.
    """
    AGGREGATE_TYPES = [
        ('monthly', 'Monthly Average'),
        ('yearly', 'Yearly Average'),
        ('seasonal', 'Seasonal Average'),
        ('decadal', 'Decadal Average'),
    ]

    region = models.ForeignKey(
        Region, 
        on_delete=models.CASCADE, 
        related_name='weather_aggregates'
    )
    parameter = models.ForeignKey(
        WeatherParameter, 
        on_delete=models.CASCADE, 
        related_name='weather_aggregates'
    )
    aggregate_type = models.CharField(max_length=20, choices=AGGREGATE_TYPES)
    period_start = models.DateField()
    period_end = models.DateField()
    avg_value = models.DecimalField(max_digits=8, decimal_places=2)
    min_value = models.DecimalField(max_digits=8, decimal_places=2)
    max_value = models.DecimalField(max_digits=8, decimal_places=2)
    record_count = models.IntegerField()
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['-period_start', 'region', 'parameter']
        unique_together = ['region', 'parameter', 'aggregate_type', 'period_start', 'period_end']
        indexes = [
            models.Index(fields=['aggregate_type', 'period_start']),
            models.Index(fields=['region', 'parameter', 'aggregate_type']),
        ]
        verbose_name = 'Weather Aggregate'
        verbose_name_plural = 'Weather Aggregates'

    def __str__(self):
        return f"{self.region.name} - {self.parameter.name} - {self.aggregate_type} ({self.period_start} to {self.period_end})"


class SeasonalSummary(models.Model):
    """
    Model to store MetOffice seasonal summary statistics.
    """
    SEASON_CHOICES = [
        ('winter', 'Winter (Dec, Jan, Feb)'),
        ('spring', 'Spring (Mar, Apr, May)'),
        ('summer', 'Summer (Jun, Jul, Aug)'),
        ('autumn', 'Autumn (Sep, Oct, Nov)'),
        ('annual', 'Annual (All 12 months)'),
    ]

    region = models.ForeignKey(
        Region, 
        on_delete=models.CASCADE, 
        related_name='seasonal_summaries'
    )
    parameter = models.ForeignKey(
        WeatherParameter, 
        on_delete=models.CASCADE, 
        related_name='seasonal_summaries'
    )
    year = models.IntegerField()
    season = models.CharField(max_length=10, choices=SEASON_CHOICES)
    value = models.DecimalField(max_digits=8, decimal_places=2)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['-year', 'season', 'region', 'parameter']
        unique_together = ['region', 'parameter', 'year', 'season']
        indexes = [
            models.Index(fields=['year', 'season']),
            models.Index(fields=['region', 'parameter', 'year']),
        ]
        verbose_name = 'Seasonal Summary'
        verbose_name_plural = 'Seasonal Summaries'

    def __str__(self):
        return f"{self.region.name} - {self.parameter.name} - {self.season} {self.year}: {self.value}"
