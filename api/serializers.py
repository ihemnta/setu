from rest_framework import serializers
from weather_data.models import (
    Region, 
    WeatherParameter, 
    WeatherRecord, 
    DataIngestionLog, 
    WeatherAggregate
)
from django.db.models import Avg, Min, Max, Count
from django.utils import timezone
from datetime import datetime, timedelta


class RegionSerializer(serializers.ModelSerializer):
    """Serializer for Region model."""
    
    class Meta:
        model = Region
        fields = ['id', 'name', 'code', 'description', 'is_active', 'created_at', 'updated_at']
        read_only_fields = ['id', 'created_at', 'updated_at']


class WeatherParameterSerializer(serializers.ModelSerializer):
    """Serializer for WeatherParameter model."""
    
    class Meta:
        model = WeatherParameter
        fields = ['id', 'name', 'display_name', 'unit', 'description', 'is_active', 'created_at', 'updated_at']
        read_only_fields = ['id', 'created_at', 'updated_at']


class WeatherRecordSerializer(serializers.ModelSerializer):
    """Serializer for WeatherRecord model."""
    
    region = RegionSerializer(read_only=True)
    parameter = WeatherParameterSerializer(read_only=True)
    region_id = serializers.PrimaryKeyRelatedField(
        queryset=Region.objects.filter(is_active=True),
        source='region',
        write_only=True
    )
    parameter_id = serializers.PrimaryKeyRelatedField(
        queryset=WeatherParameter.objects.filter(is_active=True),
        source='parameter',
        write_only=True
    )
    
    class Meta:
        model = WeatherRecord
        fields = [
            'id', 'region', 'parameter', 'region_id', 'parameter_id',
            'date', 'value', 'quality_flag', 'created_at', 'updated_at'
        ]
        read_only_fields = ['id', 'created_at', 'updated_at']


class WeatherRecordListSerializer(serializers.ModelSerializer):
    """Simplified serializer for listing weather records."""
    
    region_name = serializers.CharField(source='region.name', read_only=True)
    parameter_name = serializers.CharField(source='parameter.name', read_only=True)
    parameter_unit = serializers.CharField(source='parameter.unit', read_only=True)
    
    class Meta:
        model = WeatherRecord
        fields = [
            'id', 'region_name', 'parameter_name', 'parameter_unit',
            'date', 'value', 'quality_flag'
        ]


class DataIngestionLogSerializer(serializers.ModelSerializer):
    """Serializer for DataIngestionLog model."""
    
    region = RegionSerializer(read_only=True)
    parameter = WeatherParameterSerializer(read_only=True)
    duration = serializers.SerializerMethodField()
    
    class Meta:
        model = DataIngestionLog
        fields = [
            'id', 'region', 'parameter', 'status', 'records_processed',
            'records_created', 'records_updated', 'records_failed',
            'start_time', 'end_time', 'duration', 'error_message',
            'source_url', 'created_at'
        ]
        read_only_fields = ['id', 'start_time', 'end_time', 'created_at']
    
    def get_duration(self, obj) -> float:
        """Get duration in seconds."""
        if obj.duration:
            return obj.duration.total_seconds()
        return 0.0


class WeatherAggregateSerializer(serializers.ModelSerializer):
    """Serializer for WeatherAggregate model."""
    
    region = RegionSerializer(read_only=True)
    parameter = WeatherParameterSerializer(read_only=True)
    
    class Meta:
        model = WeatherAggregate
        fields = [
            'id', 'region', 'parameter', 'aggregate_type', 'period_start',
            'period_end', 'avg_value', 'min_value', 'max_value',
            'record_count', 'created_at', 'updated_at'
        ]
        read_only_fields = ['id', 'created_at', 'updated_at']


class WeatherDataFilterSerializer(serializers.Serializer):
    """Serializer for filtering weather data."""
    
    region = serializers.CharField(required=False, help_text="Region code (e.g., 'UK', 'England')")
    parameter = serializers.CharField(required=False, help_text="Parameter name (e.g., 'Tmax', 'Tmin')")
    start_date = serializers.DateField(required=False, help_text="Start date (YYYY-MM-DD)")
    end_date = serializers.DateField(required=False, help_text="End date (YYYY-MM-DD)")
    min_value = serializers.DecimalField(max_digits=8, decimal_places=2, required=False)
    max_value = serializers.DecimalField(max_digits=8, decimal_places=2, required=False)
    quality_flag = serializers.CharField(required=False, help_text="Quality flag filter")
    
    def validate(self, data):
        """Validate filter parameters."""
        if 'start_date' in data and 'end_date' in data:
            if data['start_date'] > data['end_date']:
                raise serializers.ValidationError("Start date must be before end date")
        
        if 'min_value' in data and 'max_value' in data:
            if data['min_value'] > data['max_value']:
                raise serializers.ValidationError("Min value must be less than max value")
        
        return data


class WeatherAggregateFilterSerializer(serializers.Serializer):
    """Serializer for filtering weather aggregates."""
    
    region = serializers.CharField(required=False)
    parameter = serializers.CharField(required=False)
    aggregate_type = serializers.ChoiceField(
        choices=WeatherAggregate.AGGREGATE_TYPES,
        required=False
    )
    start_date = serializers.DateField(required=False)
    end_date = serializers.DateField(required=False)
    
    def validate(self, data):
        """Validate filter parameters."""
        if 'start_date' in data and 'end_date' in data:
            if data['start_date'] > data['end_date']:
                raise serializers.ValidationError("Start date must be before end date")
        return data


class WeatherStatisticsSerializer(serializers.Serializer):
    """Serializer for weather statistics."""
    
    region = serializers.CharField()
    parameter = serializers.CharField()
    total_records = serializers.IntegerField()
    date_range = serializers.DictField()
    value_statistics = serializers.DictField()
    monthly_averages = serializers.ListField()
    yearly_averages = serializers.ListField()


class DataIngestionRequestSerializer(serializers.Serializer):
    """Serializer for data ingestion requests."""
    
    parameter = serializers.CharField(required=False, help_text="Weather parameter to ingest")
    region = serializers.CharField(required=False, help_text="Region to ingest")
    ingest_all = serializers.BooleanField(default=False, help_text="Ingest all available data")
    
    def validate(self, data):
        """Validate ingestion request."""
        if not data.get('ingest_all') and not data.get('parameter') and not data.get('region'):
            raise serializers.ValidationError(
                "Must specify either parameter, region, or set ingest_all to True"
            )
        return data


class WeatherDataExportSerializer(serializers.Serializer):
    """Serializer for weather data export requests."""
    
    format = serializers.ChoiceField(
        choices=['csv', 'json', 'xlsx'],
        default='csv',
        help_text="Export format"
    )
    region = serializers.CharField(required=False)
    parameter = serializers.CharField(required=False)
    start_date = serializers.DateField(required=False)
    end_date = serializers.DateField(required=False)
    include_metadata = serializers.BooleanField(default=True)
    
    def validate(self, data):
        """Validate export parameters."""
        if 'start_date' in data and 'end_date' in data:
            if data['start_date'] > data['end_date']:
                raise serializers.ValidationError("Start date must be before end date")
        return data


class APIStatusSerializer(serializers.Serializer):
    """Serializer for API status information."""
    
    status = serializers.CharField()
    timestamp = serializers.DateTimeField()
    version = serializers.CharField()
    total_records = serializers.IntegerField()
    total_regions = serializers.IntegerField()
    total_parameters = serializers.IntegerField()
    last_ingestion = serializers.DateTimeField(allow_null=True)
    database_size = serializers.CharField(allow_null=True)
    cache_status = serializers.CharField() 