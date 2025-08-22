import pytest
from django.test import TestCase
from django.core.exceptions import ValidationError
from django.utils import timezone
from decimal import Decimal
from datetime import date

from weather_data.models import (
    Region, 
    WeatherParameter, 
    WeatherRecord, 
    DataIngestionLog, 
    WeatherAggregate
)


class RegionModelTest(TestCase):
    """Test cases for Region model."""
    
    def setUp(self):
        self.region = Region.objects.create(
            name="United Kingdom",
            code="UK",
            description="United Kingdom weather data"
        )
    
    def test_region_creation(self):
        """Test region creation."""
        self.assertEqual(self.region.name, "United Kingdom")
        self.assertEqual(self.region.code, "UK")
        self.assertTrue(self.region.is_active)
    
    def test_region_str_representation(self):
        """Test string representation."""
        self.assertEqual(str(self.region), "United Kingdom (UK)")
    
    def test_unique_code_constraint(self):
        """Test that region codes must be unique."""
        with self.assertRaises(Exception):
            Region.objects.create(
                name="Another UK",
                code="UK",  # Duplicate code
                description="Another UK region"
            )


class WeatherParameterModelTest(TestCase):
    """Test cases for WeatherParameter model."""
    
    def setUp(self):
        self.parameter = WeatherParameter.objects.create(
            name="Tmax",
            display_name="Maximum Temperature",
            unit="°C",
            description="Maximum daily temperature"
        )
    
    def test_parameter_creation(self):
        """Test parameter creation."""
        self.assertEqual(self.parameter.name, "Tmax")
        self.assertEqual(self.parameter.display_name, "Maximum Temperature")
        self.assertEqual(self.parameter.unit, "°C")
        self.assertTrue(self.parameter.is_active)
    
    def test_parameter_str_representation(self):
        """Test string representation."""
        self.assertEqual(str(self.parameter), "Maximum Temperature (°C)")
    
    def test_unique_name_constraint(self):
        """Test that parameter names must be unique."""
        with self.assertRaises(Exception):
            WeatherParameter.objects.create(
                name="Tmax",  # Duplicate name
                display_name="Another Max Temp",
                unit="°C"
            )


class WeatherRecordModelTest(TestCase):
    """Test cases for WeatherRecord model."""
    
    def setUp(self):
        self.region = Region.objects.create(
            name="United Kingdom",
            code="UK"
        )
        self.parameter = WeatherParameter.objects.create(
            name="Tmax",
            display_name="Maximum Temperature",
            unit="°C"
        )
        self.record = WeatherRecord.objects.create(
            region=self.region,
            parameter=self.parameter,
            date=date(2023, 1, 1),
            value=Decimal("15.5"),
            quality_flag=""
        )
    
    def test_record_creation(self):
        """Test record creation."""
        self.assertEqual(self.record.region, self.region)
        self.assertEqual(self.record.parameter, self.parameter)
        self.assertEqual(self.record.date, date(2023, 1, 1))
        self.assertEqual(self.record.value, Decimal("15.5"))
    
    def test_record_str_representation(self):
        """Test string representation."""
        expected = "United Kingdom - Tmax - 2023-01-01: 15.5"
        self.assertEqual(str(self.record), expected)
    
    def test_unique_constraint(self):
        """Test that records must be unique per region, parameter, and date."""
        with self.assertRaises(Exception):
            WeatherRecord.objects.create(
                region=self.region,
                parameter=self.parameter,
                date=date(2023, 1, 1),  # Duplicate date
                value=Decimal("20.0")
            )
    
    def test_value_validation(self):
        """Test value validation."""
        # Test valid values
        valid_record = WeatherRecord(
            region=self.region,
            parameter=self.parameter,
            date=date(2023, 1, 2),
            value=Decimal("25.0")
        )
        valid_record.full_clean()
        
        # Test invalid values (too high)
        invalid_record = WeatherRecord(
            region=self.region,
            parameter=self.parameter,
            date=date(2023, 1, 3),
            value=Decimal("101.0")  # Above max
        )
        with self.assertRaises(ValidationError):
            invalid_record.full_clean()


class DataIngestionLogModelTest(TestCase):
    """Test cases for DataIngestionLog model."""
    
    def setUp(self):
        self.region = Region.objects.create(name="UK", code="UK")
        self.parameter = WeatherParameter.objects.create(
            name="Tmax", 
            display_name="Max Temp", 
            unit="°C"
        )
        self.log = DataIngestionLog.objects.create(
            region=self.region,
            parameter=self.parameter,
            status="completed",
            records_processed=100,
            records_created=95,
            records_updated=5,
            records_failed=0,
            source_url="https://example.com/data.txt"
        )
    
    def test_log_creation(self):
        """Test log creation."""
        self.assertEqual(self.log.status, "completed")
        self.assertEqual(self.log.records_processed, 100)
        self.assertEqual(self.log.records_created, 95)
        self.assertIsNotNone(self.log.start_time)
    
    def test_log_str_representation(self):
        """Test string representation."""
        self.assertIn("Ingestion", str(self.log))
        self.assertIn("completed", str(self.log))
    
    def test_mark_completed(self):
        """Test mark_completed method."""
        self.log.mark_completed(50, 30, 20)
        self.assertEqual(self.log.status, "completed")
        self.assertEqual(self.log.records_created, 50)
        self.assertEqual(self.log.records_updated, 30)
        self.assertEqual(self.log.records_failed, 20)
        self.assertIsNotNone(self.log.end_time)
    
    def test_mark_failed(self):
        """Test mark_failed method."""
        error_message = "Network error occurred"
        self.log.mark_failed(error_message)
        self.assertEqual(self.log.status, "failed")
        self.assertEqual(self.log.error_message, error_message)
        self.assertIsNotNone(self.log.end_time)
    
    def test_duration_property(self):
        """Test duration property."""
        # Test with end_time
        self.log.mark_completed(10, 5, 0)
        duration = self.log.duration
        self.assertIsNotNone(duration)
        self.assertGreater(duration.total_seconds(), 0)


class WeatherAggregateModelTest(TestCase):
    """Test cases for WeatherAggregate model."""
    
    def setUp(self):
        self.region = Region.objects.create(name="UK", code="UK")
        self.parameter = WeatherParameter.objects.create(
            name="Tmax", 
            display_name="Max Temp", 
            unit="°C"
        )
        self.aggregate = WeatherAggregate.objects.create(
            region=self.region,
            parameter=self.parameter,
            aggregate_type="monthly",
            period_start=date(2023, 1, 1),
            period_end=date(2023, 1, 31),
            avg_value=Decimal("12.5"),
            min_value=Decimal("5.0"),
            max_value=Decimal("20.0"),
            record_count=31
        )
    
    def test_aggregate_creation(self):
        """Test aggregate creation."""
        self.assertEqual(self.aggregate.aggregate_type, "monthly")
        self.assertEqual(self.aggregate.avg_value, Decimal("12.5"))
        self.assertEqual(self.aggregate.record_count, 31)
    
    def test_aggregate_str_representation(self):
        """Test string representation."""
        expected = "UK - Tmax - monthly (2023-01-01 to 2023-01-31)"
        self.assertEqual(str(self.aggregate), expected)
    
    def test_unique_constraint(self):
        """Test that aggregates must be unique per region, parameter, type, and period."""
        with self.assertRaises(Exception):
            WeatherAggregate.objects.create(
                region=self.region,
                parameter=self.parameter,
                aggregate_type="monthly",
                period_start=date(2023, 1, 1),  # Duplicate period
                period_end=date(2023, 1, 31),
                avg_value=Decimal("15.0"),
                min_value=Decimal("8.0"),
                max_value=Decimal("25.0"),
                record_count=31
            )


@pytest.mark.django_db
class ModelIntegrationTest(TestCase):
    """Integration tests for model relationships."""
    
    def setUp(self):
        self.region = Region.objects.create(name="UK", code="UK")
        self.parameter = WeatherParameter.objects.create(
            name="Tmax", 
            display_name="Max Temp", 
            unit="°C"
        )
    
    def test_region_weather_records_relationship(self):
        """Test relationship between region and weather records."""
        record = WeatherRecord.objects.create(
            region=self.region,
            parameter=self.parameter,
            date=date(2023, 1, 1),
            value=Decimal("15.0")
        )
        
        self.assertIn(record, self.region.weather_records.all())
        self.assertEqual(self.region.weather_records.count(), 1)
    
    def test_parameter_weather_records_relationship(self):
        """Test relationship between parameter and weather records."""
        record = WeatherRecord.objects.create(
            region=self.region,
            parameter=self.parameter,
            date=date(2023, 1, 1),
            value=Decimal("15.0")
        )
        
        self.assertIn(record, self.parameter.weather_records.all())
        self.assertEqual(self.parameter.weather_records.count(), 1)
    
    def test_cascade_delete(self):
        """Test cascade delete behavior."""
        record = WeatherRecord.objects.create(
            region=self.region,
            parameter=self.parameter,
            date=date(2023, 1, 1),
            value=Decimal("15.0")
        )
        
        # Delete region should cascade to records
        self.region.delete()
        self.assertEqual(WeatherRecord.objects.count(), 0)
        
        # Recreate for parameter test
        self.region = Region.objects.create(name="UK", code="UK")
        record = WeatherRecord.objects.create(
            region=self.region,
            parameter=self.parameter,
            date=date(2023, 1, 1),
            value=Decimal("15.0")
        )
        
        # Delete parameter should cascade to records
        self.parameter.delete()
        self.assertEqual(WeatherRecord.objects.count(), 0) 