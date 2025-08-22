from celery import shared_task
from django.db.models import Avg, Min, Max, Count, Q
from django.db.models.functions import TruncMonth, TruncYear
from django.utils import timezone
from datetime import date
from .models import WeatherRecord, WeatherAggregate
import logging

logger = logging.getLogger(__name__)


@shared_task(bind=True)
def generate_monthly_aggregates(self, region_id=None, parameter_id=None, force=False):
    """Generate monthly aggregates for weather data."""
    try:
        queryset = WeatherRecord.objects.select_related('region', 'parameter')
        
        if region_id:
            queryset = queryset.filter(region_id=region_id)
        if parameter_id:
            queryset = queryset.filter(parameter_id=parameter_id)
        
        # Group by region, parameter, and month
        aggregates = queryset.annotate(
            month=TruncMonth('date')
        ).values('region', 'parameter', 'month').annotate(
            avg_value=Avg('value'),
            min_value=Min('value'),
            max_value=Max('value'),
            record_count=Count('id')
        ).order_by('region', 'parameter', 'month')

        created_count = 0
        updated_count = 0

        for agg in aggregates:
            # Calculate period start and end
            period_start = agg['month'].replace(day=1)
            if period_start.month == 12:
                period_end = period_start.replace(year=period_start.year + 1, month=1, day=1) - timezone.timedelta(days=1)
            else:
                period_end = period_start.replace(month=period_start.month + 1, day=1) - timezone.timedelta(days=1)

            # Check if aggregate already exists
            existing = WeatherAggregate.objects.filter(
                region_id=agg['region'],
                parameter_id=agg['parameter'],
                aggregate_type='monthly',
                period_start=period_start,
                period_end=period_end
            ).first()

            if existing and not force:
                continue

            aggregate_data = {
                'region_id': agg['region'],
                'parameter_id': agg['parameter'],
                'aggregate_type': 'monthly',
                'period_start': period_start,
                'period_end': period_end,
                'avg_value': agg['avg_value'],
                'min_value': agg['min_value'],
                'max_value': agg['max_value'],
                'record_count': agg['record_count']
            }

            if existing:
                for key, value in aggregate_data.items():
                    setattr(existing, key, value)
                existing.save()
                updated_count += 1
            else:
                WeatherAggregate.objects.create(**aggregate_data)
                created_count += 1

        logger.info(f"Generated {created_count} new and {updated_count} updated monthly aggregates")
        return {
            'status': 'success',
            'created': created_count,
            'updated': updated_count,
            'aggregate_type': 'monthly'
        }
        
    except Exception as e:
        logger.error(f"Error generating monthly aggregates: {e}")
        raise self.retry(countdown=60, max_retries=3)


@shared_task(bind=True)
def generate_yearly_aggregates(self, region_id=None, parameter_id=None, force=False):
    """Generate yearly aggregates for weather data."""
    try:
        queryset = WeatherRecord.objects.select_related('region', 'parameter')
        
        if region_id:
            queryset = queryset.filter(region_id=region_id)
        if parameter_id:
            queryset = queryset.filter(parameter_id=parameter_id)
        
        # Group by region, parameter, and year
        aggregates = queryset.annotate(
            year=TruncYear('date')
        ).values('region', 'parameter', 'year').annotate(
            avg_value=Avg('value'),
            min_value=Min('value'),
            max_value=Max('value'),
            record_count=Count('id')
        ).order_by('region', 'parameter', 'year')

        created_count = 0
        updated_count = 0

        for agg in aggregates:
            # Calculate period start and end
            period_start = agg['year'].replace(month=1, day=1)
            period_end = period_start.replace(year=period_start.year + 1, month=1, day=1) - timezone.timedelta(days=1)

            # Check if aggregate already exists
            existing = WeatherAggregate.objects.filter(
                region_id=agg['region'],
                parameter_id=agg['parameter'],
                aggregate_type='yearly',
                period_start=period_start,
                period_end=period_end
            ).first()

            if existing and not force:
                continue

            aggregate_data = {
                'region_id': agg['region'],
                'parameter_id': agg['parameter'],
                'aggregate_type': 'yearly',
                'period_start': period_start,
                'period_end': period_end,
                'avg_value': agg['avg_value'],
                'min_value': agg['min_value'],
                'max_value': agg['max_value'],
                'record_count': agg['record_count']
            }

            if existing:
                for key, value in aggregate_data.items():
                    setattr(existing, key, value)
                existing.save()
                updated_count += 1
            else:
                WeatherAggregate.objects.create(**aggregate_data)
                created_count += 1

        logger.info(f"Generated {created_count} new and {updated_count} updated yearly aggregates")
        return {
            'status': 'success',
            'created': created_count,
            'updated': updated_count,
            'aggregate_type': 'yearly'
        }
        
    except Exception as e:
        logger.error(f"Error generating yearly aggregates: {e}")
        raise self.retry(countdown=60, max_retries=3)


@shared_task(bind=True)
def generate_seasonal_aggregates(self, region_id=None, parameter_id=None, force=False):
    """Generate seasonal aggregates for weather data."""
    try:
        queryset = WeatherRecord.objects.select_related('region', 'parameter')
        
        if region_id:
            queryset = queryset.filter(region_id=region_id)
        if parameter_id:
            queryset = queryset.filter(parameter_id=parameter_id)
        
        # Define seasons
        seasons = {
            'spring': (3, 5),  # March to May
            'summer': (6, 8),  # June to August
            'autumn': (9, 11), # September to November
            'winter': (12, 2)  # December to February
        }

        created_count = 0
        updated_count = 0

        for season_name, (start_month, end_month) in seasons.items():
            # Build season filter
            if start_month <= end_month:
                # Same year season (spring, summer, autumn)
                season_filter = Q(date__month__gte=start_month) & Q(date__month__lte=end_month)
            else:
                # Cross-year season (winter)
                season_filter = Q(date__month__gte=start_month) | Q(date__month__lte=end_month)

            season_queryset = queryset.filter(season_filter)
            
            # Group by region, parameter, and year
            aggregates = season_queryset.values('region', 'parameter').annotate(
                avg_value=Avg('value'),
                min_value=Min('value'),
                max_value=Max('value'),
                record_count=Count('id')
            ).order_by('region', 'parameter')

            for agg in aggregates:
                # For seasonal aggregates, we'll use the current year as period
                current_year = timezone.now().year
                
                if start_month <= end_month:
                    period_start = date(current_year, start_month, 1)
                    if end_month == 12:
                        period_end = date(current_year + 1, 1, 1) - timezone.timedelta(days=1)
                    else:
                        period_end = date(current_year, end_month + 1, 1) - timezone.timedelta(days=1)
                else:
                    # Winter spans two years
                    period_start = date(current_year - 1, start_month, 1)
                    period_end = date(current_year, end_month + 1, 1) - timezone.timedelta(days=1)

                # Check if aggregate already exists
                existing = WeatherAggregate.objects.filter(
                    region_id=agg['region'],
                    parameter_id=agg['parameter'],
                    aggregate_type='seasonal',
                    period_start=period_start,
                    period_end=period_end
                ).first()

                if existing and not force:
                    continue

                aggregate_data = {
                    'region_id': agg['region'],
                    'parameter_id': agg['parameter'],
                    'aggregate_type': 'seasonal',
                    'period_start': period_start,
                    'period_end': period_end,
                    'avg_value': agg['avg_value'],
                    'min_value': agg['min_value'],
                    'max_value': agg['max_value'],
                    'record_count': agg['record_count']
                }

                if existing:
                    for key, value in aggregate_data.items():
                        setattr(existing, key, value)
                    existing.save()
                    updated_count += 1
                else:
                    WeatherAggregate.objects.create(**aggregate_data)
                    created_count += 1

        logger.info(f"Generated {created_count} new and {updated_count} updated seasonal aggregates")
        return {
            'status': 'success',
            'created': created_count,
            'updated': updated_count,
            'aggregate_type': 'seasonal'
        }
        
    except Exception as e:
        logger.error(f"Error generating seasonal aggregates: {e}")
        raise self.retry(countdown=60, max_retries=3)


@shared_task(bind=True)
def generate_all_aggregates(self, region_id=None, parameter_id=None, force=False):
    """Generate all types of aggregates for weather data."""
    try:
        # Generate all types of aggregates asynchronously
        # Don't wait for results - let them run independently
        generate_monthly_aggregates.delay(region_id, parameter_id, force)
        generate_yearly_aggregates.delay(region_id, parameter_id, force)
        generate_seasonal_aggregates.delay(region_id, parameter_id, force)
        
        logger.info(f"Triggered aggregate generation for region_id={region_id}, parameter_id={parameter_id}")
        return {
            'status': 'success',
            'message': 'Aggregate generation tasks triggered successfully',
            'region_id': region_id,
            'parameter_id': parameter_id
        }
        
    except Exception as e:
        logger.error(f"Error triggering aggregate generation: {e}")
        raise self.retry(countdown=60, max_retries=3)
