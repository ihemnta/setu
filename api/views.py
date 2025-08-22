from rest_framework import viewsets, status, filters
from rest_framework.decorators import action, api_view, permission_classes
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticatedOrReadOnly, AllowAny, IsAuthenticated
from django_filters.rest_framework import DjangoFilterBackend
from django.db.models import Avg, Min, Max, Count, Q
from django.utils.decorators import method_decorator
from django.views.decorators.cache import cache_page
from django.core.cache import cache
from django.http import HttpResponse
from django.conf import settings
from django.utils import timezone
import json
import logging
from datetime import datetime, timedelta
from io import StringIO, BytesIO

from weather_data.models import (
    Region, 
    WeatherParameter, 
    WeatherRecord, 
    DataIngestionLog, 
    WeatherAggregate
)
from weather_data.utils import DataIngestionManager
from weather_data.redis_utils import (
    redis_manager, 
    CacheKeys, 
    cache_api_status, 
    get_cached_api_status,
    increment_api_counter,
    get_api_usage_stats
)
from .serializers import (
    RegionSerializer,
    WeatherParameterSerializer,
    WeatherRecordSerializer,
    WeatherRecordListSerializer,
    DataIngestionLogSerializer,
    WeatherAggregateSerializer,
    WeatherDataFilterSerializer,
    WeatherAggregateFilterSerializer,
    WeatherStatisticsSerializer,
    DataIngestionRequestSerializer,
    WeatherDataExportSerializer,
    APIStatusSerializer
)

logger = logging.getLogger(__name__)


class RegionViewSet(viewsets.ReadOnlyModelViewSet):
    """ViewSet for Region model."""
    
    queryset = Region.objects.filter(is_active=True)
    serializer_class = RegionSerializer
    permission_classes = [IsAuthenticatedOrReadOnly]
    filter_backends = [DjangoFilterBackend, filters.SearchFilter, filters.OrderingFilter]
    filterset_fields = ['is_active']
    search_fields = ['name', 'code', 'description']
    ordering_fields = ['name', 'code', 'created_at']
    ordering = ['name']

    @method_decorator(cache_page(60 * 15))  # Cache for 15 minutes
    def list(self, request, *args, **kwargs):
        return super().list(request, *args, **kwargs)

    @action(detail=True, methods=['get'])
    def weather_data(self, request, pk=None):
        """Get weather data for a specific region."""
        region = self.get_object()
        queryset = WeatherRecord.objects.filter(region=region).select_related('parameter')
        
        # Apply filters
        serializer = WeatherDataFilterSerializer(data=request.query_params)
        if serializer.is_valid():
            filters = serializer.validated_data
            if filters.get('parameter'):
                queryset = queryset.filter(parameter__name=filters['parameter'])
            if filters.get('start_date'):
                queryset = queryset.filter(date__gte=filters['start_date'])
            if filters.get('end_date'):
                queryset = queryset.filter(date__lte=filters['end_date'])
            if filters.get('min_value'):
                queryset = queryset.filter(value__gte=filters['min_value'])
            if filters.get('max_value'):
                queryset = queryset.filter(value__lte=filters['max_value'])
        
        # Paginate results
        page = self.paginate_queryset(queryset)
        if page is not None:
            serializer = WeatherRecordListSerializer(page, many=True)
            return self.get_paginated_response(serializer.data)
        
        serializer = WeatherRecordListSerializer(queryset, many=True)
        return Response(serializer.data)


class WeatherParameterViewSet(viewsets.ReadOnlyModelViewSet):
    """ViewSet for WeatherParameter model."""
    
    queryset = WeatherParameter.objects.filter(is_active=True)
    serializer_class = WeatherParameterSerializer
    permission_classes = [IsAuthenticatedOrReadOnly]
    filter_backends = [DjangoFilterBackend, filters.SearchFilter, filters.OrderingFilter]
    filterset_fields = ['is_active']
    search_fields = ['name', 'display_name', 'description']
    ordering_fields = ['name', 'display_name', 'created_at']
    ordering = ['name']

    @method_decorator(cache_page(60 * 15))  # Cache for 15 minutes
    def list(self, request, *args, **kwargs):
        return super().list(request, *args, **kwargs)

    @action(detail=True, methods=['get'])
    def weather_data(self, request, pk=None):
        """Get weather data for a specific parameter."""
        parameter = self.get_object()
        queryset = WeatherRecord.objects.filter(parameter=parameter).select_related('region')
        
        # Apply filters
        serializer = WeatherDataFilterSerializer(data=request.query_params)
        if serializer.is_valid():
            filters = serializer.validated_data
            if filters.get('region'):
                queryset = queryset.filter(region__code=filters['region'])
            if filters.get('start_date'):
                queryset = queryset.filter(date__gte=filters['start_date'])
            if filters.get('end_date'):
                queryset = queryset.filter(date__lte=filters['end_date'])
            if filters.get('min_value'):
                queryset = queryset.filter(value__gte=filters['min_value'])
            if filters.get('max_value'):
                queryset = queryset.filter(value__lte=filters['max_value'])
        
        # Paginate results
        page = self.paginate_queryset(queryset)
        if page is not None:
            serializer = WeatherRecordListSerializer(page, many=True)
            return self.get_paginated_response(serializer.data)
        
        serializer = WeatherRecordListSerializer(queryset, many=True)
        return Response(serializer.data)


class WeatherRecordViewSet(viewsets.ReadOnlyModelViewSet):
    """ViewSet for WeatherRecord model."""
    
    queryset = WeatherRecord.objects.select_related('region', 'parameter')
    serializer_class = WeatherRecordSerializer
    permission_classes = [IsAuthenticatedOrReadOnly]
    filter_backends = [DjangoFilterBackend, filters.SearchFilter, filters.OrderingFilter]
    filterset_fields = ['region', 'parameter', 'quality_flag']
    search_fields = ['region__name', 'parameter__name']
    ordering_fields = ['date', 'value', 'created_at']
    ordering = ['-date']

    def get_queryset(self):
        """Apply additional filters from query parameters."""
        queryset = super().get_queryset()
        
        # Apply date range filters
        start_date = self.request.query_params.get('start_date')
        end_date = self.request.query_params.get('end_date')
        
        if start_date:
            queryset = queryset.filter(date__gte=start_date)
        if end_date:
            queryset = queryset.filter(date__lte=end_date)
        
        # Apply value range filters
        min_value = self.request.query_params.get('min_value')
        max_value = self.request.query_params.get('max_value')
        
        if min_value:
            queryset = queryset.filter(value__gte=min_value)
        if max_value:
            queryset = queryset.filter(value__lte=max_value)
        
        return queryset

    @method_decorator(cache_page(60 * 10))  # Cache for 10 minutes
    def list(self, request, *args, **kwargs):
        """List weather records with caching."""
        # Increment API counter
        increment_api_counter('weather_records_list')
        return super().list(request, *args, **kwargs)

    @action(detail=False, methods=['get'])
    def aggregate(self, request):
        """Get aggregated weather statistics."""
        queryset = self.get_queryset()
        
        # Get aggregation parameters
        group_by = request.query_params.get('group_by', 'parameter')
        agg_type = request.query_params.get('type', 'avg')
        
        if group_by == 'parameter':
            queryset = queryset.values('parameter__name').annotate(
                avg_value=Avg('value'),
                min_value=Min('value'),
                max_value=Max('value'),
                count=Count('id')
            )
        elif group_by == 'region':
            queryset = queryset.values('region__name').annotate(
                avg_value=Avg('value'),
                min_value=Min('value'),
                max_value=Max('value'),
                count=Count('id')
            )
        elif group_by == 'month':
            queryset = queryset.extra(
                select={'month': "EXTRACT(month FROM date)"}
            ).values('month').annotate(
                avg_value=Avg('value'),
                min_value=Min('value'),
                max_value=Max('value'),
                count=Count('id')
            ).order_by('month')
        
        return Response(queryset)

    @action(detail=False, methods=['get'])
    def statistics(self, request):
        """Get comprehensive weather statistics."""
        queryset = self.get_queryset()
        
        # Basic statistics
        stats = queryset.aggregate(
            total_records=Count('id'),
            avg_value=Avg('value'),
            min_value=Min('value'),
            max_value=Max('value')
        )
        
        # Date range
        date_range = queryset.aggregate(
            earliest_date=Min('date'),
            latest_date=Max('date')
        )
        
        # Monthly averages for the last year
        one_year_ago = datetime.now().date() - timedelta(days=365)
        monthly_data = queryset.filter(
            date__gte=one_year_ago
        ).extra(
            select={'year': "EXTRACT(year FROM date)", 'month': "EXTRACT(month FROM date)"}
        ).values('year', 'month').annotate(
            avg_value=Avg('value')
        ).order_by('year', 'month')
        
        response_data = {
            'statistics': stats,
            'date_range': date_range,
            'monthly_averages': list(monthly_data)
        }
        
        return Response(response_data)

    @action(detail=False, methods=['post'])
    def export(self, request):
        """Export weather data in various formats."""
        serializer = WeatherDataExportSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        
        data = serializer.validated_data
        queryset = self.get_queryset()
        
        # Apply additional filters
        if data.get('region'):
            queryset = queryset.filter(region__code=data['region'])
        if data.get('parameter'):
            queryset = queryset.filter(parameter__name=data['parameter'])
        if data.get('start_date'):
            queryset = queryset.filter(date__gte=data['start_date'])
        if data.get('end_date'):
            queryset = queryset.filter(date__lte=data['end_date'])
        
        # Get records
        records = list(queryset.values(
            'region__name', 'parameter__name', 'parameter__unit',
            'date', 'value', 'quality_flag'
        ))
        
        # Export based on format
        if data['format'] == 'csv':
            import csv
            output = StringIO()
            writer = csv.writer(output)
            
            # Write header
            if records:
                writer.writerow(records[0].keys())
            
            # Write data
            for record in records:
                writer.writerow(record.values())
            
            response = HttpResponse(output.getvalue(), content_type='text/csv')
            response['Content-Disposition'] = 'attachment; filename="weather_data.csv"'
            return response
        
        elif data['format'] == 'json':
            return Response(records)
        
        elif data['format'] == 'xlsx':
            # For now, return JSON since we don't have pandas
            return Response({
                'message': 'XLSX export requires pandas. Please use CSV or JSON format.',
                'data': records
            })


class DataIngestionLogViewSet(viewsets.ReadOnlyModelViewSet):
    """ViewSet for DataIngestionLog model."""
    
    queryset = DataIngestionLog.objects.select_related('region', 'parameter')
    serializer_class = DataIngestionLogSerializer
    permission_classes = [IsAuthenticatedOrReadOnly]
    filter_backends = [DjangoFilterBackend, filters.SearchFilter, filters.OrderingFilter]
    filterset_fields = ['status', 'region', 'parameter']
    search_fields = ['error_message']
    ordering_fields = ['start_time', 'end_time', 'records_processed']
    ordering = ['-start_time']

    def get_queryset(self):
        """Apply additional filters from query parameters."""
        queryset = super().get_queryset()
        
        # Handle parameter filter by name
        parameter_name = self.request.query_params.get('parameter')
        if parameter_name:
            queryset = queryset.filter(parameter__name=parameter_name)
        
        return queryset

    @action(detail=False, methods=['post'], permission_classes=[IsAuthenticated])
    def trigger_ingestion(self, request):
        """Trigger data ingestion process."""
        serializer = DataIngestionRequestSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        
        data = serializer.validated_data
        manager = DataIngestionManager()
        
        try:
            if data.get('ingest_all'):
                logs = manager.ingest_all_data()
                message = f"Started ingestion for all parameters and regions. {len(logs)} processes initiated."
            elif data.get('parameter') and data.get('region'):
                log = manager.ingest_parameter_data(data['parameter'], data['region'])
                logs = [log]
                message = f"Started ingestion for {data['parameter']} in {data['region']}."
            elif data.get('parameter'):
                # Ingest for all regions
                regions = ['UK', 'England', 'Wales', 'Scotland', 'NI']
                logs = []
                for region in regions:
                    log = manager.ingest_parameter_data(data['parameter'], region)
                    logs.append(log)
                message = f"Started ingestion for {data['parameter']} across all regions."
            elif data.get('region'):
                # Ingest for all parameters
                parameters = ['Tmax', 'Tmin', 'Tmean', 'Rainfall', 'Sunshine']
                logs = []
                for parameter in parameters:
                    log = manager.ingest_parameter_data(parameter, data['region'])
                    logs.append(log)
                message = f"Started ingestion for all parameters in {data['region']}."
            
            return Response({
                'message': message,
                'logs': DataIngestionLogSerializer(logs, many=True).data
            }, status=status.HTTP_202_ACCEPTED)
            
        except Exception as e:
            logger.error(f"Error triggering ingestion: {e}")
            return Response({
                'error': str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class WeatherAggregateViewSet(viewsets.ReadOnlyModelViewSet):
    """ViewSet for WeatherAggregate model."""
    
    queryset = WeatherAggregate.objects.select_related('region', 'parameter')
    serializer_class = WeatherAggregateSerializer
    permission_classes = [IsAuthenticatedOrReadOnly]
    filter_backends = [DjangoFilterBackend, filters.SearchFilter, filters.OrderingFilter]
    filterset_fields = ['region', 'parameter', 'aggregate_type']
    search_fields = ['region__name', 'parameter__name']
    ordering_fields = ['period_start', 'period_end', 'avg_value']
    ordering = ['-period_start']

    def get_queryset(self):
        """Apply additional filters from query parameters."""
        queryset = super().get_queryset()
        
        # Apply date range filters
        start_date = self.request.query_params.get('start_date')
        end_date = self.request.query_params.get('end_date')
        
        if start_date:
            queryset = queryset.filter(period_start__gte=start_date)
        if end_date:
            queryset = queryset.filter(period_end__lte=end_date)
        
        return queryset




class APIStatusViewSet(viewsets.ViewSet):
    """ViewSet for API status and health checks."""
    
    serializer_class = APIStatusSerializer
    permission_classes = [IsAuthenticatedOrReadOnly]

    @action(detail=False, methods=['get'])
    def status(self, request):
        """Get API status information."""
        try:
            # Increment API counter
            increment_api_counter('api_status')
            
            # Try to get cached status first
            cached_status = get_cached_api_status()
            if cached_status:
                return Response(cached_status)
            
            # Get basic statistics
            total_records = WeatherRecord.objects.count()
            total_regions = Region.objects.filter(is_active=True).count()
            total_parameters = WeatherParameter.objects.filter(is_active=True).count()
            
            # Get last ingestion
            last_ingestion = DataIngestionLog.objects.filter(
                status='completed'
            ).order_by('-end_time').first()
            
            # Check cache status
            cache_status = 'unavailable'
            try:
                cache_key = 'api_status_test'
                cache.set(cache_key, 'test', 60)
                if cache.get(cache_key) == 'test':
                    cache_status = 'healthy'
                cache.delete(cache_key)
            except Exception:
                cache_status = 'unavailable'
            
            status_data = {
                'status': 'healthy',
                'timestamp': timezone.now(),
                'version': getattr(settings, 'API_VERSION', 'v1'),
                'total_records': total_records,
                'total_regions': total_regions,
                'total_parameters': total_parameters,
                'last_ingestion': last_ingestion.end_time if last_ingestion else None,
                'database_size': 'N/A',  # Could be implemented with database-specific queries
                'cache_status': cache_status
            }
            
            # Cache the status data
            cache_api_status(status_data)
            
            serializer = APIStatusSerializer(status_data)
            return Response(serializer.data)
            
        except Exception as e:
            logger.error(f"Error getting API status: {e}")
            return Response({
                'status': 'unhealthy',
                'error': str(e),
                'timestamp': timezone.now()
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    @action(detail=False, methods=['get'])
    def health(self, request):
        """Simple health check endpoint."""
        return Response({'status': 'healthy'})

    @action(detail=False, methods=['get'])
    def usage_stats(self, request):
        """Get API usage statistics."""
        try:
            # Increment API counter
            increment_api_counter('api_usage_stats')
            
            # Get usage statistics
            usage_stats = get_api_usage_stats()
            
            return Response({
                'date': timezone.now().strftime('%Y-%m-%d'),
                'usage_stats': usage_stats,
                'total_requests': sum(usage_stats.values())
            })
            
        except Exception as e:
            logger.error(f"Error getting API usage stats: {e}")
            return Response({
                'error': str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
