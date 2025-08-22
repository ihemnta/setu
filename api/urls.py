from django.urls import path, include
from rest_framework.routers import DefaultRouter
from rest_framework.documentation import include_docs_urls
from drf_spectacular.views import SpectacularAPIView, SpectacularSwaggerView, SpectacularRedocView
from .views import (
    RegionViewSet,
    WeatherParameterViewSet,
    WeatherRecordViewSet,
    DataIngestionLogViewSet,
    WeatherAggregateViewSet,
    APIStatusViewSet
)

# Create router and register viewsets
router = DefaultRouter()
router.register(r'regions', RegionViewSet, basename='region')
router.register(r'parameters', WeatherParameterViewSet, basename='parameter')
router.register(r'weather', WeatherRecordViewSet, basename='weather')
router.register(r'ingestion', DataIngestionLogViewSet, basename='ingestion')
router.register(r'aggregates', WeatherAggregateViewSet, basename='aggregate')
router.register(r'status', APIStatusViewSet, basename='status')

# API URL patterns
urlpatterns = [
    # API v1 endpoints
    path('v1/', include(router.urls)),
    
    # OpenAPI schema
    path('schema/', SpectacularAPIView.as_view(), name='schema'),
    path('schema/swagger-ui/', SpectacularSwaggerView.as_view(url_name='schema'), name='swagger-ui'),
    path('schema/redoc/', SpectacularRedocView.as_view(url_name='schema'), name='redoc'),
    
    # Default to v1
    path('', include(router.urls)),
] 