from django.contrib import admin
from django.utils.html import format_html
from django.urls import reverse
from django.utils.safestring import mark_safe
from .models import Region, WeatherParameter, WeatherRecord, DataIngestionLog, WeatherAggregate, SeasonalSummary


@admin.register(Region)
class RegionAdmin(admin.ModelAdmin):
    list_display = ['name', 'code', 'is_active', 'created_at', 'updated_at']
    list_filter = ['is_active', 'created_at']
    search_fields = ['name', 'code', 'description']
    readonly_fields = ['created_at', 'updated_at']
    ordering = ['name']
    
    fieldsets = (
        ('Basic Information', {
            'fields': ('name', 'code', 'description')
        }),
        ('Status', {
            'fields': ('is_active',)
        }),
        ('Timestamps', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )


@admin.register(WeatherParameter)
class WeatherParameterAdmin(admin.ModelAdmin):
    list_display = ['name', 'display_name', 'unit', 'is_active', 'created_at']
    list_filter = ['is_active', 'created_at']
    search_fields = ['name', 'display_name', 'description']
    readonly_fields = ['created_at', 'updated_at']
    ordering = ['name']
    
    fieldsets = (
        ('Basic Information', {
            'fields': ('name', 'display_name', 'unit', 'description')
        }),
        ('Status', {
            'fields': ('is_active',)
        }),
        ('Timestamps', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )


@admin.register(WeatherRecord)
class WeatherRecordAdmin(admin.ModelAdmin):
    list_display = ['region', 'parameter', 'date', 'value', 'quality_flag', 'created_at']
    list_filter = [
        'region', 
        'parameter', 
        'date', 
        'quality_flag',
        'created_at'
    ]
    search_fields = ['region__name', 'parameter__name', 'quality_flag']
    readonly_fields = ['created_at', 'updated_at']
    date_hierarchy = 'date'
    ordering = ['-date', 'region', 'parameter']
    
    fieldsets = (
        ('Weather Data', {
            'fields': ('region', 'parameter', 'date', 'value', 'quality_flag')
        }),
        ('Timestamps', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )
    
    def get_queryset(self, request):
        return super().get_queryset(request).select_related('region', 'parameter')


@admin.register(DataIngestionLog)
class DataIngestionLogAdmin(admin.ModelAdmin):
    list_display = [
        'id', 
        'region', 
        'parameter', 
        'status', 
        'records_processed',
        'records_created',
        'duration_display',
        'start_time'
    ]
    list_filter = [
        'status', 
        'region', 
        'parameter', 
        'start_time'
    ]
    search_fields = ['region__name', 'parameter__name', 'error_message']
    readonly_fields = [
        'start_time', 
        'end_time', 
        'created_at',
        'duration_display'
    ]
    ordering = ['-start_time']
    
    fieldsets = (
        ('Ingestion Details', {
            'fields': ('region', 'parameter', 'status', 'source_url')
        }),
        ('Records Statistics', {
            'fields': (
                'records_processed', 
                'records_created', 
                'records_updated', 
                'records_failed'
            )
        }),
        ('Timing', {
            'fields': ('start_time', 'end_time', 'duration_display'),
            'classes': ('collapse',)
        }),
        ('Error Information', {
            'fields': ('error_message',),
            'classes': ('collapse',)
        }),
        ('Metadata', {
            'fields': ('created_at',),
            'classes': ('collapse',)
        }),
    )
    
    def duration_display(self, obj):
        """Display duration in a human-readable format."""
        duration = obj.duration
        if duration:
            total_seconds = int(duration.total_seconds())
            hours = total_seconds // 3600
            minutes = (total_seconds % 3600) // 60
            seconds = total_seconds % 60
            return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        return "N/A"
    duration_display.short_description = "Duration"
    
    def get_queryset(self, request):
        return super().get_queryset(request).select_related('region', 'parameter')


@admin.register(WeatherAggregate)
class WeatherAggregateAdmin(admin.ModelAdmin):
    list_display = [
        'region', 
        'parameter', 
        'aggregate_type', 
        'period_start', 
        'period_end',
        'avg_value',
        'record_count'
    ]
    list_filter = [
        'region', 
        'parameter', 
        'aggregate_type', 
        'period_start'
    ]
    search_fields = ['region__name', 'parameter__name']
    readonly_fields = ['created_at', 'updated_at']
    date_hierarchy = 'period_start'
    ordering = ['-period_start', 'region', 'parameter']
    
    fieldsets = (
        ('Aggregate Information', {
            'fields': ('region', 'parameter', 'aggregate_type')
        }),
        ('Period', {
            'fields': ('period_start', 'period_end')
        }),
        ('Statistics', {
            'fields': ('avg_value', 'min_value', 'max_value', 'record_count')
        }),
        ('Timestamps', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )
    
    def get_queryset(self, request):
        return super().get_queryset(request).select_related('region', 'parameter')


@admin.register(SeasonalSummary)
class SeasonalSummaryAdmin(admin.ModelAdmin):
    list_display = [
        'region', 
        'parameter', 
        'year', 
        'season', 
        'value', 
        'created_at'
    ]
    list_filter = [
        'region', 
        'parameter', 
        'year', 
        'season', 
        'created_at'
    ]
    search_fields = ['region__name', 'parameter__name']
    readonly_fields = ['created_at', 'updated_at']
    ordering = ['-year', 'season', 'region', 'parameter']
    
    fieldsets = (
        ('Summary Information', {
            'fields': ('region', 'parameter', 'year', 'season', 'value')
        }),
        ('Timestamps', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )
    
    def get_queryset(self, request):
        return super().get_queryset(request).select_related('region', 'parameter')


# Customize admin site
admin.site.site_header = "UK MetOffice Weather Data Administration"
admin.site.site_title = "Weather Data Admin"
admin.site.index_title = "Welcome to Weather Data Administration"
