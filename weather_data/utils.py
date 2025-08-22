import requests
from datetime import datetime, date
from decimal import Decimal, InvalidOperation
from typing import List, Dict, Optional, Tuple
import logging
from django.conf import settings
from django.utils import timezone
from django.db import transaction
from .models import Region, WeatherParameter, WeatherRecord, DataIngestionLog, SeasonalSummary

logger = logging.getLogger(__name__)


class MetOfficeDataParser:
    """
    Parser for UK MetOffice weather data files.
    """
    
    def __init__(self):
        self.base_url = settings.METOFFICE_BASE_URL
        self.timeout = settings.METOFFICE_TIMEOUT
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'UK-Weather-Data-API/1.0'
        })

    def fetch_data(self, parameter: str, region: str) -> Optional[str]:
        """
        Fetch weather data from MetOffice API.
        
        Args:
            parameter: Weather parameter (e.g., 'Tmax', 'Tmin')
            region: Region code (e.g., 'UK', 'England')
            
        Returns:
            Raw data as string or None if failed
        """
        url = f"{self.base_url}/{parameter}/date/{region}.txt"
        
        try:
            logger.info(f"Fetching data from: {url}")
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            logger.error(f"Failed to fetch data from {url}: {e}")
            return None

    def parse_data(self, raw_data: str, parameter: str, region: str) -> List[Dict]:
        """
        Parse raw MetOffice data into structured format.
        
        Args:
            raw_data: Raw data string from MetOffice
            parameter: Weather parameter name
            region: Region name
            
        Returns:
            Tuple of (records, seasonal_summaries)
        """
        records = []
        seasonal_summaries = []
        lines = raw_data.strip().split('\n')
        
        # Skip header lines (usually first 5-10 lines)
        data_start = 0
        for i, line in enumerate(lines):
            line_stripped = line.strip()
            if line_stripped and not line_stripped.startswith('#'):
                # Look for the actual data header - check for both uppercase and lowercase month names
                if any(month in line_stripped.lower() for month in ['jan', 'feb', 'mar', 'apr', 'may', 'jun']):
                    data_start = i + 1
                    break
                # Also check if this line starts with a year (data line)
                parts = line_stripped.split()
                if parts and parts[0].isdigit() and len(parts) >= 13:
                    data_start = i
                    break
        
        if data_start == 0:
            logger.warning(f"Could not find data start in {parameter}/{region}")
            return records
        
        # Parse data lines
        for line in lines[data_start:]:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
                
            try:
                # Parse the data line
                parts = line.split()
                if len(parts) < 2:  # At least year + 1 month
                    continue
                    
                year = int(parts[0])
                
                # Parse monthly values (only the first 12 parts after year are months)
                # Handle partial years (like 2025 which only has 7 months)
                # Summary statistics appear after the available months, not after 12 months
                
                # Determine how many months are available based on data structure
                total_parts = len(parts)
                
                # For complete years: 18 parts (year + 12 months + 5 summary stats)
                # For partial years: fewer parts (year + available months + available summary stats)
                if total_parts >= 18:
                    # Complete year - process first 12 parts as months
                    available_months = 12
                else:
                    # Partial year - need to determine where months end and summary stats begin
                    # Let's use a more precise approach based on the data structure
                    
                    # For partial years, we need to determine the exact number of months
                    # The key insight is that summary statistics appear immediately after available months
                    
                    # Let's use a more sophisticated approach:
                    # 1. For partial years, we know the structure is: year + months + summary_stats
                    # 2. We need to determine where months end and summary stats begin
                    # 3. Let's use the fact that summary stats are typically smaller values
                    
                    # Count how many consecutive valid monthly values we have
                    month_count = 0
                    for i in range(1, min(13, total_parts)):  # Check first 12 positions
                        if i >= total_parts:
                            break
                        
                        value_str = parts[i]
                        if value_str.strip() == '---' or not value_str.strip() or value_str.strip() == '':
                            break
                        
                        try:
                            value = float(value_str)
                            # For partial years, we need to be more careful about what constitutes a month
                            # Let's use a more restrictive approach
                            
                            # If this is a partial year (less than 18 parts), be more careful
                            if total_parts < 18:
                                # For partial years, summary stats appear immediately after available months
                                # We need to detect when we've reached the end of monthly data
                                
                                # Heuristic: if we're past month 7 and the value is unusually small, it might be a summary stat
                                if i > 7 and value < 10:
                                    # This might be a summary statistic, not a month
                                    break
                            
                            # If this looks like a valid monthly temperature (reasonable range)
                            if -50 <= value <= 50:  # Reasonable temperature range
                                month_count += 1
                            else:
                                break
                        except ValueError:
                            break
                    
                    available_months = month_count
                
                for month_idx in range(available_months):
                    if month_idx + 1 >= len(parts):  # No more data available
                        break
                        
                    value_str = parts[month_idx + 1]
                    
                    # Skip empty values or dashes
                    if value_str.strip() == '---' or not value_str.strip() or value_str.strip() == '':
                        continue
                        
                    try:
                        value = float(value_str)
                        # Note: MetOffice data is already in °C, no conversion needed
                        # The old assumption about 0.1°C was incorrect
                        
                        record_date = date(year, month_idx + 1, 1)
                        
                        records.append({
                            'date': record_date,
                            'value': Decimal(str(value)),
                            'parameter': parameter,
                            'region': region,
                            'quality_flag': ''
                        })
                        
                    except (ValueError, InvalidOperation) as e:
                        logger.warning(f"Invalid value '{value_str}' for {parameter}/{region} {year}-{month_idx+1}: {e}")
                        continue
                
                # Parse seasonal summary statistics (columns 13-17 after the 12 months)
                # Header: year jan feb mar apr may jun jul aug sep oct nov dec win spr sum aut ann
                # For complete years: columns 13-17 contain all 5 seasonal summaries
                # For partial years: only some seasonal summaries are available
                seasonal_columns = [
                    ('winter', 13),    # win (Dec, Jan, Feb) - column 13
                    ('spring', 14),    # spr (Mar, Apr, May) - column 14
                    ('summer', 15),    # sum (Jun, Jul, Aug) - column 15
                    ('autumn', 16),    # aut (Sep, Oct, Nov) - column 16
                    ('annual', 17),    # ann (All 12 months) - column 17
                ]
                
                for season, col_idx in seasonal_columns:
                    if col_idx < len(parts):
                        value_str = parts[col_idx]
                        if value_str.strip() != '---' and value_str.strip():
                            try:
                                value = float(value_str)
                                seasonal_summaries.append({
                                    'year': year,
                                    'season': season,
                                    'value': Decimal(str(value)),
                                    'parameter': parameter,
                                    'region': region
                                })
                            except (ValueError, InvalidOperation) as e:
                                logger.warning(f"Invalid seasonal value '{value_str}' for {parameter}/{region} {year} {season}: {e}")
                                continue
                        
            except (ValueError, IndexError) as e:
                logger.warning(f"Failed to parse line '{line}' for {parameter}/{region}: {e}")
                continue
        
        logger.info(f"Parsed {len(records)} records and {len(seasonal_summaries)} seasonal summaries for {parameter}/{region}")
        return records, seasonal_summaries

    def validate_record(self, record: Dict) -> bool:
        """
        Validate a weather record.
        
        Args:
            record: Weather record dictionary
            
        Returns:
            True if valid, False otherwise
        """
        try:
            # Check required fields
            required_fields = ['date', 'value', 'parameter', 'region']
            for field in required_fields:
                if field not in record:
                    logger.warning(f"Missing required field: {field}")
                    return False
            
            # Validate date
            if not isinstance(record['date'], date):
                logger.warning(f"Invalid date format: {record['date']}")
                return False
            
            # Validate value
            value = record['value']
            if not isinstance(value, Decimal):
                logger.warning(f"Invalid value type: {type(value)}")
                return False
            
            # Parameter-specific validation
            parameter = record['parameter']
            if parameter in ['Tmax', 'Tmin', 'Tmean']:
                # Temperature should be between -50 and 50°C
                if value < -50 or value > 50:
                    logger.warning(f"Temperature value out of range: {value}°C")
                    return False
            elif parameter == 'Rainfall':
                # Rainfall should be positive
                if value < 0:
                    logger.warning(f"Negative rainfall value: {value}")
                    return False
            elif parameter == 'Sunshine':
                # Sunshine hours should be between 0 and 24
                if value < 0 or value > 24:
                    logger.warning(f"Sunshine hours out of range: {value}")
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error validating record: {e}")
            return False

    @transaction.atomic
    def save_records(self, records: List[Dict], seasonal_summaries: List[Dict], ingestion_log: DataIngestionLog) -> Tuple[int, int, int]:
        """
        Save weather records and seasonal summaries to database.
        
        Args:
            records: List of weather record dictionaries
            seasonal_summaries: List of seasonal summary dictionaries
            ingestion_log: DataIngestionLog instance
            
        Returns:
            Tuple of (created_count, updated_count, failed_count)
        """
        created_count = 0
        updated_count = 0
        failed_count = 0
        
        # Get or create region and parameter
        if records:
            region_obj, _ = Region.objects.get_or_create(
                code=records[0]['region'],
                defaults={'name': records[0]['region'], 'is_active': True}
            )
            
            parameter_obj, _ = WeatherParameter.objects.get_or_create(
                name=records[0]['parameter'],
                defaults={
                    'display_name': records[0]['parameter'],
                    'unit': self._get_unit_for_parameter(records[0]['parameter']),
                    'is_active': True
                }
            )
        elif seasonal_summaries:
            region_obj, _ = Region.objects.get_or_create(
                code=seasonal_summaries[0]['region'],
                defaults={'name': seasonal_summaries[0]['region'], 'is_active': True}
            )
            
            parameter_obj, _ = WeatherParameter.objects.get_or_create(
                name=seasonal_summaries[0]['parameter'],
                defaults={
                    'display_name': seasonal_summaries[0]['parameter'],
                    'unit': self._get_unit_for_parameter(seasonal_summaries[0]['parameter']),
                    'is_active': True
                }
            )
        else:
            logger.warning("No records or seasonal summaries to save")
            return 0, 0, 0
        
        # Process records in batches
        batch_size = settings.INGESTION_BATCH_SIZE
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            
            for record_data in batch:
                try:
                    # Validate record
                    if not self.validate_record(record_data):
                        failed_count += 1
                        continue
                    
                    # Create or update record
                    record, created = WeatherRecord.objects.update_or_create(
                        region=region_obj,
                        parameter=parameter_obj,
                        date=record_data['date'],
                        defaults={
                            'value': record_data['value'],
                            'quality_flag': record_data.get('quality_flag', '')
                        }
                    )
                    
                    if created:
                        created_count += 1
                    else:
                        updated_count += 1
                        
                except Exception as e:
                    logger.error(f"Failed to save record {record_data}: {e}")
                    failed_count += 1
        
        # Save seasonal summaries
        seasonal_created = 0
        seasonal_updated = 0
        for summary_data in seasonal_summaries:
            try:
                summary, created = SeasonalSummary.objects.update_or_create(
                    region=region_obj,
                    parameter=parameter_obj,
                    year=summary_data['year'],
                    season=summary_data['season'],
                    defaults={
                        'value': summary_data['value']
                    }
                )
                
                if created:
                    seasonal_created += 1
                else:
                    seasonal_updated += 1
                    
            except Exception as e:
                logger.error(f"Failed to save seasonal summary {summary_data}: {e}")
                failed_count += 1
        
        # Update ingestion log
        total_processed = len(records) + len(seasonal_summaries)
        total_created = created_count + seasonal_created
        total_updated = updated_count + seasonal_updated
        
        ingestion_log.records_processed = total_processed
        ingestion_log.records_created = total_created
        ingestion_log.records_updated = total_updated
        ingestion_log.records_failed = failed_count
        
        if failed_count == 0:
            ingestion_log.mark_completed(created_count, updated_count, failed_count)
        else:
            ingestion_log.status = 'partial'
            ingestion_log.end_time = timezone.now()
            ingestion_log.save()
        
        logger.info(f"Saved {total_created} new, {total_updated} updated, {failed_count} failed records (including {seasonal_created} seasonal summaries)")
        
        # Trigger aggregate generation in background if records were created/updated
        if created_count > 0 or updated_count > 0:
            try:
                from .tasks import generate_all_aggregates
                # Trigger background task for aggregate generation
                generate_all_aggregates.delay(
                    region_id=region_obj.id,
                    parameter_id=parameter_obj.id,
                    force=False
                )
                logger.info(f"Triggered background aggregate generation for {region_obj.name} - {parameter_obj.name}")
            except Exception as e:
                logger.error(f"Failed to trigger aggregate generation: {e}")
        
        return created_count, updated_count, failed_count

    def _get_unit_for_parameter(self, parameter: str) -> str:
        """Get the unit for a weather parameter."""
        units = {
            'Tmax': '°C',
            'Tmin': '°C',
            'Tmean': '°C',
            'Rainfall': 'mm',
            'Sunshine': 'hours',
            'AirFrost': 'days',
            'RainDays1mm': 'days',
            'RainDays10mm': 'days',
        }
        return units.get(parameter, 'unknown')


class DataIngestionManager:
    """
    Manager for coordinating data ingestion processes.
    """
    
    def __init__(self):
        self.parser = MetOfficeDataParser()
    
    def ingest_parameter_data(self, parameter: str, region: str) -> DataIngestionLog:
        """
        Ingest weather data for a specific parameter and region.
        
        Args:
            parameter: Weather parameter name
            region: Region code
            
        Returns:
            DataIngestionLog instance
        """
        # Create ingestion log
        ingestion_log = DataIngestionLog.objects.create(
            parameter=WeatherParameter.objects.filter(name=parameter).first(),
            region=Region.objects.filter(code=region).first(),
            status='in_progress',
            source_url=f"{self.parser.base_url}/{parameter}/date/{region}.txt"
        )
        
        try:
            # Fetch data
            raw_data = self.parser.fetch_data(parameter, region)
            if not raw_data:
                ingestion_log.mark_failed("Failed to fetch data from MetOffice")
                return ingestion_log
            
            # Parse data
            records, seasonal_summaries = self.parser.parse_data(raw_data, parameter, region)
            if not records and not seasonal_summaries:
                ingestion_log.mark_failed("No valid records or seasonal summaries found in data")
                return ingestion_log
            
            # Save records and seasonal summaries
            self.parser.save_records(records, seasonal_summaries, ingestion_log)
            
        except Exception as e:
            logger.error(f"Error during data ingestion for {parameter}/{region}: {e}")
            ingestion_log.mark_failed(str(e))
        
        return ingestion_log
    
    def ingest_all_data(self) -> List[DataIngestionLog]:
        """
        Ingest data for all available parameters and regions.
        
        Returns:
            List of DataIngestionLog instances
        """
        # Define available parameters and regions
        parameters = ['Tmax', 'Tmin', 'Tmean', 'Rainfall', 'Sunshine']
        regions = ['UK', 'England', 'Wales', 'Scotland', 'Northern_Ireland']
        
        logs = []
        for parameter in parameters:
            for region in regions:
                try:
                    log = self.ingest_parameter_data(parameter, region)
                    logs.append(log)
                except Exception as e:
                    logger.error(f"Failed to ingest {parameter}/{region}: {e}")
        
        return logs 