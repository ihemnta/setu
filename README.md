# UK Weather Data API

A Django-based REST API for parsing and serving UK MetOffice weather data. This project was built to handle historical weather data from the UK MetOffice, providing both raw data access and calculated aggregates through a clean REST API.

## What This Does

The API fetches weather data from the UK MetOffice website, parses it, stores it in a PostgreSQL database, and serves it through REST endpoints. It also calculates various aggregates (monthly, yearly, seasonal, decadal) and provides caching for better performance.

## Features

- **Data Ingestion**: Automatically fetches and parses MetOffice weather data
- **REST API**: Full CRUD operations for weather data with filtering
- **Authentication**: JWT-based auth for POST operations
- **Caching**: Redis caching for frequently accessed data
- **Background Processing**: Celery handles data aggregation in the background
- **Docker**: Containerized setup for easy deployment

## Tech Stack

- **Backend**: Django 4.2.7 + Django REST Framework
- **Database**: PostgreSQL (using psycopg3)
- **Cache**: Redis
- **Background Jobs**: Celery
- **Containerization**: Docker + Docker Compose
- **Auth**: JWT tokens (djangorestframework-simplejwt)
- **Docs**: drf-spectacular for OpenAPI/Swagger

## Quick Start

### Prerequisites
- Docker and Docker Compose installed
- Git

### Setup

1. **Clone and navigate to the project**
   ```bash
   git clone <your-repo-url>
   cd setu
   ```

2. **Set up environment variables**
   ```bash
   cp env.example .env
   ```
   
   Edit the `.env` file with your database credentials and other settings.

3. **Start the services**
   ```bash
   docker-compose up --build -d
   ```

4. **Create an admin user**
   ```bash
   docker-compose exec web python manage.py createsuperuser
   ```

5. **Access the application**
   - API: http://localhost:8000/api/
   - API Docs: http://localhost:8000/api/docs/
   - Admin: http://localhost:8000/admin/

## API Endpoints

### Core Endpoints
- `GET /api/v1/regions/` - List UK regions
- `GET /api/v1/parameters/` - List weather parameters (Tmax, Tmin, etc.)
- `GET /api/v1/records/` - Weather records with filtering
- `GET /api/v1/aggregates/` - Calculated aggregates
- `GET /api/v1/ingestion/` - Ingestion logs

### Data Ingestion (requires auth)
- `POST /api/v1/ingestion/trigger_ingestion/` - Start data ingestion

### Filtering Examples
```
/api/v1/records/?region=England&parameter=Tmax&year=2020
/api/v1/aggregates/?aggregation_type=monthly&parameter=Tmax
/api/v1/ingestion/?status=completed&parameter=Tmax
```

## Data Models

The system handles several types of weather data:

- **Regions**: England, Scotland, Wales, Northern Ireland
- **Parameters**: Temperature (max/min), rainfall, sunshine hours
- **Weather Records**: Monthly weather data
- **Seasonal Summaries**: Winter, spring, summer, autumn, annual data
- **Aggregates**: Calculated statistics at various time scales

## Environment Variables

Key variables you need to set in `.env`:

```bash
# Database
DATABASE_URL=postgresql://user:password@db:5432/weather_db
POSTGRES_DB=weather_db
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password

# Redis
REDIS_URL=redis://redis:6379/0

# Django
SECRET_KEY=your-secret-key
DEBUG=False

# Docker ports
DB_PORT=5433
REDIS_PORT=6379
WEB_PORT=8000
```

## Development Notes

### Data Parsing
The MetOffice data parser handles various edge cases:
- Partial years (like 2025 with incomplete data)
- Missing seasonal summaries
- Data format variations across different parameters
- Temperature conversions and validations

### Background Processing
When new data is ingested, Celery automatically triggers:
- Monthly aggregate calculations
- Yearly statistics
- Seasonal summaries
- Decadal averages

### Caching Strategy
- Frequently accessed data is cached in Redis
- Cache TTL is configurable
- Cache invalidation happens on data updates

## Testing

### Unit Tests
Run the test suite:
```bash
docker-compose exec web python manage.py test tests.test_models -v 2
```

### API Testing
I've included a Postman collection (`UK_Weather_Data_API.postman_collection.json`) with all the API endpoints pre-configured.

## Production Deployment

The Docker setup is production-ready with:
- Gunicorn WSGI server
- Nginx reverse proxy
- Health checks for all services
- Proper logging configuration

For cloud deployment, just update the environment variables with your cloud provider's credentials.

## Troubleshooting

### Common Issues

**Database connection errors**: Make sure PostgreSQL is running and credentials are correct in `.env`

**Redis connection issues**: Check if Redis container is healthy and port isn't conflicting

**Port conflicts**: If you get port binding errors, check if ports 5433, 6379, or 8000 are already in use

**Permission errors**: Make sure Docker has proper permissions to create volumes

### Logs
Check container logs:
```bash
docker-compose logs web
docker-compose logs db
docker-compose logs redis
docker-compose logs celery
```

## Project Structure

```
setu/
├── api/                    # API views, serializers, URLs
├── weather_data/          # Core models and data processing
├── weather_api/           # Django settings and config
├── tests/                 # Unit tests
├── docker-compose.yml     # Multi-container setup
├── Dockerfile            # Application container
├── nginx.conf            # Nginx config
├── requirements.txt      # Python dependencies
└── README.md            # This file
```

Built with Django, PostgreSQL, Redis, and Docker. The project demonstrates modern web API development practices including containerization, caching, background processing, and comprehensive testing.
