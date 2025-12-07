# A system for processing and archiving real-time financial data in an on-premises environment

A distributed, event-driven system for ingesting, processing, and analyzing
cryptocurrency market data in real-time using Go microservices, featuring a dual-tier
storage architecture with MinIO data lake and MongoDB.

## Architecture Overview

The system is built on a microservice architecture with the following components:

- **Stream Server**: Fetches real-time market data from Binance WebSocket API
- **Data Connector**: JetStream-based event streaming connecting all microservices for inter-service communication
- **Data Lake (MinIO)**: Object storage separating data into cold and hot storage to reduce MongoDB load
- **MongoDB**: Stores indexed data until it is archived to MinIO
- **ETL Service**: Extracts, transforms, and loads data from raw streams into curated formats
- **Analytics Engine**: Processes curated data to generale aggregated statistics
- **GraphQL API**: Provides query interface for accessing trade data and notifications in MongoDB
- **Telegram Bot**: Sends real-time alerts and notifications based on market events

## Prerequisites

- **Go** 1.20 or higher
- **Docker CLI** & **Docker Compose**
- **Telegram Bot Token** (from BotFather) for notification service

## Setup Instructions

### 1. Environment Configuration

Copy the sample environment file and configure it:

```bash
cp .sample.env .env
```

### 2. Update Required Variables

Edit `.env` and add your Telegram credentials:

```env
TELEGRAM_BOT_TOKEN=<your_bot_token_from_botfather>
TELEGRAM_CHAT_ID=<your_telegram_chat_id>
```

All other variables are pre-configured in the sample file. The rest of the setup is ready to go.

### 3. Start the Application

Create local network connecting containers:

```bash
docker network create app-network
```

Run the startup script to initialize all services:

```bash
./scripts/start.sh
```

This will:

- Start Docker containers (MongoDB, NATS JetStream, MinIO, Stream Server)
- Initialize the data lake structure
- Start all microservices
- Set up the GraphQL API endpoint

### 4. Stop the Application

To gracefully shut down all services:

```bash
./scripts/stop.sh
```

## Project Structure

```
├── cmd/                    # Application entry points
│   ├── data_connector/          # JetStream service connector
│   ├── etl/                     # ETL pipeline service
│   ├── graphql/                 # GraphQL API server
│   ├── mongo_sub/               # MongoDB subscriber service
│   ├── stream_server/           # Binance WebSocket streaming
│   └── telegram_bot/            # Telegram notification bot
├── internal/               # Internal packages
│   ├── analytics/               # Analytics engine for aggregations
│   ├── dataconnector/           # Data connector configuration
│   ├── etl/                     # ETL transformation logic
│   ├── graphql/                 # GraphQL client & config
│   ├── jetstream/               # JetStream config, publisher and subscriber
│   ├── minio/                   # MinIO client (cold/hot storage)
│   ├── models/                  # Data models
│   ├── mongo/                   # MongoDB config
│   ├── repository/              # Data access layer
│   ├── streamserver/            # Stream server client
│   ├── telegrambot/             # Telegram bot logic
│   └── utils/                   # Utility functions
├── graph/                  # GraphQL schema & resolvers
│   ├── generated/               # Auto-generated GraphQL code
│   ├── model/                   # GraphQL model definitions
│   ├── resolvers/               # Query resolvers
│   └── schemas/                 # GraphQL schema files
├── docker/                 # Docker configurations
│   └── compose-*.yaml           # Docker Compose files
├── scripts/                # Operational scripts for running containers
│   ├── start.sh                 # Start all services
│   ├── stop.sh                  # Stop all services
└── └── restart.sh               # Restart all services


```

## Data Lake Architecture

The system implements a dual-tier storage approach:

- **Hot Storage**: Active, recent data in MongoDB for low-latency queries provided by the GraphQL API
- **Cold Storage**: Historical and archived data in MinIO for cost-effective long-term retention

This separation reduces MongoDB load and optimizes performance by keeping only necessary working data in the database
while archiving older data to object storage. Transitions between hot and cold storage are managed by transfer cron
jobs.

## Key Features

- **Real-Time Data Streaming**: Live cryptocurrency market data from Binance
- **Event-Driven Architecture**: JetStream-based asynchronous communication between services
- **Scalable Data Storage**: Dual-tier storage with MinIO and MongoDB
- **GraphQL API**: Modern query interface for data access
- **Alerts & Notifications**: Telegram bot for real-time alerts

## Development

### Database Operations

MongoDB operations are handled through the repository layer in `internal/repository/`.

### GraphQL Code Generation

Update GraphQL schemas in `graph/schemas/` and regenerate:

```bash
gqlgen generate
```

## Docker Deployment

The project includes Docker Compose configurations for containerized deployment:

- `docker/compose-jetstream.yaml` - NATS JetStream broker, data connector and subscribers
- `docker/compose-storage.yaml` - MongoDB and MinIO, GraphQL API service and ETL service
- `docker/compose-streamserver.yaml` - Stream server service

## Environment Variables Reference

Refer to `.sample.env` for complete list of configuration options. Key variables:

- `SYMBOLS` - Comma-separated list of trading pairs to monitor (e.g., BTCUSDT,ETHUSDT)
- `TELEGRAM_CHAT_ID` and `TELEGRAM_BOT_TOKEN` - Telegram bot configuration required for notifications system
- `ETL_RUN_INTERVAL_MINUTES` - Mongo -> MinIO cron job interval in minutes


---