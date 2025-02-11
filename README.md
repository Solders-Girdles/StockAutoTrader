StockAutoTrader is a production-ready, automated trading system built on a microservices architecture. The project integrates real-time market data, quantitative trading strategies, risk management, and order execution into a cohesive platform. Recent updates focus on enhanced observability, robust logging, and comprehensive testing to ensure reliability as new features are added.

Table of Contents
	1.	Project Overview
	2.	Key Features
	3.	Repository Structure
	4.	Getting Started
	5.	Running the Application
	6.	Running Tests
	7.	Observability & Logging
	8.	Contribution Guidelines
	9.	Future Enhancements
	10.	License and Acknowledgments

Project Overview

StockAutoTrader is an automated trading platform designed to evolve seamlessly as new contributions are integrated. Built on a microservices paradigm, the project ensures that each service—data ingestion, trading strategy, risk management, and order execution—remains modular, maintainable, and independently testable. Our primary goals are to continuously enhance observability, maintain stability under integration, and foster an active contribution process.

Key Features
	•	Real-Time Market Data Ingestion:
	•	Integration with primary and secondary market data feeds.
	•	Automatic failover to backup data sources when necessary.
	•	Schema validation that supports both complete and partial data inputs.
	•	Automated Trading Strategies (Quant):
	•	Implementation of various quantitative strategies (e.g., MACD, Bollinger Bands, RSI, and ML-based signals).
	•	Signal generation integrated into strategy classes for clarity and modularity.
	•	Risk Management (RiskOps):
	•	Pre-trade validations including exposure limits, margin requirements, and circuit breaker logic.
	•	Dynamic portfolio updates based on trade signals.
	•	Detailed risk reporting and performance metrics.
	•	Order Execution (ExecConnect):
	•	Robust order submission to broker APIs.
	•	Handling for successful, failed, and partially filled orders.
	•	Fallback logic to safely manage errors and exceptions.
	•	Observability & Logging:
	•	Centralized JSON logging across all microservices using a custom logging helper.
	•	Propagation of correlation IDs for end-to-end traceability.
	•	Integration with Prometheus for metrics collection and Grafana for dashboard visualization.
	•	Optional OpenTelemetry stubs for future distributed tracing.
	•	Testing Suite:
	•	A comprehensive test suite covering unit, integration, and observability tests.
	•	Automated tests are discoverable and runnable via a centralized test runner script.

Repository Structure

StockAutoTrader/
├── dataflow/             # Market data ingestion, API integrations, failover logic
├── quant/                # Trading strategies and performance metrics
│   ├── consumer.py
│   ├── performance.py
│   └── strategies.py
├── riskops/              # Risk evaluation, trade processing, and portfolio updates
├── execconnect/          # Order execution and broker API integration
├── common/               # Shared utilities (e.g., logging_helper.py, telemetry stubs)
├── tests/                # Test suite for the project
│   ├── dataflow/         # Unit and integration tests for DataFlow
│   ├── quant_tests/      # Tests for the Quant service (strategies, consumer, etc.)
│   ├── riskops/          # Tests for risk evaluation and reporting
│   └── execconnect/      # Tests for order execution and broker API simulation
└── run_tests.py          # Centralized test runner script

Getting Started

Prerequisites
	•	Python: Version 3.12 or later.
	•	Dependencies:
Install required libraries using pip (see the provided requirements.txt):

pip install -r requirements.txt


	•	Environment Variables:
Configure the following environment variables:
	•	POLYGON_API_KEY – API key for the primary market data feed.
	•	SECONDARY_POLYGON_BASE_URL – URL for the secondary data feed.
	•	RABBITMQ_HOST, RABBITMQ_USER, RABBITMQ_PASS – RabbitMQ connection parameters.
	•	WATCHLIST – Comma-separated list of symbols (e.g., “AAPL,MSFT,GOOG,TSLA”).
	•	DATA_INTERVAL – Market data interval (e.g., “1m”).

Installation
	1.	Clone the repository:

git clone https://github.com/Solders-Girdles/StockAutoTrader.git
cd StockAutoTrader


	2.	Install dependencies:

pip install -r requirements.txt

Running the Application

Each microservice can be launched individually. For example:
	•	DataFlow (Market Data Publisher):

python dataflow/main.py


	•	Quant (Trading Strategies and Signal Generation):
Ensure the Quant service is configured to subscribe to market data and generate signals.
	•	RiskOps (Risk Evaluation):

python riskops/main.py


	•	ExecConnect (Order Execution):

python execconnect/main.py



All services are designed to interact via RabbitMQ and share configuration from common environment files.

Running Tests

A comprehensive test suite is provided to ensure system reliability. To run all tests, execute the test runner script from the project root:

python run_tests.py

This script:
	•	Adds the project root to sys.path.
	•	Discovers all test files in the tests/ directory (files matching test*.py).
	•	Runs tests in verbose mode.
	•	Exits with a nonzero code if any test fails.

Observability & Logging

Logging
	•	Centralized Logging:
A custom logging helper (common/logging_helper.py) formats logs as JSON and attaches critical metadata (e.g., service name, correlation ID).
	•	Correlation IDs:
Generated in the DataFlow service and propagated through the system to enable end-to-end traceability.
	•	Example Log Output:

{
  "timestamp": "2025-02-11T05:20:47.121037+00:00",
  "service": "Quant",
  "level": "INFO",
  "message": "Received event",
  "correlation_id": "1234"
}



Metrics & Tracing
	•	Prometheus Integration:
Services expose key metrics (trade counts, order latency, queue depth) via Prometheus.
	•	Grafana Dashboards:
Pre-configured dashboards help visualize system health.
	•	Distributed Tracing (Optional):
OpenTelemetry stubs are available for future enhancement.

Contribution Guidelines

We welcome contributions! Please follow these guidelines:
	•	Branching Strategy:
Use feature branches (e.g., feature/observations) for new work and create pull requests with clear integration summaries.
	•	Code Style:
Adhere to our coding standards and ensure consistency, especially in logging and module structure.
	•	Testing:
All new features must be accompanied by unit and integration tests. Ensure your changes pass the test suite by running:

python run_tests.py


	•	Documentation:
			Update relevant documentation (README, in-code comments, etc.) to reflect changes.
Future Enhancements

	•	Security Improvements:
			Migrate sensitive configurations to a secure secrets store (e.g., Vault or AWS Secrets Manager).

	•	Scalability:
			Transition from Docker Compose to Kubernetes for orchestration and enable Horizontal Pod Autoscaling.

	•	Advanced Observability:
			Expand distributed tracing capabilities using OpenTelemetry and enhance Prometheus metrics.

	•		Strategy Expansion:
				Explore additional quantitative strategies and machine learning-based signals for improved trade decisions.
