StockAutoTrader is an automated stock trading application built as a set of asynchronous microservices, each with a focused responsibility: Dataflow (market data ingestion), Quant (signal generation), RiskOps (risk management), and ExecConnect (trade execution). The system uses RabbitMQ for inter-service communication and Docker for containerized deployment.

Table of Contents
	1.	Overview
	2.	Architecture
	3.	Services
	•	Dataflow
	•	Quant
	•	RiskOps
	•	ExecConnect
	4.	Setup & Installation
	5.	Configuration
	•	Environment Variables
	•	Logging
	6.	Running the System
	7.	Testing
	8.	Roadmap
	9.	License

Overview

StockAutoTrader aims to provide an end-to-end pipeline for automated trading:
	•	Dataflow retrieves market data from Polygon and pushes it to RabbitMQ.
	•	Quant consumes this data, applies trading strategies, and publishes signals.
	•	RiskOps evaluates those signals against predefined risk thresholds.
	•	ExecConnect executes approved trades on a broker API (or a simulated environment).

This design keeps components loosely coupled, scalable, and easier to maintain.

Architecture

                   +------------+
                   |  Dataflow  |
                   |  (Polygon) |
                   +-----+------+
                         |
                         | (market data)
                         v
+--------+       +---------------+       +-----------------+
| Rabbit | <---- |   RabbitMQ    | ----> |  (PostgreSQL)   |
|  MQ    |       +---------------+       +-----------------+
+--------+
                         ^
                         | (signals, risk approvals...)
+-----------+     +------+-------+    +-------------+
|   Quant   | --> |  RiskOps     | -->| ExecConnect |
|(Strategies|     |(Risk Checks) |    |(Trade Exec) |
|(Signal Gen)     +--------------+    +-------------+

Each service is containerized and orchestrated through Docker Compose, allowing straightforward deployment and scaling.

Services

Dataflow
	•	Purpose: Fetch real-time or historical market data from the Polygon API and publish messages to RabbitMQ.
	•	Key Files:
	•	polygon_service.py: Connects to the Polygon API and handles retry logic.
	•	rabbitmq_publisher.py: Publishes fetched data to RabbitMQ.
	•	Features:
	•	Exponential Backoff for network/API failures.
	•	JSON-Structured Logging with timestamps, service name, and message content.
	•	Fallback Mechanisms if API calls repeatedly fail.

Quant
	•	Purpose: Receive and process market data to generate buy/sell signals based on custom strategies.
	•	Key Files:
	•	SymbolStrategy.py: Core signal generation logic (e.g., momentum, mean reversion).
	•	publish_trade_signal.py: Publishes resulting trade signals to RabbitMQ for RiskOps.
	•	Features:
	•	Data Validation for required fields (symbol, price).
	•	Exponential Backoff when publishing signals.
	•	JSON-Structured Logging (e.g., warnings if volume or timestamp are missing).

RiskOps
	•	Purpose: Evaluate the signals from Quant to ensure they comply with risk parameters (confidence threshold, margin requirements, leverage limits, etc.).
	•	Key Files:
	•	risk.py: Contains the core logic for position sizing, margin checks, and confidence rules.
	•	mq_util.py: Handles RabbitMQ operations for subscribing to signals and publishing approvals/rejections.
	•	Features:
	•	JSON-Structured Logging with warnings/error levels on threshold breaches.
	•	Configurable Parameters (confidence threshold, max leverage) from environment variables.
	•	Unit Tests covering various risk scenarios.

ExecConnect
	•	Purpose: Execute approved trades on a broker API or a simulated execution environment.
	•	Key Files:
	•	trade_executor.py: Orchestrates order preparation and submission logic.
	•	broker_api.py: Abstracts connectivity/authentication to an external broker.
	•	Features:
	•	Exponential Backoff for broker/API call failures.
	•	Structured Logging capturing order IDs, symbols, quantities, and outcomes (e.g., FILLED, REJECTED).
	•	Risk Approval Check to ensure only trades validated by RiskOps are executed.

Setup & Installation
	1.	Clone the Repository

git clone https://github.com/Solders-Girdles/StockAutoTrader.git
cd StockAutoTrader


	2.	Install Docker & Docker Compose
	•	Ensure you have Docker Engine and Docker Compose installed on your system.
	3.	Check Environment Variables
	•	Copy or create a .env file in the project’s root directory.
	•	Set variables like POLYGON_API_KEY, RABBITMQ_HOST, RABBITMQ_USER, RABBITMQ_PASS, etc.
	4.	(Optional) Local Python Setup
	•	If you choose not to use Docker, install dependencies manually in each service folder:

cd Dataflow
pip install -r requirements.txt
# repeat for other services...

Configuration

Environment Variables

Below are commonly used environment variables (set them in a .env file or export them in your shell):

Variable	Description	Example
POLYGON_API_KEY	API key for Polygon market data	your_polygon_key
RABBITMQ_HOST	RabbitMQ server host	localhost or rabbitmq
RABBITMQ_USER	RabbitMQ username	guest
RABBITMQ_PASS	RabbitMQ password	guest
BROKER_API_KEY	Broker API key (ExecConnect)	demo_broker_key
BROKER_API_SECRET	Broker API secret (ExecConnect)	demo_broker_secret
CONFIDENCE_THRESH	Minimum confidence for trade approval (RiskOps)	0.75

Include other variables as needed for your environment.

Logging
	•	All services log in JSON format, including keys like timestamp, service, level, and message.
	•	Some services may also log a correlation_id for end-to-end tracing of specific trades or requests.
	•	Logs can be aggregated via tools like the ELK stack, Splunk, or AWS CloudWatch.

Running the System
	1.	Docker Compose
	•	From the project root, run:

docker-compose up --build


	•	This starts all microservices, plus RabbitMQ (and PostgreSQL if configured).

	2.	Manual Startup (without Docker)
	•	In separate terminals or background processes, start each service:

cd Dataflow
python main.py

cd ../Quant
python main.py

# ... repeat for RiskOps and ExecConnect


	•	Ensure RabbitMQ is running locally or configure each service to point to a remote RabbitMQ instance.

	3.	Verification
	•	Check each service’s logs to confirm successful connections, data publishing/subscribing, and trade executions.

Testing

Each microservice has its own tests. Run them as follows:
	•	Dataflow:

cd Dataflow
python -m unittest discover tests


	•	Quant:

cd Quant
python main.py test

(Or run individual test files from the tests/ folder.)

	•	RiskOps:

cd RiskOps
python -m unittest discover tests

Includes tests for leverage, margin checks, and malformed signal scenarios.

	•	ExecConnect:

cd ExecConnect
python -m unittest discover tests

Mocks broker API responses and verifies partial fills, rejections, and timeouts.

Setting up a CI/CD pipeline (e.g., GitHub Actions) is recommended to automate these tests on every commit or pull request.

Roadmap
	•	Correlation ID Standardization: Ensure each service logs and propagates a correlation_id across the entire workflow.
	•	Advanced Order Types: Extend ExecConnect to handle limit, stop-loss, and other order types.
	•	Performance Monitoring: Integrate metrics dashboards (Prometheus + Grafana or similar) for real-time insights.
	•	Dynamic Risk Models: Expand RiskOps to use volatility or portfolio-level metrics for position sizing and leverage.
	•	CI/CD Integration: Automate build, test, and deployment steps for robust continuous delivery.

License

This project is released under the MIT License. Refer to the LICENSE file for details.