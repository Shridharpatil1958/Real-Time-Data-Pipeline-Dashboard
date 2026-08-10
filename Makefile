# Makefile — Real-Time Pipeline helpers
.PHONY: help infra producer consumer spark dashboard stop clean test

help:
	@echo ""
	@echo "⚡  Real-Time Data Pipeline"
	@echo "═══════════════════════════════════════"
	@echo "  make infra      Start Kafka, MySQL, MongoDB, Redis"
	@echo "  make producer   Run the Kafka producer"
	@echo "  make consumer   Run the Kafka consumer + anomaly detector"
	@echo "  make spark      Run the PySpark streaming job"
	@echo "  make dashboard  Launch the Streamlit dashboard"
	@echo "  make all        Start everything (4 terminals)"
	@echo "  make stop       Stop all Docker services"
	@echo "  make clean      Stop + remove volumes"
	@echo "  make test       Run test suite"
	@echo ""

infra:
	docker compose -f docker/docker-compose.yml up -d
	@echo "⏳ Waiting for services to be ready …"
	@sleep 10
	@echo "✅ Infrastructure ready"
	@echo "   Kafka UI   → http://localhost:8080"
	@echo "   Kafka      → localhost:29092"
	@echo "   MySQL      → localhost:3306"
	@echo "   MongoDB    → localhost:27017"
	@echo "   Redis      → localhost:6379"

producer:
	PYTHONPATH=. python producer/transaction_producer.py

consumer:
	PYTHONPATH=. python consumer/transaction_consumer.py

spark:
	PYTHONPATH=. python spark_processing/spark_stream.py

dashboard:
	PYTHONPATH=. streamlit run dashboard/app.py --server.port 8501

# Convenience: open four terminals
all:
	@echo "Start the following in separate terminals:"
	@echo "  1. make producer"
	@echo "  2. make consumer"
	@echo "  3. make spark"
	@echo "  4. make dashboard"

stop:
	docker compose -f docker/docker-compose.yml stop

clean:
	docker compose -f docker/docker-compose.yml down -v
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

test:
	PYTHONPATH=. pytest tests/ -v
