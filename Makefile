prepare:
	mkdir -p ./logs ./plugins
	docker compose up airflow-init

airflow-run:
	docker compose -f ./docker-compose.yaml build airflow-worker airflow-scheduler airflow-webserver
	docker compose -f ./docker-compose.yaml up airflow-worker airflow-scheduler airflow-webserver --remove-orphans

stop-airflow:
	docker compose down --volumes --rmi all

