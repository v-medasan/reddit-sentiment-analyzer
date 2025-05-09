build:
	docker-compose -f docker/docker-compose.yml build

up:
	docker-compose -f docker/docker-compose.yml up

down:
	docker-compose -f docker/docker-compose.yml down

bash:
	docker exec -it $$(docker ps -qf "ancestor=reddit-sentiment-analyzer_prefect") bash

deploy:
	make bash && prefect deployment build flows/reddit_etl_flow.py:reddit_sentiment_pipeline -n "reddit-flow"
