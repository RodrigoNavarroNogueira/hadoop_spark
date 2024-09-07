build-yarn:
	docker compose -f docker-compose.yarn.yml build

build-yarn-nc:
	docker compose -f docker-compose.yarn.yml build --no-cache

down-yarn:
	docker compose -f docker-compose.yarn.yml down --remove-orphans

run-yarn:
	make down-yarn && make build-yarn && docker compose -f docker-compose.yarn.yml up

run-yarn-scaled:
	make down-yarn && make build-yarn && docker compose -f docker-compose.yarn.yml up --scale spark-yarn-worker=3

stop-yarn:
	docker compose -f docker-compose.yarn.yml stop

submit-yarn-test:
	docker exec da-spark-yarn-master spark-submit --master yarn --deploy-mode cluster ./examples/src/main/python/pi.py

submit-yarn-cluster:
	docker exec da-spark-yarn-master spark-submit --master yarn --deploy-mode cluster ./apps/$(app)

rm-results:
	rm -r book_data/results/*
