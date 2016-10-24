help:
	@echo "build - Build container"
	@echo "run - Run container"
	@echo "restart - Restart the container"

build:
	docker build -t="sonat/twitter-kafka" .

run: build
	docker run -t -i  -v $(CURDIR)/app:/opt/app --network=kafkadocker_default --name twitter-kafka sonat/twitter-kafka

stop:
	docker stop twitter-kafka

start:
	docker start -a twitter-kafka