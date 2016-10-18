help:
	@echo "build - Build container"
	@echo "run - Run container"
	@echo "restart - Restart the container"

build:
	docker build -t="sonat/twitter-kafka" .

run: build
	docker run -t -i -d  -v $(CURDIR)/app:/opt/app --net=host --name twitter-kafka sonat/twitter-kafka

stop:
	docker stop twitter-kafka

start:
	docker start twitter-kafka