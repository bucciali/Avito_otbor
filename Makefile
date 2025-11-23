APP_NAME=pr_service
build:
	go build -o $(APP_NAME) .

run:
	go run main.go

docker-up:
	docker-compose up --build

docker-down:
	docker-compose down