APP=example

clean:
	rm -f ./${APP}

build:
	go build -o ${APP} ./main.go

run-leader: build
	./${APP} --config-file ./node-leader-conf.yml

run-follower: build
	./${APP} --config-file ./node-follower-conf.yml
