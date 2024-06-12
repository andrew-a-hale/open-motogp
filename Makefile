test:
	MOTOGP_ENV=test pytest --pyargs motogp -vv 
	rm test-motogp.db test-processing.db

run-inc:
	MOTOGP_ENV=dev python ./src/motogp/consumer.py 0 inc

run-full:
	MOTOGP_ENV=dev python ./src/motogp/consumer.py 0 full

run-some:
	MOTOGP_ENV=dev python ./src/motogp/consumer.py 10 inc

run-one:
	MOTOGP_ENV=dev python ./src/motogp/consumer.py 1 inc

produce-one:
	MOTOGP_ENV=dev python ./src/motogp/producer.py 1 inc

produce-some:
	MOTOGP_ENV=dev python ./src/motogp/producer.py 10 inc

produce-full:
	MOTOGP_ENV=dev python ./src/motogp/producer.py 0 full

produce-inc:
	MOTOGP_ENV=dev python ./src/motogp/producer.py 0 inc

process-one:
	MOTOGP_ENV=dev python ./src/motogp/producer.py 1 inc
	MOTOGP_ENV=dev python ./src/motogp/consumer.py 1 inc

process-some:
	MOTOGP_ENV=dev python ./src/motogp/producer.py 10 inc
	MOTOGP_ENV=dev python ./src/motogp/consumer.py 10 inc

process-full:
	MOTOGP_ENV=dev python ./src/motogp/producer.py 0 full
	MOTOGP_ENV=dev python ./src/motogp/consumer.py 0 full

process-inc:
	MOTOGP_ENV=dev python ./src/motogp/producer.py 0 inc
	MOTOGP_ENV=dev python ./src/motogp/consumer.py 0 inc