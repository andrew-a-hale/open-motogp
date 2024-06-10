test:
	MOTOGP_ENV=test pytest --pyargs motogp -vv
	rm test-motogp.db test-processing.db
run-inc:
	MOTOGP_ENV=dev python ./src/motogp/main.py 0 inc
run-full:
	MOTOGP_ENV=dev python ./src/motogp/database.py
	MOTOGP_ENV=dev python ./src/motogp/main.py 0 full
run-some:
	MOTOGP_ENV=dev python ./src/motogp/main.py 10 inc

