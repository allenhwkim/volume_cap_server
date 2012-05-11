all: debug

debug:
	rm -f ebin/*.beam
	erlc -DTEST -Ddebug -o ebin src/*.erl test/*.erl

compile:
	rm -f ebin/*.beam
	erlc -o ebin src/*.erl 

test: debug
	escript test/run
