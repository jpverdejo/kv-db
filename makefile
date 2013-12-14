main: build/db.exe
	@echo "Compiling finished"

build/db.exe: src/db.c
	@echo "Compiling DB"
	@gcc -o build/db.exe src/db.c -lpthread -lm 	