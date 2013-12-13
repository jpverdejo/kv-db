#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <dirent.h>
#include <semaphore.h>
#include <fcntl.h>
#include <math.h>
#include <unistd.h>
#include <string.h>

//Como las paginas tendran un maximo de 1MB podemos tener 1000 workers que trabajen con archivos
#define NWRITE 200
#define NREAD 200
#define NFIND 600
#define ENTRIESPERPAGE 5

typedef struct {
	char letter;
	int number;
	int index;
	char * filename;
} Page;

typedef struct {
	char command[5];
	char word[100];
	int index;
	pthread_t thread;
	char * response;
	pthread_mutex_t mutex;
} Instruction;

typedef struct {
	char *word;
	char *filename;
	int *positions;
	size_t n_found;
} FindObject;

// Listamos la cantidad de paginas para cada letra + 1 para el resto
int pages_count[27];
pthread_mutex_t * pages_mutexs[27];

// Productor consumidor para writes
Instruction writes[NWRITE];
int Wnextin, Wnextout;
sem_t * Wavailable;
sem_t * WseccionIn;
sem_t * WseccionOut;
sem_t * Wcount;

sem_t * Gavailable;
sem_t * Favailable;

int instructionsCounter, nextInstruction;
Instruction * instructions;
sem_t * Icounter;

void printMessage(Instruction * i, char * message) {
	if(isatty(0)) {
		printf("%s\n", message);
	}
	else {
		i->response = (char *) malloc(sizeof(char) * strlen(message));
		strcpy(i->response, message);
	}
}

void initialize() {
	int i,j;

	for (i=0;i<=27;i++) {
		pages_count[i] = 0;
	}

	//Codigo para listar archivos sacado de: http://stackoverflow.com/questions/12489/how-do-you-get-a-directory-listing-in-c
	DIR *dir;
	struct dirent *ent;
	if ((dir = opendir ("./data/")) != NULL) {
		/* print all the files and directories within directory */
		while ((ent = readdir (dir)) != NULL) {
			if (strcmp(ent->d_name, ".") != 0 && strcmp(ent->d_name, "..") != 0) {
				char f = ent->d_name[0];

				if (f>=97 && f <=122) {
					int index = f-97;
					pages_count[index]++;
				} else {
					int index = 26;
					pages_count[index]++;
				}
			}
		}
		closedir (dir);
	} else {
		/* could not open directory */
		perror ("El directorio ./data/ es inaccesible.");
		exit(1);
	}

	for (i=0;i<=27;i++) {
		if (pages_count[i] == 0)
			pages_count[i] = 1;
	}

	for (i=0;i<=27;i++) {
		pages_mutexs[i] = (pthread_mutex_t *) malloc(sizeof(pthread_mutex_t));

		pthread_mutex_init(pages_mutexs[i], NULL);
	}

	// Initialize producer/consumer writes
	Wnextin=0;
	Wnextout=0;

	sem_unlink("Wavailable");
	sem_unlink("WseccionIn");
	sem_unlink("WseccionOut");
	sem_unlink("Wcount");
	sem_unlink("Icounter");

	sem_unlink("Gavailable");
	sem_unlink("Favailable");


	Wavailable = sem_open("Wavailable", O_CREAT, 0644, NWRITE);
	WseccionIn = sem_open("WseccionIn", O_CREAT, 0644, 1);
	WseccionOut = sem_open("WseccionOut", O_CREAT, 0644, 1);
	Wcount = sem_open("Wcount", O_CREAT, 0644, 0);
	Icounter = sem_open("Icounter", O_CREAT, 0644, 0);

	Gavailable = sem_open("Gavailable", O_CREAT, 0644, NREAD);
	Favailable = sem_open("Favailable", O_CREAT, 0644, NFIND);

	instructionsCounter = -1;
	nextInstruction = 0;
}

Page nextPage(char * word) {
	char letter = tolower(word[0]);

	int index = 27;

	if (letter>=97 && letter <=122)
		index = letter-97;


	Page page;
	page.letter = letter;
	page.index = 0;

	size_t filename_size;
	filename_size = snprintf(NULL, 0, "./data/%c.%d", letter, pages_count[index]);

	char * filename = (char *)malloc(filename_size + 1);
	snprintf(filename, filename_size+1, "./data/%c.%d", letter, pages_count[index]);

	if( access( filename, F_OK ) == -1 ) {
		filename_size = snprintf(NULL, 0, "touch %s", filename);
		char * create_file = (char *)malloc(filename_size + 1);
		snprintf(create_file, filename_size+1, "touch %s", filename);

		system(create_file);
	}

	if( access( filename, F_OK ) != -1 ) {
		FILE *fp;
		char result[50];

		char command[strlen("wc -l ") + strlen(filename) +1];
		snprintf(command, sizeof(command), "wc -l %s", filename);

		/* Open the command for reading. */
		fp = popen(command, "r");
		if (fp == NULL) {
			printf("Error al ejecutar comando \"wc\"\n" );
			exit;
		}

		/* Read the output a line at a time - output it. */
		fgets(result, sizeof(result)-1, fp);

		int number;
		sscanf(result, "%d", &number);

		page.index = number;

		if (number >= ENTRIESPERPAGE) {	
			pages_count[index]++;
			page.index = 0;
		}

		/* close */
		pclose(fp);

		page.number = pages_count[index];

		filename_size = snprintf(NULL, 0, "./data/%c.%d", letter, page.number);

		filename = (char *)malloc(filename_size + 1);
		snprintf(filename, filename_size+1, "./data/%c.%d", letter, page.number);

		page.filename = filename;
	}
	else {
		perror("No se puede crear nueva pagina de BD");
		exit(-1);
	}

	return page;
}

void * writeOnFile(void * input) {
	while (1) {
		int value;

		sem_wait(Wcount);
		sem_wait(WseccionOut);

		Instruction i = writes[Wnextout];
		
		Wnextout = (Wnextout + 1) % 100;

		Page page = nextPage(i.word);

		int index = 27;
		if (page.letter>=97 && page.letter <=122)
			index = page.letter-97;

		pthread_mutex_lock(pages_mutexs[index]);
		FILE *fp = fopen(page.filename, "a");

		if(fp != NULL) {
			fprintf (fp, "%s\n", i.word);
		}
		fclose(fp);

		size_t string_size = snprintf(NULL, 0, "[SAVE] %c.%d.%d:%s", page.letter, page.number, page.index, i.word);
		char * string = (char *)malloc(string_size + 1);
		snprintf(string, string_size +1, "[SAVE] %c.%d.%d:%s", page.letter, page.number, page.index, i.word);
		printMessage(&i, string);

		pthread_mutex_unlock(pages_mutexs[index]);

		sem_post(WseccionOut);
		sem_post(Wavailable);
		pthread_mutex_unlock(&i.mutex);
	}
}

void * save(void * input) {
	Instruction * i = (Instruction *) input;
	sem_wait(Wavailable);
	sem_wait(WseccionIn);

	writes[Wnextin] = *i;
	Wnextin = (Wnextin + 1) % NWRITE;

	sem_post(WseccionIn);
	sem_post(Wcount);
}

void * getFromFile(void * input) {

	sem_wait(Gavailable);
	Instruction * ins = (Instruction *) input;
	char * index = ins->word;

	char * copyIndex = (char *) malloc(sizeof(char) * (strlen(index) +1));
	strcpy(copyIndex, index);

	Page p;
	p.letter = index[0];

	int found = 0;

	char * token;

	token = strtok(index, ".");

	token = strtok(NULL, ".");
	if (token != NULL) {
		p.number = atoi(token);

		token = strtok(NULL, ".");
		if (token != NULL) {
			p.index = atoi(token);

			size_t filename_size;
			char * filename;

			filename_size = snprintf(NULL, 0, "./data/%c.%d", p.letter, p.number);

			filename = (char *)malloc(filename_size + 1);
			snprintf(filename, filename_size+1, "./data/%c.%d", p.letter, p.number);

			char * word = NULL;
			size_t size;
			int i;

			FILE *fp = fopen(filename, "r");

			if(fp != NULL) {
				for (i=0; i<=p.index && !feof(fp); i++) {
					getline(&word, &size, fp);
				}

				if (!(strcmp(word, "") == 0 || i<=p.index)) {
					word[(strlen(word)-1)] = '\0';

					found = 1;
					size_t string_size = snprintf(NULL, 0, "[GET] %s:%s", copyIndex, word);
					char * string = (char *)malloc(string_size + 1);
					snprintf(string, string_size +1, "[GET] %s:%s", copyIndex, word);
					printMessage(ins, string);
				}
			}
		}
	}
	
	if (!found) {
		size_t string_size = snprintf(NULL, 0, "[GET] El indice %s no existe.", copyIndex);
		char * string = (char *)malloc(string_size + 1);
		snprintf(string, string_size +1, "[GET] El indice %s no existe.", copyIndex);
		printMessage(ins, string);
	}

	sem_post(Gavailable);
	pthread_mutex_unlock(&ins->mutex);
}

void * findFromFile(void * input) {
	sem_wait(Favailable);
	FindObject *m = (FindObject*) input;

	FILE * fp;
	char * line = NULL;
	size_t len = 0, filename_size;
	ssize_t read;

	char * filename;

	filename_size = snprintf(NULL, 0, "./data/%s", m->filename);
	filename = (char *)malloc(filename_size + 1);
	snprintf(filename, filename_size+1, "./data/%s", m->filename);

	fp = fopen(filename, "r");
	
	if (fp != NULL){
		m->n_found=0;
		m->positions = (int *) malloc(sizeof(int));

		int count = 1;
		int found = 0;

		while ((read = getline(&line, &len, fp)) != -1) {
			int length = (strlen(line) -1);
			line[length] = '\0';
			
			if (strcmp(line, m->word) == 0) {
				found++;

				m->positions = (int *) realloc(m->positions, (sizeof(int) * found));

				m->positions[found -1] = count;
				m->n_found++;
			}
			count++;
		}

		fclose(fp);
		sem_post(Favailable);

		pthread_exit((void *)m);
	}

	sem_post(Favailable);
	return NULL;
}

void * find(void * input) {
	Instruction * instruccion = (Instruction *) input;

	char letter = tolower(instruccion->word[0]);

	int index = 27, counter, numberPages, i, k;
	if (letter>=97 && letter <=122)
		index = letter-97;

	int n_workers = pages_count[index];

	char ** files = (char **) malloc(sizeof(char *) * n_workers);
	size_t filename_size;

	for (i = 0; i < n_workers; i++) {
		filename_size = snprintf(NULL, 0, "%c.%d", letter, (i+1));
		files[i] = (char *)malloc(filename_size + 1);
		snprintf(files[i], filename_size+1, "%c.%d", letter, (i +1));
	}

	int *pthread_ids;

	pthread_t *workers;

	pthread_ids = (int*) malloc(sizeof(int) * n_workers);
	workers = (pthread_t*) malloc(sizeof(pthread_t) * n_workers);

	for (i = 0;i < n_workers; i++) {
		FindObject *C = (FindObject *) malloc(sizeof(FindObject));
		C->word = instruccion->word;
		C->filename = files[i];
		pthread_ids[i] = pthread_create(&workers[i], NULL, findFromFile, C);
	}

	char ** resultIDs;
	int numberResults = 0;

	size_t indexLength;

	for (i = 0; i < n_workers; i++) {
		FindObject *m;
		pthread_join(workers[i], (void *)&m);
		if (m != NULL && m->n_found > 0) {
			for (k = 0; k < m->n_found; k++) {
				numberResults++;
				
				resultIDs = (char **) realloc(resultIDs, (sizeof(char *) * numberResults));

				indexLength = snprintf(NULL, 0, "%s.%d", m->filename, m->positions[k]);
				resultIDs[numberResults -1] = (char *)malloc(indexLength + 1);
				snprintf(resultIDs[numberResults -1], indexLength+1, "%s.%d", m->filename, m->positions[k]);
			}
		}
	}

	char * message;
	size_t message_size;

	if (numberResults) {

		message_size = snprintf(NULL, 0, "[FIND] %s: ", instruccion->word);
		message = (char *)malloc(message_size + 1);
		snprintf(message, message_size+1, "[FIND] %s: ", instruccion->word);

		for (i = 0; i < numberResults; i++) {
			message_size = snprintf(NULL, 0, "%s%s", message, resultIDs[i]);
			message = (char *)malloc(message_size + 1);
			snprintf(message, message_size+1, "%s%s", message, resultIDs[i]);
		}
	}
	else {
		message_size = snprintf(NULL, 0, "[FIND] No se encontro la palabra: %s\n", instruccion->word);
		message = (char *)malloc(message_size + 1);
		snprintf(message, message_size+1, "[FIND] No se encontro la palabra: %s\n", instruccion->word);
	}

	printMessage(instruccion, message);
	pthread_mutex_unlock(&instruccion->mutex);
}

void * manageInstructions(void * input) {
	while(1) {
		sem_wait(Icounter);

		Instruction i = instructions[nextInstruction];

		if (strcmp(i.command, "save") == 0) {
			pthread_create(&i.thread, NULL, save, &instructions[nextInstruction]);
		} else if (strcmp(i.command, "find") == 0) {
			pthread_create(&i.thread, NULL, find, &instructions[nextInstruction]);
		} else if (strcmp(i.command, "get") == 0) {
			pthread_create(&i.thread, NULL, getFromFile, &instructions[nextInstruction]);
		}

		nextInstruction++;
	}
}

void increaseInstructionsList() {
	instructionsCounter++;
	instructions = (Instruction *) realloc(instructions, (sizeof(Instruction) * (instructionsCounter +1)));
}

main() {
	initialize();

	pthread_t writeOnFileThread;
	pthread_create(&writeOnFileThread, NULL, writeOnFile, NULL);

	pthread_t instructionsManager;
	pthread_create(&instructionsManager, NULL, manageInstructions, NULL);
	
	char * input = NULL;
	size_t size;
	ssize_t read;

	if(isatty(0))
		printf ("> ");

	while (read = getline(&input, &size, stdin) != -1) {
		int length = (strlen(input) -1);
		input[length] = '\0';

		char command[5];
		char word[101];

		char *sep;
		sep = strchr(input, ' ');

		if (sep) {
			size_t position = sep - input;

			strncpy(command, input, position);
			command[position] = '\0';

			int wordLength = (strlen(input) - position -1);

			strncpy(word, input + position + 1, wordLength);
			word[wordLength] = '\0';

			if (strcmp(command, "save") == 0 || strcmp(command, "find") == 0 || strcmp(command, "get") == 0) {
				increaseInstructionsList();

				strcpy(instructions[instructionsCounter].command, command);
				strcpy(instructions[instructionsCounter].word, word);
				instructions[instructionsCounter].index = instructionsCounter;

				pthread_mutex_init(&instructions[instructionsCounter].mutex, NULL);
				pthread_mutex_lock(&instructions[instructionsCounter].mutex);

				sem_post(Icounter);
			} else {
				printf("Comando invalido. Los comandos disponibles son: save, find, get.\n");
			}
		}
		else {
			if (!(strcmp(input, "") == 0 || strcmp(input, "exit") == 0)) {
				printf("Comando invalido. Los comandos disponibles son: save, find, get.\n");
			}
		}

		if(isatty(0))
			printf ("> ");
	}
	
	int i;

	// Esperamos que todas las instruccions terminen
	for(i=0; i <= instructionsCounter; i++) {
		printf("Esperando instruccion %d\n", i);
		pthread_mutex_lock(&instructions[i].mutex);
	}
}