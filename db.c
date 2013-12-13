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
#define NWRITE 600
#define NREAD 600
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

// OS X no permite saber el valor de un semaforo (no esta implementado sem_getvalue)
// por lo que mantendre un segundo contador
int WCountInt;
int getsCount;
int savesCount;

int instructionsCounter, nextInstruction;
Instruction * instructions;
sem_t * Icounter;

void printMessage(Instruction i, char * message) {
	if(isatty(0)) {
		printf("%s\n", message);
	}
	else {
		printf("(*)%s\n", message);	
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

	sem_unlink("Ravailable");
	sem_unlink("Favailable");


	Wavailable = sem_open("Wavailable", O_CREAT, 0644, NWRITE);
	WseccionIn = sem_open("WseccionIn", O_CREAT, 0644, 1);
	WseccionOut = sem_open("WseccionOut", O_CREAT, 0644, 1);
	Wcount = sem_open("Wcount", O_CREAT, 0644, 0);
	Icounter = sem_open("Icounter", O_CREAT, 0644, 0);

	Gavailable = sem_open("Gavailable", O_CREAT, 0644, NREAD);
	Favailable = sem_open("Favailable", O_CREAT, 0644, NFIND);

	WCountInt = 0;

	getsCount = 0;
	savesCount = 0;

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
		//printMessage(i, string);
		printf("%s\n", string);

		pthread_mutex_unlock(pages_mutexs[index]);

		WCountInt--;

		sem_post(WseccionOut);
		sem_post(Wavailable);
	}
}

void * save(void * input) {
	Instruction * i = (Instruction *) input;
	sem_wait(Wavailable);
	sem_wait(WseccionIn);

	writes[Wnextin] = *i;
	Wnextin = (Wnextin + 1) % NWRITE;

	WCountInt++;

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

	char * token;

	token = strtok(index, ".");

	token = strtok(NULL, ".");
	p.number = atoi(token);

	token = strtok(NULL, ".");
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

	int found = 0;

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
			//printMessage(i, string);
			printf("%s\n", string);
		}
	}
	
	if (!found) {
		size_t string_size = snprintf(NULL, 0, "[GET] El indice %s no existe.", copyIndex);
		char * string = (char *)malloc(string_size + 1);
		snprintf(string, string_size +1, "[GET] El indice %s no existe.", copyIndex);
		//printMessage(i, string);
		printf("%s\n", string);
	}

	sem_post(Gavailable);
}

void * findFromFile(void * input) {

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
	
	if (fp == NULL)
		return NULL;

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

	pthread_exit((void *)m);
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

	if (numberResults) {
		printf("[FIND] %s: ", instruccion->word);
		for (i = 0; i < numberResults; i++) {
			printf("%s ", resultIDs[i]);
		}
		printf("\n");
	}
	else {
		printf("[FIND] No se encontro la palabra: %s\n", instruccion->word);
	}
}

void * manageInstructions(void * input) {
	while(1) {
		sem_wait(Icounter);

		Instruction i = instructions[nextInstruction];

		if (strcmp(i.command, "save") == 0) {
			savesCount++;

			pthread_create(&i.thread, NULL, save, &instructions[nextInstruction]);
		} else if (strcmp(i.command, "find") == 0) {
			pthread_create(&i.thread, NULL, find, &instructions[nextInstruction]);
		} else if (strcmp(i.command, "get") == 0) {
			getsCount++;

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

	FILE *fp = fopen("input", "r");
	//do {
	while(1) {
		size_t size;
		char command[5];
		char word[101];

		if(isatty(0))
			printf ("> ");

		ssize_t charsNumber = getline(&input, &size, fp);

		if(input[charsNumber-1] == '\n') {
			input[charsNumber-1] = '\0';
		}


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
	}
	//} while (strcmp(input, "") != 0);
}