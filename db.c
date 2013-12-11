#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <dirent.h>
#include <semaphore.h>
#include <math.h>
#include <unistd.h>
#include <string.h>

#define NWRITE 100
#define ENTRIESPERPAGE 10000

// Listamos la cantidad de paginas para cada letra + 1 para el resto
int pages_count[27];
pthread_mutex_t * pages_mutexs[27];

// Productor consumidor para writes
char * writes[NWRITE];
int Wnextin, Wnextout;
sem_t Wavailable, Wseccion, Wcount;

typedef struct {
	char letter;
	int number;
	char * filename;
} Page;

void initialize() {
	int i,j;

	for (i=0;i<=27;i++) {
		pages_count[i] = 1;
	}

	//Codigo para listar archivos sacado de: http://stackoverflow.com/questions/12489/how-do-you-get-a-directory-listing-in-c
	DIR *dir;
	struct dirent *ent;
	if ((dir = opendir ("./data/")) != NULL) {
		/* print all the files and directories within directory */
		while ((ent = readdir (dir)) != NULL) {
			if (strcmp(ent->d_name, ".") != 0 && strcmp(ent->d_name, "..") != 0) {
				char f = ent->d_name[0];
				//printf ("\n----\n%s\n%c\n", ent->d_name, f);

				if (f>=97 && f <=122) {
					int index = f-97;
					//printf("Increasing %d\n", index);
					pages_count[index]++;
				} else {
					int index = 26;
					//printf("Increasing %d\n", index);
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
		pages_mutexs[i] = (pthread_mutex_t *) malloc(sizeof(pthread_mutex_t) * pages_count[i]);

		for (j=0; j < pages_count[i]; i++) {
			pthread_mutex_init(&pages_mutexs[i], NULL);
		}
	}

	// Initialize producer/consumer writes
	Wnextin=0;
	Wnextout=0;

	sem_init(&Wavailable, 0, NWRITE);
	sem_init(&Wseccion, 0, 1);
	sem_init(&Wcount, 0, 0);

	for (i=0; i<NWRITE; i++) {
		// Maximo 99 caracteres por palabra
		writes[i] = (char *) malloc(sizeof(char) * 100);
	}

}

Page nextPage(char * word) {
	char letter = tolower(word[0]);

	int index = 27;

	if (letter>=97 && letter <=122)
		index = letter-97;


	Page page;
	page.letter = letter;

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
			printf("Failed to run command\n" );
			exit;
		}

		/* Read the output a line at a time - output it. */
		fgets(result, sizeof(result)-1, fp);

		int number;
		sscanf(result, "%d", &number);

		if (number > ENTRIESPERPAGE) {	
			pages_count[index]++;

			pages_mutexs[index] = (pthread_mutex_t *) realloc(pages_mutexs[index], sizeof(pthread_mutex_t) * pages_count[index]);

			pthread_mutex_init(&pages_mutexs[(index -1)], NULL);
		}

		/* close */
		pclose(fp);
	}

	page.number = pages_count[index];

	filename_size = snprintf(NULL, 0, "./data/%c.%d", letter, page.number);

	filename = (char *)malloc(filename_size + 1);
	snprintf(filename, filename_size+1, "./data/%c.%d", letter, page.number);

	page.filename = filename;

	return page;
}

void * writeOnFile(void * input) {
	while (1) {
		int value; 
      sem_getvalue(&Wcount, &value); 
      printf("The value of the semaphors is %d\n", value);
		sem_wait(&Wcount);
		sem_wait(&Wseccion);

		int wordLength = strlen(writes[Wnextout]);
		char * word = (char *) malloc(sizeof(char) * wordLength);
		strcpy(word, writes[Wnextout]);
		Wnextout = (Wnextout + 1) % 100;

		sem_post(&Wseccion);
		sem_post(&Wavailable);

		Page page = nextPage(word);

		FILE *fp = fopen(page.filename, "a");

        if(fp != NULL) {
			fprintf (fp, "%s\n", word);
        }
        fclose(fp);
	}
}

void * save(void * input) {
	char * word = (char *) input;

	sem_wait(&Wavailable);
	sem_wait(&Wseccion);

	strcpy(writes[Wnextin], word);
	Wnextin = (Wnextin + 1) % NWRITE;

	sem_post(&Wseccion);
	sem_post(&Wcount);
}

main() {
	initialize();
	save("hola");
	writeOnFile(NULL);
}