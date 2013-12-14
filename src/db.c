#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <dirent.h>
#include <semaphore.h>
#include <fcntl.h>
#include <math.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>
#include <ctype.h>
#include <errno.h>

//Como las paginas tendran un maximo de 1MB (palabras de 100 bytes * 10.000 lineas) podemos tener 1000 workers que trabajen con archivos
#define NWRITE 200
#define NREAD 200
#define NFIND 600
#define ENTRIESPERPAGE 10000

// Estructura para definir una pagina donde escribir una nueva entrada
typedef struct {
	char letter;
	int number;
	int index;
	char * filename;
} Page;

// Estructura que define una instruccion
typedef struct {
	char command[5];
	char word[100];
	int index;
	pthread_t thread;
	char * response;
	pthread_mutex_t mutex;
} Instruction;

// Estructura que define un objeto de busqueda
typedef struct {
	char *word;
	char *filename;
	int *positions;
	size_t n_found;
} FindObject;

// Listamos la cantidad de paginas para cada letra + 1 para el resto y creamos el mutex para la escritura
int pages_count[27];
pthread_mutex_t * pages_mutexs[27];

// Mutex para el arreglo de instrucciones. Como se realloc-ea(?) es necesario bloquear su escritura completa
// (Si alguien lo usa mientras se hace el realloc (no-atomico) explota todo. Comprobado.)
pthread_mutex_t instructionsMutex;

// Productor consumidor para writes
Instruction writes[NWRITE];
int Wnextin, Wnextout;
sem_t * Wavailable;
sem_t * WseccionIn;
sem_t * WseccionOut;
sem_t * Wcount;

// Semaforos para limitar la cantidad de archivos abiertos por funcion
sem_t * Gavailable;
sem_t * Favailable;

// Contador, arreglo y semaforo para el listado de instrucciones
int instructionsCounter;
Instruction * instructions;
sem_t * Icounter;
sem_t * workingInstruction;

// En esta funcion manejaremos la impresion de mensajes por instruccion
// Si estamos en modo "terminal" imprimimos directamente
// Si no lo guardamos en el campo "response" de la instruccion dada para imprimirlo al final en el archivo
void printMessage(Instruction i, char * message) {
	printf("(*) %s\n", message);
	if(isatty(0)) {
		printf("%s\n", message);
	}
	else {
		pthread_mutex_lock(&instructionsMutex);
		instructions[i.index].response = (char *) malloc(sizeof(char) * (strlen(message)+1));
		strncpy(instructions[i.index].response, message, (strlen(message) +1));
		pthread_mutex_unlock(&instructionsMutex);
	}
}

// Funcion para inicializar la DB.
void initialize() {

	// Contamos la cantidad de paginas por indice
	int i;
	for (i=0;i<27;i++) {
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

				int index = 26;

				if (f>=97 && f <=122) {
					index = f-97;
					
				} 

				pages_count[index]++;
			}
		}
		closedir (dir);
	} else {
		/* could not open directory */
		perror ("El directorio ./data/ es inaccesible.");
		exit(1);
	}

	// Si para algun indice no hay paginas seteamos el default a 1
	for (i=0;i<27;i++) {
		if (pages_count[i] == 0)
			pages_count[i] = 1;
	}

	// Iniciamos el mutex de escritura para cada indice
	for (i=0;i<27;i++) {
		pages_mutexs[i] = (pthread_mutex_t *) malloc(sizeof(pthread_mutex_t));
		pthread_mutex_init(pages_mutexs[i], NULL);
	}

	// Inicializamos el productor consumidor para writes
	Wnextin=0;
	Wnextout=0;

	sem_unlink("Wavailable");
	sem_unlink("WseccionIn");
	sem_unlink("WseccionOut");
	sem_unlink("Wcount");
	sem_unlink("Icounter");

	sem_unlink("workingInstruction");

	sem_unlink("Gavailable");
	sem_unlink("Favailable");

	Wavailable = sem_open("Wavailable", O_CREAT, 0644, NWRITE);
	WseccionIn = sem_open("WseccionIn", O_CREAT, 0644, 1);
	WseccionOut = sem_open("WseccionOut", O_CREAT, 0644, 1);
	Wcount = sem_open("Wcount", O_CREAT, 0644, 0);
	Icounter = sem_open("Icounter", O_CREAT, 0644, 0);

	workingInstruction = sem_open("workingInstruction", O_CREAT, 0644, 0);

	Gavailable = sem_open("Gavailable", O_CREAT, 0644, NREAD);
	Favailable = sem_open("Favailable", O_CREAT, 0644, NFIND);

	// Inicializamos el mutex para el arreglo de instrucciones
	pthread_mutex_init(&instructionsMutex, NULL);
	instructionsCounter = -1;
}

// Funcion para contar las lineas en un archivo
int linesInFile(char * filename) {
	FILE *fp;

	char * line = NULL;
	size_t size;
	ssize_t line_size;
	int counter = 0;

	fp = fopen(filename, "r");

	if(fp == NULL)
		return 0;

	while((line_size = getline(&line, &size, fp)) !=  -1) {
		counter++;
	}

	pclose(fp);

	return counter;
}

// Pagina para obtener la primera parte del indice de una palabra. Si el primer caracter no es una letra trabajaremos con "%"
char getLetter(char * word) {
	char letter = tolower(word[0]);

	if (!(letter>=97 && letter <=122))
		letter = '%';

	return letter;
}

int getIndex(char letter) {
	int index = 26;

	if (letter>=97 && letter <=122)
		index = letter-97;

	return index;
}

// Funcion para obtener la siguiente posicion de escritura.
Page nextPage(char * word) {
	char letter = getLetter(word);

	int index = getIndex(letter);

	Page page;
	page.letter = letter;
	page.index = 0;
	page.number = pages_count[index];

	size_t filename_size;
	filename_size = snprintf(NULL, 0, "./data/%c.%d", letter, pages_count[index]);

	char * filename = (char *)malloc(filename_size + 1);
	snprintf(filename, filename_size+1, "./data/%c.%d", letter, pages_count[index]);

	// Si el archivo existe y lo podemos leer revisamos la cantidad de lineas
	if( access( filename, F_OK ) != -1 ) {
		int counter = linesInFile(filename);

		page.index = counter;

		// Si la cantidad de lineas es igual o mayor al maximo permitido pasamos a la siguiente pagina para el indice
		if (counter >= ENTRIESPERPAGE) {	
			pages_count[index]++;
			page.index = 0;
		}

		page.number = pages_count[index];

		filename_size = snprintf(NULL, 0, "./data/%c.%d", letter, page.number);

		filename = (char *)malloc(filename_size + 1);
		snprintf(filename, filename_size+1, "./data/%c.%d", letter, page.number);

	}

	page.filename = filename;

	return page;
}


// Funcion que escribe una palabra en un archivo (Consumidor)
void * writeOnFile(void * input) {
	while (1) {
		sem_wait(Wcount);
		sem_wait(WseccionOut);

		// Creamos una copia de la instruccion correpondiente
		// Guardamos aparte la palabra para evitar problemas con un posible futuro realloc del arreglo de instrucciones
		pthread_mutex_lock(&instructionsMutex);
		Instruction i = writes[Wnextout];
		char * word = i.word;
		pthread_mutex_unlock(&instructionsMutex);
		
		Wnextout = (Wnextout + 1) % 100;

		Page page = nextPage(word);
		int index = getIndex(page.letter);

		pthread_mutex_lock(pages_mutexs[index]);
		
		page = nextPage(word);
		index = getIndex(page.letter);

		FILE *fp = fopen(page.filename, "a");
		if(fp != NULL) {
			fprintf (fp, "%s\n", word);
		}
		fclose(fp);

		size_t string_size = snprintf(NULL, 0, "[SAVE] %c.%d.%d:%s", page.letter, page.number, page.index, word);
		char * string = (char *)malloc(string_size + 1);
		snprintf(string, string_size +1, "[SAVE] %c.%d.%d:%s", page.letter, page.number, page.index, word);
		printMessage(i, string);

		pthread_mutex_unlock(pages_mutexs[index]);

		sem_post(WseccionOut);
		sem_post(Wavailable);
	}
}

// Funcion productora para la escritura
void * save(void * input) {
	// Obtenemos un indice de instruccion, con eso obtendremos el resto de la informacion
	int indice = (int) input;

	sem_wait(Wavailable);
	sem_wait(WseccionIn);

	pthread_mutex_lock(&instructionsMutex);
	writes[Wnextin] = instructions[indice];
	pthread_mutex_unlock(&instructionsMutex);

	Wnextin = (Wnextin + 1) % NWRITE;

	sem_post(WseccionIn);
	sem_post(Wcount);

	return NULL;
}


// Funcion para obtener una palabra en base a un indice
void * getFromFile(void * input) {
	sem_wait(Gavailable);

	// Obtenemos un indice de instruccion, con eso obtendremos el resto de la informacion
	int indice = (int) input;

	pthread_mutex_lock(&instructionsMutex);
	Instruction instruccion = instructions[indice];

	// Guardamos 2 copias de la palabra. La primera se usará para buscar las distintas partes del indice
	// La segunda es para el mensaje de salida
	char * index = instruccion.word;
	

	char * copyIndex = (char *) malloc(sizeof(char) * (strlen(index) +1));
	strcpy(copyIndex, index);
	pthread_mutex_unlock(&instructionsMutex);
	
	int found = 0;

	Page p;
	p.letter = index[0];

	char * token;

	// Verificamos que la palabra tenga por lo menos 2 puntos.
	token = strtok(index, ".");

	token = strtok(NULL, ".");

	size_t output_message_size;
	char * output_message;

	if (token != NULL) {
		p.number = atoi(token);

		token = strtok(NULL, ".");
		if (token != NULL) {
			p.index = atoi(token);

			// Verificamos que despues de las transformaciones el indice sea el mismo
			char * checkIndex;
			size_t checkIndexSize;
			checkIndexSize = snprintf(NULL, 0, "%c.%d.%d", p.letter, p.number, p.index);
			checkIndex = (char *)malloc(checkIndexSize + 1);
			snprintf(checkIndex, checkIndexSize+1, "%c.%d.%d", p.letter, p.number, p.index);

			if(strcmp(copyIndex, checkIndex) == 0) {
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
					// Recorremos la linea hasta la que buscamos
					for (i=0; i<=p.index && !feof(fp); i++) {
						getline(&word, &size, fp);
					}

					// Si la palabra que encontramos no es una palabra vacia y el bucle alcanzo a llegar hasta la posicion entonces el indice existe
					if (!(strcmp(word, "") == 0 || i<=p.index)) {
						// Eliminamos el salto de linea
						word[(strlen(word)-1)] = '\0';

						// Seteamos el flag para identificar si encontramos la palabra
						found = 1;

						// Creamos el string de salida
						output_message_size = snprintf(NULL, 0, "[GET] %s:%s", copyIndex, word);
						output_message = (char *)malloc(output_message_size + 1);
						snprintf(output_message, output_message_size +1, "[GET] %s:%s", copyIndex, word);
					}
				}

				fclose(fp);
			}
		}
	}
	
	// Si no encontramos valor para el indice dado imprimimos un mensaje que lo indica
	if (!found) {
		output_message_size = snprintf(NULL, 0, "[GET] El indice %s no existe.", copyIndex);
		output_message = (char *)malloc(output_message_size + 1);
		snprintf(output_message, output_message_size +1, "[GET] El indice %s no existe.", copyIndex);
	}

	printMessage(instruccion, output_message);
	
	sem_post(Gavailable);

	return NULL;
}


// Funcion que busca una palabra en un archivo. Esta es levantada en multiples hebras por la funcion find()
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

	// Abrimos el archivo correspondiente
	fp = fopen(filename, "r");
	
	if (fp != NULL){
		m->n_found=0;
		m->positions = (int *) malloc(sizeof(int));

		// En esta variable guardaremos el numero de linea
		int count = 0;
		
		// Leemos linea a linea
		while ((read = getline(&line, &len, fp)) != -1) {
			int length = (strlen(line) -1);
			line[length] = '\0';

			// Si encontramos la palabra aumentamos el contador, y guardamos la posicion			
			if (strcmp(line, m->word) == 0) {
				m->positions = (int *) realloc(m->positions, (sizeof(int) * (m->n_found +1)));

				m->positions[m->n_found] = count;
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


// Funcion que maneja las busquedas por valor
void * find(void * input) {

	// Obtenemos el indice entregado, con eso buscaremos la informacion necesaria 
	int indice = (int) input;

	pthread_mutex_lock(&instructionsMutex);
	Instruction instruccion = instructions[indice];
	char * word = (char *) malloc(sizeof(char) * (strlen(instruccion.word) +1));
	strcpy(word, instruccion.word);
	pthread_mutex_unlock(&instructionsMutex);

	char letter = getLetter(word);
	int index = getIndex(letter);

	// Seteamos la cantidad de workers que necesitaremos
	int n_workers = pages_count[index];

	// Creamos un arreglo de strings donde guardaremos los archivos que necesitaremos
	char ** files = (char **) malloc(sizeof(char *) * n_workers);
	size_t filename_size;

	// Llenamos el arreglo de strings con los nombres de archivos
	int i, k;
	for (i = 0; i < n_workers; i++) {
		filename_size = snprintf(NULL, 0, "%c.%d", letter, (i+1));
		files[i] = (char *)malloc(filename_size + 1);
		snprintf(files[i], filename_size+1, "%c.%d", letter, (i +1));
	}

	int *pthread_ids;

	pthread_t *workers;

	// Creamos el arreglo de pthread_t para los workers
	pthread_ids = (int*) malloc(sizeof(int) * n_workers);
	workers = (pthread_t*) malloc(sizeof(pthread_t) * n_workers);

	// Levantamos los workers necesarios
	for (i = 0;i < n_workers; i++) {
		FindObject *C = (FindObject *) malloc(sizeof(FindObject));
		C->word = word;
		C->filename = files[i];
		pthread_ids[i] = pthread_create(&workers[i], NULL, findFromFile, C);
	}

	// Creamos un arreglo de strings dodne guardaremos los IDs que encontremos
	char ** resultIDs;
	int numberResults = 0;

	size_t indexLength;

	// Por cada worker esperamos que vuelva (con un join) y leemos el resultado
	for (i = 0; i < n_workers; i++) {
		FindObject *m;
		pthread_join(workers[i], (void *)&m);

		// Si encontramos la palabra:
		if (m != NULL && m->n_found > 0) {
			// Recorremos el listado de posiciones donde se encontro
			for (k = 0; k < m->n_found; k++) {
				numberResults++;
				
				// Agrandamos el arreglo de strings y guardamos el ID
				resultIDs = (char **) realloc(resultIDs, (sizeof(char *) * numberResults));

				indexLength = snprintf(NULL, 0, "%s.%d", m->filename, m->positions[k]);
				resultIDs[numberResults -1] = (char *)malloc(indexLength + 1);
				snprintf(resultIDs[numberResults -1], indexLength+1, "%s.%d", m->filename, m->positions[k]);
			}
		}
	}

	char * message = (char *) malloc(1);
	size_t message_size;

	if (numberResults) {
		// Si encontramos resultados generamos un string con los IDs asociados
		message_size = snprintf(NULL, 0, "[FIND] %s: ", word);
		message = (char *) realloc(message, message_size + 1);
		snprintf(message, message_size+1, "[FIND] %s: ", word);

		for (i = 0; i < numberResults; i++) {
			message_size = snprintf(NULL, 0, "%s%s, ", message, resultIDs[i]);
			message = (char *) realloc(message, message_size + 1);
			snprintf(message, message_size+1, "%s%s, ", message, resultIDs[i]);
		}

		message[(strlen(message) -2)] = '\0';
	}
	else {
		// SI no encontramos resultados generamos un mensaje con la informacion
		message_size = snprintf(NULL, 0, "[FIND] No se encontro la palabra: %s", word);
		message = (char *)malloc(message_size + 1);
		snprintf(message, message_size+1, "[FIND] No se encontro la palabra: %s", word);
	}

	printMessage(instruccion, message);

	return NULL;
}

// Funcion que maneja la lista de instrucciones
void * manageInstructions(void * input) {
	int nextInstruction = 0;
	while(1) {
		// Se usa un semaforo que se aumenta cada vez que se agrega una nueva instruccion al arreglo
		sem_wait(Icounter);

		pthread_mutex_lock(&instructionsMutex);

		// Como el valor de la instruccion se pasa por referencia se debe copiar (despues de la SC se aumenta)
		int currentInstruction = nextInstruction;
		
		// Se levanta el thread en la funcion correspondiente
		if (strcmp(instructions[currentInstruction].command, "save") == 0) {
			pthread_create(&instructions[currentInstruction].thread, NULL, save, (void *)currentInstruction);
		} else if (strcmp(instructions[currentInstruction].command, "find") == 0) {
			pthread_create(&instructions[currentInstruction].thread, NULL, find, (void *)currentInstruction);
		} else if (strcmp(instructions[currentInstruction].command, "get") == 0) {
			pthread_create(&instructions[currentInstruction].thread, NULL, getFromFile, (void *)currentInstruction);
		}

		sem_post(workingInstruction);
		pthread_mutex_unlock(&instructionsMutex);

		nextInstruction++;
	}
}

// Funcion que incrementa el tamaño del arreglo de instrucciones
void increaseInstructionsList() {
	instructionsCounter++;
	pthread_mutex_lock(&instructionsMutex);
	// Como el instructionsCounter empieza en -1, el tamaño del arreglo debe ser de instructionsCounter+1
	instructions = (Instruction *) realloc(instructions, (sizeof(Instruction) * (instructionsCounter +1)));
	pthread_mutex_unlock(&instructionsMutex);
}

int main() {
	// Inicializamos la DB
	initialize();

	// Levantamos los threads del consumidor de escrituras y del administrador de instrucciones
	pthread_t writeOnFileThread;
	pthread_create(&writeOnFileThread, NULL, writeOnFile, NULL);

	pthread_t instructionsManager;
	pthread_create(&instructionsManager, NULL, manageInstructions, NULL);
	
	char * input = NULL;
	size_t size;
	ssize_t read;

	// Si estamos en modo terminal mostramos el caracter >
	if(isatty(0))
		printf ("> ");

	// Leemos lineas desde stdin, si ingresamos un archivo con "./build/db < inputFile" obtendremos un EOF al final
	while ((read = getline(&input, &size, stdin)) != -1) {
		int length = (strlen(input) -1);
		input[length] = '\0';

		char command[5];
		char word[101];

		char *sep;

		// Revisamos si tenemos un espacio en la linea
		sep = strchr(input, ' ');

		if (sep) {
			size_t position = sep - input;

			// Si es asi separamos lo que esta antes del primer espacio como el comando y lo que esta despues como la palabra o indice
			strncpy(command, input, position);
			command[position] = '\0';

			int wordLength = (strlen(input) - position -1);

			strncpy(word, input + position + 1, wordLength);
			word[wordLength] = '\0';

			// Si el comando es valido:
			if (strcmp(command, "save") == 0 || strcmp(command, "find") == 0 || strcmp(command, "get") == 0) {
				// Agregamos una entrada al listado de instrucciones
				increaseInstructionsList();

				// Agregamos los valores necesarios a la instruccion
				strcpy(instructions[instructionsCounter].command, command);
				strcpy(instructions[instructionsCounter].word, word);
				instructions[instructionsCounter].index = instructionsCounter;

				// Aumentamos el semaforo de instrucciones
				sem_post(Icounter);
			} else {
				printf("Comando invalido. Los comandos disponibles son: save, find, get.\n");
			}
		}
		else {
			if (!(strcmp(input, "") == 0 || strcmp(input, "exit") == 0)) {
				printf("Comando invalido. Los comandos disponibles son: save, find, get.\n");
			}
			else {
				break;
			}
		}

		if(isatty(0))
			printf ("> ");
	}
	
	FILE *fp;

	// Si estamos leyendo desde un archivo abrimos el archivo de salida
	if(!isatty(0)) {
		fp = fopen("query.out", "w");
		if (fp == NULL) {
			printf("No se pudo abrir el archivo de salida query.out\n");
			return 0;
		}
	}

	int i;
	// Esperamos que cada thread de funcion termine, en orden
	for(i=0; i <= instructionsCounter; i++) {
		// Nos aseguramos que el thread ya se haya creado
		sem_wait(workingInstruction);

		// Esperamos que el thread termine
		pthread_join(instructions[i].thread, NULL);
	
		pthread_mutex_lock(&instructionsMutex);

		// Si estamos leyendo desde un archivo escribimos la respuesta para esa instruccion
		if(!isatty(0)){
			fputs(instructions[i].response, fp);
			fputs("\n", fp);
		}
		pthread_mutex_unlock(&instructionsMutex);
	}
	
	if(!isatty(0))
		fclose(fp);

	return 0;
}