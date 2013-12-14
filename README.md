#Base de datos Llave - Valor
######Lab 2 - Sistemas Operativos - USACH 

###Arquitectura de la aplicación:
Se tiene una estructura llamada "Instruction" que mantiene la información de cada instruccion:
 - Comando (Char *)
 - Palabra (Char *)
 - Indice (int)
   - Es el indice dentro del arreglo de instrucciones. Como se va a realloc-ear el arreglo de instrucciones trabajaremos casi siempre con copias del objeto
 - thread (pthread_t)
 - response (char *)
   - Guardaremos la respuesta para cada instruccion en la misma estructura, en caso de ser necesario para el query.out

Se tiene un arreglo de "Instruction"s que se va ampliando cada vez que se recibe una nueva instruccion

Se tiene un administrador de instrucciones que recorre el arreglo de instruciones (como consumidor) y se encarga de levantar una hebra para cada instruccion en la función correspondiente (save, find, get).

La funcion find levanta una hebra (findFromFile) por cada pagina del indice (primera letra de la palabra o % si empieza con otro caracter)

Todas las hebras que leen archivos (save, get, findFromFile) tienen semaforos contadores por funcion, porque no es posible abrir mas de N archivos por proceso (En el caso de OS X: 20)

Al finalizar todas las instrucciones se ejecuta la funcion finishExecution() para que en caso de que se haya usado un archivo de entrada, se impriman los resultados en query.out

###Ejecución:
 - Compilacion:
 	```make```

 - Ejecución modo "terminal"
  - ```./build/db.exe```

 - Ejecución con archivo de entrada
  - ```./build/db.exe < <archivo input>```