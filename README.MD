# Sistema de Memoria Compartida Distribuida (DSM)

> Proyecto programado — Principios de Sistemas Operativos\
> Instituto Tecnológico de Costa Rica

## Descripción

Este repositorio contiene la implementación de un sistema de **Memoria Compartida
Distribuida (DSM)** en espacio de usuario sobre Linux. Su propósito es ofrecer a
procesos locales un espacio de direcciones virtual que representa memoria distribuida
entre varios nodos conectados mediante una red TCP/IP.

El sistema utiliza paginación bajo demanda y un protocolo de coherencia MSI
(*Modified, Shared, Invalid*) para coordinar las copias de páginas entre nodos.

## Alcance funcional

La implementación contempla:

-   Reserva y administración de memoria virtual mediante `mmap`.
-   Protección de páginas mediante `mprotect`.
-   Detección de fallos de acceso con `SIGSEGV` y `sigaction`.
-   Compartir páginas entre nodos mediante sockets TCP.
-   Coherencia de páginas mediante MSI.
-   Concurrencia mediante pthreads y sincronización POSIX.
-   Registro de nodos, disponibilidad mediante heartbeat y consulta del
    estado del sistema.
-   Configuración centralizada de los parámetros de ejecución.
-   API para que aplicaciones cliente utilicen el espacio DSM.
-   Aplicaciones de validación y pruebas de funcionamiento.

## Tecnologías y requisitos

| Elemento | Requisito |
|-|-|
| Sistema operativo | Linux x86_64 |
| Lenguaje          | C11 (`gcc -std=gnu11`) |
| Comunicación      | Sockets TCP/IP |
| Concurrencia      | Pthreads |
| Sincronización    | Semáforos POSIX y mutexes |
| Memoria virtual   | `mmap`, `mprotect` |
| Manejo de fallos  | `sigaction`, `SIGSEGV` |
| Compilación       | Makefile |
| Red               | Nodos conectados por una red IP |

Se requiere `gcc` con soporte de C11, `make` y glibc 2.30 o superior (por
`sem_clockwait` y `memfd_create`). No se usan bibliotecas externas. Si cambian las
dependencias, se actualizará esta sección.

## Estructura del repositorio

Estructura objetivo; los directorios y archivos se crean conforme se implementan. Hoy
existen la configuración, el paginador, la red, los nodos, la coherencia MSI, la primera
parte de la API (inicio, salida, hilos y asignación de memoria) y el programa
`dsm_system`, y las consultas globales. Están pendientes la réplica y las aplicaciones.

``` text
.
├── apps/                 aplicaciones adaptadas al API
├── include/
│   └── dsm.h             API pública (header único para las aplicaciones)
├── src/
│   ├── estructuras.h     estructuras internas compartidas entre módulos
│   ├── dsm.c             API: inicio y salida del nodo, hilos, asignación y consultas
│   ├── pager.c           mmap/memfd, mprotect, SIGSEGV, servicio de fallos
│   ├── coherence.c       directorio y transiciones MSI
│   ├── alloc.c           asignación de páginas entre nodos (dsm_alloc, dsm_free, nombres)
│   ├── stats.c           contadores y consultas (procesos, páginas, asignaciones, métricas)
│   ├── network.c         sockets, framing, listener y envío
│   ├── node.c            membresía, heartbeat, consultas y estadísticas
│   ├── config.c          lectura y validación de dsm.conf
│   └── main.c            programa dsm_system (nodo y su consola)
├── tests/                pruebas por componente
├── dsm.conf              configuración compartida por todos los nodos
├── Makefile              compila libdsm.a, dsm_system y las pruebas
└── README.MD
```

| Ruta | Responsabilidad |
|---|---|
| `include/` | Interfaz pública del DSM; lo único que incluyen las aplicaciones. |
| `src/`     | Implementación del DSM (biblioteca `libdsm.a`) y programa `dsm_system`. |
| `apps/`    | Aplicaciones de validación adaptadas al API `dsm.h`. |
| `tests/`   | Pruebas de componentes y de comportamiento. |
| `dsm.conf` | Parámetros comunes de todos los nodos. |
| `Makefile` | Compilación y tareas disponibles. |

## Cambios de esta actualización (API, asignación y consultas)

Respecto al primer prototipo (paginador, red TCP, MSI y membresía), esta versión agrega la
API pública, el asignador distribuido y las consultas globales. Para usarla:

1. **Recompilar todo** con `make clean && make`. Ahora se genera `libdsm.a` (todos los
   módulos menos `main.c`) y `dsm_system` se enlaza con ella. Los `.o` del prototipo
   anterior no son compatibles.
2. **Usar la misma versión en todos los nodos.** Hay mensajes nuevos (`ALLOC_MAP`,
   `ALLOC_RESERVE`, `ALLOC_RELEASE`, `NAME_LOOKUP`, `STATS_REQUEST`, `PAGETABLE_REQUEST`,
   `ALLOCS_REQUEST`), y un nodo del prototipo anterior cierra la conexión al recibirlos.
3. **Revisar `dsm.conf`.** Los puertos van de 27900 a 27999 (`port_min` / `port_max`) y las
   líneas `node` deben usar puertos dentro de ese rango, sin repetir ninguno.
4. **Aplicaciones:** incluyen solo `include/dsm.h` y se enlazan con `libdsm.a`
   (ver [API](#api)). El proceso que llama a `dsm_init` es un nodo del clúster: no hace
   falta correr `dsm_system` aparte en la misma máquina. En una app, `malloc`/`free` sobre
   los datos compartidos se cambian por `dsm_alloc`/`dsm_free`.
5. **Hilos:** ya no hay que registrar cada hilo. Cada uno toma su lugar en su primer fallo
   de página; `dsm_thread_register` queda como opción.
6. **Nodo automático:** `./dsm_system dsm.conf auto` (o `dsm_init(..., DSM_NODE_AUTO)`)
   elige la primera línea `node` cuya IP es de este equipo y cuyos puertos están libres.
   También se puede fijar con la variable de entorno `DSM_NODE`.
7. **Consola:** órdenes nuevas `alloc`, `attach`, `free`, `allocs`, `allocs global`,
   `pages global`, `stats [nodo]` y `procs` (ver [Ejecución](#ejecución)).
8. **Unión al clúster:** un nodo configurado que no responde ya no detiene el arranque (se
   espera una sola vez y luego se reintenta cada segundo), y Ctrl+C funciona también
   mientras el nodo se une.
9. **Compilación:** `_GNU_SOURCE` se define al inicio de cada archivo que lo necesita
   (`pager.c`, `network.c`, `stats.c`), no en el `Makefile`.

## Configuración

El sistema utiliza un archivo de configuración común para establecer los parámetros
de los nodos: `dsm.conf`. Al iniciar se valida todo el archivo. Si hay un error, se
informa el parámetro (y la línea, si el error es de formato), y el nodo no arranca. Los
nodos comparan un hash del contenido normalizado; si no coincide, se rechaza la unión.
Entre los valores
contemplados están:

- Tamaño de página, con valor predeterminado de 4 KiB según el enunciado.
- Cantidad de memoria aportada por cada nodo.
- Direcciones IP o nombres de los equipos participantes.
- Puerto principal de comunicación (datos y heartbeat).
- Intervalos entre respuestas y timeouts.
- Threads, workers y cantidad máxima de solicitudes que se contemplan estando on air.
- Cantidad máxima de payload que viaja en las comunicaciones.
- Nivel de información que se reportará si se solicita.
- Se usa # para escribir comentarios
- Los parámetros se escriben con el formato clave = valor
- Los nodos se identifican con la etiqueta`node =` seguido (y separado por
  espacios) se espera que se defina en orden el `id`, `host`, `data_port`,
  `hb_port` y `memoria` (este último es opcional).
- Los `id` de los nodos deben ser consecutivos desde **0**.
- Ningún puerto se puede repetir entre nodos, aunque estén en computadoras distintas, y
  todos deben estar entre `port_min` y `port_max` (en `dsm.conf`: 27900 a 27999).

Antes de ejecutar los nodos:

1.  Configure las direcciones y puertos correspondientes a los equipos.
2.  Verifique que todos los nodos utilicen parámetros compatibles.
3.  Compruebe la conectividad de red entre los equipos (`ping`, `nc -vz <ip> <puerto>`).
4.  Verifique que los puertos de datos y de heartbeat de todos los nodos estén
    disponibles y permitidos por el firewall, en ambos sentidos.

## Compilación

Desde la raíz del repositorio (de momento se tiene):

``` bash
make            # compila libdsm.a (la biblioteca) y dsm_system (el programa del nodo)
make run        # compila y valida dsm.conf (make run CONF=otro.conf)
make test       # pruebas de la configuración, del paginador, del clúster (3 nodos locales) y de la API
make asan       # recompila dsm_system y las pruebas con AddressSanitizer
make clean      # borra objetos y ejecutables
```

Los objetivos nuevos se documentarán aquí cuando existan en el `Makefile`.


## Ejecución

Para validar la configuración sin iniciar un nodo:

``` bash
./dsm_system dsm.conf
```

Imprime los parámetros, los nodos y el rango de memoria global, o el error con su
línea.

Para iniciar un nodo se indica su `id` (el de su línea `node` en `dsm.conf`). Cada
nodo es un proceso: uno por terminal en la misma máquina, o uno por computadora
usando en `dsm.conf` la IP de cada equipo. Todos deben usar el mismo `dsm.conf`.

``` bash
./dsm_system dsm.conf 0     # terminal o computadora del nodo 0
./dsm_system dsm.conf 1     # terminal o computadora del nodo 1
./dsm_system dsm.conf auto  # el primer nodo de dsm.conf que es de este equipo y está libre
```

Los nodos pueden iniciarse en cualquier orden y se unen solos al clúster. Un nodo
configurado que no responde no impide el arranque: se reintenta la unión cada segundo. La memoria
disponible es la de los nodos que están en línea. Cada nodo ofrece una consola:

| Orden | Qué hace |
|---|---|
| `nodes` | Tabla de nodos, su estado, sus páginas libres y la memoria disponible y sin asignar. |
| `pages` | Páginas usadas: estado MSI local y, en su home, el directorio de copias y su asignación. |
| `read <página>` | Lee el texto guardado en la página. |
| `write <página> <texto>` | Escribe un texto en la página. |
| `incr <página> <n> <veces>` | Suma 1 `<veces>` veces al contador `n` de la página. |
| `get <página> <n>` | Muestra el contador `n` de la página. |
| `alloc <bytes> [nombre]` | Asigna memoria DSM con `dsm_alloc`; con nombre, otros procesos pueden adjuntarla. |
| `attach <nombre>` | Adjunta una asignación con nombre creada en otro proceso. |
| `free <página>` | Libera la asignación de este proceso que empieza en esa página. |
| `allocs` | Asignaciones de este proceso y asignaciones con páginas en este nodo. |
| `allocs global` | Asignaciones de todo el clúster: proceso (pid), nodo, páginas y nombre. |
| `pages global` | Páginas en uso de todo el clúster: home, copias, dueño, asignación y orden dentro de ella. |
| `stats [nodo]` | Métricas de este nodo o de otro en línea. |
| `procs` | Procesos del clúster (en ejecución y los que ya salieron) con sus métricas. |
| `quit` | Sale del clúster de forma ordenada (igual que Ctrl+D o Ctrl+C). |

Al salir de forma ordenada, el nodo devuelve a su home las páginas de otros nodos que
tenía modificadas. Las páginas que aportaba el nodo que sale dejan de estar disponibles.

Pendiente de implementar y documentar:

1.  Cómo ejecutar las aplicaciones de validación.
2.  Cómo consultar las estadísticas locales y globales.

## API

Las aplicaciones incluyen solo `include/dsm.h` y se enlazan con `libdsm.a`:

``` bash
gcc -std=gnu11 -Iinclude mi_app.c libdsm.a -pthread -o mi_app
```

El proceso que llama a `dsm_init` pasa a ser un nodo del clúster. Funciones disponibles:

| Función | Qué hace | Errores |
|---|---|---|
| `int dsm_init(const char *config_path, int node_id)` | Inicia el proceso como el nodo `node_id`: reserva el espacio DSM, abre sus puertos y se une a los nodos en línea. Con `DSM_NODE_AUTO` usa el nodo de la variable de entorno `DSM_NODE` o, si no existe, el primer nodo de `dsm.conf` que es de este equipo y está libre. | `DSM_ERR_STATE`, `DSM_ERR_CONFIG`, `DSM_ERR_NODE`, `DSM_ERR_MEMORY`, `DSM_ERR_NETWORK`, `DSM_ERR_REJECTED` |
| `int dsm_shutdown(void)` | Sale del clúster de forma ordenada y libera el espacio DSM. | `DSM_ERR_STATE` |
| `int dsm_node_id(void)` | Nodo que es este proceso. | `DSM_ERR_STATE` |
| `int dsm_thread_register(void)` / `int dsm_thread_unregister(void)` | Opcionales: reservan o devuelven el lugar de un hilo. | `DSM_ERR_STATE`, `DSM_ERR_THREADS` |
| `void *dsm_alloc(size_t size, const char *tag)` | Reserva `size` bytes (redondeados a páginas) y devuelve su dirección, que es la misma en todos los nodos. Usa primero las páginas de este nodo; si no alcanzan, un tramo contiguo que puede cruzar a otros nodos en línea. La memoria empieza en cero. `tag` es una etiqueta opcional. | `DSM_ERR_STATE`, `DSM_ERR_ARG`, `DSM_ERR_NOMEM` |
| `void *dsm_alloc_named(const char *name, size_t size)` | Igual, con un nombre (hasta 31 caracteres) para que otros procesos la adjunten. No se libera sola al terminar quien la creó. | los de `dsm_alloc` y `DSM_ERR_EXISTS` |
| `void *dsm_attach(const char *name, size_t *size)` | Dirección (y tamaño) de la asignación con ese nombre, creada por cualquier proceso. | `DSM_ERR_STATE`, `DSM_ERR_ARG`, `DSM_ERR_NOTFOUND` |
| `int dsm_free(void *ptr)` | Libera una asignación de este proceso (creada o adjuntada): las páginas quedan libres en sus nodos, se invalidan sus copias y vuelven a cero. | `DSM_ERR_STATE`, `DSM_ERR_ARG` |
| `int dsm_get_nodes(dsm_node_info_t *out, int max)` | Tabla de nodos de `dsm.conf`: estado, puertos, páginas que aporta y libres. | `DSM_ERR_STATE` |
| `int dsm_get_page_table(int node, dsm_page_info_t *out, int max)` | Páginas en uso de un nodo, o de todo el clúster con `DSM_ALL_NODES`: estado local, directorio, asignación, proceso y orden. | `DSM_ERR_STATE`, `DSM_ERR_NODE` |
| `int dsm_get_allocs(int node, dsm_alloc_info_t *out, int max)` | Asignaciones con páginas en un nodo, o todas con `DSM_ALL_NODES`. | `DSM_ERR_STATE`, `DSM_ERR_NODE` |
| `int dsm_get_stats(int node, dsm_stats_t *out)` | Proceso y métricas de un nodo en línea: fallos, latencia, páginas e invalidaciones enviadas y recibidas, mensajes, bytes, hilos, páginas usadas. | `DSM_ERR_STATE`, `DSM_ERR_NODE` |
| `int dsm_get_procs(dsm_stats_t *out, int max)` | Procesos del clúster: los de los nodos en línea y el último de cada nodo que salió de forma ordenada. | `DSM_ERR_STATE` |
| `int dsm_last_error(void)` | Código del último error de este hilo en una función que devuelve puntero. | — |
| `const char *dsm_strerror(int code)` | Texto de un código de error. | — |

Las consultas que llenan un arreglo devuelven cuántos elementos hay (puede ser más que
`max`; solo se copian `max`). Las demás funciones que devuelven `int` dan `DSM_OK` (0) o un código negativo; las que devuelven
puntero dan `NULL` si fallan y el motivo se consulta con `dsm_last_error()`.

Ejemplo mínimo:

``` c
#include "dsm.h"

int main(void) {
    if (dsm_init("dsm.conf", DSM_NODE_AUTO) != DSM_OK) return 1;
    double *datos = dsm_alloc(100000 * sizeof(double), "datos");   // en vez de malloc
    if (!datos) return 1;
    datos[99999] = 3.14;       // las páginas de otros nodos llegan solas al usarlas
    dsm_free(datos);           // en vez de free
    return dsm_shutdown() != DSM_OK;
}
```

Sobre las asignaciones:

- Una asignación es un tramo **contiguo** de páginas. Solo cuentan las páginas de los nodos
  en línea, y un tramo no puede saltar sobre un nodo que no está.
- Al terminar un proceso (`dsm_shutdown`, o caída de su nodo) se liberan sus asignaciones
  sin nombre. Las que tienen nombre siguen hasta que alguien llame a `dsm_free`.
- Si dos nodos piden las mismas páginas a la vez, uno gana y el otro vuelve a buscar.
 Los hilos creados con
`pthread_create` usan la memoria DSM sin registrarse: cada uno ocupa uno de los
`max_threads` lugares en su primer fallo de página, y el lugar de un hilo que terminó se
reutiliza. Puede haber más hilos en total que `max_threads`, pero no más a la vez.

## Aplicaciones de validación

El proyecto requiere al menos dos aplicaciones diferentes e independientes adaptadas
para utilizar el API DSM, en lugar de realizar sus operaciones sobre memoria
tradicional y completamente local, estas aplicaciones, se buscaron que necesitaran
de uso elevado de memoria para que aprovechen el DSM. Las cuáles son:

1. **Difusión de calor (método de Jacobi)**:
2. **Radix Sort**:.

Para cada aplicación se documentará más adelante:

-   Problema que resuelve.
-   Motivo por el que requiere una cantidad considerable de memoria.
-   Descripción de los datos de entrada.
-   Forma de ejecución.
-   Resultado esperado.
-   Evidencia de que utiliza páginas distribuidas.
-   Resultados y estadísticas obtenidos.

## Pruebas

Las pruebas comprobarán el funcionamiento de la paginación bajo demanda, la
transferencia de páginas, las transiciones MSI, el acceso concurrente y la
sincronización entre nodos.
