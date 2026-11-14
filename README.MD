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

Estructura objetivo, los directorios y archivos se crean conforme se implementan; al
día de hoy solo existen los documentos y carga de la configuración.

``` text
.
├── apps/                 aplicaciones adaptadas al API
├── include/
│   └── dsm.h             API pública (header único para las aplicaciones)
├── src/
│   ├── estructuras.h     estructuras internas compartidas entre módulos
│   ├── dsm.c             inicialización, API, asignación, locks y barreras
│   ├── pager.c           mmap/memfd, mprotect, SIGSEGV, servicio de fallos
│   ├── coherence.c       directorio y transiciones MSI
│   ├── network.c         sockets, framing, listener y envío
│   ├── node.c            membresía, heartbeat, consultas y estadísticas
│   ├── config.c          lectura y validación de dsm.conf
│   └── main.c            programa dsm_node y su consola
├── tests/                pruebas por componente
├── dsm.conf              configuración compartida por todos los nodos
├── Makefile              compila libdsm, dsm_node, apps y tests
└── README.MD
```

| Ruta | Responsabilidad |
|---|---|
| `include/` | Interfaz pública del DSM; lo único que incluyen las aplicaciones. |
| `src/`     | Implementación del DSM (biblioteca `libdsm.a` y programa `dsm_node`). |
| `apps/`    | Aplicaciones de validación adaptadas al API `dsm.h`. |
| `tests/`   | Pruebas de componentes y de comportamiento. |
| `dsm.conf` | Parámetros comunes de todos los nodos. |
| `Makefile` | Compilación y tareas disponibles. |

## Configuración

El sistema utiliza un archivo de configuración común para establecer los parámetros
de los nodos: `dsm.conf`. Al iniciar se valida todo el archivo. Si hay un error, se
informa la línea y el parámetro, y el nodo no arranca. Los nodos comparan un hash del
contenido normalizado; si no coincide, se rechaza la unión.Entre los valores
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
- Los parámetros se escibre con el formato clave = valor
- Los nodos se identifican con la etiqueta`node =` seguido (y separado por
  espacios) se espera que se defina en orden el `id`, `host`, `data_port`,
  `hb_port` y `memoria` (este último es opcional).
- Los `id` de los nodos deben ser consecutivos desde **0**.

Antes de ejecutar los nodos:

1.  Configure las direcciones y puertos correspondientes a los equipos.
2.  Verifique que todos los nodos utilicen parámetros compatibles.
3.  Compruebe la conectividad de red entre los equipos (cuando su módulo este
    disponible).
4.  Verifique que los puertos configurados estén disponibles y permitidos por el
    firewall (igual, hasta que se implemente su módulo).

## Compilación

Desde la raíz del repositorio (de momento se tiene):

``` bash
make            # compila dsm_system
make run        # compila y valida dsm.conf (make run CONF=otro.conf)
make test       # corre las pruebas de la configuración, del paginador y del clúster (3 nodos locales)
make asan       # recompila con AddressSanitizer
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
```

Los nodos pueden iniciarse en cualquier orden y se unen solos al clúster. La memoria
disponible es la de los nodos que están en línea. Cada nodo ofrece una consola:

| Orden | Qué hace |
|---|---|
| `nodes` | Tabla de nodos, su estado y la memoria disponible. |
| `pages` | Páginas usadas: estado MSI local y, en su home, el directorio de copias. |
| `read <página>` | Lee el texto guardado en la página. |
| `write <página> <texto>` | Escribe un texto en la página. |
| `incr <página> <n> <veces>` | Suma 1 `<veces>` veces al contador `n` de la página. |
| `get <página> <n>` | Muestra el contador `n` de la página. |
| `quit` | Sale del clúster de forma ordenada (igual que Ctrl+D o Ctrl+C). |

Al salir de forma ordenada, el nodo devuelve a su home las páginas de otros nodos que
tenía modificadas. Las páginas que aportaba el nodo que sale dejan de estar disponibles.

Pendiente de implementar y documentar:

1.  Cómo ejecutar las aplicaciones de validación.
2.  Cómo consultar las estadísticas locales y globales.

## Aplicaciones de validación

El proyecto requiere al menos dos aplicaciones diferentes e independientes adaptadas
para utilizar el API DSM, en lugar de realizar sus operaciones sobre memoria
tradicional y completam,ente local, estas aplicaciones, se buscaron que necesitaran
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
