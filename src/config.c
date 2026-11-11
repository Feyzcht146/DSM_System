// Programa que interpreta dsm.conf
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>

#include "estructuras.h"

#define LINE_MAX_LEN 256 // max len de cada linea del .conf

// Codificación de las claves para identificarlas por k_id (sin contar nodes)
enum {
    K_PAGE_SIZE = 0, K_DEFAULT_NODE_MEMORY, K_BASE_ADDRESS, K_PORT_MIN, K_PORT_MAX,
    K_HB_INTERVAL, K_HB_TIMEOUT, K_REQUEST_TIMEOUT, K_REQUEST_RETRIES,
    K_FAULT_THREADS, K_SERVICE_THREADS, K_PROTOCOL_WORKERS, K_MAX_THREADS,
    K_MAX_PENDING, K_MAX_PAYLOAD, K_LOG_LEVEL,
    NUM_KEYS
};

static const char* KEYS[NUM_KEYS] = {
    "page_size", "default_node_memory", "base_address", "port_min", "port_max",
    "heartbeat_interval_ms", "heartbeat_timeout_ms", "request_timeout_ms", "request_retries",
    "fault_threads", "service_threads", "protocol_workers", "max_threads",
    "max_pending", "max_payload", "log_level"
};

static void set_defaults(dsm_config_t *c) {
    memset(c, 0, sizeof *c);
    c->default_node_memory = 1048576;
    c->base_address = 0x200000000000ULL;
    c->page_size = 4096;
    c->hb_interval_ms = 1000; c->hb_timeout_ms = 3500;
    c->request_timeout_ms = 2000; c->request_retries = 3;
    c->max_payload = 65536;
    c->port_min = 27100; c->port_max = 27199;
    c->max_threads = 64; c->max_pending = 128;
    c->fault_threads = 2; c->service_threads = 2; c->protocol_workers = 4;
    c->log_level = LOG_INFO;
}

static char* trim(char* s) {
    while (isspace((unsigned char)*s)) s++;
    char* e = s + strlen(s); // guarda el final de la linea '\0'
    while (e > s && isspace((unsigned char)e[-1])) *(--e) = '\0';
    return s;
}

// Convierte un número decimal o hexadecimal en formato char a uint64_t.
static int parse_u64(const char *s, uint64_t *out) {
    char *end;
    errno = 0;
    unsigned long long v = strtoull(s, &end, 0);
    if (errno || end == s || *end != '\0') return -1;
    *out = v;
    return 0;
}

static void hash_line(uint64_t *h, const char *s) {
    // ULL: sufijo literal que indica el tipo del numero unsigned long long
    for (; *s; s++) { *h ^= (unsigned char)*s; *h *= 1099511628211ULL; }
    *h ^= '\n'; *h *= 1099511628211ULL; // constante fija del alg
}

static int find_key(const char *key) {
    for (int i = 0; i < NUM_KEYS; i++)
        if (strcmp(key, KEYS[i]) == 0) return i;
    return -1;
}

// Guarda val en una key segun k_id; 0 si se pudo convertir, -1 si no
static int set_param(dsm_config_t *c, int k_id, const char *val) {
    if (k_id == K_LOG_LEVEL) {
        const char *levels[] = { "error", "warn", "info", "debug" };
        for (int k = 0; k < 4; k++) {
            if (strcmp(val, levels[k]) == 0) {
                c->log_level = (uint8_t)k;
                return 0;
            }
        }
        return -1;
    }

    uint64_t v;
    if (parse_u64(val, &v)) return -1;

    switch (k_id) {
        case K_PAGE_SIZE:           c->page_size           = (uint32_t)v; break;
        case K_DEFAULT_NODE_MEMORY: c->default_node_memory = v;           break;
        case K_BASE_ADDRESS:        c->base_address        = v;           break;
        case K_PORT_MIN:            c->port_min            = (uint16_t)v; break;
        case K_PORT_MAX:            c->port_max            = (uint16_t)v; break;
        case K_HB_INTERVAL:         c->hb_interval_ms      = (uint32_t)v; break;
        case K_HB_TIMEOUT:          c->hb_timeout_ms       = (uint32_t)v; break;
        case K_REQUEST_TIMEOUT:     c->request_timeout_ms  = (uint32_t)v; break;
        case K_REQUEST_RETRIES:     c->request_retries     = (uint8_t)v;  break;
        case K_FAULT_THREADS:       c->fault_threads       = (uint8_t)v;  break;
        case K_SERVICE_THREADS:     c->service_threads     = (uint8_t)v;  break;
        case K_PROTOCOL_WORKERS:    c->protocol_workers    = (uint8_t)v;  break;
        case K_MAX_THREADS:         c->max_threads         = (uint16_t)v; break;
        case K_MAX_PENDING:         c->max_pending         = (uint16_t)v; break;
        case K_MAX_PAYLOAD:         c->max_payload         = (uint32_t)v; break;
    }
    return 0;
}

// Lectura de nodos
static int parse_node(dsm_config_t *c, const char *path, int node_n,
            int* node_ln, char node_lines[DSM_MAX_NODES][LINE_MAX_LEN]) {
    c->node_count = node_n; // ya el n max fue evaluado, y por ende aprobado
    char host[DSM_HOST_MAX + 1]; // +1 para '\0'
    unsigned id, dport, hport;
    for(int i = 0; i < node_n; i++) {
        unsigned long long mem = c->default_node_memory;
        int n = sscanf(node_lines[i], "%u %64s %u %u %llu", &id, host, &dport, &hport, &mem);
        if (n != 4 && n != 5) {
            fprintf(stderr, "%s:%d: node: error, formato esperado 'id host data_port hb_port [memoria]'\n", path, node_ln[i]);
            return -1;
        }
        if (id != (unsigned)i) {
            fprintf(stderr, "%s:%d: node id=%u, se esperaba %u (consecutivos desde 0)\n", path, node_ln[i], id, (unsigned)i);
            return -1;
        }
        if (strlen(host) >= DSM_HOST_MAX) {
            fprintf(stderr, "%s:%d: host demasiado largo (máx. %d)\n", path, node_ln[i], DSM_HOST_MAX); return -1;
        }
        node_config_t* node = &c->nodes[i];
        node->node_id = (uint16_t)id;
        strcpy(node->host, host);
        node->data_port = (uint16_t)(dport > 65535 ? 0 : dport);
        node->hb_port = (uint16_t)(hport > 65535 ? 0 : hport);
        node->memory_bytes = mem;
    }
    return 0;
}

static int validate(const dsm_config_t *c, const char *path) {
    long os_page = sysconf(_SC_PAGESIZE);

    // valida que sea potencia de 2 con las propiedas de numeros binarios
    if ((c->page_size & (c->page_size - 1)) || c->page_size % os_page) {
        fprintf(stderr, "%s: page_size=%u debe ser potencia de 2 y múltiplo de %ld\n",
                path, c->page_size, os_page);
        return -1;
    }
    if (c->default_node_memory % c->page_size) {
        fprintf(stderr, "%s: default_node_memory no es múltiplo de page_size\n", path);
        return -1;
    }
    // que no empiece a mitad de una pagina
    if (c->base_address % c->page_size) {
        fprintf(stderr, "%s: base_address no es múltiplo de page_size\n", path);
        return -1;
    }
    if (c->port_min >= c->port_max) {
        fprintf(stderr, "%s: port_min >= port_max\n", path);
        return -1;
    }
    // margen de 3 mensajes sin recibir para declarar como offline
    if (c->hb_timeout_ms <= 3 * c->hb_interval_ms) {
        fprintf(stderr, "%s: heartbeat_timeout_ms debe ser mayor que 3 x heartbeat_interval_ms\n", path);
        return -1;
    }
    if (c->max_payload < c->page_size) {
        fprintf(stderr, "%s: max_payload debe ser >= page_size\n", path);
        return -1;
    }

    uint64_t total = 0;

    // validaciones de nodos y puertos sin repetir
    for (int i = 0; i < c->node_count; i++) {
        const node_config_t *node = &c->nodes[i];
        if (node->memory_bytes == 0 || node->memory_bytes % c->page_size) {
            fprintf(stderr, "%s: node %d: memoria no es múltiplo de page_size\n", path, i);
            return -1;
        }
        if (node->data_port < c->port_min || node->data_port > c->port_max ||
            node->hb_port < c->port_min || node->hb_port > c->port_max) {
            fprintf(stderr, "%s: node %d: puertos fuera de [%u, %u]\n", path, i, c->port_min, c->port_max);
            return -1;
        }
        if (node->data_port == node->hb_port) {
            fprintf(stderr, "%s: node %d: data_port igual a hb_port\n", path, i);
            return -1;
        }
        // revisar que no haya puertos repetidos con otros nodos
        for (int j = 0; j < i; j++) {
            const node_config_t *tmp = &c->nodes[j];
            if (node->data_port == tmp->data_port || node->data_port == tmp->hb_port ||
                node->hb_port   == tmp->data_port || node->hb_port   == tmp->hb_port) {
                fprintf(stderr, "%s: node %d: puerto repetido con node %d\n", path, i, j);
                return -1;
            }
        }
        total += node->memory_bytes;
    }

    // que no exceda el rango maximo de direcciones disponibles para usuarios en x64_86
    if (c->base_address + total > 0x7fffffffffffULL) {
        fprintf(stderr, "%s: base_address + memoria total aportada por los nodos sale del espacio de usuario\n", path);
        return -1;
    }
    return 0;
}

int dsm_config_load(const char* path, dsm_config_t* c) {
    FILE *file = fopen(path, "r");
    if (file == NULL) {
        fprintf(stderr, "%s: %s\n", path, strerror(errno));
        return -1;
    }
    set_defaults(c);

    char line[LINE_MAX_LEN];
    int ln = 0, res = -1;
    // Las líneas node se guardan por aparte; [][] matriz fxc
    char node_lines[DSM_MAX_NODES][LINE_MAX_LEN];
    int node_ln[DSM_MAX_NODES], node_n = 0; // numero de linea con la info; numero de nodos

    char saved[NUM_KEYS] = {0};   // claves ya leídas, para detectar repetidas

    uint64_t hash = 14695981039346656037ULL; // constante fija del alg

    while (fgets(line, sizeof line, file) != NULL) {
        ln++;
        if (!strchr(line, '\n') && !feof(file)) {
            fprintf(stderr, "%s:%d: línea demasiado larga\n", path, ln); goto out;
        }
        // Cortar comentarios '#' y quitar espacios; si queda vacia, se ignora
        char* comment = strchr(line, '#');
        if (comment) *comment = '\0';
        char* s = trim(line);
        if (*s == '\0') continue;

        // Separar en clave y valor
        char *eq = strchr(s, '=');
        if (!eq) { fprintf(stderr, "%s:%d: falta '='\n", path, ln); goto out; }
        *eq = '\0';
        char *key = trim(s), *val = trim(eq + 1);

        if (*key == '\0') { fprintf(stderr, "%s:%d: clave vacia\n", path, ln); goto out; }

        char norm[LINE_MAX_LEN]; // clave=valor: sin espacios extra
        snprintf(norm, sizeof norm, "%s=%s", key, val);
        hash_line(&hash, norm);

        // Guarda lso nodos leidos para evaluarlos al final
        if (strcmp(key, "node") == 0) {
            if (node_n >= DSM_MAX_NODES) { fprintf(stderr, "%s:%d: demasiados nodos\n", path, ln); goto out; }
            snprintf(node_lines[node_n], LINE_MAX_LEN, "%s", val); node_ln[node_n++] = ln;
            continue;
        }

        // Identificar la clave, guarda el valor y marca como ya guardada
        int k_id = find_key(key);
        if (k_id < 0) {
            fprintf(stderr, "%s:%d: clave desconocida '%s'\n", path, ln, key); goto out;
        }
        if (saved[k_id]) {
            fprintf(stderr, "%s:%d: '%s' repetido\n", path, ln, key); goto out;
        }
        if (set_param(c, k_id, val)) {
            fprintf(stderr, "%s:%d: valor inválido en %s='%s'\n", path, ln, key, val); goto out;
        }
        saved[k_id] = 1;
    }
    if (ferror(file)) { fprintf(stderr, "%s: error de lectura\n", path); goto out; }

    if (node_n == 0) { fprintf(stderr, "%s: no hay ninguna línea de nodos\n", path); goto out; }

    // Nodos ya con default_node_memory definido; luego se valida todo
    if (parse_node(c, path, node_n, node_ln, node_lines)) goto out;
    if (validate(c, path)) goto out;
    c->config_hash = hash;
    
    res = 0;
out:
    fclose(file);
    return res;
}
