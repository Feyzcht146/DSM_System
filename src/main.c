#include <stdio.h>
#include "config.h"

static void print_config(const dsm_config_t *c) {
    const char *levels[] = { "error", "warn", "info", "debug" };

    printf("page_size             = %u\n", c->page_size);
    printf("default_node_memory   = %lu\n", c->default_node_memory);
    printf("base_address          = 0x%lx\n", c->base_address);
    printf("port_min / port_max   = %u / %u\n", c->port_min, c->port_max);
    printf("heartbeat_interval_ms = %u\n", c->hb_interval_ms);
    printf("heartbeat_timeout_ms  = %u\n", c->hb_timeout_ms);
    printf("request_timeout_ms    = %u\n", c->request_timeout_ms);
    printf("request_retries       = %u\n", c->request_retries);
    printf("fault_threads         = %u\n", c->fault_threads);
    printf("service_threads       = %u\n", c->service_threads);
    printf("protocol_workers      = %u\n", c->protocol_workers);
    printf("max_threads           = %u\n", c->max_threads);
    printf("max_pending           = %u\n", c->max_pending);
    printf("max_payload           = %u\n", c->max_payload);
    printf("log_level             = %s\n", c->log_level <= LOG_DEBUG ? levels[c->log_level] : "?");
    printf("config_hash           = %016lx\n", c->config_hash);

    uint64_t total = 0;
    printf("\nnodos (%u):\n", c->node_count);
    for (int i = 0; i < c->node_count; i++) {
        const node_config_t *n = &c->nodes[i];
        printf("  %u  %-15s data=%u hb=%u mem=%lu\n",
               n->node_id, n->host, n->data_port, n->hb_port, n->memory_bytes);
        total += n->memory_bytes;
    }
    printf("memoria global = %lu bytes (0x%lx - 0x%lx)\n",
           total, c->base_address, c->base_address + total);
}

int main(int argc, char **argv) {
    const char *path = argc > 1 ? argv[1] : "../dsm.conf";
    dsm_config_t config;

    if (dsm_config_load(path, &config) != 0)
        return 1;

    printf("Configuracion cargada correctamente: %s\n\n", path);
    print_config(&config);
    return 0;
}
