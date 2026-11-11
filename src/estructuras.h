#ifndef ESTRUCTURAS_H
#define ESTRUCTURAS_H

#include <stdint.h>

#define DSM_MAX_NODES 50
#define DSM_HOST_MAX  64 // max len para IPs o hostnames

enum { LOG_ERROR = 0, LOG_WARN, LOG_INFO, LOG_DEBUG };

typedef struct {
    uint16_t node_id;
    char     host[DSM_HOST_MAX];
    uint16_t data_port;
    uint16_t hb_port; // hb = heartbeat
    uint64_t memory_bytes;
} node_config_t;

typedef struct {
    uint64_t default_node_memory;
    uint64_t base_address;
    uint64_t config_hash;
    uint32_t page_size;
    uint32_t hb_interval_ms;
    uint32_t hb_timeout_ms;
    uint32_t request_timeout_ms;
    uint32_t max_payload;
    uint16_t port_min, port_max;
    uint16_t max_threads, max_pending, node_count;
    uint8_t  request_retries, fault_threads, service_threads, protocol_workers;
    uint8_t  log_level;
    node_config_t nodes[DSM_MAX_NODES];
} dsm_config_t;

#endif
