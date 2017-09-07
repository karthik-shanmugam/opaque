#include <cstddef>
#include <cstdint>

#ifndef DAG_H
#define DAG_H

void ecall_get_dependencies_for_node(
  uint8_t *dag_ptr, size_t dag_length,
  uint32_t **output_tokens, size_t *output_tokens);

#endif // DAG_H

