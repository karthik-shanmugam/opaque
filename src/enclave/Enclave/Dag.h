#include <cstddef>
#include <cstdint>

#ifndef DAG_H
#define DAG_H
using namespace edu::berkeley::cs::rise::opaque;

void get_dependencies_for_node(
  uint8_t *dag_ptr, size_t dag_length, int node,
  uint32_t **output_tokens, size_t *output_tokens_length);

#endif // DAG_H

