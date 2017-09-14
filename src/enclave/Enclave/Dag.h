// #include <cstddef>
// #include <cstdint>

#include <functional>
#include <typeinfo>
#include <queue>

#include <unordered_set>


#include "Flatbuffers.h"
#include "ExpressionEvaluation.h"
#include "common.h"

#ifndef DAG_H
#define DAG_H
using namespace edu::berkeley::cs::rise::opaque;

void get_dependencies_for_node(
  uint8_t *dag_ptr, size_t dag_length, int node,
  uint32_t **output_tokens, size_t *output_tokens_length);

tuix::DAGNode *find_node(
    const tuix::DAG *dag, int token);

void add_dependencies(
    std::queue<tuix::DAGNode *> *fringe,
    std::unordered_set<int> *visited,
    tuix::DAGNode *curr);

#endif // DAG_H

