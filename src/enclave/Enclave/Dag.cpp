#include "Aggregate.h"
#include "Dag.h"
#include "ExpressionEvaluation.h"
#include "common.h"
#include <queue>
#include <unordered_set>

void get_dependencies_for_node(
  uint8_t *dag_ptr, size_t dag_length, int node,
  uint32_t **output_tokens, size_t *output_tokens_length) {
  (void) dag_length;

  tuix::DAG *dag = flatbuffers::GetRoot<tuix::DAG>(dag_ptr);

  tuix::DAGNode *target = find_node(dag, node);
  if (target == nullptr) {
    *output_tokens = nullptr;
    *output_tokens_length = 0;
    return;
  }
  // uint8_t *buf = nullptr;
  ocall_malloc(target->dependencies()->size() * sizeof(int), (uint8_t **) output_tokens);
  *output_tokens_length = target->dependencies()->size();

  for (size_t i=0; i < target->dependencies()->size(); i++) {
    *output_tokens[i] = target->dependencies()->Get(i)->token();
  }
  *output_tokens_length = target->dependencies()->size();
  // tuix::DAGNode *target = nullptr;

  // std::unordered_set<int> visited = {};
  
  // for (ptr = ar.begin(); ptr < ar.end(); ptr++)

}

tuix::DAGNode *find_node(
    tuix::DAG *dag,
    int token) {

    std::unordered_set<int> visited = {};

    std::queue<tuix::DAGNode *> fringe;
  
    for (auto ptr = dag->outputs()->begin(); ptr != dag->outputs()->end(); ptr++) {
        add_dependencies(&fringe, &visited, ptr);
    }

    while (!fringe.empty()) {
        tuix::DAGNode *curr = fringe.pop();
        if (curr->token() == token) {
            return curr;
        } else {
            add_dependencies(&fringe, &visited, curr);
        }
    }
    return nullptr;
}

void add_dependencies(
    std::queue<tuix::DAGNode *> *fringe,
    std::unordered_set<int> *visited,
    tuix::DAGNode *curr) {
    for (auto ptr = curr->dependencies()->begin(); ptr != curr->dependencies()->end(); ptr++) {
        if (visited->count(ptr->token()) == 0) {
            visited->insert(ptr->token());
            fringe->push(ptr);
        }
    }

}