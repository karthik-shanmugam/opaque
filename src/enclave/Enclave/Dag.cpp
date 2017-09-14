// #include "Aggregate.h"
#include "Dag.h"
// #include "ExpressionEvaluation.h"
// #include "common.h"
#include <queue>
#include <unordered_set>

void get_dependencies_for_node(
  uint8_t *dag_ptr, size_t dag_length, int node,
  uint32_t **output_tokens, size_t *output_tokens_length) {
  (void) dag_length;

  const tuix::DAG *dag = flatbuffers::GetRoot<tuix::DAG>(dag_ptr);

  tuix::DAGNode *target = find_node(dag, node);
  if (target == nullptr) {
    // *output_tokens = nullptr;
    // *output_tokens_length = 0;
    // return;

    ocall_malloc(sizeof(int) * 1, (uint8_t **) output_tokens);
    *output_tokens_length = 1;
    *output_tokens[0] = 420;
    return;


  }


    // ocall_malloc(sizeof(int) * 2, (uint8_t **) output_tokens);
    // *output_tokens_length = target->dependencies()->size();
    // *output_tokens[0] = 1996 + target->dependencies()->size();
    // *output_tokens[1] = target->dependencies()->Get(0)->token();
    // return;

    ocall_malloc(target->dependencies()->size() * sizeof(uint32_t)*4, (uint8_t **) output_tokens);
    // *output_tokens_length = target->dependencies()->size();

    for (size_t i=0; i < target->dependencies()->size(); i++) {
        int temp = target->dependencies()->Get(i)->token();
	(*output_tokens)[i] = temp;//target->dependencies()->Get(i)->token();
    }
    *output_tokens_length = target->dependencies()->size();


}

tuix::DAGNode *find_node(
    const tuix::DAG *dag,
    int token) {

    std::unordered_set<int> visited = {};

    std::queue<tuix::DAGNode *> fringe;
  

    for (size_t i=0; i < dag->outputs()->size(); i++) {

        fringe.push((tuix::DAGNode *) dag->outputs()->Get(i));
        add_dependencies(&fringe, /*&visited,*/ (tuix::DAGNode *) dag->outputs()->Get(i));
    }

    // for (auto ptr = dag->outputs()->begin(); ptr != dag->outputs()->end(); ptr++) {
    //     fringe.push((tuix::DAGNode *) *ptr);
    //     add_dependencies(&fringe, /*&visited,*/ (tuix::DAGNode *) *ptr);
    // }

    while (!fringe.empty()) {
        tuix::DAGNode *curr = fringe.front();
        fringe.pop();
        if (curr->token() == token) {
            return curr;
        } else {
            add_dependencies(&fringe, /*&visited,*/ (tuix::DAGNode *) curr);
        }
    }
    return nullptr;
}

void add_dependencies(
    std::queue<tuix::DAGNode *> *fringe,
    // std::unordered_set<int> *visited,
    tuix::DAGNode *curr) {
    // for (auto ptr = curr->dependencies()->begin(); ptr != curr->dependencies()->end(); ptr++) {
    //     if (1  && visited->count(ptr->token()) == 0) {
    //         // visited->insert(ptr->token());
    //         fringe->push((tuix::DAGNode *) *ptr);
    //     }
    // }
    for (size_t i=0; i < curr->dependencies()->size(); i++) {
      fringe->push((tuix::DAGNode *) curr->dependencies()->Get(i));
    }

}
