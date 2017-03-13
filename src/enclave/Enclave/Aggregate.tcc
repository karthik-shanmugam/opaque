// -*- mode: C++ -*-

#include "util.h"
#include "NewInternalTypes.h"

int printf(const char *fmt, ...);

template<typename AggregatorType>
void aggregate_step1(Verify *verify_set,
                     uint8_t *input_rows, uint32_t input_rows_length,
                     uint32_t num_rows,
                     uint8_t *output_rows, uint32_t output_rows_length,
                     uint32_t *actual_size) {
  (void)output_rows_length;

  RowReader r(input_rows, input_rows + input_rows_length, verify_set);
  IndividualRowWriterV w(output_rows);
  w.set_self_task_id(verify_set->get_self_task_id());
  NewRecord cur;
  AggregatorType a;

  for (uint32_t i = 0; i < num_rows; i++) {
    r.read(&cur);
    if (i == 0) {
      w.write(&cur);
    }
    a.aggregate(&cur);
  }

  a.set_offset(a.get_num_distinct() - 1);
  w.write(&a);
  w.close();
  *actual_size = w.bytes_written();
}

template<typename AggregatorType>
void aggregate_process_boundaries(Verify *verify_set,
                                  uint8_t *input_rows, uint32_t input_rows_length,
                                  uint32_t num_rows,
                                  uint8_t *output_rows, uint32_t output_rows_length,
                                  uint32_t *actual_output_rows_length, uint32_t *num_distinct_groups) {
  (void)input_rows_length;
  (void)output_rows_length;

  // 1. Calculate the global number of distinct items, compensating for runs that span partition
  // boundaries

  uint32_t num_distinct = 0;
  {
    AggregatorType prev_last_agg, cur_last_agg;
    NewRecord cur_first_row;
    IndividualRowReaderV r(input_rows, verify_set);
    for (uint32_t i = 0; i < num_rows; i++) {
      if (i > 0) prev_last_agg.set(&cur_last_agg);

      r.read(&cur_first_row);
      r.read(&cur_last_agg);

      num_distinct += cur_last_agg.get_num_distinct();
      if (i > 0 && prev_last_agg.grouping_attrs_equal(&cur_first_row)) {
        // The current partition begins with a run of items continued from the previous partition,
        // so we shouldn't double count it
        num_distinct--;
      }
    }
  }

  // 2. Send the following items to each partition:
  //    (a) global number of distinct items,
  //    (b) the last partial aggregate from the previous partition (augmented with previous runs),
  //    (c) the global offset for the item involved in (b) within the set of distinct items
  //    (d) the first row of the next partition
  IndividualRowWriterV w(output_rows);
  w.set_self_task_id(verify_set->get_self_task_id());
  IndividualRowReaderV r(input_rows, verify_set);
  AggregatorType prev_last_agg, cur_last_agg;
  NewRecord cur_first_row, next_first_row;
  uint32_t prev_last_agg_offset = 0, cur_last_agg_offset = 0;
  AggregatorType output;
  for (uint32_t i = 0; i < num_rows; i++) {
    // Populate the prev, cur, next variables to enable lookahead and lookbehind
    if (i > 0) prev_last_agg.set(&cur_last_agg);
    if (i > 0) prev_last_agg_offset = cur_last_agg_offset;
    if (i == 0) r.read(&cur_first_row); else cur_first_row.set(&next_first_row);
    r.read(&cur_last_agg);
    if (i < num_rows - 1) r.read(&next_first_row);

    // Augment cur_last_agg with previous runs (b)
    if (i > 0 && prev_last_agg.grouping_attrs_equal(&cur_last_agg)) {
      // The same value in the group by column spans multiple machines
      cur_last_agg.aggregate(&prev_last_agg);
    }

    // Calculate cur_last_agg_offset, compensating for runs that span partition boundaries (c)
    cur_last_agg_offset += cur_last_agg.get_num_distinct();
    if (i > 0 && prev_last_agg.grouping_attrs_equal(&cur_first_row)) {
      // The current partition begins with a run of items continued from the previous partition,
      // so we shouldn't double count it
      cur_last_agg_offset--;
    }

    // Send the results to the current partition
    if (i > 0) output.set(&prev_last_agg);
    if (i > 0) output.set_offset(prev_last_agg_offset);
    output.set_num_distinct(num_distinct);
    w.write(&output);
    if (i < num_rows - 1) {
      w.write(&next_first_row);
    } else {
      // The final partition has no next partition, so we send it a dummy row instead
      cur_first_row.mark_dummy();
      w.write(&cur_first_row);
    }
  }

  w.close();
  *actual_output_rows_length = w.bytes_written();
  *num_distinct_groups = num_distinct;
}

template<typename AggregatorType>
void aggregate_step2(Verify *verify_set,
                     uint8_t *input_rows, uint32_t input_rows_length,
                     uint32_t num_rows,
                     uint8_t *boundary_info_rows, uint32_t boundary_info_rows_length,
                     uint8_t *output_rows, uint32_t output_rows_length,
                     uint32_t *actual_size) {
  (void)boundary_info_rows_length;
  (void)output_rows_length;

  RowReader r(input_rows, input_rows + input_rows_length, verify_set);
  RowWriter w(output_rows);
  w.set_self_task_id(verify_set->get_self_task_id());
  NewRecord cur, next;
  AggregatorType a;

  IndividualRowReaderV boundary_info_reader(boundary_info_rows, verify_set);
  AggregatorType boundary_info;
  NewRecord next_partition_first_row;
  boundary_info_reader.read(&boundary_info);
  boundary_info_reader.read(&next_partition_first_row);

  // Use the last row partial aggregate from the previous partition as the initial aggregate for
  // this partition
  a.set(&boundary_info);

  for (uint32_t i = 0; i < num_rows; i++) {
    // Populate cur and next to enable lookahead
    if (i == 0) r.read(&cur); else cur.set(&next);
    if (i < num_rows - 1) r.read(&next); else next.set(&next_partition_first_row);

    a.aggregate(&cur);

    // The current aggregate is final if it is the last aggregate for its run
    bool a_is_final = !a.grouping_attrs_equal(&next);

    cur.clear();
    a.append_result(&cur, !a_is_final);
    w.write(&cur);
  }

  w.close();
  *actual_size = w.bytes_written();
}


template<typename AggregatorType>
void aggregate_step2_low_cardinality(Verify *verify_set,
                     uint8_t *input_rows, uint32_t input_rows_length,
                     uint32_t num_rows,
                     uint8_t *boundary_info_rows, uint32_t boundary_info_rows_length,
                     uint32_t s, // s is number of paritions in next step
                     uint8_t *output_rows, uint32_t output_rows_length,
                     uint32_t *actual_size) {
  (void)boundary_info_rows_length;
  (void)output_rows_length;


  RowReader r(input_rows, input_rows + input_rows_length, verify_set);
  IndividualRowWriterV w(output_rows);
  w.set_self_task_id(verify_set->get_self_task_id());
  NewRecord cur, next;
  AggregatorType a;

  IndividualRowReaderV boundary_info_reader(boundary_info_rows, verify_set);
  AggregatorType boundary_info;
  NewRecord next_partition_first_row;
  boundary_info_reader.read(&boundary_info);
  boundary_info_reader.read(&next_partition_first_row);

  int num_distinct = boundary_info.get_num_distinct();
  int aggregates_per_pass; // TODO Karthik: how many partial aggregates we can fit into memory
  int num_passes = num_distinct % aggregates_per_pass ? num_distinct / aggregates_per_pass + 1 : num_distinct / aggregates_per_pass;
  // So this would be a buffer that fits into the EPC and we do multiple passes with this?
  NewRecord *agg_buf = malloc(sizeof(NewRecord) * aggregates_per_pass);



  for (int i = 0; i < num_passes; i++) {

    // fill the buffer with dummies
    for (int j = 0; j < num_distinct_groups; j++) {
      a.append_result(&agg_buf[j], true);
    }

    // aggregate the input block
    for (uint32_t j = 0; j < num_rows; j++) {

      // Populate cur and next to enable lookahead
      if (j == 0) r.read(&cur); else cur.set(&next);
      if (j < num_rows - 1) r.read(&next); else next.set(&next_partition_first_row);

      a.aggregate(&cur);

      // The current aggregate is final if it is the last aggregate for its run
      bool a_is_final = !a.grouping_attrs_equal(&next);

      // karthik: only write one partial aggregate per distinct group
      if (a_is_final || j == num_rows - 1) {
        int agg_buf_index = (boundary_info.get_offset() + a.get_num_distinct()) - (i * aggregates_per_pass);
        
        // check if this group belongs in the buffer for this pass
        if (0 <= agg_buf_index && agg_buf_index < aggregates_per_pass) {
          // can the OS see these accesses?
          a.append_result(&agg_buf[agg_buf_index], false);
        }   
      }

    }

    // write to the buffer for this pass. The second condition is to account for the tail case on the last pass
    for (int j = 0; j < aggregates_per_pass && (j + (i*aggregates_per_pass)) < num_distinct_groups; j++) {
      w.write(&agg_buf[j]);
    }
  }

  w.close();
  *actual_size = w.bytes_written();
}

// // karthik: this function should take the partial aggregates from each partition and aggregate them together
// // num_rows should be equal to num_distinct_groups*num_partitions
// // Assumptions:
// // row i*num_distinct_groups + j is partition i's partial aggregate for group j
// // input_rows should be the outputs of all the "aggregate_step2_low_cardinality" calls concatenated together
// template<typename AggregatorType>
// void aggregate_process_boundaries2_low_cardinality(Verify *verify_set,
//                                   uint8_t *input_rows, uint32_t input_rows_length,
//                                   uint32_t num_rows,
//                                   uint8_t *output_rows, uint32_t output_rows_length,
//                                   uint32_t num_distinct_groups, // number of groups for this partition to aggregate
//                                   uint32_t *actual_output_rows_length) {

//   // allocate an aggregator for each group
//   // TODO karthik: Should I try to avoid this malloc somehow?
//   AggregatorType *aggregates = malloc(sizeof(AggregatorType) * num_distinct_groups);

//   for (int i = 0; i < num_distinct_groups; i++) {
//     aggregates[i] = AggregatorType();
//   }

//   RowReader r(input_rows, input_rows + input_rows_length, verify_set);
//   RowWriter w(output_rows);
//   // assumes num_rows = num_partitions * num_distinct_groups
//   for (int i = 0; i < num_rows; i++) {

//     // index is the index of the group the next row corresponds to
//     int index = i % num_distinct_groups;

//     NewRecord cur;
//     r.read(&cur);

//     if (!cur.is_dummy()) {
//       aggregates[index].aggregate(&cur);     
//     }

//   }
//   for (int i = 0; i < num_distinct_groups; i++) {
//     NewRecord cur;
//     aggregates[i].append_result(&cur, false);
//     w.write(&cur);
//   }

//   w.close();
//   *actual_size = w.bytes_written();
// }

// Very simple final aggregation step.
// Input should be all the partial aggregates from step 2 for ONE group
// Outputs ONE final aggregate
template<typename AggregatorType>
void aggregate_final_low_cardinality(Verify *verify_set,
                                  uint8_t *input_rows, uint32_t input_rows_length,
                                  uint32_t num_rows,
                                  uint8_t *output_rows, uint32_t output_rows_length,
                                  uint32_t *actual_output_rows_length) {
  AggregatorType res = AggregatorType();
  IndividualRowReaderV r(input_rows, input_rows + input_rows_length, verify_set);
  IndividualRowWriterV w(output_rows);
  for (int i = 0; i < num_rows; i++) {
    NewRecord cur;
    r.read(&cur);
    res.aggregate(&cur);
  }
  NewRecord out;
  res.append_result(&cur, false);
  w.write(&cur);
  w.close();
  *actual_output_rows_length = w.bytes_written();
}


// non-oblivious aggregation
// a single scan, assume that external_sort is already called
template<typename AggregatorType>
void non_oblivious_aggregate(Verify *verify_set,
                             uint8_t *input_rows, uint32_t input_rows_length,
                             uint32_t num_rows,
                             uint8_t *output_rows, uint32_t output_rows_length,
                             uint32_t *actual_output_rows_length, uint32_t *num_output_rows) {

  (void)output_rows_length;
  
  RowReader reader(input_rows, input_rows + input_rows_length, verify_set);
  RowWriter writer(output_rows);
  writer.set_self_task_id(verify_set->get_self_task_id());

  NewRecord prev_row, cur_row, output_row;
  AggregatorType agg;

  uint32_t num_output_rows_result = 0;
  for (uint32_t i = 0; i < num_rows; i++) {
    if (i == 0) {
      reader.read(&prev_row);
      continue;
    }

    agg.aggregate(&prev_row);
    reader.read(&cur_row);

    if (!agg.grouping_attrs_equal(&cur_row)) {
      output_row.clear();
      agg.append_result(&output_row, false);
      writer.write(&output_row);
      num_output_rows_result++;
    }

    if (i == num_rows - 1) {
      agg.aggregate(&cur_row);

      output_row.clear();
      agg.append_result(&output_row, false);
      writer.write(&output_row);
      num_output_rows_result++;
    }

    prev_row.set(&cur_row);
  }

  if (num_rows == 1) {
    agg.aggregate(&prev_row);
    output_row.clear();
    agg.append_result(&output_row, false);
    writer.write(&output_row);
    num_output_rows_result++;
  }

  writer.close();
  *actual_output_rows_length = writer.bytes_written();
  *num_output_rows = num_output_rows_result;
}
