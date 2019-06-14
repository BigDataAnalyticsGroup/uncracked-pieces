/*
 * Copyright Information Systems Group, Saarland University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 * experiment.h
 *
 *  Created on: Aug 4, 2011
 *      Author: alekh
 */

#ifndef EXPERIMENT_H_
#define EXPERIMENT_H_

#include <fstream>
#include <string>
#include <sstream>
#include "table.h"
#include "utils.h"
#include "timer.h"

#define RANDOM_PATTERN 1
#define SEQUENTIAL_PATTERN 2
#define SKEWED_PATTERN 3

//#define DEBUG

// experimental setup
#ifdef DEBUG
 #define DATA_SIZE 10000000
 #define DUP_FACTOR 100
 #define NUM_QUERIES 1000
 #define QUERY_ACCESS_PATTERN RANDOM_PATTERN
 #define ZIPF_ALPHA 2.0
 #define SELECTIVITY 1
 #define RUN_SIZE 10
 #define NUMBER_OF_REPETITIONS 1
 #define NUMBER_OF_COLUMNS 1				// the number of columns the test table consists of
 #define NUMBER_OF_PARTITIONS 100 			// assumes that each partition covers a value range of equal size, i.e. (DATA_SIZE / DUP_FACTOR) % NUMBER_OF_PARTITIONS == 0
 #define BPTREE_ELEMENTSPERNODE  128
 #define HEAP_SIZE 10
 #define BUFFERED_QUERIES 100
 #define FIXED_SEED
 #define DATA_SEED 5345464646
 #define QUERY_SEED 2342342424
#else
 #define DATA_SIZE 100000000	// 100 million
 #define DUP_FACTOR 1000
 #define NUM_QUERIES 1000
 #define QUERY_ACCESS_PATTERN RANDOM_PATTERN
 #define ZIPF_ALPHA 2.0
 #define SELECTIVITY 1
 #define RUN_SIZE 10000
 #define NUMBER_OF_REPETITIONS 1   // each run is executed three times with the same seed and the average is taken
 #define NUMBER_OF_COLUMNS 1
 #define NUMBER_OF_PARTITIONS 100
 #define BPTREE_ELEMENTSPERNODE  16384
 #define HEAP_SIZE 1000000
 #define BUFFERED_QUERIES 1000
 #define FIXED_SEED					// guarantees that for each repetition, the same data and queryset is generated
 #define DATA_SEED 5345464646
 #define QUERY_SEED 2342342424
#endif

#define MAXKEY (DATA_SIZE / DUP_FACTOR)

struct QueryOutput{
	internal_int_t sum;					// stores the sum result
	internal_int_t unfiltered_sum;		// stores sum without post filtering

	// stochastic cracking
	IndexEntry *view1;					// stores a materialized view of the lower part
	internal_int_t view_size1;			// stores the size of the view of the lower part
	IndexEntry *middlePart;				// if there is a middle part, store the address
	internal_int_t middlePart_size;		// and the corresponding size here
	IndexEntry *view2;					// stores a materialized view of the upper part
	internal_int_t view_size2;			// stores the size of the view of the upper part
};

struct QueryStats{
	const char *name;
	Timer ct;					// cracking timer
	Timer c_il;					// cracking index lookup timer
	Timer c_do;					// cracking data organization timer
	Timer c_iu;					// cracking index update timer
	Timer c_ip;					// cracking index partitioning timer

	Timer qt;					// query timer
	Timer q_il;					// query index lookup timer
	Timer q_pf;					// query post filter timer
	Timer q_da;					// query data access timer

	Counter sc;					// swap count
	Counter t_sc;				// total swap count

	Counter cc;					// comparison count
	Counter t_cc;				// total comparison count

	internal_int_t num_proj;	// number of projected attributes
	internal_int_t num_cover;	// number of covered attributes
	internal_int_t cover_type;	// used method for covering

	internal_int_t sortedness;	// stores the sum over all distances between current and final position of element

	struct QueryOutput *qo;		// actual query output

	bool cracked;				// stores whether the query actually triggered a cracking or if the partition was to small to be cracked further.
};

typedef struct QueryStats *Result;
typedef Result* BurstResult;
Result get_empty_result(const char *name, const internal_int_t num_proj);
Result get_empty_result(const char *name, const internal_int_t num_proj, const internal_int_t num_cover, const internal_int_t cover_type);
Result get_copy_of_result(Result r);
BurstResult get_burst_result();

typedef enum crack_exp_type {EXP_CRACK_STD=0,
							EXP_CRACK_STC,
							EXP_CRACK_HCS, EXP_CRACK_HRS, EXP_CRACK_HSS,
							EXP_SCAN,
							EXP_CRACK_PARTITIONED_STANDARD,
							EXP_CRACK_BUFFERED
							} CRACK_EXP_TYPE;
typedef enum sort_exp_type {EXP_SORT_AVL=0, EXP_SORT_BIN, EXP_SORT_BPTREE, EXP_SORT_BPTREE_BULK, EXP_SORT_ART} SORT_EXP_TYPE;
typedef enum covering_type {NO_COVER=0, COVER_DIRECT, NEARBY_CLUSTERING_EXACT} COVERING_TYPE;

BurstResult standard_cracking3(IndexEntry*& data, table_t* table, internal_int_t *queriesL, internal_int_t *queriesH, internal_int_t numberOfProjectedAttributes, internal_int_t cover_type, sideways_data_t sideways_data);
BurstResult stochastic_cracking3(IndexEntry*& data, table_t* table, internal_int_t *queriesL, internal_int_t *queriesH, internal_int_t numberOfProjectedAttributes, internal_int_t cover_type, sideways_data_t sideways_data);
BurstResult hybridcs_cracking3(IndexEntry*& data, table_t* table, internal_int_t *queriesL, internal_int_t *queriesH, internal_int_t numberOfProjectedAttributes, internal_int_t cover_type, sideways_data_t sideways_data);
BurstResult hybridrs_cracking3(IndexEntry*& data, table_t* table, internal_int_t *queriesL, internal_int_t *queriesH, internal_int_t numberOfProjectedAttributes, internal_int_t cover_type, sideways_data_t sideways_data);
BurstResult hybridss_cracking3(IndexEntry*& data, table_t* table, internal_int_t *queriesL, internal_int_t *queriesH, internal_int_t numberOfProjectedAttributes, internal_int_t cover_type, sideways_data_t sideways_data);
BurstResult buffered_cracking3(IndexEntry*& data, table_t* table, internal_int_t *queriesL, internal_int_t *queriesH, internal_int_t numberOfProjectedAttributes, internal_int_t cover_type, sideways_data_t sideways_data);
BurstResult partitioned_cracking3_standard(IndexEntry*& data, table_t *table, internal_int_t *queriesL, internal_int_t *queriesH, internal_int_t numberOfProjectedAttributes, internal_int_t cover_type, sideways_data_t sideways_data);
BurstResult full_scan3(IndexEntry*& data, table_t* table, internal_int_t *queriesL, internal_int_t *queriesH, internal_int_t numberOfProjectedAttributes, internal_int_t cover_type, sideways_data_t sideways_data);
BurstResult avl_index3(IndexEntry*& data, table_t* table, internal_int_t *queriesL, internal_int_t *queriesH, internal_int_t numberOfProjectedAttributes, internal_int_t cover_type, sideways_data_t sideways_data, void (*sort)(IndexEntry *c, internal_int_t n));
BurstResult bptree_index3(IndexEntry*& data, table_t* table, internal_int_t *queriesL, internal_int_t *queriesH, internal_int_t numberOfProjectedAttributes, internal_int_t cover_type, sideways_data_t sideways_data, void (*sort)(IndexEntry *c, internal_int_t n));
BurstResult bptree_bulk_index3(IndexEntry*& data, table_t* table, internal_int_t *queriesL, internal_int_t *queriesH, internal_int_t numberOfProjectedAttributes, internal_int_t cover_type, sideways_data_t sideways_data, void (*sort)(IndexEntry *c, internal_int_t n));
BurstResult art_index3(IndexEntry*& data, table_t* table, internal_int_t *queriesL, internal_int_t *queriesH, internal_int_t numberOfProjectedAttributes, internal_int_t cover_type, sideways_data_t sideways_data, void (*sort)(IndexEntry *c, internal_int_t n));
BurstResult full_sort3(IndexEntry*& data, table_t* table, internal_int_t *queriesL, internal_int_t *queriesH, internal_int_t numberOfProjectedAttributes, internal_int_t cover_type, sideways_data_t sideways_data, void (*sort)(IndexEntry *c, internal_int_t n));


extern BurstResult (*CRACK3_EXPERIMENT[])(IndexEntry*& data,
											table_t* table,
											internal_int_t *queriesL,
											internal_int_t *queriesH,
											internal_int_t numberOfProjectedAttributes,
											internal_int_t cover_type,
											sideways_data_t sideways_data);
extern BurstResult (*SORT3_EXPERIMENT[])(IndexEntry*& data,
											table_t* table,
											internal_int_t *queriesL,
											internal_int_t *queriesH,
											internal_int_t numberOfProjectedAttributes,
											internal_int_t cover_type,
											sideways_data_t sideways_data,
											void (*sort)(IndexEntry *c, internal_int_t n));


bool verifyPostFiltering(Result r);
bool verifyResults(Result r1, Result r2);

void print_burst(BurstResult burst);

#endif /* EXPERIMENT_H_ */
