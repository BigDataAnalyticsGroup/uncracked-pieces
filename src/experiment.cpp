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
 * experiment.cpp
 *
 * Information Systems Group
 * Saarland University
 * 2012 - 2013
 */

#include <stdio.h>
#include <stdlib.h>
#include "../includes/cracking.h"
#include "../includes/sorting.h"
#include "../includes/query.h"
#include "../includes/index.h"

INDEX_RUNS* index_runs;
extern bool partitioned;

BurstResult (*CRACK3_EXPERIMENT[])(IndexEntry*& data,
									table_t *table,
									internal_int_t *queriesL,
									internal_int_t *queriesH,
									internal_int_t numberOfProjectedAttributes,
									internal_int_t cover_type,
									sideways_data_t sideways_data) = {&standard_cracking3,
																			&stochastic_cracking3, &hybridcs_cracking3, &hybridrs_cracking3, &hybridss_cracking3,
																			&full_scan3,
																			&partitioned_cracking3_standard, &buffered_cracking3
																			};
BurstResult (*SORT3_EXPERIMENT[])(IndexEntry*& data,
									table_t *table,
									internal_int_t *queriesL,
									internal_int_t *queriesH,
									internal_int_t numberOfProjectedAttributes,
									internal_int_t cover_type,
									sideways_data_t sideways_data,
									void (*sort)(IndexEntry *c, internal_int_t n)) = {&avl_index3, &full_sort3, &bptree_index3, &bptree_bulk_index3, &art_index3};

Result get_empty_result(const char *name, const internal_int_t num_proj) {
	return get_empty_result(name, num_proj, 0, NO_COVER);
}

Result get_empty_result(const char *name, const internal_int_t num_proj, const internal_int_t num_cover, const internal_int_t cover_type){
	Result r = (Result) malloc(sizeof(struct QueryStats));
	r->name = name;

	r->ct = get_timer();
	r->c_il = get_timer();
	r->c_do = get_timer();
	r->c_iu = get_timer();
	r->c_ip = get_timer();

	r->qt = get_timer();
	r->q_il = get_timer();
	r->q_pf = get_timer();
	r->q_da = get_timer();

	r->sc = 0;
	r->t_sc = 0;
	r->cc = 0;
	r->t_cc = 0;
	r->num_proj = num_proj;
	r->num_cover = num_cover;
	r->cover_type = cover_type;

	r->qo = (QueryOutput*) malloc(sizeof(struct QueryOutput));

	t_reset(r->ct);
	t_reset(r->c_il);
	t_reset(r->c_do);
	t_reset(r->c_iu);
	t_reset(r->c_ip);

	t_reset(r->qt);
	t_reset(r->q_il);
	t_reset(r->q_pf);
	t_reset(r->q_da);

	r->qo->sum = 0;
	r->qo->unfiltered_sum = 0;

	r->sortedness = -1;

	r->cracked = true;

	return r;
}

Result get_copy_of_result(Result r) {
	Result new_r = get_empty_result(r->name, r->num_proj, r->num_cover, r->cover_type);

	copy_timer(new_r->ct, r->ct);
	copy_timer(new_r->c_il, r->c_il);
	copy_timer(new_r->c_do, r->c_do);
	copy_timer(new_r->c_iu, r->c_iu);
	copy_timer(new_r->c_ip, r->c_ip);

	copy_timer(new_r->qt, r->qt);
	copy_timer(new_r->q_il, r->q_il);
	copy_timer(new_r->q_pf, r->q_pf);
	copy_timer(new_r->q_da, r->q_da);

	new_r->sc = r->sc;				// swap count
	new_r->t_sc = r->t_sc;			// total swap count

	new_r->cc = r->cc;				// swap count
	new_r->t_cc = r->t_cc;			// total swap count

	new_r->num_proj = r->num_proj;
	new_r->num_cover = r->num_cover;
	new_r->cover_type = r->cover_type;

	*new_r->qo = *r->qo;

	new_r->sortedness = r->sortedness;
	new_r->cracked = r->cracked;

	return new_r;
}

BurstResult get_burst_result() {
	return (BurstResult) malloc(sizeof(Result) * NUM_QUERIES);
}

void print_burst(BurstResult burst) {
	internal_int_t i;
	for(i=0;i<NUM_QUERIES;++i) {
		printCrackResult(burst[i]);
	}
}

/*
 * Standard Cracking
 */
BurstResult standard_cracking3(IndexEntry*& data, table_t *table, internal_int_t *queriesL, internal_int_t *queriesH, internal_int_t numberOfProjectedAttributes, internal_int_t cover_type, sideways_data_t sideways_data){
	internal_int_t q;
	AvlTree T = MakeEmpty(NULL);

	BurstResult burstResult = get_burst_result();
	Result r = get_empty_result("SC", numberOfProjectedAttributes, NUMBER_OF_COVERED_ATTRIBUTES, cover_type);
	bptree_t* partitionBeginToIndex = NULL;
	internal_int_t* rowIdToPartition = NULL;
	sortedPartitions.clear();

	for(q=0;q<NUM_QUERIES;q++){

		// crack
		if(sideways_data.coveredAttribute < numberOfProjectedAttributes ||
				(sideways_data.coveredAttribute == numberOfProjectedAttributes && numberOfProjectedAttributes == 1)) {
			t_on(r->ct);
			T = CRACK_IN_THREE[STANDARD](data, DATA_SIZE, T, queriesL[q], queriesH[q], r, sideways_data, (COVERING_TYPE) cover_type, partitionBeginToIndex, rowIdToPartition);
			t_off(r->ct);

			// query
			t_on(r->qt);
			r->qo->sum += query3(T, data, table, queriesL[q], queriesH[q], numberOfProjectedAttributes, cover_type, sideways_data, DATA_SIZE-1, Q_AVL, r, rowIdToPartition);
			t_off(r->qt);

		}

		debug(INFO, "SUM=""%lld""",r->qo->sum);
		burstResult[q] = get_copy_of_result(r);
	}

	return burstResult;
}

BurstResult stochastic_cracking3(IndexEntry*& data, table_t *table, internal_int_t *queriesL, internal_int_t *queriesH, internal_int_t numberOfProjectedAttributes, internal_int_t cover_type, sideways_data_t sideways_data){
	internal_int_t q;
	AvlTree T = MakeEmpty(NULL);

	BurstResult burstResult = get_burst_result();
	Result r = get_empty_result("STC", numberOfProjectedAttributes, NUMBER_OF_COVERED_ATTRIBUTES, cover_type);
	bptree_t* partitionBeginToIndex = NULL;
	internal_int_t* rowIdToPartition = NULL;

	for(q=0;q<NUM_QUERIES;q++){
		r->qo->view1 = NULL;
		r->qo->view_size1 = 0;
		r->qo->view2 = NULL;
		r->qo->view_size2 = 0;
		r->qo->middlePart = NULL;
		r->qo->middlePart_size = 0;

		// crack
		t_on(r->ct);
		T = CRACK_IN_THREE[STOCHASTIC](data, DATA_SIZE, T, queriesL[q], queriesH[q], r, sideways_data, (COVERING_TYPE) cover_type, partitionBeginToIndex, rowIdToPartition);
		//T = crackInTwo(data, T, queries[q], STANDARD, r);
		t_off(r->ct);

		// query
		t_on(r->qt);
		if(r->qo->view1) {
			r->qo->sum += query3(T, r->qo->view1, table, queriesL[q], queriesH[q], numberOfProjectedAttributes, cover_type, sideways_data, r->qo->view_size1-1, VIEW, r, rowIdToPartition);
		}
		if(r->qo->middlePart_size > 0) {
			r->qo->sum += query3(T, r->qo->middlePart, table, queriesL[q], queriesH[q], numberOfProjectedAttributes, cover_type, sideways_data, r->qo->middlePart_size-1, VIEW, r, rowIdToPartition);
		}
		if(r->qo->view2) {
			r->qo->sum += query3(T, r->qo->view2, table, queriesL[q], queriesH[q], numberOfProjectedAttributes, cover_type, sideways_data, r->qo->view_size2-1, VIEW, r, rowIdToPartition);
		}
		t_off(r->qt);
		if(r->qo->view1) {
			free(r->qo->view1);
			r->qo->view1 = NULL;
		}
		if(r->qo->view2) {
			free(r->qo->view2);
			r->qo->view2 = NULL;
		}

		debug(INFO, "SUM=""%lld""",r->qo->sum);

		burstResult[q] = get_copy_of_result(r);
	}

	//_printAVLTree(T);

	return burstResult;
}


/*
 * Hybrid Cracking
 */
BurstResult hybridcs_cracking3(IndexEntry*& data, table_t *table, internal_int_t *queriesL, internal_int_t *queriesH, internal_int_t numberOfProjectedAttributes, internal_int_t cover_type, sideways_data_t sideways_data){
	internal_int_t q;

	BurstResult burstResult = get_burst_result();

	Result r = get_empty_result("HCS", numberOfProjectedAttributes, NUMBER_OF_COVERED_ATTRIBUTES, cover_type);
	bptree_t* partitionBeginToIndex = NULL;
	internal_int_t* rowIdToPartition = NULL;
	printIndexArray(data, DATA_SIZE);

	for(q=0;q<NUM_QUERIES;q++){
		t_reset(r->c_do);
		t_reset(r->c_il);
		t_reset(r->c_iu);
		t_reset(r->c_ip);

		// crack
		t_on(r->ct);
		CRACK_IN_THREE[HYBRID_CS](data, DATA_SIZE, NULL, queriesL[q], queriesH[q], r, sideways_data, (COVERING_TYPE) cover_type, partitionBeginToIndex, rowIdToPartition);
		//crackInTwo(data, NULL, queries[q], HYBRID, r);
		t_off(r->ct);

		// query
		t_on(r->qt);
		r->qo->sum += query3(NULL, data, table, queriesL[q], queriesH[q], numberOfProjectedAttributes, cover_type,
								sideways_data, index_runs[std::max((int)(sideways_data.coveredAttribute-1), 0)]->final_size-1, Q_HYBRID, r, rowIdToPartition);
		t_off(r->qt);

		debug(INFO, "SUM=""%lld""",r->qo->sum);

		burstResult[q] = get_copy_of_result(r);
	}

	return burstResult;
}

BurstResult hybridrs_cracking3(IndexEntry*& data, table_t *table, internal_int_t *queriesL, internal_int_t *queriesH, internal_int_t numberOfProjectedAttributes, internal_int_t cover_type, sideways_data_t sideways_data){
	internal_int_t q;

	BurstResult burstResult = get_burst_result();
	Result r = get_empty_result("HRS", numberOfProjectedAttributes, NUMBER_OF_COVERED_ATTRIBUTES, cover_type);
	bptree_t* partitionBeginToIndex = NULL;
	internal_int_t* rowIdToPartition = NULL;

	printIndexArray(data, DATA_SIZE);

	for(q=0;q<NUM_QUERIES;q++){

		// crack
		t_on(r->ct);
		CRACK_IN_THREE[HYBRID_RS](data, DATA_SIZE, NULL, queriesL[q], queriesH[q], r, sideways_data, (COVERING_TYPE) cover_type, partitionBeginToIndex, rowIdToPartition);
		//crackInTwo(data, NULL, queries[q], HYBRID, r);
		t_off(r->ct);

		// query
		t_on(r->qt);
		r->qo->sum += query3(NULL, data, table, queriesL[q], queriesH[q], numberOfProjectedAttributes, cover_type,
								sideways_data, index_runs[std::max((int)(sideways_data.coveredAttribute-1), 0)]->final_size-1, Q_HYBRID, r, rowIdToPartition);
		t_off(r->qt);

		debug(INFO, "SUM=""%lld""",r->qo->sum);

		burstResult[q] = get_copy_of_result(r);
	}

	return burstResult;
}

BurstResult hybridss_cracking3(IndexEntry*& data, table_t *table, internal_int_t *queriesL, internal_int_t *queriesH, internal_int_t numberOfProjectedAttributes, internal_int_t cover_type, sideways_data_t sideways_data){
	internal_int_t q;

	BurstResult burstResult = get_burst_result();
	Result r = get_empty_result("HSS", numberOfProjectedAttributes, NUMBER_OF_COVERED_ATTRIBUTES, cover_type);
	bptree_t* partitionBeginToIndex = NULL;
	internal_int_t* rowIdToPartition = NULL;

	printIndexArray(data, DATA_SIZE);

	for(q=0;q<NUM_QUERIES;q++){

		// crack
		t_on(r->ct);

		CRACK_IN_THREE[HYBRID_SS](data, DATA_SIZE, NULL, queriesL[q], queriesH[q], r, sideways_data, (COVERING_TYPE) cover_type, partitionBeginToIndex, rowIdToPartition);

		//crackInTwo(data, NULL, queries[q], HYBRID, r);
		t_off(r->ct);

		// query
		t_on(r->qt);
		r->qo->sum += query3(NULL, data, table, queriesL[q], queriesH[q], numberOfProjectedAttributes, cover_type,
								sideways_data, index_runs[std::max((int)(sideways_data.coveredAttribute-1), 0)]->final_size-1, Q_HYBRID, r, rowIdToPartition);
		t_off(r->qt);

		debug(INFO, "SUM=""%lld""",r->qo->sum);

		burstResult[q] = get_copy_of_result(r);
	}

	return burstResult;
}

/*
 * Buffered Cracking
 */

BurstResult buffered_cracking3(IndexEntry*& data, table_t *table, internal_int_t *queriesL, internal_int_t *queriesH, internal_int_t numberOfProjectedAttributes, internal_int_t cover_type, sideways_data_t sideways_data){
	internal_int_t q;
	AvlTree T = MakeEmpty(NULL);

	BurstResult burstResult = get_burst_result();
	Result r = get_empty_result("SCBUF", numberOfProjectedAttributes, NUMBER_OF_COVERED_ATTRIBUTES, cover_type);
	bptree_t* partitionBeginToIndex = NULL;
	internal_int_t* rowIdToPartition = NULL;

	for(q=0;q<NUM_QUERIES;q++){
		// crack
		if(sideways_data.coveredAttribute < numberOfProjectedAttributes ||
				(sideways_data.coveredAttribute == numberOfProjectedAttributes && numberOfProjectedAttributes == 1)) {

			if(q < BUFFERED_QUERIES) {
				t_on(r->ct);
				T = CRACK_IN_THREE[BUFFERED](data, DATA_SIZE, T, queriesL[q], queriesH[q], r, sideways_data, (COVERING_TYPE) cover_type, partitionBeginToIndex, rowIdToPartition);
				t_off(r->ct);
			}
			else {
				t_on(r->ct);
				T = CRACK_IN_THREE[STANDARD](data, DATA_SIZE, T, queriesL[q], queriesH[q], r, sideways_data, (COVERING_TYPE) cover_type, partitionBeginToIndex, rowIdToPartition);
				t_off(r->ct);
			}
		}

		// query
		t_on(r->qt);
		r->qo->sum += query3(T, data, table, queriesL[q], queriesH[q], numberOfProjectedAttributes, cover_type, sideways_data, DATA_SIZE-1, Q_AVL, r, rowIdToPartition);
		t_off(r->qt);

		debug(INFO, "SUM=""%lld""",r->qo->sum);
		burstResult[q] = get_copy_of_result(r);
	}

	return burstResult;
}

/**
 * Partitioned Cracking
 */
BurstResult partitioned_cracking3_standard(IndexEntry*& data, table_t *table, internal_int_t *queriesL, internal_int_t *queriesH, internal_int_t numberOfProjectedAttributes, internal_int_t cover_type, sideways_data_t sideways_data){
	internal_int_t q;
	AvlTree T = MakeEmpty(NULL);

	BurstResult burstResult = get_burst_result();
	Result r = get_empty_result("SC_PS", numberOfProjectedAttributes, NUMBER_OF_COVERED_ATTRIBUTES, cover_type);
	bptree_t* partitionBeginToIndex = NULL;
	internal_int_t* rowIdToPartition = NULL;

	partitioned = false;
	for(q=0;q<NUM_QUERIES;q++){

		// crack
		t_on(r->ct);
		T = CRACK_IN_THREE[PARTITIONED_STANDARD](data, DATA_SIZE, T, queriesL[q], queriesH[q], r, sideways_data, (COVERING_TYPE) cover_type, partitionBeginToIndex, rowIdToPartition);
		//T = crackInTwo(data, T, queries[q], STANDARD, r);
		t_off(r->ct);

		// query
		t_on(r->qt);
		r->qo->sum += query3(T, data, table, queriesL[q], queriesH[q], numberOfProjectedAttributes, cover_type, sideways_data, DATA_SIZE-1, Q_AVL, r, rowIdToPartition);
		t_off(r->qt);

		if(rowIdToPartition) {
			free(rowIdToPartition);
			rowIdToPartition = NULL;
		}

		debug(INFO, "SUM=""%lld""",r->qo->sum);

		burstResult[q] = get_copy_of_result(r);
	}

	return burstResult;
}

/**
 * Full Scan
 */
BurstResult full_scan3(IndexEntry*& data, table_t *table, internal_int_t *queriesL, internal_int_t *queriesH, internal_int_t numberOfProjectedAttributes, internal_int_t cover_type, sideways_data_t sideways_data){
	internal_int_t q;

	BurstResult burstResult = get_burst_result();
	Result r = get_empty_result("SN", numberOfProjectedAttributes, 0, NO_COVER);
	internal_int_t* rowIdToPartition = NULL;

	for(q=0;q<NUM_QUERIES;q++){

		// query
		t_on(r->qt);
		r->qo->sum += query3(NULL, data, table, queriesL[q], queriesH[q], numberOfProjectedAttributes, cover_type, sideways_data, DATA_SIZE-1, FILTER, r, rowIdToPartition);
		t_off(r->qt);

		burstResult[q] = get_copy_of_result(r);
	}

	return burstResult;
}

/**
 * Full Indexing
 */
BurstResult avl_index3(IndexEntry*& data, table_t *table, internal_int_t *queriesL, internal_int_t *queriesH, internal_int_t numberOfProjectedAttributes, internal_int_t cover_type, sideways_data_t sideways_data, void (*sort)(IndexEntry *c, internal_int_t n)){
	internal_int_t q;

	BurstResult burstResult = get_burst_result();
	Result r = get_empty_result("AI", numberOfProjectedAttributes, NUMBER_OF_COVERED_ATTRIBUTES, cover_type);
	internal_int_t* rowIdToPartition = NULL;

	t_on(r->ct);
	AvlTree T = (AvlTree)fullIndex(data, DATA_SIZE, sort, INDEX[AVL], r);
	t_off(r->ct);

	for(q=0;q<NUM_QUERIES;q++){

		// query
		t_on(r->qt);
		r->qo->sum += query3(T, data, table, queriesL[q], queriesH[q], numberOfProjectedAttributes, cover_type, sideways_data, DATA_SIZE-1, Q_AVL, r, rowIdToPartition);
		t_off(r->qt);

		debug(INFO, "SUM=""%lld""",r->qo->sum);

		burstResult[q] = get_copy_of_result(r);

		t_clear(r->ct);
	}

	return burstResult;
}

BurstResult art_index3(IndexEntry*& data, table_t *table, internal_int_t *queriesL, internal_int_t *queriesH, internal_int_t numberOfProjectedAttributes, internal_int_t cover_type, sideways_data_t sideways_data, void (*sort)(IndexEntry *c, internal_int_t n)){
	internal_int_t q;

	BurstResult burstResult = get_burst_result();
	Result r = get_empty_result("ART", numberOfProjectedAttributes, NUMBER_OF_COVERED_ATTRIBUTES, cover_type);
	internal_int_t* rowIdToPartition = NULL;

	t_on(r->ct);
	ARTTree T = (ARTTree)fullIndex(data, DATA_SIZE, sort, INDEX[ART], r);
	t_off(r->ct);

	for(q=0;q<NUM_QUERIES;q++){

		// query
		t_on(r->qt);
		r->qo->sum += query3(T, data, table, queriesL[q], queriesH[q], numberOfProjectedAttributes, cover_type, sideways_data, DATA_SIZE-1, Q_ART, r, rowIdToPartition);
		t_off(r->qt);

		debug(INFO, "SUM=""%lld""",r->qo->sum);

		burstResult[q] = get_copy_of_result(r);

		t_clear(r->ct);
	}

	return burstResult;
}

/**
 * 	BPTREE
 */
BurstResult bptree_index3(IndexEntry*& data, table_t *table, internal_int_t *queriesL, internal_int_t *queriesH, internal_int_t numberOfProjectedAttributes, internal_int_t cover_type, sideways_data_t sideways_data, void (*sort)(IndexEntry *c, internal_int_t n)){
	internal_int_t q;

	BurstResult burstResult = get_burst_result();
	Result r = get_empty_result("BP", numberOfProjectedAttributes, NUMBER_OF_COVERED_ATTRIBUTES, cover_type);
	internal_int_t* rowIdToPartition = NULL;

	t_on(r->ct);
	BPTree T = (BPTree)fullIndex(data, DATA_SIZE, sort, INDEX[BPTREE], r);
	t_off(r->ct);

	for(q=0;q<NUM_QUERIES;q++){

		// query
		t_on(r->qt);
		r->qo->sum += query3(T, data, table, queriesL[q], queriesH[q], numberOfProjectedAttributes, cover_type, sideways_data, DATA_SIZE-1, Q_BPTREE, r, rowIdToPartition);
		t_off(r->qt);

		debug(INFO, "SUM=""%lld""",r->qo->sum);

		burstResult[q] = get_copy_of_result(r);

		t_clear(r->ct);
	}

	T->clear();

	return burstResult;
}

BurstResult bptree_bulk_index3(IndexEntry*& data, table_t *table, internal_int_t *queriesL, internal_int_t *queriesH, internal_int_t numberOfProjectedAttributes, internal_int_t cover_type, sideways_data_t sideways_data, void (*sort)(IndexEntry *c, internal_int_t n)){
	internal_int_t q;

	BurstResult burstResult = get_burst_result();
	Result r = get_empty_result("BPBULK", numberOfProjectedAttributes, NUMBER_OF_COVERED_ATTRIBUTES, cover_type);
	internal_int_t* rowIdToPartition = NULL;

	t_on(r->ct);
	BulkBPTree* T = (BulkBPTree*) fullIndex(data, DATA_SIZE, sort, INDEX[BPTREE_BULK], r);
	t_off(r->ct);

	//T->printLeaves(data);

	for(q=0;q<NUM_QUERIES;q++){

		// query
		t_on(r->qt);
		r->qo->sum += query3(T, data, table, queriesL[q], queriesH[q], numberOfProjectedAttributes, cover_type, sideways_data, DATA_SIZE-1, Q_BPTREE_BULK, r, rowIdToPartition);
		t_off(r->qt);

		debug(INFO, "SUM=""%lld""",r->qo->sum);

		burstResult[q] = get_copy_of_result(r);

		t_clear(r->ct);
	}



	return burstResult;
}

/**
 * Full Sorting
 */
BurstResult full_sort3(IndexEntry*& data, table_t *table, internal_int_t *queriesL, internal_int_t *queriesH, internal_int_t numberOfProjectedAttributes, internal_int_t cover_type, sideways_data_t sideways_data, void (*sort)(IndexEntry *c, internal_int_t n)){
	internal_int_t q;

	BurstResult burstResult = get_burst_result();
	Result r = get_empty_result("BS", numberOfProjectedAttributes, NUMBER_OF_COVERED_ATTRIBUTES, cover_type);
	internal_int_t* rowIdToPartition = NULL;

	t_on(r->ct); t_on(r->c_do);
	sort(data, DATA_SIZE);
	t_off(r->c_do); t_off(r->ct);

	// Clustered Index
	if(cover_type == NEARBY_CLUSTERING_EXACT) {
		rowIdToPartition = (internal_int_t*) malloc(DATA_SIZE * INT_SIZE);
		for(internal_int_t i = 0; i < DATA_SIZE; ++i) {
			rowIdToPartition[data[i].m_rowId] = i;
			data[i].m_rowId = i;
		}
	}

	for(q=0;q<NUM_QUERIES;q++){

		// query
		t_on(r->qt);
		r->qo->sum += query3(NULL, data, table, queriesL[q], queriesH[q], numberOfProjectedAttributes, cover_type, sideways_data, DATA_SIZE-1, SEARCH, r, rowIdToPartition);
		t_off(r->qt);

		burstResult[q] = get_copy_of_result(r);

		t_clear(r->c_do); t_clear(r->ct);
	}

	return burstResult;
}

bool verifyPostFiltering(Result r){
	return  r->qo->unfiltered_sum != r->qo->sum;
}

bool verifyResults(Result r1, Result r2){
	printf("sum1=""%lld"", sum2=""%lld""\n\n", r1->qo->sum, r2->qo->sum);
	return  r1->qo->sum == r2->qo->sum;
}
