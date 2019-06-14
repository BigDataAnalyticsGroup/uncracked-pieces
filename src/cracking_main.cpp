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
 * cracking_main.cpp
 *
 * Information Systems Group
 * Saarland University
 * 2012 - 2013
 */


#include "../includes/exp_util.h"
#include "../includes/experiment.h"

#include "../includes/sorting.h"
#include "../includes/index.h"
#include "../includes/print.h"
#include "../includes/cracking.h"
#include "../includes/timer.h"
#include "../includes/gheap/gheap.h"

extern INDEX_RUNS* index_runs;

BurstResult compute_average(BurstResult* intermediate_results) {

	BurstResult average_results = (BurstResult) malloc(sizeof(Result) * NUM_QUERIES);
	internal_int_t i,j;

	for(i=0;i<NUM_QUERIES;++i) {
		Result average_result = get_empty_result(intermediate_results[0][0]->name,
													intermediate_results[0][0]->num_proj,
													intermediate_results[0][0]->num_cover,
													intermediate_results[0][0]->cover_type);

		for(j=0;j<NUMBER_OF_REPETITIONS;++j) {

			add_time(average_result->ct, intermediate_results[j][i]->ct);
			add_time(average_result->c_il, intermediate_results[j][i]->c_il);
			add_time(average_result->c_do, intermediate_results[j][i]->c_do);
			add_time(average_result->c_iu, intermediate_results[j][i]->c_iu);
			add_time(average_result->c_ip, intermediate_results[j][i]->c_ip);

			add_time(average_result->qt, intermediate_results[j][i]->qt);
			add_time(average_result->q_il, intermediate_results[j][i]->q_il);
			add_time(average_result->q_pf, intermediate_results[j][i]->q_pf);
			add_time(average_result->q_da, intermediate_results[j][i]->q_da);

			average_result->sc += intermediate_results[j][i]->sc;
			average_result->t_sc += intermediate_results[j][i]->t_sc;

			average_result->cc += intermediate_results[j][i]->cc;
			average_result->t_cc += intermediate_results[j][i]->t_cc;

			*average_result->qo = *intermediate_results[j][i]->qo;

			average_result->sortedness += intermediate_results[j][i]->sortedness;

			average_result->cracked = intermediate_results[j][i]->cracked;
		}

		compute_average_time(average_result->ct, NUMBER_OF_REPETITIONS);
		compute_average_time(average_result->c_il, NUMBER_OF_REPETITIONS);
		compute_average_time(average_result->c_do, NUMBER_OF_REPETITIONS);
		compute_average_time(average_result->c_iu, NUMBER_OF_REPETITIONS);
		compute_average_time(average_result->c_ip, NUMBER_OF_REPETITIONS);

		compute_average_time(average_result->qt, NUMBER_OF_REPETITIONS);
		compute_average_time(average_result->q_il, NUMBER_OF_REPETITIONS);
		compute_average_time(average_result->q_pf, NUMBER_OF_REPETITIONS);
		compute_average_time(average_result->q_da, NUMBER_OF_REPETITIONS);

		average_result->sc /= NUMBER_OF_REPETITIONS;
		average_result->t_sc /= NUMBER_OF_REPETITIONS;

		average_result->cc /= NUMBER_OF_REPETITIONS;
		average_result->t_cc /= NUMBER_OF_REPETITIONS;

		average_result->sortedness /= NUMBER_OF_REPETITIONS;


		average_results[i] = get_copy_of_result(average_result);
	}

	return average_results;
}

void addToResultBurst(BurstResult dest, BurstResult src) {
	internal_int_t i;

	for(i=0;i<NUM_QUERIES;++i) {
		add_time(dest[i]->ct, src[i]->ct);
		add_time(dest[i]->c_il, src[i]->c_il);
		add_time(dest[i]->c_do, src[i]->c_do);
		add_time(dest[i]->c_iu, src[i]->c_iu);
		add_time(dest[i]->c_ip, src[i]->c_ip);

		add_time(dest[i]->qt, src[i]->qt);
		add_time(dest[i]->q_il, src[i]->q_il);
		add_time(dest[i]->q_pf, src[i]->q_pf);
		add_time(dest[i]->q_da, src[i]->q_da);

		dest[i]->sc += src[i]->sc;
		dest[i]->t_sc += src[i]->t_sc;

		dest[i]->qo->sum += src[i]->qo->sum;
		dest[i]->qo->unfiltered_sum += src[i]->qo->unfiltered_sum;

		dest[i]->sortedness += src[i]->sortedness;
	}
}


void run_cracking3_sideways(
		internal_int_t n,
		internal_int_t w,
		internal_int_t *crack_variants,
		internal_int_t num_crack_variants,
		internal_int_t *sort_variants,
		internal_int_t num_sort_variants,
		internal_int_t *sort_algos,
		internal_int_t num_sort_algos,
		internal_int_t numberOfProjectedAttributes,
		internal_int_t *covering_types,
		internal_int_t num_covering_types,
		float selectivity
		)
{
	SETUP s = get_setup(n, w);
	prepare_queries3_with_projection(s, MAXKEY, numberOfProjectedAttributes, selectivity);
	internal_int_t i,j,k,l,m,result_iteration=0;

	IndexEntry* data = (IndexEntry*) malloc_huge(DATA_SIZE*INDEX_ENTRY_SIZE);
	generateRandomIndexData(data, DATA_SIZE, MAXKEY, true);

	Result crack_results[num_crack_variants*num_covering_types];
	for(i=0;i<num_crack_variants;i++){
		for(l=0;l<num_covering_types;++l) {
			BurstResult intermediate_crack_results[NUMBER_OF_REPETITIONS];
			for(k=0;k<NUMBER_OF_REPETITIONS;k++) {
				prepare_data(s, DATA_SIZE, MAXKEY, true);
				BurstResult result_per_index[NUMBER_OF_INDEXES];
				//if(!index_runs)
				//	create_index_runs(s->index_data, RUN_SIZE);
				if(crack_variants[i] == EXP_SCAN || covering_types[l] == NO_COVER) {
					sideways_data_t sideways_data = {false, -1};
					result_per_index[0] = CRACK3_EXPERIMENT[crack_variants[i]](s->index_data[0], &s->table, s->queriesL, s->queriesH, s->numberOfProjectedAttributes, covering_types[l], sideways_data);
				}
				else {
					for(m=0;m<NUMBER_OF_INDEXES;++m) {
						sideways_data_t sideways_data = {true, m+1};	// pass index of covered attribute in source table
						result_per_index[m] = CRACK3_EXPERIMENT[crack_variants[i]](s->index_data[m], &s->table, s->queriesL, s->queriesH, s->numberOfProjectedAttributes, covering_types[l], sideways_data);
						if(m>0) {
							addToResultBurst(result_per_index[0], result_per_index[m]);
						}
					}
				}
				intermediate_crack_results[k] = result_per_index[0];
			}

			BurstResult average = compute_average(intermediate_crack_results);
			print_burst(average);

			crack_results[result_iteration] = average[NUM_QUERIES-1];

			if((result_iteration > 0) && !verifyResults(crack_results[result_iteration], crack_results[result_iteration-1]))
				printError("Results among cracking methods do not match!");

			++result_iteration;
		}
	}


	result_iteration = 0;
	Result sort_results[num_sort_variants*num_sort_algos*num_covering_types];

	for(i=0;i<num_sort_variants;i++){
		for(j=0;j<num_sort_algos;j++){
			for(l=0;l<num_covering_types;++l){
				BurstResult intermediate_sort_results[NUMBER_OF_REPETITIONS];
				for(k=0;k<NUMBER_OF_REPETITIONS;++k) {
					prepare_data(s, DATA_SIZE, MAXKEY, true);
					BurstResult result_per_index[NUMBER_OF_INDEXES];
					if(!index_runs)
						create_index_runs(s->index_data, RUN_SIZE);
					if(crack_variants[i] == EXP_SCAN || covering_types[l] == NO_COVER) {
						sideways_data_t sideways_data = {false, -1};
						result_per_index[0] = SORT3_EXPERIMENT[sort_variants[i]](s->index_data[0], &s->table, s->queriesL, s->queriesH, s->numberOfProjectedAttributes, covering_types[l], sideways_data, SORT[sort_algos[j]]);
					}
					else {
						for(m=0;m<NUMBER_OF_INDEXES;++m) {
							sideways_data_t sideways_data = {true, m+1};	// pass index of covered attribute in source table
							result_per_index[m] = SORT3_EXPERIMENT[sort_variants[i]](s->index_data[m], &s->table, s->queriesL, s->queriesH, s->numberOfProjectedAttributes, covering_types[l], sideways_data, SORT[sort_algos[j]]);
							if(m>0) {
								addToResultBurst(result_per_index[0], result_per_index[m]);
							}
						}
					}
					intermediate_sort_results[k] = result_per_index[0];
				}

				BurstResult average = compute_average(intermediate_sort_results);
				print_burst(average);

				sort_results[result_iteration] = average[NUM_QUERIES-1];
				if((result_iteration > 0) && !verifyResults(sort_results[result_iteration], sort_results[result_iteration-1]))
					printError("Results do not match");

				++result_iteration;
			}
		}
	}

	result_iteration = 0;
	for(i=0;i<num_crack_variants;i++) {
		for(l=0;l<num_covering_types;++l) {
			printTotalCrackResult(crack_results[result_iteration++]);
		}
	}

	result_iteration = 0;
	for(i=0;i<num_sort_variants;i++) {
		for(j=0;j<num_sort_algos;j++) {
			for(l=0;l<num_covering_types;++l) {
				printTotalCrackResult(sort_results[result_iteration++]);
			}
		}
	}

	free_setup(s);

	printf("Outputs OK!\n");
}


void run_cracking3(
		internal_int_t n,
		internal_int_t w,
		internal_int_t *crack_variants,
		internal_int_t num_crack_variants,
		internal_int_t *sort_variants,
		internal_int_t num_sort_variants,
		internal_int_t *sort_algos,
		internal_int_t num_sort_algos,
		internal_int_t numberOfProjectedAttributes,
		internal_int_t *covering_types,
		internal_int_t num_covering_types,
		float selectivity
		)
{
	SETUP s = get_setup(n, w);
	prepare_queries3_with_projection(s, MAXKEY, numberOfProjectedAttributes, selectivity);
	sideways_data_t sideways_data = {false, -1};
	internal_int_t i,j,k,l,result_iteration=0;

	Result crack_results[num_crack_variants*num_covering_types];
	for(i=0;i<num_crack_variants;i++){
		for(l=0;l<num_covering_types;++l) {
			BurstResult intermediate_crack_results[NUMBER_OF_REPETITIONS];
			for(k=0;k<NUMBER_OF_REPETITIONS;k++) {
				printf("Processing iteration %lld ...\n", k);
				prepare_data(s, DATA_SIZE, MAXKEY, covering_types[l] != NO_COVER && covering_types[l] != NEARBY_CLUSTERING_EXACT);
				if(!index_runs)
					create_index_runs(s->index_data, RUN_SIZE);
				intermediate_crack_results[k] = CRACK3_EXPERIMENT[crack_variants[i]](s->index_data[0], &s->table, s->queriesL, s->queriesH, s->numberOfProjectedAttributes, covering_types[l], sideways_data);
			}

			BurstResult average = compute_average(intermediate_crack_results);
			print_burst(average);

			crack_results[result_iteration] = average[NUM_QUERIES-1];
			if(result_iteration > 0 && !verifyResults(crack_results[result_iteration],crack_results[result_iteration-1]))
				printError("Results do not match!");

			++result_iteration;
		}
	}

	result_iteration = 0;
	Result sort_results[num_sort_variants*num_sort_algos*num_covering_types];
	for(i=0;i<num_sort_variants;i++){
		for(j=0;j<num_sort_algos;j++){
			for(l=0;l<num_covering_types;++l) {
				BurstResult intermediate_sort_results[NUMBER_OF_REPETITIONS];
				for(k=0;k<NUMBER_OF_REPETITIONS;++k) {
					printf("Processing iteration %lld ...\n", k);
					prepare_data(s, DATA_SIZE, MAXKEY, covering_types[l] != NO_COVER && covering_types[l] != NEARBY_CLUSTERING_EXACT);
					if(!index_runs)
						create_index_runs(s->index_data, RUN_SIZE);
					intermediate_sort_results[k] = SORT3_EXPERIMENT[sort_variants[i]](s->index_data[0], &s->table, s->queriesL, s->queriesH, s->numberOfProjectedAttributes, covering_types[l], sideways_data, SORT[sort_algos[j]]);
				}

				BurstResult average = compute_average(intermediate_sort_results);
				print_burst(average);

				sort_results[result_iteration] = average[NUM_QUERIES-1];
				if((result_iteration > 0) && !verifyResults(sort_results[result_iteration], sort_results[result_iteration-1]))
					printError("Results do not match");

				++result_iteration;
			}
		}
	}

	result_iteration = 0;
	for(i=0;i<num_crack_variants;i++) {
		for(l=0;l<num_covering_types;++l) {
			printTotalCrackResult(crack_results[result_iteration++]);
		}
	}

	result_iteration = 0;
	for(i=0;i<num_sort_variants;i++) {
		for(j=0;j<num_sort_algos;j++) {
			for(l=0;l<num_covering_types;++l) {
				printTotalCrackResult(sort_results[result_iteration++]);
			}
		}
	}

	free_setup(s);

	printf("Outputs OK!\n");
}

int main (int argc, const char * argv[]) {

	initialize();

	// cracking types: 	EXP_CRACK_STD, EXP_CRACK_STC,
	//					EXP_CRACK_HCS, EXP_CRACK_HRS, EXP_CRACK_HSS,
	//					EXP_SCAN,
	//					EXP_CRACK_PARTITIONED_STANDARD,
	//					EXP_CRACK_BUFFERED
	internal_int_t crk_types[] = {EXP_CRACK_STD, EXP_SCAN, EXP_CRACK_STC};
	internal_int_t num_crk = 3;

	// index types: 	EXP_SORT_AVL, EXP_SORT_BIN, EXP_SORT_BPTREE (std::map in current version, can be replaced by stx::btree),
	// 					EXP_SORT_BPTREE_BULK, EXP_SORT_ART (ART works only with unique values)
	internal_int_t idx_types[] = {};
	internal_int_t num_idx = 0;

	// sort algorithms: QUICK, INSERT, MERGE, RADIX, QUICK_INSERT, RADIX_INSERT, RADIX_QUICK, RADIX_QUICK_INSERT
	internal_int_t srt_types[] = {};
	internal_int_t num_srt = 0;

	// covering types: NO_COVER, COVER_DIRECT, NEARBY_CLUSTERING_EXACT (only supported in combination with partitioned cracking)
	// please note that the covering methods are only supported in combination with EXP_CRACK_STD
	// please check utils.h when setting this to set the corresponding macros!
	internal_int_t covering_types[] = {NO_COVER};
	internal_int_t num_covering = 1;

	// projecting 1 attributes means accessing the index only
	// projecting n attributes means retrieving the first attribute from the index and the remaining n-1 from the table
	// the first column of the table is always the indexed column
	internal_int_t numberOfProjectedAttributes = 1;
	if(numberOfProjectedAttributes > NUMBER_OF_COLUMNS) {
		printError("Error: The table contains less columns than projected.");
	}

	run_cracking3(DATA_SIZE, NUM_QUERIES, crk_types, num_crk, idx_types, num_idx, srt_types, num_srt,
			numberOfProjectedAttributes, covering_types, num_covering, SELECTIVITY);

//	for(numberOfProjectedAttributes = 1; numberOfProjectedAttributes <= NUMBER_OF_COLUMNS; ++numberOfProjectedAttributes) {
//		run_cracking3(DATA_SIZE, NUM_QUERIES, crk_types, num_crk, idx_types, num_idx, srt_types, num_srt,
//				numberOfProjectedAttributes, covering_types, num_covering, SELECTIVITY);
//	}

//	run_cracking3_sideways(DATA_SIZE, NUM_QUERIES, crk_types, num_crk, idx_types, num_idx, srt_types, num_srt,
//					numberOfProjectedAttributes, covering_types, num_covering, SELECTIVITY);

//	for(numberOfProjectedAttributes = 1; numberOfProjectedAttributes <= NUMBER_OF_COLUMNS; ++numberOfProjectedAttributes) {
//		run_cracking3_sideways(DATA_SIZE, NUM_QUERIES, crk_types, num_crk, idx_types, num_idx, srt_types, num_srt,
//				numberOfProjectedAttributes, covering_types, num_covering);
//	}

	terminate();

	return 0;
}
