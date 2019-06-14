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
 * exp_util.cpp
 *
 * Information Systems Group
 * Saarland University
 * 2012 - 2013
 */

#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <signal.h>

#include "../includes/cracking.h"
#include "../includes/sorting.h"
#include "../includes/index.h"
#include "../includes/signals.h"
#include "../includes/print.h"

#include "../includes/exp_util.h"

const char *print_filename = "output.txt";

FILE *print_file;

void initialize(){
	#ifndef DEBUG
	print_file = fopen (print_filename,"w");
	if (print_file==NULL)
		printError("Failed to open print file!");
	else
		printf("Writing all out to file: %s\n",print_filename);
	#endif
	(void) signal(SIGINT,leave_graceful);
}

void terminate(){
	#ifndef DEBUG
	printf("closing output file\n");
	fclose(print_file);
	#endif
    printf("DONE!\n");
}

SETUP get_setup(internal_int_t data_size, internal_int_t wkld_size){
	SETUP s = (SETUP)malloc(sizeof(struct setup));

	s->index_data = (IndexEntry**)malloc(NUMBER_OF_INDEXES*sizeof(IndexEntry*));
	internal_int_t i;
	for(i = 0; i < NUMBER_OF_INDEXES; ++i) {
		s->index_data[i] = (IndexEntry*)malloc_huge(data_size*INDEX_ENTRY_SIZE);
	}
	s->queries = (internal_int_t *)malloc(wkld_size*INT_SIZE);
	s->queriesL = (internal_int_t *)malloc(wkld_size*INT_SIZE);
	s->queriesH = (internal_int_t *)malloc(wkld_size*INT_SIZE);

	s->table = create_empty_table(data_size, NUMBER_OF_COLUMNS, getSeed(DATA_SEED));
	return s;
}

void free_setup(SETUP s) {
	debug(INFO, "%s", "Deleting data ...");
	internal_int_t i;
	for(i = 0; i < NUMBER_OF_INDEXES; ++i) {
		if(s->index_data[i]) munmap(s->index_data[i], DATA_SIZE * INDEX_ENTRY_SIZE);
	}
	if(s->index_data) free(s->index_data);
	if(s->queries) free(s->queries);
	if(s->queriesL) free(s->queriesL);
	if(s->queriesH) free(s->queriesH);
	destroy_table(&s->table);
	//destroy_table(&s->table_copy);
	debug(INFO, "%s", "Deleted data!");
}

void prepare_data(SETUP s, internal_int_t size, internal_int_t maxKey, bool covering){
	generateRandomIndexData(s->index_data[0], size, maxKey, covering);
//	generateUniqueIndexData(s->index_data[0], size, maxKey, covering);

	internal_int_t i;
	for(i = 1; i < NUMBER_OF_INDEXES; ++i) {
		memcpy(s->index_data[i], s->index_data[0], size*INDEX_ENTRY_SIZE);
	}
	printIndexArray(s->index_data[0], size);

	fill_table_with_random_data_based_on_index(&s->table, maxKey, s->index_data[0]);

	// reset the data that is held to support the hybrid experiments
	reset_index_runs();
}

//void prepare_queries3(SETUP s, internal_int_t maxKey){
//	prepare_queries3_with_projection(s, maxKey, 1, SELECTIVITY);
//}

void prepare_queries3_with_projection(SETUP s, internal_int_t maxKey, internal_int_t numberOfProjectedAttributes, float selectivity){
	#if QUERY_ACCESS_PATTERN == RANDOM_PATTERN
		generate_random_queries_3(NUM_QUERIES, s->queriesL, s->queriesH, maxKey, selectivity);
	#elif QUERY_ACCESS_PATTERN == SEQUENTIAL_PATTERN
		generate_sequential_queries_3(NUM_QUERIES, s->queriesL, s->queriesH, maxKey, selectivity);
	#elif QUERY_ACCESS_PATTERN == SKEWED_PATTERN
		generate_skewed_queries_3(NUM_QUERIES, s->queriesL, s->queriesH, maxKey, selectivity);
	#endif

	s->numberOfProjectedAttributes = numberOfProjectedAttributes;
}

