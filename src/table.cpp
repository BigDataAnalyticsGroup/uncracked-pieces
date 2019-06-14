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
 * table.cpp
 *
 * Information Systems Group
 * Saarland University
 * 2012 - 2013
 */
#include "../includes/table.h"

void allocation_sanity_check(void* mem) {
	if(!mem) {
		printf("Error: Allocation not possible.");
		exit(1);
	}
}

table_t create_empty_table(internal_int_t numRows, internal_int_t numCols, internal_int_t seed) {
	table_t table;
	table.numRows = numRows;
	table.numCols = numCols;
	table.nearbyClustered = false;
	table.seed = seed;

	allocation_sanity_check(table.cells = (columns_t) malloc(sizeof(column_t) * numCols));
	internal_int_t i;
	for(i = 0; i < numCols; ++i) {
		allocation_sanity_check(table.cells[i] = (column_t) malloc(sizeof(cell_t) * numRows));
	}

	table.initialized = true;

	return table;
}

void destroy_table(table_t* table) {
	if(table->initialized) {
		internal_int_t i;
		for(i = 0; i < table->numCols; ++i) {
			if(table->cells[i]) {
				free(table->cells[i]);
				table->cells[i] = NULL;
			}
		}
		if(table->cells) {
			free(table->cells);
			table->cells = NULL;
		}
	}
}

void copy_table(table_t* dest, table_t* src) {
	internal_int_t i;
	for(i = 0; i < src->numCols; ++i) {
		memcpy(dest->cells[i], src->cells[i], sizeof(internal_int_t) * src->numRows);
	}
	dest->initialized = src->initialized;
	dest->seed = src->seed;
	dest->nearbyClustered = src->nearbyClustered;
}

void fill_table_with_random_data_in_range(table_t* table,
												internal_int_t maxKey,
												internal_int_t lowerboundCol,
												internal_int_t upperboundCol,
												internal_int_t lowerboundRow,
												internal_int_t upperboundRow) {

	if(lowerboundCol < 0 || lowerboundCol > table->numCols - 1  ||
			upperboundCol < 0 || upperboundCol > table->numCols ||
			lowerboundRow < 0 || lowerboundRow > table->numRows - 1 ||
			upperboundRow < 0 || upperboundRow > table->numRows) {
		printf("Error: Supplied ranges for table filling invalid.");
		exit(1);
	}

	internal_int_t i,j;

	srand(table->seed);

	for(i = lowerboundCol; i < upperboundCol; ++i) {
		for(j = lowerboundRow; j < upperboundRow; ++j) {
			table->cells[i][j] = randomNumber(maxKey);
		}
	}
}

void fill_table_with_random_data(table_t* table, internal_int_t maxKey) {
	fill_table_with_random_data_in_range(table, maxKey, 0, table->numCols, 0, table->numRows);
}

void fill_table_with_random_data_based_on_index(table_t* table, internal_int_t maxKey, IndexEntry* index) {
	// fill first column with index data
	internal_int_t i;
	for(i = 0; i < table->numRows; ++i) {
		table->cells[0][i] = index[i].m_key;
	}

	// fill the remaining columns with random data
	if(table->numCols > 1) {
		fill_table_with_random_data_in_range(table, maxKey, 1, table->numCols, 0, table->numRows);
	}
}

void print_table(table_t* table) {
	internal_int_t i,j;
	for(i = 0; i < table->numRows; ++i) {
		for(j = 0; j < table->numCols; ++j) {
			printf("%lld\t", table->cells[j][i]);
		}
		printf("\n");
	}
}










