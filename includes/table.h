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
 * table.h
 *
 * Information Systems Group
 * Saarland University
 * 2012 - 2013
 */

#ifndef TABLE_H_
#define TABLE_H_

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
#include "utils.h"

typedef internal_int_t cell_t;
typedef cell_t* column_t;
typedef column_t* columns_t;

typedef struct table {

	bool initialized;
	internal_int_t numRows;
	internal_int_t numCols;
	columns_t cells;				// 2D array to represent the table cells (cells[column][row])
	bool nearbyClustered;
	internal_int_t seed;
} table_t;

/*
 * 	Initializes the table memory
 */
table_t create_empty_table(internal_int_t numRows, internal_int_t numCols, internal_int_t seed);

/*
 * 	Frees all memory held by the table structure
 */
void destroy_table(table_t* table);

/*
 * 	Copies the data from the src to the dest table.
 *  The dest table must have been allocated already.
 */
void copy_table(table_t* dest, table_t* src);

/*
 * 	Fills the table with random data within the given ranges.
 * 	As usual, the lower bounds are inclusive, the upper bounds not
 */
void fill_table_with_random_data_in_range(table_t* table,
												internal_int_t maxKey,
												internal_int_t lowerboundCol,
												internal_int_t upperboundCol,
												internal_int_t lowerboundRow,
												internal_int_t upperboundRow);

/*
 * 	Fills the entire table with random data
 */
void fill_table_with_random_data(table_t* table, internal_int_t maxKey);

/*
 *	Fills the table with random data for all columns except of the first one, which is
 *	filled with the data of the index column
 */
void fill_table_with_random_data_based_on_index(table_t* table, internal_int_t maxKey, IndexEntry* index);

/*
 * 	Prints the entire table.
 */
void print_table(table_t* table);

void allocation_sanity_check(void* mem);

#endif /* TABLE_H_ */
