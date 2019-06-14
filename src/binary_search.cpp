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
 * binary_search.cpp
 *
 * Information Systems Group
 * Saarland University
 * 2012 - 2013
 */

#include "../includes/utils.h"
#include "../includes/sorting.h"
#include "../includes/print.h"
#include "../includes/binary_search.h"

void *build_binary_tree(IndexEntry *c, internal_int_t n)
{
    // C is already sorted, so just return it.
    return c;
}

internal_int_t binary_search(IndexEntry *c, internal_int_t key, internal_int_t lower, internal_int_t upper, bool* foundKey ){

    *foundKey = false;
    // binary search: iterative version with early termination
    while(lower <= upper) {
        internal_int_t middle = (lower + upper) / 2;
        IndexEntry middleElement = c[middle];

        if(middleElement < key) {
        	lower = middle + 1;
        }
        else if(middleElement > key) {
       		upper = middle - 1;
        }
        else {
        	*foundKey = true;
            return middle;
    	}
    }
    return upper;
}

internal_int_t binary_search_lt(IndexEntry *c, internal_int_t key, internal_int_t start, internal_int_t end){
    bool found = false;
    internal_int_t pos = binary_search(c, key, start, end, &found);
    if(found)
    {
        // The element in "c[pos] < key". So iterating until finding the right pos.
        while(--pos >= start && c[pos] == key);
    }
    // If not found then pos points either to the end or 
    // to the last element which which satisfies the c[pos] < key
    return pos;
}

internal_int_t binary_search_gte(IndexEntry *c, internal_int_t key, internal_int_t start, internal_int_t end){
	bool found = false;
    internal_int_t pos = binary_search(c, key, start, end, &found);
    if(found)
    {
        // The element in c[pos] >= key.
        while(--pos >= start && c[pos] == key);
    }
    // To get to the first c[pos] == key element.
    ++pos;
    return pos;
}


void verify(IndexEntry expected, IndexEntry received)
{
	debug(INFO, "expected=""%lld"", received=""%lld""\n", expected, received);
    if(expected != received)
        printError("Results do not match. Expected:""%lld"", but received:""%lld""\n", expected, received);
}
