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
 * bptree_wrapper.cpp
 *
 * Information Systems Group
 * Saarland University
 * 2012 - 2013
 */

#include "../includes/index.h"
#include "../includes/bptree_wrapper.h"

void *build_bptree(IndexEntry *c, internal_int_t n)
{
	// assumes that the underlying data is already sorted by key
    bptree_t* bpt = new bptree_t;
    for(internal_int_t i = 0; i < n; ++i) {
        bpt->insert(std::make_pair(c[i].m_key, i));
    }
    return bpt;
}


IntPair bptreeSearchLT(BPTree t, internal_int_t key){
	// to get the first element smaller than the key
	// we ask for the lower bound to get the first entry
	// greater or equals than the key.
	// by decrementing this by 1, we get the last element
	// smaller than the key
	IntPair p = (IntPair) malloc(sizeof(struct int_pair));
	BPTreeIterator iter = t->lower_bound(key);
	p->first = -1;				// unused as we want only the last value of the range
	--iter;
	p->second = iter == t->end() ? 0 : iter->second;
	return p;
}

IntPair bptreeSearchGTE(BPTree t, internal_int_t key){
	// lower_bound
	IntPair p = (IntPair) malloc(sizeof(struct int_pair));
	BPTreeIterator iter = t->lower_bound(key);
	p->first = iter->second;
	p->second = -1;				// unused as we want only the first value of the range
	return p;
}
