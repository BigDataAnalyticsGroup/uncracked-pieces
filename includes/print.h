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
 * print.h
 *
 * Information Systems Group
 * Saarland University
 * 2012 - 2013
 */

#ifndef PRINT_H_
#define PRINT_H_


#include <stdio.h>
#include <string>
#include "experiment.h"
#include "avltree.h"


// printing utilities (print.c)

typedef enum debug_level {INFO, WARNING, ERROR} DEBUG_LEVEL;
typedef enum print_level {STDOUT, FILEOUT} PRINT_LEVEL;


#ifdef DEBUG
	#define debug(level, format, ...) _debug(level, format, __VA_ARGS__)
	#define print(format, ...) _print(STDOUT, format, __VA_ARGS__)
	#define printArray(c, n) _printArray(c, n)
	#define printIndexArray(c, n) _printIndexArray(c, n)
	#define printAVLTree(T) _printAVLTree(T)
#else
	#define debug(format, ...)
	#define print(format, ...) _print(FILEOUT, format, __VA_ARGS__)
	#define printArray(c, n)
	#define printIndexArray(c, n)
	#define printIndexArrayBuckets(c, n)
	#define printCompressedIndexArray(c, n)
	#define printAVLTree(T)
#endif


void _debug(DEBUG_LEVEL level, const char* format, ...);
void _print(PRINT_LEVEL level, const char* format, ...);
void _printArray(internal_int_t *c, internal_int_t n);
void _printIndexArray(IndexEntry *c, internal_int_t n);
void _printAVLTree(AvlTree T);

void printError(const char* format, ...);
void printCrackResult(Result r);
void printTotalCrackResult(Result r);
void printChunkIndexingResult(Result r);


#endif /* PRINT_H_ */
