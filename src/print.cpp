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
 * print.cpp
 *
 * Information Systems Group
 * Saarland University
 * 2012 - 2013
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include "../includes/cracking.h"

extern FILE *print_file;

void _debug(DEBUG_LEVEL level, const char* format, ...) {
	va_list args;

	switch(level){
	case INFO:		fprintf(stdout, "[INFO] ");
					va_start(args, format);
					vfprintf(stdout, format, args);
					va_end(args);
					fprintf(stdout, "\n");
					break;

	case WARNING:	fprintf(stderr, "[WARNING] ");
					va_start(args, format);
					vfprintf(stderr, format, args);
					va_end(args);
					fprintf(stderr, "\n");
					break;

	case ERROR:		fprintf(stderr, "[ERROR] ");
					va_start(args, format);
					vfprintf(stderr, format, args);
					va_end(args);
					fprintf(stderr, "\n");
					exit(EXIT_FAILURE);
					break;
	}
}

void _print(PRINT_LEVEL level, const char* format, ...){
	va_list args;

	switch(level){
	case STDOUT:	va_start(args, format);
					vfprintf(stdout, format, args);
					va_end(args);
					fprintf(stdout, "\n");
					break;

	case FILEOUT:	va_start(args, format);
					vfprintf(print_file, format, args);
					va_end(args);
					fprintf(print_file, "\n");
					fflush(print_file);
					break;
	}
}


void _printArray(internal_int_t *c, internal_int_t n){
	internal_int_t i;
	printf("[ARRAY] ");
	for(i=0;i<n;i++){
		printf("""%lld""=""%lld""", i,*(c++));
		if(i<n-1)
			printf(",");
	}
	printf("\n");
}

void _printIndexArray(IndexEntry *c, internal_int_t n){
	internal_int_t i;
	internal_int_t sum = 0;
	printf("[ARRAY] ");
	for(i=0;i<n;i++){
		sum += c->m_key;
		printf("""%lld""=""%lld""", i,(c++)->m_key);
		if(i<n-1)
			printf(",");
	}
	printf("\n");
	printf("[ARRAY SUM] = %lld\n", sum);
}

void _printAVLTree(AvlTree T){
	printf("[AVLTREE] ");
	Print(T);
	printf("\n");
}


void printError(const char* format, ...){
	va_list args;
	va_start(args, format);
	vfprintf(stderr, format, args);
	va_end(args);
	fprintf(stderr, "\n");
	exit(EXIT_FAILURE);
}

std::string getStringVersion(COVERING_TYPE type) {
	switch(type) {
		case NO_COVER: return "NO_COVER";
		case COVER_DIRECT: return "COVER_DIRECT";
		case NEARBY_CLUSTERING_EXACT: return "NEARBY_CLUSTERING";
		default: printError("Unknown covering type printed."); return "";
	}
}

// HEADER  QT  Q_IL  Q_DA 	Q_PF  CT  C_IL  C_DO  C_IU 	C_IP 	SC
void printCrackResult(Result r){
	print("%s\t%f\t%f\t%f\t%f\t%f\t%f\t%f\t%f\t%f\t%lld\t%lld\t%s\t%lld",
			r->name,
			get_lastLapTime(r->qt),
			get_lastLapTime(r->q_il),
			get_lastLapTime(r->q_da),
			get_lastLapTime(r->q_pf),
			get_lastLapTime(r->ct),
			get_lastLapTime(r->c_il),
			get_lastLapTime(r->c_do),
			get_lastLapTime(r->c_iu),
			get_lastLapTime(r->c_ip),
            r->sc,
			r->num_proj,
			getStringVersion((COVERING_TYPE) r->cover_type).c_str(),
			r->num_cover
			);
}

// HEADER  QT  Q_IL  Q_DA 	Q_PF  CT  C_IL  C_DO  C_IU  C_IP  T_SC
void printTotalCrackResult(Result r){
	print("%s\t%f\t%f\t%f\t%f\t%f\t%f\t%f\t%f\t%f\t%lld\t%lld\t%s\t%lld",
			r->name,
			get_totalTime(r->qt),
			get_totalTime(r->q_il),
			get_totalTime(r->q_da),
			get_totalTime(r->q_pf),
			get_totalTime(r->ct),
			get_totalTime(r->c_il),
			get_totalTime(r->c_do),
			get_totalTime(r->c_iu),
			get_totalTime(r->c_ip),
            r->t_sc,
			r->num_proj,
			getStringVersion((COVERING_TYPE) r->cover_type).c_str(),
			r->num_cover
			);
}

// HEADER  QT  Q_IL  Q_DA	Q_PF  CT  C_IL  C_DO  C_IU C_IP SC
void printChunkIndexingResult(Result r){
	print("%s\t%f\t%f\t%f\t%f\t%f\t%f\t%f\t%f\t%f\t%lld\t%lld\t%s\t%lld",
			r->name,
			get_totalTime(r->qt),
			get_totalTime(r->q_il),
			get_totalTime(r->q_da),
			get_totalTime(r->q_pf),
			get_totalTime(r->ct),
			get_totalTime(r->c_il),
			get_totalTime(r->c_do),
			get_totalTime(r->c_iu),
			get_totalTime(r->c_ip),
            r->sc,
			r->num_proj,
			getStringVersion((COVERING_TYPE) r->cover_type).c_str(),
			r->num_cover
			);
}
