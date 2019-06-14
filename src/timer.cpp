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
 * timer.cpp
 *
 * Information Systems Group
 * Saarland University
 * 2012 - 2013
 */

#include <stdlib.h>
#include <time.h>
#include <sys/time.h>
#ifdef __MACH__
#include <mach/clock.h>
#include <mach/mach.h>
#endif

#include "../includes/timer.h"

struct TimeData {
//	clock_t start;
	timespec_t start;
	timespec_t stop;

	double lastLapTime;
	double totalTime;
};

Timer get_timer(){
	Timer t = (Timer) malloc(sizeof(struct TimeData));
	t->start = (timespec_t) malloc(sizeof(struct timespec));
	t->stop = (timespec_t) malloc(sizeof(struct timespec));
	return t;
}

void copy_timer(Timer dest, Timer src) {
	dest->lastLapTime = src->lastLapTime;
	dest->totalTime = src->totalTime;
}

void destroy_timer(Timer t) {
    free(t);
}

inline double timespecDiff(timespec_t stop, timespec_t start)
{
  return ( (double)((stop->tv_sec * 1000000000) + stop->tv_nsec) - ((start->tv_sec * 1000000000) + start->tv_nsec) ) / 1000000.0;
}

inline void current_utc_time(struct timespec *ts) {
	#ifdef __MACH__ // OS X does not have clock_gettime, use clock_get_time
	  clock_serv_t cclock;
	  mach_timespec_t mts;
	  host_get_clock_service(mach_host_self(), CALENDAR_CLOCK, &cclock);
	  clock_get_time(cclock, &mts);
	  mach_port_deallocate(mach_task_self(), cclock);
	  ts->tv_sec = mts.tv_sec;
	  ts->tv_nsec = mts.tv_nsec;
	#else
	  clock_gettime(CLOCK_REALTIME, ts);
	#endif
}



void t_on(Timer t){
//	t->start = clock();
	current_utc_time(t->start);
}

void t_off(Timer t){
	current_utc_time(t->stop);
//	t->lastLapTime = ((double) (clock() - t->start)) / CLOCKS_PER_SEC;
	t->lastLapTime = timespecDiff(t->stop, t->start);
	t->totalTime += t->lastLapTime;
}

void t_extendLap(Timer t) {
	current_utc_time(t->stop);
	t->lastLapTime += timespecDiff(t->stop, t->start);
	t->totalTime += timespecDiff(t->stop, t->start);
}

void t_lap(Timer t){
	t_off(t);
//	t->start = clock();
	current_utc_time(t->start);
}

void t_subLastLapTime(Timer t1, Timer t2){
	t1->totalTime -= t1->lastLapTime;
	t1->lastLapTime -= t2->lastLapTime;
	t1->totalTime += t1->lastLapTime;
}

void t_clear(Timer t){
	t->lastLapTime = 0;
}

void t_reset(Timer t){
	t->lastLapTime = 0;
	t->totalTime = 0;
}

double get_lastLapTime(Timer t){
	return t->lastLapTime;
}

double get_totalTime(Timer t){
	return t->totalTime;
}

// adds the totalTime of second to the totalTime of first
void add_time(Timer dest, Timer src) {
	dest->lastLapTime += src->lastLapTime;
	dest->totalTime += src->totalTime;
}

void compute_average_time(Timer t, const internal_int_t number_of_repetitions) {
	t->lastLapTime /= number_of_repetitions;
	t->totalTime /= number_of_repetitions;
}
