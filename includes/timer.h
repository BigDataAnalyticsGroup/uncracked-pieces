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
 * timer.h
 *
 * Information Systems Group
 * Saarland University
 * 2012 - 2013
 */

#ifndef TIMER_H_
#define TIMER_H_

#include "utils.h"

typedef struct TimeData *Timer;
typedef int Counter;
typedef struct timespec *timespec_t;

Timer get_timer();
void copy_timer(Timer dest, Timer src);
void destroy_timer(Timer t);
void t_on(Timer t);
void t_off(Timer t);
void t_lap(Timer t);
void t_extendLap(Timer t);
void t_subLastLapTime(Timer t1, Timer t2);
void t_clear(Timer t);
void t_reset(Timer t);
double get_lastLapTime(Timer t);
double get_totalTime(Timer t);
void add_time(Timer dest, Timer src);
void compute_average_time(Timer t, const internal_int_t number_of_repetitions);

#endif /* TIMER_H_ */
