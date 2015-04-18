#pragma once

#include <libcuckoo-windows\cuckoohash_map.hh>
#include <concurrent_queue.h>

class task
{
public:
	virtual void run() = 0;
	virtual bool isSuccessful() = 0;
};

enum class task_state
{
	INVALID_TASK_ID,
	NEW_TASK,
	TASK_IN_PROCESS,
	TASK_FINISHED
};

enum class error_type
{
	STATUS_OK,
	TASK_ALREADY_EXIST,
	INVALID_TASK_ID,
	TASK_NOT_SUBMITTED
};

class task_pool
{
public:
	task_pool(unsigned long thread_number = 1);
	~task_pool();
	long new_task();
	task_state query_task_state(long task_id);
	error_type wait_for_task(long task_id);
	error_type submit_task(long task_id, task *task_ptr);
	error_type release_task(long task_id);
private:
	static unsigned int __stdcall worker_thread(void *ctx);
	concurrency::concurrent_queue<std::pair<long, task*>> task_queue;
	cuckoohash_map<long, volatile task_state*> task_id_map;
	cuckoohash_map<long, HANDLE> waiting_map;
	volatile int *thread_available;
	volatile int exit_signal;
	HANDLE *thread_handles;
	unsigned long thread_number;
	volatile long max_task_id;
};