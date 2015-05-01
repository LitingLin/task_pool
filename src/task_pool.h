#pragma once

#include <libcuckoo-windows\cuckoohash_map.hh>
#include <concurrent_queue.h>

class task
{
public:
	virtual void run() = 0;
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
	TASK_NOT_SUBMITTED,
	TASK_QUEUE_FULL
};
#ifndef _WIN64
#define __align__ __declspec(align(32))
#else
#define __align__ __declspec(align(64))
#endif // !_WIN64

class task_pool
{
public:
	//0 for task_queue_size means unlimited
	task_pool(unsigned long thread_number = 1, bool is_autorelease = false, unsigned long task_queue_size = 0);
	~task_pool();
	long new_task();
	task_state query_task_state(long task_id);
	error_type wait_for_task(long task_id);
	error_type submit_task(long task_id, task *task_ptr);
	error_type release_task(long task_id);
	void set_is_autorelease(bool autorelease);
	void set_queue_size_reserved(unsigned long size);//0 for size means unlimited
	unsigned long get_queue_size();
private:
	static unsigned int __stdcall worker_thread(void *ctx);
	concurrency::concurrent_queue<std::pair<long, task*>> m_task_queue;
	cuckoohash_map<long, std::pair<volatile task_state*,volatile HANDLE*>> m_task_id_map;
	volatile int *m_thread_available;
	volatile int m_exit_signal;
	HANDLE *m_thread_handles;
	HANDLE *m_thread_awake_event;
	unsigned long m_thread_number;
	__align__ volatile long m_max_task_id;
	volatile unsigned long m_queue_size_reserved;
	__align__ volatile unsigned long m_queue_size;
	bool m_is_autorelease;
};