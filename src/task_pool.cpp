#include "task_pool.h"

#include <Windows.h>
#include <process.h>

#include <stdint.h>
#include <utility>
//Caution! The correctness of the code is relied on the assurance of memory order on volatile variable. (Supported by MSVC since version 8.0)  

#ifdef _DEBUG
#include <iostream>
#define ABORT_WITH_DBGINFO \
std::cout << "Abort in function " << __FUNCSIG__ << " file " << __FILE__ << " line " << __LINE__ << std::endl; \
abort(); 
#define PRINT_ERROR_VALUE(val) \
std::cout << "Error value in function " << __FUNCSIG__ << " file " << __FILE__ << " line " << __LINE__ << ", " << val << std::endl;
#define PRINT_WIN32_ERRORCODE \
std::cout << "Error code in function " << __FUNCSIG__ << " file " << __FILE__ << " line " << __LINE__ << ", " << GetLastError() << std::endl;
#define CHECK_ERRORCODE(rc,unexpected) \
if (rc == unexpected) { \
ABORT_WITH_DBGINFO \
}
#else
#define CHECK_ERRORCODE(rc,unexpected)
#define ABORT_WITH_DBGINFO
#define PRINT_WIN32_ERRORCODE
#define PRINT_ERROR_VALUE(val)
#endif

struct thread_context
{
	thread_context(task_pool *class_ptr, unsigned long serial_number) :class_ptr(class_ptr), serial_number(serial_number){}
	task_pool *class_ptr;
	unsigned long serial_number;
};

unsigned int __stdcall task_pool::worker_thread(void *ctx)
{
	task_pool *class_ptr = static_cast<thread_context*>(ctx)->class_ptr;
	unsigned long serial_number = static_cast<thread_context*>(ctx)->serial_number;

	delete ctx;

	HANDLE thread_handle = GetCurrentThread();

	concurrency::concurrent_queue<std::pair<long, task*>> &task_queue = class_ptr->task_queue;
	cuckoohash_map<long, volatile task_state*> &task_id_map = class_ptr->task_id_map;
	cuckoohash_map<long, HANDLE> &waiting_map = class_ptr->waiting_map;

	std::pair<long, task*> task_entity;
	long task_id;
	task *task_ptr;
	volatile task_state* state;

	HANDLE waiting_thread_handle;

	for (;;)
	{
		if (class_ptr->task_queue.empty())
			if (class_ptr->exit_signal)
				break;
			else{
				class_ptr->thread_available[serial_number] = 1;
				SuspendThread(thread_handle);
			}

		else
		{
			class_ptr->thread_available[serial_number] = 0;

			do 
			{
				if (task_queue.try_pop(task_entity))
					break;
			} while (!task_queue.empty());

			task_id = task_entity.first;
			task_ptr = task_entity.second;

			state = task_id_map.find(task_id);

			*state = task_state::TASK_IN_PROCESS;

			task_ptr->run();

			*state = task_state::TASK_FINISHED;

			while (waiting_map.contains(task_id))
			{
				try
				{
					waiting_thread_handle = waiting_map.find(task_id);
				}
				catch (std::out_of_range)
				{
					break;
				}
				unsigned long error = ResumeThread(waiting_thread_handle);
#ifdef _DEBUG
				if (error == -1)
					PRINT_WIN32_ERRORCODE
#endif // _DEBUG
			}
		}
	}
	return 0;
}

task_pool::task_pool(unsigned long thread_number /*= 1*/) : thread_number(thread_number), exit_signal(0), max_task_id(0)
{
	thread_handles = new HANDLE[thread_number];
	thread_available = new int[thread_number];
	for (unsigned long i = 0; i != thread_number; i++)
	{
		thread_context *ctx = new thread_context(this,i);
		thread_handles[i] = reinterpret_cast<HANDLE>(_beginthreadex(NULL, 0, worker_thread,ctx,NULL,NULL));
#ifdef _DEBUG
		if (!thread_handles[i])
			PRINT_WIN32_ERRORCODE
#endif
		thread_available[i] = 1;
	}
}

task_pool::~task_pool()
{
	exit_signal = 1;
	unsigned long error;
	do
	{
		for (unsigned long i = 0; i != thread_number; i++)
		{
			error = ResumeThread(thread_handles[i]);

#ifdef _DEBUG
			if (error == -1)
				PRINT_WIN32_ERRORCODE
#endif // _DEBUG
		}

		error = WaitForMultipleObjectsEx(thread_number, thread_handles, true, 10, false);

#ifdef _DEBUG
		if (error < WAIT_OBJECT_0 && error >= (WAIT_OBJECT_0 + thread_number) && error != WAIT_TIMEOUT)
		{
			PRINT_ERROR_VALUE(error);
		}
#endif

	} while (error == WAIT_TIMEOUT);

	delete[]thread_handles;
	delete[]thread_available;
}

long task_pool::new_task()
{
	return InterlockedIncrement(&max_task_id) - 1;
}

task_state task_pool::query_task_state(long task_id)
{
	try{
		return *task_id_map.find(task_id);
	}
	catch (std::out_of_range)
	{
		return task_state::INVALID_TASK_ID;
	}
}

HANDLE get_current_thread_handle()
{
	HANDLE thread_handle;
	int error = DuplicateHandle(GetCurrentProcess(), GetCurrentThread(), GetCurrentProcess(), &thread_handle, NULL, TRUE, DUPLICATE_SAME_ACCESS);
	CHECK_ERRORCODE(error,0);
	return thread_handle;
}

error_type task_pool::wait_for_task(long task_id)
{
	volatile task_state *state;
	try{
		state = task_id_map.find(task_id);
	}
	catch (std::out_of_range)
	{
		return error_type::TASK_NOT_SUBMITTED;
	}
			
	if (*state == task_state::TASK_FINISHED)
		return error_type::STATUS_OK;

#ifdef _DEBUG
	if (*state != task_state::NEW_TASK && *state!=task_state::TASK_FINISHED&&*state !=task_state::TASK_IN_PROCESS)
		abort();
#endif
	bool error;

	HANDLE current_thread_handle = get_current_thread_handle();
	error = waiting_map.insert(task_id, current_thread_handle);
	CHECK_ERRORCODE(error, 0);

	if (*state == task_state::TASK_FINISHED){
		error = waiting_map.erase(task_id);
		CHECK_ERRORCODE(error, 0);

		return error_type::STATUS_OK;
	}

	do{
		SuspendThread(current_thread_handle);
	} while (*state != task_state::TASK_FINISHED);

	error = waiting_map.erase(task_id);
	CHECK_ERRORCODE(error, 0);

	return error_type::STATUS_OK;
}

error_type task_pool::submit_task(long task_id, task *task_ptr)
{
	if (task_id >= max_task_id)
		return error_type::INVALID_TASK_ID;

	std::pair<long, task*> task_entity = std::make_pair(task_id, task_ptr);
	volatile task_state *state = new task_state(task_state::NEW_TASK);
	if (!task_id_map.insert(task_id, state))
		return error_type::TASK_ALREADY_EXIST;
	
	task_queue.push(task_entity);

	for (unsigned int i = 0; i < thread_number; i++)
	{
		if (thread_available[i])
		{
			unsigned long error;
			do
			{
				error = ResumeThread(thread_handles[i]);
			} while (error < 0 && thread_available[i]);
			break;
		}
	}

	return error_type::STATUS_OK;
}

error_type task_pool::release_task(long task_id)
{
	volatile task_state *state;
	try{
		state = task_id_map.find(task_id);
	}
	catch (std::out_of_range)
	{
		return error_type::INVALID_TASK_ID;
	}
	delete state;

	bool rc = task_id_map.erase(task_id);
	CHECK_ERRORCODE(rc, false);

	return error_type::STATUS_OK;
}
