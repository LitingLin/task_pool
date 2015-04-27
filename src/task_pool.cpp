#include "task_pool.h"

#include <Windows.h>
#include <process.h>

#include <stdint.h>
#include <utility>
//Caution! The correctness of the code is relied on the assurance of memory order on volatile variable. (Supported by MSVC since version 8.0)  

#ifdef _DEBUG
#include <iostream>
#define PRINT_ERROR_VALUE(val) \
std::cout << "Error occured in function " << __FUNCSIG__ << " file " << __FILE__ << " line " << __LINE__ << ", invalid value:" << val << std::endl;
#define PRINT_WIN32_ERRORCODE \
std::cout << "Error occured in function " << __FUNCSIG__ << " file " << __FILE__ << " line " << __LINE__ << ", Win32 error code:" << GetLastError() << std::endl;
#define ASSERT_WITH_WIN32_ERRORCODE(val) \
	if (val) {\
	assert(val); \
	PRINT_WIN32_ERRORCODE;}
#else
#define PRINT_ERROR_VALUE(val)
#define PRINT_WIN32_ERRORCODE
#define ASSERT_WITH_WIN32_ERRORCODE(val)
#endif

struct thread_context
{
	thread_context(task_pool *class_ptr, unsigned long thread_id) :class_ptr(class_ptr), thread_id(thread_id){}
	task_pool *class_ptr;
	unsigned long thread_id;
};

unsigned int __stdcall task_pool::worker_thread(void *ctx)
{
	task_pool *class_ptr = static_cast<thread_context*>(ctx)->class_ptr;
	unsigned long thread_id = static_cast<thread_context*>(ctx)->thread_id;

	delete ctx;

	HANDLE thread_handle = GetCurrentThread();
	HANDLE thread_awake_event = class_ptr->m_thread_awake_event[thread_id];

	volatile int &isThreadIdle = class_ptr->m_thread_available[thread_id];

	concurrency::concurrent_queue<std::pair<long, task*>> &task_queue = class_ptr->m_task_queue;
	cuckoohash_map<long, volatile task_state*> &task_id_map = class_ptr->m_task_id_map;
	cuckoohash_map<long, HANDLE> &waiting_map = class_ptr->m_waiting_map;

	std::pair<long, task*> task_entity;
	long task_id;
	task *task_ptr;
	volatile task_state* state;

	HANDLE waiting_thread_handle;

	for (;;)
	{
		if (class_ptr->m_task_queue.empty())
			if (class_ptr->m_exit_signal)
				break;
			else{
				isThreadIdle = 1;
#ifdef TASK_POOL_VERBOSE
				printf("Thread %d slept.\n", thread_id);
#endif // TASK_POOL_VERBOSE

				WaitForSingleObject(thread_awake_event, INFINITE);
#ifdef TASK_POOL_VERBOSE
				printf("Thread %d awoke.\n", thread_id);
#endif // TASK_POOL_VERBOSE
				ResetEvent(thread_awake_event);
			}

		else
		{
			isThreadIdle = 0;

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
				ASSERT_WITH_WIN32_ERRORCODE(error != -1);
			}

			if (class_ptr->m_is_autorelease)
				class_ptr->release_task(task_id);
		}
	}
	return 0;
}

task_pool::task_pool(unsigned long thread_number /*= 1*/, bool is_autorelease /*= false*/, unsigned long queue_size /*= INFINITE*/) : m_thread_number(thread_number), m_queue_size(queue_size), m_is_autorelease(is_autorelease), m_exit_signal(0), m_max_task_id(0)
{
	m_thread_handles = new HANDLE[thread_number];
	m_thread_awake_event = new HANDLE[thread_number];
	m_thread_available = new int[thread_number];
	for (unsigned long i = 0; i != thread_number; i++)
	{
		thread_context *ctx = new thread_context(this, i);
		m_thread_handles[i] = reinterpret_cast<HANDLE>(_beginthreadex(NULL, 0, worker_thread, ctx, NULL, NULL));
		ASSERT_WITH_WIN32_ERRORCODE(!m_thread_handles[i]);
		m_thread_awake_event[i] = CreateEvent(NULL, TRUE, FALSE, NULL);
		ASSERT_WITH_WIN32_ERRORCODE(!m_thread_awake_event[i]);
		m_thread_available[i] = 1;
	}
}

task_pool::~task_pool()
{
	m_exit_signal = 1;
	unsigned long error;

	for (unsigned long i = 0; i != m_thread_number; i++)
	{
		error = SetEvent(m_thread_awake_event[i]);
	}

	error = WaitForMultipleObjectsEx(m_thread_number, m_thread_handles, true, INFINITE, false);

#ifdef _DEBUG
	if (error < WAIT_OBJECT_0 && error >= (WAIT_OBJECT_0 + m_thread_number) && error != WAIT_TIMEOUT)
	{
		PRINT_ERROR_VALUE(error);
		PRINT_WIN32_ERRORCODE;
	}
#endif

	for (unsigned long i = 0; i != m_thread_number; i++)
	{
		CloseHandle(m_thread_awake_event[i]);
	}

	delete[]m_thread_handles;
	delete[]m_thread_available;
	delete[]m_thread_awake_event;
}

long task_pool::new_task()
{
	return InterlockedIncrement(&m_max_task_id) - 1;
}

task_state task_pool::query_task_state(long task_id)
{
	try{
		return *m_task_id_map.find(task_id);
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
	ASSERT_WITH_WIN32_ERRORCODE(error != 0);
	return thread_handle;
}

error_type task_pool::wait_for_task(long task_id)
{
	volatile task_state *state;
	try{
		state = m_task_id_map.find(task_id);
	}
	catch (std::out_of_range)
	{
		return error_type::TASK_NOT_SUBMITTED;
	}

	if (*state == task_state::TASK_FINISHED)
		return error_type::STATUS_OK;

#ifdef _DEBUG
	if (*state != task_state::NEW_TASK && *state != task_state::TASK_FINISHED&&*state != task_state::TASK_IN_PROCESS)
		abort();
#endif
	bool error;

	HANDLE current_thread_handle = get_current_thread_handle();
	error = m_waiting_map.insert(task_id, current_thread_handle);
	assert(error != 0);

	if (*state == task_state::TASK_FINISHED)
		goto case_sucessful;

	SuspendThread(current_thread_handle);

case_sucessful:
	error = m_waiting_map.erase(task_id);
	assert(error != 0);

	return error_type::STATUS_OK;
}

error_type task_pool::submit_task(long task_id, task *task_ptr)
{
	if (task_id >= m_max_task_id)
		return error_type::INVALID_TASK_ID;

	if (m_task_id_map.size() >= m_queue_size)
	{
#ifdef TASK_POOL_VERBOSE
		printf("Task %d was dismissed(Waiting queue are full.).\n", task_id);
#endif

		return error_type::TASK_QUEUE_FULL;
	}

	std::pair<long, task*> task_entity = std::make_pair(task_id, task_ptr);
	volatile task_state *state = new task_state(task_state::NEW_TASK);
	if (!m_task_id_map.insert(task_id, state))
		return error_type::TASK_ALREADY_EXIST;

	m_task_queue.push(task_entity);

#ifdef TASK_POOL_VERBOSE
	printf("Task %d submitted.\n", task_id);
#endif

	for (unsigned int i = 0; i < m_thread_number; i++)
	{
		if (m_thread_available[i])
		{
			unsigned long error;
			error = SetEvent(m_thread_awake_event[i]);
			break;
		}
#ifdef TASK_POOL_VERBOSE
		printf("Task %d entered waiting queue(Executors are busy.).\n", task_id);
#endif
	}

	return error_type::STATUS_OK;
}

error_type task_pool::release_task(long task_id)
{
	volatile task_state *state;
	try{
		state = m_task_id_map.find(task_id);
	}
	catch (std::out_of_range)
	{
		return error_type::INVALID_TASK_ID;
	}

	bool rc = m_task_id_map.erase(task_id);
	assert(rc);

	delete state;

	return error_type::STATUS_OK;
}


void task_pool::set_is_autorelease(bool autorelease)
{
	m_is_autorelease = autorelease;
}

void task_pool::set_queue_size(unsigned long size)
{
	m_queue_size = size;
}
