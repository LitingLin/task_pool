#include <iostream>
#include <task_pool.h>

#include "background_file_reader.hpp"

int main()
{
	task_pool task_exector;
	background_file_reader<char> file_reader(L"test.txt");
	long task_id = task_exector.new_task();
	task_exector.submit_task(task_id, file_reader);
	task_exector.wait_for_task(task_id);
	std::cout << file_reader.get_buffer();
	task_exector.release_task(task_id);
}