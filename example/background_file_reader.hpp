#pragma once

#include <Windows.h>
#include <stdint.h>
#include <string>

#include "task_pool.h"

//Caution! The correctness of the code is relied on the assurance of memory order on volatile variable. (Supported by MSVC since version 8.0)  

template <typename T>
class background_file_reader : public task
{
public:
	background_file_reader(const wchar_t *file_path) :file_path(file_path), buffer(0), m_isSuccessful(false){}
	~background_file_reader()
	{
		if (buffer)
			free(buffer);
	}
	virtual void run()
	{
		int error;

		HANDLE hFile;
		hFile = CreateFile(file_path, GENERIC_READ, FILE_SHARE_READ, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_SEQUENTIAL_SCAN, NULL);
		
		if (hFile == INVALID_HANDLE_VALUE)
			return;
		
		LARGE_INTEGER file_size;

		error = GetFileSizeEx(hFile, &file_size);

		if (!error)
			goto open_file_error_handle;

		if (file_size.QuadPart == 0)
			goto open_file_error_handle;

		buffer = (char*)malloc(file_size.QuadPart);
		if (buffer == NULL)
			goto open_file_error_handle;

		HANDLE hFileMapping = CreateFileMapping(hFile, NULL, PAGE_READONLY, 0, 0, NULL);

		if (hFileMapping == NULL)
			goto open_file_error_handle;

		void *file_ptr = MapViewOfFile(hFileMapping, FILE_MAP_READ, 0, 0, 0);

		if (!file_ptr)
			goto create_file_mapping_error_handle;
		
		error = memcpy_s(buffer, file_size.QuadPart, file_ptr, file_size.QuadPart);
		
		UnmapViewOfFile(file_ptr);

		if (error)
			goto create_file_mapping_error_handle;

		bytes_read = file_size.QuadPart;

		CloseHandle(hFileMapping);
		CloseHandle(hFile);

		m_isSuccessful = true;

		return;

	open_file_error_handle:
		{
			CloseHandle(hFile);
			return;
		}

	create_file_mapping_error_handle:
		{
			CloseHandle(hFileMapping);
			CloseHandle(hFile);

			return;
		}
	}

	bool isSuccessful()
	{
		return m_isSuccessful;
	}

	int64_t get_bytes_read()
	{
		return bytes_read;
	}
	const T* get_buffer()
	{
		return buffer;
	}
private:
	volatile bool m_isSuccessful;
	const wchar_t *file_path;
	T * volatile buffer;
	int64_t bytes_read;
};