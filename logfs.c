/**
 * Tony Givargis
 * Copyright (C), 2023
 * University of California, Irvine
 *
 * CS 238P - Operating Systems
 * logfs.c
 */

#include <pthread.h>
#include "device.h"
#include "logfs.h"

#define WCACHE_BLOCKS 32
#define RCACHE_BLOCKS 256

/**
 * Needs:
 *   pthread_create()
 *   pthread_join()
 *   pthread_mutex_init()
 *   pthread_mutex_destroy()
 *   pthread_mutex_lock()
 *   pthread_mutex_unlock()
 *   pthread_cond_init()
 *   pthread_cond_destroy()
 *   pthread_cond_wait()
 *   pthread_cond_signal()
 */

/* research the above Needed API and design accordingly */

struct cache_block {
        uint64_t block_number;
        void *buffer;
};

struct logfs {
        struct device* device;             
        uint8_t is_completed;              
        pthread_t worker_thread;           
        uint64_t offset;                   
        uint64_t block_size;               
        uint64_t available_space;          
        void *current_block;               
        void *current_offset;              
        
        struct write_cache_info {          
            pthread_mutex_t mutex;         
            pthread_cond_t user_condition; 
            pthread_cond_t worker_condition; 
            uint8_t head;                  
            uint8_t tail;                  
            uint8_t written_blocks_count;
        } wc_info;

        struct cache_block write_cache[WCACHE_BLOCKS]; 
        struct cache_block read_cache[RCACHE_BLOCKS];  
};


uint64_t calculate_block_boundary(struct logfs *logfs, uint64_t offset) {
        return offset - (offset % logfs->block_size);
}


void flush_data(struct logfs* logfs) {

        /* Allocate temporary buffer and align it to block size */
        void *tmp_buffer = malloc(2 * logfs->block_size);
        void *aligned_buffer = memory_align(tmp_buffer, logfs->block_size);
        struct cache_block *write_cache_block;

        /* Lock the mutex for thread-safe access to the write cache */
        if (pthread_mutex_lock(&logfs->wc_info.mutex) != 0) {
                TRACE("Error: Failed to lock mutex in flush_data");
                exit(1);
        }

        /* Wait until there are written blocks to flush or the operation is completed */
        while ((!logfs->is_completed) && (logfs->wc_info.written_blocks_count == 0)) {
                if (pthread_cond_wait(&logfs->wc_info.worker_condition, &logfs->wc_info.mutex) != 0) {
                        TRACE("Error: Failed to wait on condition variable in flush_data");
                        exit(1);
                }
        }

        /* Exit early if the operation is marked as completed */
        if (logfs->is_completed) {
                free(tmp_buffer);
                if (pthread_mutex_unlock(&logfs->wc_info.mutex) != 0) {
                        TRACE("Error: Failed to unlock mutex in flush_data");
                        exit(1);
                }
                return;
        }

        /* Access the next block in the write cache */
        write_cache_block = &logfs->write_cache[logfs->wc_info.tail];
        memcpy(aligned_buffer, write_cache_block->buffer, logfs->block_size);

        /* Update the tail and decrement the written blocks count */
        logfs->wc_info.tail = (logfs->wc_info.tail + 1) % WCACHE_BLOCKS;
        logfs->wc_info.written_blocks_count--;
        pthread_cond_signal(&logfs->wc_info.user_condition);

        /* Unlock the mutex after making the changes */
        if (pthread_mutex_unlock(&logfs->wc_info.mutex) != 0) {
                TRACE("Error: Failed to unlock mutex in flush_data");
                exit(1);
        }

        /* Write the block to the device */
        if (device_write(logfs->device, aligned_buffer, write_cache_block->block_number, logfs->block_size) != 0) {
                TRACE("Error: Failed to write block to device in flush_data");
                exit(1);
        }

        /* Free the temporary buffer */
        free(tmp_buffer);
}


void* worker_thread(void* arg) {
        struct logfs *logfs = (struct logfs*)arg;

        while (!logfs->is_completed) {
                flush_data(logfs);
        }
        pthread_exit(NULL);
}


struct logfs *logfs_open(const char *pathname) {
        struct logfs* logfs;
        int i;

        logfs = (struct logfs*) malloc(sizeof(struct logfs));

        logfs->device = device_open(pathname);
        if (NULL == logfs->device) {
                TRACE("unable to open device");
                free(logfs);
                return NULL;
        }

        logfs->offset = 0;
        logfs->is_completed = 0;
        logfs->block_size = device_block(logfs->device);
        logfs->wc_info.head = 0;
        logfs->wc_info.tail = 0;
        logfs->wc_info.written_blocks_count = 0;

        logfs->current_block = malloc(logfs->block_size);
        memset(logfs->current_block, 0, logfs->block_size);
        logfs->available_space = logfs->block_size;

        logfs->current_offset = logfs->current_block;

        for (i=0; i<RCACHE_BLOCKS; i++) {
                logfs->read_cache[i].block_number = 13; 
                logfs->read_cache[i].buffer = malloc(logfs->block_size);
        }

        for (i=0; i<WCACHE_BLOCKS; i++) {
                logfs->write_cache[i].block_number = 13; 
                logfs->write_cache[i].buffer = malloc(logfs->block_size);
        }

        if (0 != pthread_cond_init(&logfs->wc_info.worker_condition, NULL)) {
                exit(1);
        }

        if (0 != pthread_cond_init(&logfs->wc_info.user_condition, NULL)) {
                exit(1);
        }

        if (0 != pthread_mutex_init(&logfs->wc_info.mutex, NULL)) {
                exit(1);
        }

        if (0 != pthread_create(&logfs->worker_thread, NULL, &worker_thread, (void*)logfs)) {
                exit(1);
        }

        return logfs;
}


void logfs_close(struct logfs* logfs) {
    int x;  

    logfs->is_completed = 1; 

    /* Signal any threads waiting on `worker_condition` and `user_condition` condition variables. */ 
    pthread_cond_signal(&logfs->wc_info.worker_condition);
    pthread_cond_signal(&logfs->wc_info.user_condition);

    /* Destroy the mutex associated with the `logfs` instance. */ 
    pthread_mutex_destroy(&logfs->wc_info.mutex);

    /* Destroy the condition variables used for signaling worker and user threads. */ 
    pthread_cond_destroy(&logfs->wc_info.user_condition);
    pthread_cond_destroy(&logfs->wc_info.worker_condition);

    if (0 != pthread_join(logfs->worker_thread, NULL)) {
        /* If `pthread_join` fails, log the error (e.g., using TRACE macro). */ 
        TRACE("Thread Merging Failed");
    }

    device_close(logfs->device);

    /* Free the memory allocated for the current block being processed. */ 
    free(logfs->current_block);

    /* `RCACHE_BLOCKS` free buffer*/ 
    for (x = 0; x < RCACHE_BLOCKS; x++) {
        free(logfs->read_cache[x].buffer);
    }

    /* `WCACHE_BLOCKS` free buffer */ 
    for (x = 0; x < WCACHE_BLOCKS; x++) {
        free(logfs->write_cache[x].buffer);
    }

    memset(logfs, 0, sizeof(struct logfs));
    free(logfs);
}

void invalidate_read_cache(struct logfs *logfs, uint64_t block_number) {
    int offset_rcache = (block_number / logfs->block_size) % RCACHE_BLOCKS;

    /* Invalidate the cache if it contains the block being processed */ 
    if (block_number == logfs->read_cache[offset_rcache].block_number) {
        logfs->read_cache[offset_rcache].block_number = 13; /* 13 indicates invalidation */ 
    }
}

int write_current_block_to_cache(struct logfs *logfs) {
    struct cache_block *write_cache_block;

    /* Lock the write cache mutex to ensure thread-safe access */ 
    if (pthread_mutex_lock(&logfs->wc_info.mutex) != 0) {
        TRACE("Error locking mutex in write_current_block_to_cache");
        return -1; 
    }

    /* Wait until there is space available in the write cache */ 
    while (logfs->wc_info.written_blocks_count >= WCACHE_BLOCKS) {
        if (pthread_cond_wait(&logfs->wc_info.user_condition, &logfs->wc_info.mutex) != 0) {
            TRACE("Error waiting on condition variable in write_current_block_to_cache");
            pthread_mutex_unlock(&logfs->wc_info.mutex);
            return -1; 
        }
    }

    /* Get the next available block in the write cache */ 
    write_cache_block = &logfs->write_cache[logfs->wc_info.head];
    write_cache_block->block_number = calculate_block_boundary(logfs, logfs->offset - 1);
    memcpy(write_cache_block->buffer, logfs->current_block, logfs->block_size);

    /* Update the write cache state */ 
    logfs->wc_info.head = (logfs->wc_info.head + 1) % WCACHE_BLOCKS;
    logfs->wc_info.written_blocks_count++;

    /* Signal the worker thread that a new block is available */ 
    pthread_cond_signal(&logfs->wc_info.worker_condition);

    if (pthread_mutex_unlock(&logfs->wc_info.mutex) != 0) {
        TRACE("Error unlocking mutex in write_current_block_to_cache");
        return -1; 
    }

    return 0; 
}

void reset_current_block(struct logfs *logfs) {
    /* Clear the current block's buffer */ 
    memset(logfs->current_block, 0, logfs->block_size);
    logfs->available_space = logfs->block_size;
    logfs->current_offset = logfs->current_block;
}

void copy_to_current_block(struct logfs *logfs, const void **ptr_, uint64_t *data_left, uint64_t size) {
    memcpy(logfs->current_offset, *ptr_, size);
    /* Advance the pointer to the next unread data in the input buffer */ 
    *ptr_ = (const void *)((const char *)(*ptr_) + size);

    logfs->offset += size;
    *data_left -= size;

    /* Update the current block offset and available space */ 
    logfs->current_offset = (void *)((char *)logfs->current_offset + size);
    logfs->available_space -= size;
}

int logfs_append(struct logfs *logfs, const void *buffer, uint64_t len) {
    const void *ptr_ = buffer;
    uint64_t data_left = len;

    /* Determine the block where the current offset resides */ 
    uint64_t block_number = calculate_block_boundary(logfs, logfs->offset);

    invalidate_read_cache(logfs, block_number); 
    while (data_left >= logfs->available_space) {
        copy_to_current_block(logfs, &ptr_, &data_left, logfs->available_space);

        /* Mark the current block as unavailable */ 
        logfs->available_space = 0;

        if (write_current_block_to_cache(logfs) != 0) {
            return -1;
        }
        reset_current_block(logfs);
    }

    /* If there is remaining data, copy it into the current block */ 
    if (data_left > 0) {
        copy_to_current_block(logfs, &ptr_, &data_left, data_left);
    }

    return 0; 
}

int check_in_cache(struct logfs *logfs, const uint64_t block_start_address, void *buffer) {

        int i;
        /* Lock the mutex to ensure thread-safe access to write_cache */ 
        pthread_mutex_lock(&logfs->wc_info.mutex);

        /* Iterate over the cache to check if the block exists */ 
        for (i = 0; i < WCACHE_BLOCKS; i++) {
                if (logfs->write_cache[i].block_number == block_start_address) {
                        /* If block is found, copy the cached data to the provided buffer */ 
                        memcpy(buffer, logfs->write_cache[i].buffer, logfs->block_size);

                        /* Unlock the mutex and return success (found) */ 
                        pthread_mutex_unlock(&logfs->wc_info.mutex);
                        return 1;
                }
        }

        /* If we reach here, the block was not found in the cache */
        pthread_mutex_unlock(&logfs->wc_info.mutex);
        return 0;
}

int read_data(struct logfs *logfs, const uint64_t block_start_address, void *buffer, uint64_t offset, size_t len) {
    void *start;
    int readCacheOffset = (block_start_address / logfs->block_size) % RCACHE_BLOCKS;
    struct cache_block *read_cache_block = &logfs->read_cache[readCacheOffset];
    
    /* Allocate and align a temporary buffer for the block data */ 
    void *tmp_buf = malloc(2 * logfs->block_size);
    void *aligned_buf = memory_align(tmp_buf, logfs->block_size);

    if (!tmp_buf) {
        TRACE("Memory allocation failed");
        return -1;
    }


    /* Check if the requested block is in the cache */ 
    if (block_start_address != read_cache_block->block_number) {
        read_cache_block->block_number = block_start_address;

        /* Load data into cache block based on the block's boundary or write cache */ 
        if (block_start_address == calculate_block_boundary(logfs, logfs->offset)) {
            memcpy(aligned_buf, logfs->current_block, logfs->block_size);
        } else if (!check_in_cache(logfs, block_start_address, aligned_buf)) {
            if (device_read(logfs->device, aligned_buf, block_start_address, logfs->block_size)) {
                free(tmp_buf);
                return -1;
            }
        }

        /* Cache the read data */ 
        memcpy(read_cache_block->buffer, aligned_buf, logfs->block_size);
    }

    /* Calculate the offset within the cache and copy the data into the user buffer */ 
    uint64_t block_off = offset - block_start_address;
    start = (char*)read_cache_block->buffer + block_off;
    memcpy(buffer, start, len);

    free(tmp_buf);
    return 0;
}

static int read_block_data(struct logfs *logfs, uint64_t block_address, void *buffer, uint64_t offset_within_block, size_t bytes_to_read) {
    if (read_data(logfs, block_address, buffer, offset_within_block, bytes_to_read)) {
        return -1;
    }
    return 0;
}

int logfs_read(struct logfs *logfs, void *buffer, uint64_t offset, size_t len) {
    uint64_t current_block_address = calculate_block_boundary(logfs, offset);
    uint64_t bytes_remaining = len;
    uint64_t current_offset = offset;
    uint8_t *buffer_ptr = (uint8_t *)buffer;

    /* Read the first block, which may be a partial read */ 
    size_t first_block_read_len = MIN(len, (current_block_address + logfs->block_size - offset));
    if (read_block_data(logfs, current_block_address, buffer_ptr, current_offset, first_block_read_len)) {
        return -1;
    }

    /* Update for subsequent blocks */ 
    current_block_address += logfs->block_size;
    buffer_ptr += first_block_read_len;
    current_offset += first_block_read_len;
    bytes_remaining -= first_block_read_len;

    /* Read full blocks until remaining data is less than a block */ 
    while (bytes_remaining >= logfs->block_size) {
         if (read_block_data(logfs, current_block_address, buffer_ptr, current_offset, logfs->block_size)) {
            return -1;
        }
        current_block_address += logfs->block_size;
        buffer_ptr += logfs->block_size;
        current_offset += logfs->block_size;
        bytes_remaining -= logfs->block_size;
    }

    /* Read the final partial block if any data remains */ 
    if (bytes_remaining > 0) {
        if (read_block_data(logfs, current_block_address, buffer_ptr, current_offset, bytes_remaining)) {
            return -1;
        }
    }

    return 0;
}