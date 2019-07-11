/* slabs memory allocation */
#ifndef SLABS_H
#define SLABS_H

/** Init the subsystem. 1st argument is the limit on no. of bytes to allocate,
    0 if no limit. 2nd argument is the growth factor; each slab will use a chunk
    size equal to the previous slab's chunk size times this factor.
    3rd argument specifies if the slab allocator should allocate all memory
    up front (if true), or allocate memory in chunks as it is needed (if false)
*/


#include <stdlib.h>

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif
#include "memcached.h"
void slabs_init(const size_t limit, const double factor, const bool prealloc);


#define TO_MB(mem) ((double)(mem) / (double)(1UL<<20))

/**
 * Given object size, return id to use when allocating/freeing memory for object
 * 0 means error: can't store such a large object
 */

unsigned int slabs_clsid(const size_t size);

/** Allocate object of given length. 0 on error */ /*@null@*/
void *slabs_alloc(const size_t size, unsigned int id);

/** Free previously allocated object */
void slabs_free(void *ptr, size_t size, unsigned int id);

/** Adjust the stats for memory requested */
void slabs_adjust_mem_requested(unsigned int id, size_t old, size_t ntotal);

/** Return a datum for stats in binary protocol */
bool get_stats(const char *stat_type, int nkey, ADD_STAT add_stats, void *c);

/** Fill buffer with stats */ /*@null@*/
void slabs_stats(ADD_STAT add_stats, void *c);

int start_slab_maintenance_thread(void);
void stop_slab_maintenance_thread(void);

enum reassign_result_type {
    REASSIGN_OK=0, REASSIGN_RUNNING, REASSIGN_BADCLASS, REASSIGN_NOSPARE,
    REASSIGN_SRC_DST_SAME, REASSIGN_KILL_FEW

};

/** Reassignment (dst>0):
    If src > 0 and dst > 0:   reassign 1 slab from src to dst.
    If src < 0 and dst > 0:   reassign 1 slab from anywhere to dst.
    Todo: reassign -src slabs from anywhere to dst?

    Shrinkage ( src>0, dst=0): shrink num_slabs slabs from src.

    num_slabs is currently supported only in shrinkage.
    In reassignment it is always 1.
*/
enum reassign_result_type slabs_reassign(int src, int dst, int num_slabs);

/** Actually process the change of maxbytes*/
long long memory_shrink_expand(const size_t new_mem_limit);

#endif
