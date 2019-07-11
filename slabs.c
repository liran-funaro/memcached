/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Slabs memory allocation, based on powers-of-N. Slabs are up to 1MB in size
 * and are divided into chunks. The chunk sizes start off at the size of the
 * "item" structure plus space for a small key and value. They increase by
 * a multiplier factor from there, up to half the maximum slab size. The last
 * slab size is always 1MB, since that's the maximum item size allowed by the
 * memcached protocol.
 */
#include "memcached.h"
#include <malloc.h>
#include <limits.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/signal.h>
#include <sys/resource.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>
#include <stdbool.h>
#include "slabs.h"

/* powers-of-N allocation structures */

typedef struct {
    unsigned int size;      /* sizes of items */
    unsigned int perslab;   /* how many items per slab */

    void *slots;           /* list of item ptrs */
    unsigned int sl_curr;   /* total free items in list */

    unsigned int slabs;     /* how many slabs were allocated for this class */

    void **slab_list;       /* array of slab pointers */
    unsigned int list_size; /* size of prev array */

    unsigned int killing;  /* index+1 of dying slab, or zero if none */
    size_t requested; /* The number of requested bytes */
} slabclass_t;

static slabclass_t slabclass[MAX_NUMBER_OF_SLAB_CLASSES];
static size_t mem_limit = 0;
static size_t mem_malloced = 0;
static size_t mem_malloced_slablist = 0;
static int power_largest;

static void *mem_base = NULL;
static void *mem_current = NULL;
static size_t mem_avail = 0;

/**
 * Access to the slab allocator is protected by this lock
 */
static pthread_mutex_t slabs_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t slabs_rebalance_lock = PTHREAD_MUTEX_INITIALIZER;

/*
 * Forward Declarations
 */
static int do_slabs_newslab(const unsigned int id);
static void *memory_allocate(size_t size);
static void do_slabs_free(void *ptr, const size_t size, unsigned int id);

/* Preallocate as many slab pages as possible (called from slabs_init)
   on start-up, so users don't get confused out-of-memory errors when
   they do have free (in-slab) space, but no space to make new slabs.
   if maxslabs is 18 (POWER_LARGEST - POWER_SMALLEST + 1), then all
   slab types can be made.  if max memory is less than 18 MB, only the
   smaller ones will be made.  */
static void slabs_preallocate (const unsigned int maxslabs);

/*If we want to make the accounting more global,
  we need to add more memory counters here.
  The current accounting policy is to count many things,
  but only reduce the number of slabs.
  The hash table might also requireshrinkage, but it should be
  of small consequence.
*/
#define TOTAL_MALLOCED (mem_malloced+mem_malloced_slablist+tell_hashsize())

/*
 * Figures out which slab class (chunk size) is required to store an item of
 * a given size.
 *
 * Given object size, return id to use when allocating/freeing memory for object
 * 0 means error: can't store such a large object
 */


/**For debugging memory allocations*/
//#define DEBUG_SLABS
#undef DEBUG_SLABS
#ifdef DEBUG_SLABS
static uint print_counter= 0 ;
#endif
static void print_statm(const char * const str){
#ifdef DEBUG_SLABS
    uint pid=getpid();
    char sys_str[500];
    fprintf(stderr,"%u:%s\n",(uint)pthread_self(),str);
    //fprintf(stderr,"gdb memcached-debug %d\n",pid);
    snprintf(sys_str,sizeof(sys_str),"echo statm `cat /proc/%d/statm`",pid);
    if (-1==system(sys_str))
      fprintf(stderr,"statm failed\n");
    malloc_stats();
    fprintf (stderr,
             "%umalloced %u limit %u slablist %u hash %u total %u\n",
             ++print_counter,
             (uint)mem_malloced,(uint)mem_limit,(uint)mem_malloced_slablist,
             tell_hashsize(),(uint)TOTAL_MALLOCED);

#else
    (void)str;
#endif
    return;
}


unsigned int slabs_clsid(const size_t size) {
    int res = POWER_SMALLEST;

    if (size == 0)
        return 0;
    while (size > slabclass[res].size)
        if (res++ == power_largest)     /* won't fit in the biggest slab */
            return 0;
    return res;
}

/**
 * Determines the chunk sizes and initializes the slab class descriptors
 * accordingly.
 */
void slabs_init(const size_t limit, const double factor, const bool prealloc) {
    int i = POWER_SMALLEST - 1;
    unsigned int size = sizeof(item) + settings.chunk_size;

    mem_limit = limit;

    if (prealloc) {
        /* Allocate everything in a big chunk with malloc */
        print_statm("before init");
        mem_base = malloc(mem_limit);
        print_statm("after init");
        if (mem_base != NULL) {
            mem_current = mem_base;
            mem_avail = mem_limit;
        } else {
            fprintf(stderr, "Warning: Failed to allocate requested memory in"
                    " one large chunk.\nWill allocate in smaller chunks\n");
        }
    }

    memset(slabclass, 0, sizeof(slabclass));

    while (++i < POWER_LARGEST && size <= settings.item_size_max / factor) {
        /* Make sure items are always n-byte aligned */
        if (size % CHUNK_ALIGN_BYTES)
            size += CHUNK_ALIGN_BYTES - (size % CHUNK_ALIGN_BYTES);

        slabclass[i].size = size;
        slabclass[i].perslab = settings.item_size_max / slabclass[i].size;
        size *= factor;
        if (settings.verbose > 1) {
            fprintf(stderr, "slab class %3d: chunk size %9u perslab %7u\n",
                    i, slabclass[i].size, slabclass[i].perslab);
        }
    }

    power_largest = i;
    slabclass[power_largest].size = settings.item_size_max;
    slabclass[power_largest].perslab = 1;
    if (settings.verbose > 1) {
        fprintf(stderr, "slab class %3d: chunk size %9u perslab %7u\n",
                i, slabclass[i].size, slabclass[i].perslab);
    }

    /* for the test suite:  faking of how much we've already malloc'd */
    {
        char *t_initial_malloc = getenv("T_MEMD_INITIAL_MALLOC");
        if (t_initial_malloc) {
            mem_malloced = (size_t)atol(t_initial_malloc);
        }

    }

    if (prealloc) {
        slabs_preallocate(power_largest);
    }
}

static void slabs_preallocate (const unsigned int maxslabs) {
    int i;
    unsigned int prealloc = 0;

    /* pre-allocate a 1MB slab in every size class so people don't get
       confused by non-intuitive "SERVER_ERROR out of memory"
       messages.  this is the most common question on the mailing
       list.  if you really don't want this, you can rebuild without
       these three lines.  */

    for (i = POWER_SMALLEST; i <= POWER_LARGEST; i++) {
        if (++prealloc > maxslabs)
            return;
        if (do_slabs_newslab(i) == 0) {
            fprintf(stderr, "Error while preallocating slab memory!\n"
                "If using -L or other prealloc options, max memory must be "
                "at least %d megabytes.\n", power_largest);
            exit(1);
        }
    }

}

static int grow_slab_list (const unsigned int id) {
    slabclass_t *p = &slabclass[id];
    print_statm("before grow");

    if (p->slabs == p->list_size) {
        size_t new_size =  (p->list_size != 0) ? p->list_size * 2 : 16;
        size_t required_addition=(new_size-p->list_size)* sizeof(void *);
        int not_enough_mem=mem_limit &&
            ((TOTAL_MALLOCED + required_addition) > mem_limit) &&
            (p->slabs > 0);
        if (not_enough_mem)
            return 0;
        else{
            void *new_list = realloc(p->slab_list, new_size * sizeof(void *));
            if (new_list == 0) return 0;
            /*For accurate memory accounting, pointer sizes must also be counted*/
            mem_malloced_slablist+=required_addition;
            print_statm("after grow");
            p->list_size = new_size;
            p->slab_list = new_list;
        }
    }
    return 1;
}

static void split_slab_page_into_freelist(char *ptr, const unsigned int id) {
    slabclass_t *p = &slabclass[id];
    int x;
    for (x = 0; x < p->perslab; x++) {
        do_slabs_free(ptr, 0, id);
        ptr += p->size;
    }
}

static int do_slabs_newslab(const unsigned int id) {
    slabclass_t *p = &slabclass[id];
    int len = settings.slab_reassign ? settings.item_size_max
        : p->size * p->perslab;
    char *ptr;

    /*mem_limit>0 means we have a memory limitation.
      Only in this case we check that if we allocate the slab, we do not go over the top.
      p->slabs>0 if we already have some slabs of this class.
      Otherwise, we allocate anyhow to the class because it is the first one.
      If automove is active, this may make us go over the top. In this case
      shrinkage will be activated*/
    /*Thus is a tentative evaluation, because we don't know
      yet if we would need to grow the slab list*/
    int not_enough_mem=(mem_limit &&
                        (TOTAL_MALLOCED + len > mem_limit) &&
                        p->slabs > 0);

    int grow_slab_list_failed=not_enough_mem?1:(grow_slab_list(id) == 0);
    /*re-evaluate because the list might have grown*/
    if (!grow_slab_list_failed)
        not_enough_mem=(mem_limit &&
                        ((TOTAL_MALLOCED + len) > mem_limit) &&
                        p->slabs > 0);

    if (not_enough_mem ||
        (grow_slab_list_failed) ||
        ((ptr = memory_allocate((size_t)len)) == 0)) {

        MEMCACHED_SLABS_SLABCLASS_ALLOCATE_FAILED(id);
        return 0;
    }


    memset(ptr, 0, (size_t)len);
    split_slab_page_into_freelist(ptr, id);

    p->slab_list[p->slabs++] = ptr;
    MEMCACHED_SLABS_SLABCLASS_ALLOCATE(id);

    return 1;
}

/*@null@*/
static void *do_slabs_alloc(const size_t size, unsigned int id) {
    slabclass_t *p;
    void *ret = NULL;
    item *it = NULL;

    if (id < POWER_SMALLEST || id > power_largest) {
        MEMCACHED_SLABS_ALLOCATE_FAILED(size, 0);
        return NULL;
    }

    p = &slabclass[id];
    assert(p->sl_curr == 0 || ((item *)p->slots)->slabs_clsid == 0);

    /* fail unless we have space at the end of a recently allocated page,
       we have something on our freelist, or we could allocate a new page */
    if (! (p->sl_curr != 0 || do_slabs_newslab(id) != 0)) {
        /* We don't have more memory available */
        ret = NULL;
    } else if (p->sl_curr != 0) {
        /* return off our freelist */
        it = (item *)p->slots;
        p->slots = it->next;
        if (it->next) it->next->prev = 0;
        p->sl_curr--;
        ret = (void *)it;
    }

    if (ret) {
        p->requested += size;
        MEMCACHED_SLABS_ALLOCATE(size, id, p->size, ret);
    } else {
        MEMCACHED_SLABS_ALLOCATE_FAILED(size, id);
    }

    return ret;
}

static void do_slabs_free(void *ptr, const size_t size, unsigned int id) {
    slabclass_t *p;
    item *it;

    assert(((item *)ptr)->slabs_clsid == 0);
    assert(id >= POWER_SMALLEST && id <= power_largest);
    if (id < POWER_SMALLEST || id > power_largest)
        return;

    MEMCACHED_SLABS_FREE(size, id, ptr);
    p = &slabclass[id];

    it = (item *)ptr;
    it->it_flags |= ITEM_SLABBED;
    it->prev = 0;
    it->next = p->slots;
    if (it->next) it->next->prev = it;
    p->slots = it;

    p->sl_curr++;
    p->requested -= size;
    print_statm("in slabs free, just catching other stuff");
    return;
}

static int nz_strcmp(int nzlength, const char *nz, const char *z) {
    int zlength=strlen(z);
    return (zlength == nzlength) && (strncmp(nz, z, zlength) == 0) ? 0 : -1;
}

bool get_stats(const char *stat_type, int nkey, ADD_STAT add_stats, void *c) {
    bool ret = true;

    if (add_stats != NULL) {
        if (!stat_type) {
            /* prepare general statistics for the engine */
            STATS_LOCK();
            APPEND_STAT("bytes", "%llu", (unsigned long long)stats.curr_bytes);
            APPEND_STAT("curr_items", "%u", stats.curr_items);
            APPEND_STAT("total_items", "%u", stats.total_items);
            APPEND_STAT("evictions", "%llu",
                        (unsigned long long)stats.evictions);
            APPEND_STAT("reclaimed", "%llu",
                        (unsigned long long)stats.reclaimed);
            STATS_UNLOCK();
        } else if (nz_strcmp(nkey, stat_type, "items") == 0) {
            item_stats(add_stats, c);
        } else if (nz_strcmp(nkey, stat_type, "slabs") == 0) {
            slabs_stats(add_stats, c);
        } else if (nz_strcmp(nkey, stat_type, "sizes") == 0) {
            item_stats_sizes(add_stats, c);
        } else {
            ret = false;
        }
    } else {
        ret = false;
    }

    return ret;
}

/*@null@*/
static void do_slabs_stats(ADD_STAT add_stats, void *c) {
    int i, total;
    /* Get the per-thread stats which contain some interesting aggregates */
    struct thread_stats thread_stats;
    threadlocal_stats_aggregate(&thread_stats);

    total = 0;
    for(i = POWER_SMALLEST; i <= power_largest; i++) {
        slabclass_t *p = &slabclass[i];
        if (p->slabs != 0) {
            uint32_t perslab, slabs;
            slabs = p->slabs;
            perslab = p->perslab;

            char key_str[STAT_KEY_LEN];
            char val_str[STAT_VAL_LEN];
            int klen = 0, vlen = 0;

            APPEND_NUM_STAT(i, "chunk_size", "%u", p->size);
            APPEND_NUM_STAT(i, "chunks_per_page", "%u", perslab);
            APPEND_NUM_STAT(i, "total_pages", "%u", slabs);
            APPEND_NUM_STAT(i, "total_chunks", "%u", slabs * perslab);
            APPEND_NUM_STAT(i, "used_chunks", "%u",
                            slabs*perslab - p->sl_curr);
            APPEND_NUM_STAT(i, "free_chunks", "%u", p->sl_curr);
            /* Stat is dead, but displaying zero instead of removing it. */
            APPEND_NUM_STAT(i, "free_chunks_end", "%u", 0);
            APPEND_NUM_STAT(i, "mem_requested", "%llu",
                            (unsigned long long)p->requested);
            APPEND_NUM_STAT(i, "get_hits", "%llu",
                    (unsigned long long)thread_stats.slab_stats[i].get_hits);
            APPEND_NUM_STAT(i, "cmd_set", "%llu",
                    (unsigned long long)thread_stats.slab_stats[i].set_cmds);
            APPEND_NUM_STAT(i, "delete_hits", "%llu",
                    (unsigned long long)thread_stats.slab_stats[i].delete_hits);
            APPEND_NUM_STAT(i, "incr_hits", "%llu",
                    (unsigned long long)thread_stats.slab_stats[i].incr_hits);
            APPEND_NUM_STAT(i, "decr_hits", "%llu",
                    (unsigned long long)thread_stats.slab_stats[i].decr_hits);
            APPEND_NUM_STAT(i, "cas_hits", "%llu",
                    (unsigned long long)thread_stats.slab_stats[i].cas_hits);
            APPEND_NUM_STAT(i, "cas_badval", "%llu",
                    (unsigned long long)thread_stats.slab_stats[i].cas_badval);
            APPEND_NUM_STAT(i, "touch_hits", "%llu",
                    (unsigned long long)thread_stats.slab_stats[i].touch_hits);
            total++;
        }
    }

    /* add overall slab stats and append terminator */

    APPEND_STAT("active_slabs", "%d", total);
    APPEND_STAT("total_malloced", "%llu", (unsigned long long)mem_malloced);
    add_stats(NULL, 0, NULL, 0, c);
}

static void *memory_allocate(size_t size) {
    void *ret;

    if (mem_base == NULL) {
        print_statm("before memory allocate");
        /* We are not using a preallocated large memory chunk */
        ret = malloc(size);
        if (ret)
            mem_malloced+=size;
        print_statm("after memory allocate");

    } else {
        ret = mem_current;

        if (size > mem_avail) {
            return NULL;
        }

        /* mem_current pointer _must_ be aligned!!! */
        if (size % CHUNK_ALIGN_BYTES) {
            size += CHUNK_ALIGN_BYTES - (size % CHUNK_ALIGN_BYTES);
        }

        mem_current = ((char*)mem_current) + size;
        if (size < mem_avail) {
            mem_avail -= size;
        } else {
            mem_avail = 0;
        }
    }

    return ret;
}

void *slabs_alloc(size_t size, unsigned int id) {
    void *ret;

    pthread_mutex_lock(&slabs_lock);
    ret = do_slabs_alloc(size, id);
    pthread_mutex_unlock(&slabs_lock);
    return ret;
}

void slabs_free(void *ptr, size_t size, unsigned int id) {
    pthread_mutex_lock(&slabs_lock);
    do_slabs_free(ptr, size, id);
    pthread_mutex_unlock(&slabs_lock);
}

void slabs_stats(ADD_STAT add_stats, void *c) {
    pthread_mutex_lock(&slabs_lock);
    do_slabs_stats(add_stats, c);
    pthread_mutex_unlock(&slabs_lock);
}

void slabs_adjust_mem_requested(unsigned int id, size_t old, size_t ntotal)
{
    pthread_mutex_lock(&slabs_lock);
    slabclass_t *p;
    if (id < POWER_SMALLEST || id > power_largest) {
        fprintf(stderr, "Internal error! Invalid slab class\n");
        abort();
    }

    p = &slabclass[id];
    p->requested = p->requested - old + ntotal;
    pthread_mutex_unlock(&slabs_lock);
}

static pthread_cond_t maintenance_cond = PTHREAD_COND_INITIALIZER;
static pthread_cond_t slab_rebalance_cond = PTHREAD_COND_INITIALIZER;
static volatile int do_run_slab_thread = 1;
static volatile int do_run_slab_rebalance_thread = 1;

#define DEFAULT_SLAB_BULK_CHECK 1
int slab_bulk_check = DEFAULT_SLAB_BULK_CHECK;

static int slab_rebalance_start(void) {
    slabclass_t *s_cls;
    int no_go = 0;
    bool shrink=(slab_rebal.d_clsid==0);


    pthread_mutex_lock(&cache_lock);
    pthread_mutex_lock(&slabs_lock);

    if (slab_rebal.s_clsid < POWER_SMALLEST ||
        slab_rebal.s_clsid > power_largest  ||
        (!shrink && (
                     slab_rebal.d_clsid < POWER_SMALLEST ||
                     slab_rebal.d_clsid > power_largest  ) )||
        slab_rebal.s_clsid == slab_rebal.d_clsid)
        no_go = -2;

    s_cls = &slabclass[slab_rebal.s_clsid];

    /* check only when reasigning, not when shrinking*/
    if (slab_rebal.d_clsid && (!grow_slab_list(slab_rebal.d_clsid))) {
        no_go = -1;
    }

    /*If we take more than 1, we make the decision once, but run
      the mechanism several times.
      If the mechanism changes to actually changing several slabs each time,
      this check should be
      if (s_cls->slabs < 1 + slab_rebal.num_slabs)
    */
    if (s_cls->slabs < 2)
        no_go = -3;

    if (no_go != 0) {
        pthread_mutex_unlock(&slabs_lock);
        pthread_mutex_unlock(&cache_lock);
        return no_go; /* Should use a wrapper function... */
    }

    /*If controlling several slabs at once is supported, this should be
      s_cls->killing = slab_rebal.num_slabs;
    */
    s_cls->killing = 1;
    --slab_rebal.num_slabs;

    /*Can several slabs be supported at once?*/
    slab_rebal.slab_start = s_cls->slab_list[s_cls->killing - 1];
    slab_rebal.slab_end   = (char *)slab_rebal.slab_start +
        (s_cls->size * s_cls->perslab);
    slab_rebal.slab_pos   = slab_rebal.slab_start;
    slab_rebal.done       = 0;

    /* Also tells do_item_get to search for items in this slab */
    slab_rebalance_signal = 2;

    if (settings.verbose > 1) {
        fprintf(stderr, "Started a slab %s\n",slab_rebal.d_clsid?"rebalance":"shrink");
    }

    pthread_mutex_unlock(&slabs_lock);
    pthread_mutex_unlock(&cache_lock);

    STATS_LOCK();
    stats.slab_reassign_running = true;
    STATS_UNLOCK();

    return 0;
}

enum move_status {
    MOVE_PASS=0, MOVE_DONE, MOVE_BUSY
};

/* refcount == 0 is safe since nobody can incr while cache_lock is held.
 * refcount != 0 is impossible since flags/etc can be modified in other
 * threads. instead, note we found a busy one and bail. logic in do_item_get
 * will prevent busy items from continuing to be busy
 */
static int slab_rebalance_move(void) {
    slabclass_t *s_cls;
    int x;
    int was_busy = 0;
    int refcount = 0;
    enum move_status status = MOVE_PASS;

    pthread_mutex_lock(&cache_lock);
    pthread_mutex_lock(&slabs_lock);

    s_cls = &slabclass[slab_rebal.s_clsid];

    for (x = 0; x < slab_bulk_check; x++) {
        item *it = slab_rebal.slab_pos;
        status = MOVE_PASS;
        if (it->slabs_clsid != 255) {
            refcount = refcount_incr(&it->refcount);
            if (refcount == 1) { /* item is unlinked, unused */
                if (it->it_flags & ITEM_SLABBED) {
                    /* remove from slab freelist */
                    if (s_cls->slots == it) {
                        s_cls->slots = it->next;
                    }
                    if (it->next) it->next->prev = it->prev;
                    if (it->prev) it->prev->next = it->next;
                    s_cls->sl_curr--;
                    status = MOVE_DONE;
                } else {
                    status = MOVE_BUSY;
                }
            } else if (refcount == 2) { /* item is linked but not busy */
                if ((it->it_flags & ITEM_LINKED) != 0) {
                    do_item_unlink_nolock(it, hash(ITEM_key(it), it->nkey, 0));
                    status = MOVE_DONE;
                } else {
                    /* refcount == 1 + !ITEM_LINKED means the item is being
                     * uploaded to, or was just unlinked but hasn't been freed
                     * yet. Let it bleed off on its own and try again later */
                    status = MOVE_BUSY;
                }
            } else {
                if (settings.verbose > 2) {
                    fprintf(stderr, "Slab reassign hit a busy item: refcount: %d (%d -> %d)\n",
                        it->refcount, slab_rebal.s_clsid, slab_rebal.d_clsid);
                }
                status = MOVE_BUSY;
            }
        }

        switch (status) {
            case MOVE_DONE:
                it->refcount = 0;
                it->it_flags = 0;
                it->slabs_clsid = 255;
                break;
            case MOVE_BUSY:
                slab_rebal.busy_items++;
                was_busy++;
                refcount_decr(&it->refcount);
                break;
            case MOVE_PASS:
                break;
        }

        slab_rebal.slab_pos = (char *)slab_rebal.slab_pos + s_cls->size;
        if (slab_rebal.slab_pos >= slab_rebal.slab_end)
            break;
    }

    if (slab_rebal.slab_pos >= slab_rebal.slab_end) {
        /* Some items were busy, start again from the top */
        if (slab_rebal.busy_items) {
            slab_rebal.slab_pos = slab_rebal.slab_start;
            slab_rebal.busy_items = 0;
        } else {
            slab_rebal.done++;
        }
    }

    pthread_mutex_unlock(&slabs_lock);
    pthread_mutex_unlock(&cache_lock);

    return was_busy;
}

static void slab_rebalance_finish(void) {
    slabclass_t *s_cls;
    slabclass_t *d_cls;
    bool shrink=(slab_rebal.d_clsid==0);

    pthread_mutex_lock(&cache_lock);
    pthread_mutex_lock(&slabs_lock);

    s_cls = &slabclass[slab_rebal.s_clsid];

    /* At this point the stolen slab is completely clear */
    s_cls->slab_list[s_cls->killing - 1] =
        s_cls->slab_list[s_cls->slabs - 1];
    s_cls->slabs--;
    s_cls->killing = 0;
    /* Todo: The slab_list array seems to be growing indefinatelly.
       It should be re-alloced from time to time, if many slabs were shrunk or reassigned.*/

    if (shrink){
        ((item *)(slab_rebal.slab_start))->slabs_clsid = 0;
        print_statm("before shrink");
        if (mem_base==NULL){
            free(slab_rebal.slab_start);
            malloc_trim(settings.item_size_max);
            mem_malloced -= settings.item_size_max;
        print_statm("after shrink");
        }
    }else{

        memset(slab_rebal.slab_start, 0, (size_t)settings.item_size_max);

        d_cls   = &slabclass[slab_rebal.d_clsid];
        d_cls->slab_list[d_cls->slabs++] = slab_rebal.slab_start;
        split_slab_page_into_freelist(slab_rebal.slab_start,
                                      slab_rebal.d_clsid);
    }


    if (slab_rebal.num_slabs){
        /*we are not done yet, keep old data and go into another loop*/
        slab_rebalance_signal = 1;
        /*We do not have to set
          slab_rebal.done       = 0;
          because the next thing we do is set it to 0
          in slab_rebalance_start
          because we just set slab_rebalance_signal = 1;
        */
    }else{
        slab_rebalance_signal = 0;
        slab_rebal.done       = 0;
        slab_rebal.s_clsid    = 0;
        slab_rebal.d_clsid    = 0;

    }

    slab_rebal.slab_start = NULL;
    slab_rebal.slab_end   = NULL;
    slab_rebal.slab_pos   = NULL;

    pthread_mutex_unlock(&slabs_lock);
    pthread_mutex_unlock(&cache_lock);

    STATS_LOCK();
    stats.slab_reassign_running = false;
    if (shrink)
        stats.slabs_shrunk++;
    else
        stats.slabs_moved++;
    STATS_UNLOCK();

    if (settings.verbose > 1) {
        fprintf(stderr, "Finished a slab %s\n",shrink?"shrink":"move");
    }
}

/**Divide integers and get the ceiling value,
   without linking to the math lib and converting to floating point operations.*/
static unsigned long long ceil_divide(const unsigned long long a, const unsigned long long b) {
    return (a + b - 1) / b;
}


#define DECISION_SECONDS_SHORT 1
#define DECISION_SECONDS_LONG 10
/** Return 1 means a decision was reached for the source.
 * Return 2 menas a decision was reached for a destination as well.
 * Return 0 if no decision was made.
 * Move to its own thread (created/destroyed as needed) once automover is more
 * complex.
 */
static int slab_automove_decision(int *src, int *dst, int *const num_slabs,
                                  const bool shrink_now) {
    static uint64_t evicted_old[POWER_LARGEST];

    /*Record the number of consecutive times
      in which a slab had zero evictions*/
    static unsigned int slab_zeroes[POWER_LARGEST];
    static unsigned int slab_winner = 0;
    static unsigned int slab_wins   = 0;
    uint64_t evicted_new[POWER_LARGEST];
    uint64_t evicted_diff[POWER_LARGEST];
    uint64_t evicted_max  = 0;
    uint64_t evicted_min  = ULONG_MAX;
    unsigned int highest_slab = 0;
    unsigned int total_pages[POWER_LARGEST];
    int i;
    int source = 0;
    int emergency_source = 0;
    int dest = 0;
    static rel_time_t next_run;

    /* Run less frequently than the slabmove tester. */
    if (current_time >= next_run) {
        next_run = current_time + 10;
        int decision_seconds=(settings.slab_automove>1)?
            DECISION_SECONDS_SHORT:DECISION_SECONDS_LONG;
        next_run = current_time + decision_seconds;

    } else {
        return 0;
    }

    item_stats_evictions(evicted_new);
    pthread_mutex_lock(&cache_lock);
    for (i = POWER_SMALLEST; i < power_largest; i++) {
        total_pages[i] = slabclass[i].slabs;
    }
    pthread_mutex_unlock(&cache_lock);

    /* Find a candidate source; something with zero evicts 3+ times.
       This algorithm prefers larger powers as a source.  */
    for (i = POWER_SMALLEST; i < power_largest; i++) {
        evicted_diff[i] = evicted_new[i] - evicted_old[i];
        if (evicted_diff[i] == 0 && total_pages[i] > 2) {
            slab_zeroes[i]++;
            if (source == 0 && slab_zeroes[i] >= 3)
                source = i;
        } else {/*Search for the best destination according
                  to the current statistics*/
            slab_zeroes[i] = 0;
            if (evicted_diff[i] > evicted_max) {
                evicted_max = evicted_diff[i];
                highest_slab = i;
            }
        }

        if (settings.verbose > 2 && total_pages[i]) {
            fprintf(stderr,
                    "total pages: slab class %d diff %ld slabs %d\n",
                    i,(long int)evicted_diff[i],total_pages[i]);
        }


        /*prepare an emergency source for the aggressive mode*/
        if ((settings.slab_automove>1) &&
            evicted_diff[i] < evicted_min && (total_pages[i] >= 2)){
            /*We verify that there are enough slabs in the emergency source,
              otherwise we don't have anything to take from.
              If we wait to slab_reassign with this check we might hit a neverending loop.*/

            /*The evicted diff statistic may be misguiding
              where the statistic is checked too often,
              so we allow a tie breaker. this is not pure logic -
              one can insert any kind of
              weight function over total_pages and evicted_diff.*/
            if (emergency_source==0 ||
                ( evicted_diff[i] < evicted_min) ||
                ( /*evicted diff is equal and*/ total_pages[i] >total_pages[emergency_source])){
                evicted_min=evicted_diff[i];
                if (shrink_now) {
                    fprintf(stdout, "emergency source changed from %d to %d\n",
                            emergency_source, i);
                    fflush(stdout);
                }
                emergency_source=i;
            }

        }

        evicted_old[i] = evicted_new[i];
    }

    /* Pick a valid destination: a destination which won 3 times in a row */
    if (slab_winner != 0 && slab_winner == highest_slab) {
        slab_wins++;
        if ((!shrink_now) && (slab_wins >= 3))
            dest = slab_winner;
    } else {
        slab_wins = 1;
        slab_winner = highest_slab;
    }


    if ((settings.slab_automove>1) && !source)
        source=emergency_source;

    if (source){/*Decide on num_slabs, currently only for shrinkage*/
    	unsigned long long total = TOTAL_MALLOCED;

        if (total <= mem_limit) {
			/*Not shrinking. just moving*/
			*num_slabs = 1;
        } else {
            /*To hasten the process, this variable can be increased,
             and then there will be less repeating attempts to balance
             the shrinkage across slab classes*/
            unsigned long long mem_gap = TOTAL_MALLOCED - mem_limit;
            const unsigned int minimal_size_for_one_go = 1;
            unsigned long long slabs_gap = ceil_divide(mem_gap, settings.item_size_max);
            if (slabs_gap <= minimal_size_for_one_go)
                *num_slabs = slabs_gap;
            else {

                /*Count the active slab classes, to compute the minimal number of
                 slabs that will be taken from the leading candidate*/
                unsigned int number_of_active_slab_classes = 0;
                for (i = POWER_SMALLEST; i < power_largest; i++) {
                    if (total_pages[i] > 1)/*only those that are eligible*/
                        ++number_of_active_slab_classes;
                }

                /*Compute a conservative bound on the number of slabs to kill
                 from the first class candidate.
                 If all active slab classes are to donate an equal share,
                 this would be it. If one class is a better candidate, then we got it now.
                 Next time we will check again who is a good candidate after we took from
                 the best candidate at least its even share*/

                *num_slabs = ceil_divide(slabs_gap, number_of_active_slab_classes);
                if (number_of_active_slab_classes * *num_slabs < slabs_gap)
                    ++*num_slabs; /*round up - better lose a bit too much from
                     the first class than drag the process long*/

                /*Yet, we will not leave the source slab with less than one slab.
                 This criterion can be fastened, as the distribution of
                 slabs may change over time, and an old slab class can be
                 no longer needed.*/

                if (total_pages[source] - 1 < *num_slabs)
                    *num_slabs = total_pages[source] - 1;
            }
        }

        /*return values*/
        *src = source;
        *dst = dest;
        if (dest)
            return 2;
        else
            return 1;
    } else
        /*By now, if we got no source, then we do not have any class
         with at least two pages, which means the reassignment will
         fail if we use it (unless there is a mechanism to completely
         clearing a class of slabs*/

        *num_slabs = 0;/*Not killing slabs if we do not have a source*/

    return 0;
}

/* Slab rebalancer thread.
 * Does not use spinlocks since it is not timing sensitive. Burn less CPU and
 * go to sleep if locks are contended
 */
static void *slab_maintenance_thread(void *arg) {
    int src, dest, num_slabs;

    while (do_run_slab_thread) {

        bool shrink_now= mem_limit &&  (TOTAL_MALLOCED> mem_limit);

        if (settings.slab_automove || shrink_now) {

            int decision=slab_automove_decision
                (&src, &dest, &num_slabs, shrink_now);
            /* Blind to the return codes. It will retry on its own */

            /*Give precedence to shrinkage over moving*/
            if (shrink_now && decision > 0) {
                /*We do not pass dest here, but rather 0,
                  so that even if a destination was found,
                  shrinkage will happen*/
                slabs_reassign(src, 0, num_slabs);

            }else if (decision == 2) {
                /*Only automove memory when no shrinkage is required,
                  and a pair was found*/
                /*Todo - in angry birds mode, pass a negative src
                  if src was not found ? Or do we cover this in the decision taking?*/

                slabs_reassign(src, dest, num_slabs);
            }

            sleep(DECISION_SECONDS_SHORT);/*It does not have to be the same as in
                                            automove_decision,
                                            but it was probably meant to be no less*/
        } else {
            /* Don't wake as often if we're not enabled.
             * This is lazier than setting up a condition right now. */
            sleep(5);
        }
    }
    return NULL;
}

/* Slab mover thread.
 * Sits waiting for a condition to jump off and shovel some memory about
 */
static void *slab_rebalance_thread(void *arg) {
    int was_busy = 0;

    while (do_run_slab_rebalance_thread) {
        if (slab_rebalance_signal == 1) {
            if (slab_rebalance_start() < 0) {
                /* Handle errors with more specifity as required. */
                slab_rebalance_signal = 0;
            }

            was_busy = 0;
        } else if (slab_rebalance_signal && slab_rebal.slab_start != NULL) {
            was_busy = slab_rebalance_move();
        }

        if (slab_rebal.done) {
            slab_rebalance_finish();
        } else if (was_busy) {
            /* Stuck waiting for some items to unlock, so slow down a bit
             * to give them a chance to free up */
            usleep(50);
        }

        if (slab_rebalance_signal == 0) {
            /* always hold this lock while we're running */
            pthread_cond_wait(&slab_rebalance_cond, &slabs_rebalance_lock);
        }
    }
    return NULL;
}

/* Iterate at most once through the slab classes and pick a "random" source.
 * I like this better than calling rand() since rand() is slow enough that we
 * can just check all of the classes once instead.
 */
static int slabs_reassign_pick_any(int dst) {
    static int cur = POWER_SMALLEST - 1;
    int tries = power_largest - POWER_SMALLEST + 1;
    for (; tries > 0; tries--) {
        cur++;
        if (cur > power_largest)
            cur = POWER_SMALLEST;
        if (cur == dst)
            continue;
        if (slabclass[cur].slabs > 1) {
            return cur;
        }
    }
    return -1;
}

static enum reassign_result_type do_slabs_reassign(int src, int dst, int num_slabs) {
    if (slab_rebalance_signal != 0)
        return REASSIGN_RUNNING;

    if (src == dst)
        return REASSIGN_SRC_DST_SAME;

    /* Special indicator to choose ourselves. */
    if (src == -1) {
        src = slabs_reassign_pick_any(dst);
        /* TODO: If we end up back at -1, return a new error type */
    }

    if (src < POWER_SMALLEST || src > power_largest ||
        (dst!=0 && (dst < POWER_SMALLEST || dst > power_largest)))
        return REASSIGN_BADCLASS;

    if (num_slabs < 1)
        return REASSIGN_KILL_FEW;

    if (slabclass[src].slabs < 1 + num_slabs)
        return REASSIGN_NOSPARE;

    slab_rebal.s_clsid = src;
    slab_rebal.d_clsid = dst;
    slab_rebal.num_slabs = num_slabs;

    slab_rebalance_signal = 1;
    pthread_cond_signal(&slab_rebalance_cond);

    return REASSIGN_OK;
}

enum reassign_result_type slabs_reassign(int src, int dst, int num_slabs) {
    enum reassign_result_type ret;
    if (pthread_mutex_trylock(&slabs_rebalance_lock) != 0) {
        return REASSIGN_RUNNING;
    }
    ret = do_slabs_reassign(src, dst, num_slabs);
    pthread_mutex_unlock(&slabs_rebalance_lock);
    return ret;
}

static pthread_t maintenance_tid;
static pthread_t rebalance_tid;

int start_slab_maintenance_thread(void) {
    int ret;
    slab_rebalance_signal = 0;
    slab_rebal.slab_start = NULL;
    char *env = getenv("MEMCACHED_SLAB_BULK_CHECK");
    if (env != NULL) {
        slab_bulk_check = atoi(env);
        if (slab_bulk_check == 0) {
            slab_bulk_check = DEFAULT_SLAB_BULK_CHECK;
        }
    }

    if (pthread_cond_init(&slab_rebalance_cond, NULL) != 0) {
        fprintf(stderr, "Can't intiialize rebalance condition\n");
        return -1;
    }
    pthread_mutex_init(&slabs_rebalance_lock, NULL);

    if ((ret = pthread_create(&maintenance_tid, NULL,
                              slab_maintenance_thread, NULL)) != 0) {
        fprintf(stderr, "Can't create slab maint thread: %s\n", strerror(ret));
        return -1;
    }
    if ((ret = pthread_create(&rebalance_tid, NULL,
                              slab_rebalance_thread, NULL)) != 0) {
        fprintf(stderr, "Can't create rebal thread: %s\n", strerror(ret));
        return -1;
    }
    return 0;
}

void stop_slab_maintenance_thread(void) {
    mutex_lock(&cache_lock);
    do_run_slab_thread = 0;
    do_run_slab_rebalance_thread = 0;
    pthread_cond_signal(&maintenance_cond);
    pthread_mutex_unlock(&cache_lock);

    /* Wait for the maintenance thread to stop */
    pthread_join(maintenance_tid, NULL);
    pthread_join(rebalance_tid, NULL);
}

/**\return -2 when the requested amount is less than one slab.
   \return -1 when memory is inflexible because it was
   allocated as a single chunk.
   \return non-negative value as the number of slabs that need to be killed to reach this size.*/
long long memory_shrink_expand(const size_t new_mem_limit) {
    print_statm("shrink_expand command");
    if (mem_base != NULL)
        return -1;
    /* We are not using a preallocated large memory chunk */
    if (new_mem_limit < settings.item_size_max)
        return -2;

    pthread_mutex_lock(&slabs_lock);
    size_t old_mem_limit = mem_limit;
    mem_limit = new_mem_limit;/*note that this does not set settings.max`bytes*/
    pthread_mutex_unlock(&slabs_lock);

    unsigned long long total = TOTAL_MALLOCED;
    if (total <= new_mem_limit)
        return 0;

    unsigned long long gap = total - new_mem_limit;
    unsigned long long slabs_gap = ceil_divide(gap, settings.item_size_max);
    fprintf(stdout, "[memory gap: %lld, slabs gap: %lld] "
            "from %.2f MB to %.2f MB when currently using %.2f MB\n", gap, slabs_gap,
            TO_MB(old_mem_limit), TO_MB(new_mem_limit), TO_MB(TOTAL_MALLOCED));
    fflush(stdout);

    return slabs_gap;
}
