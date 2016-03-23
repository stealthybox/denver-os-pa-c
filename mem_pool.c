
#include <stdlib.h>
#include <assert.h>
#include <stdio.h> // for perror()

#include "mem_pool.h"

/*************/
/*           */
/* Constants */
/*           */
/*************/
static const float      MEM_FILL_FACTOR                 = 0.75;
static const unsigned   MEM_EXPAND_FACTOR               = 2;

static const unsigned   MEM_POOL_STORE_INIT_CAPACITY    = 20;
static const float      MEM_POOL_STORE_FILL_FACTOR      = 0.75;
static const unsigned   MEM_POOL_STORE_EXPAND_FACTOR    = 2;

static const unsigned   MEM_NODE_HEAP_INIT_CAPACITY     = 40;
static const float      MEM_NODE_HEAP_FILL_FACTOR       = 0.75;
static const unsigned   MEM_NODE_HEAP_EXPAND_FACTOR     = 2;

static const unsigned   MEM_GAP_IX_INIT_CAPACITY        = 40;
static const float      MEM_GAP_IX_FILL_FACTOR          = 0.75;
static const unsigned   MEM_GAP_IX_EXPAND_FACTOR        = 2;

/*********************/
/*                   */
/* Type declarations */
/*                   */
/*********************/
typedef struct _node {
    alloc_t alloc_record;
    unsigned used;
    unsigned allocated;
    struct _node *next, *prev; // doubly-linked list for gap deletion
} node_t, *node_pt;

typedef struct _gap {
    size_t size;
    node_pt node;
} gap_t, *gap_pt;

typedef struct _pool_mgr {
    pool_t pool;
    node_pt node_heap;
    unsigned total_nodes;
    unsigned used_nodes;
    gap_pt gap_ix;
    unsigned gap_ix_capacity;
} pool_mgr_t, *pool_mgr_pt;

/***************************/
/*                         */
/* Static global variables */
/*                         */
/***************************/
static pool_mgr_pt *pool_store = NULL; // an array of pointers, only expand
static unsigned pool_store_size = 0;
static unsigned pool_store_capacity = 0;

/********************************************/
/*                                          */
/* Forward declarations of static functions */
/*                                          */
/********************************************/
static alloc_status _mem_resize_pool_store();
static alloc_status _mem_resize_node_heap(pool_mgr_pt pool_mgr);
static alloc_status _mem_resize_gap_ix(pool_mgr_pt pool_mgr);
static alloc_status _mem_add_to_gap_ix(pool_mgr_pt pool_mgr, size_t size, node_pt node);
static alloc_status _mem_remove_from_gap_ix(pool_mgr_pt pool_mgr, size_t size, node_pt node);
static alloc_status _mem_sort_gap_ix(pool_mgr_pt pool_mgr);

/****************************************/
/*                                      */
/* Definitions of user-facing functions */
/*                                      */
/****************************************/
alloc_status mem_init() {
    // ensure that it's called only once until mem_free
    if(pool_store != NULL) {
        return ALLOC_CALLED_AGAIN;
    }
        // allocate the pool store with initial capacity
    else {
        pool_store = calloc(MEM_POOL_STORE_INIT_CAPACITY, sizeof(pool_mgr_pt));
        pool_store_size = 0;
        pool_store_capacity = MEM_POOL_STORE_INIT_CAPACITY;
        return ALLOC_OK;
    }
}

alloc_status mem_free() {
    // ensure that it's called only once for each mem_init
    if(pool_store == NULL) {
        return ALLOC_CALLED_AGAIN;
    }
    // make sure all pool managers have been deallocated
    for(int i = 0; i < pool_store_size; i++) {
        if(pool_store[i] != NULL) {
            return ALLOC_FAIL;
        }
    }
    // can free the pool store array
    free(pool_store);
    // update static variables
    pool_store = NULL;
    pool_store_size = 0;
    pool_store_capacity = 0;
    return ALLOC_OK;
}

pool_pt mem_pool_open(size_t size, alloc_policy policy) {
    // make sure there the pool store is allocated
    if(pool_store == NULL) {
        return NULL;
    }
    // expand the pool store, if necessary
    _mem_resize_pool_store();
    // allocate a new mem pool mgr
    pool_mgr_pt pool_mgr = malloc( sizeof(pool_mgr_t) );
    // check success, on error return null
    if(pool_mgr == NULL) {
        return NULL;
    }
    // allocate a new memory pool
    pool_mgr->pool.mem = calloc( size, sizeof(char) );
    // check success, on error deallocate mgr and return null
    if(pool_mgr->pool.mem == NULL) {
        free(pool_mgr);
        return NULL;
    }
    // allocate a new node heap and gap index
    pool_mgr->node_heap = calloc(MEM_NODE_HEAP_INIT_CAPACITY, sizeof(node_t));
    pool_mgr->gap_ix = calloc(MEM_GAP_IX_INIT_CAPACITY, sizeof(gap_t));
    // check success, on error deallocate mgr/pool/heap and return null
    if(pool_mgr->gap_ix == NULL || pool_mgr->gap_ix == NULL) {
        free(pool_mgr->node_heap);
        free(pool_mgr->gap_ix);
        free(pool_mgr->pool.mem);
        free(pool_mgr);
        return NULL;
    }
    // assign all the pointers and update meta data:
    // initialize top node of node heap
    pool_mgr->node_heap[0].alloc_record.size = size;
    pool_mgr->node_heap[0].alloc_record.mem = pool_mgr->pool.mem;
    pool_mgr->node_heap[0].used = 1;
    pool_mgr->node_heap[0].allocated = 0;
    pool_mgr->used_nodes = 1;
    pool_mgr->total_nodes = MEM_NODE_HEAP_INIT_CAPACITY;
    // initialize top node of gap index
    pool_mgr->gap_ix[0].node = &pool_mgr->node_heap[0];
    pool_mgr->gap_ix[0].size = pool_mgr->node_heap[0].alloc_record.size;
    pool_mgr->gap_ix_capacity = MEM_GAP_IX_INIT_CAPACITY;
    // initialize pool mgr
    pool_mgr->pool.policy = policy;
    pool_mgr->pool.total_size = size;
    pool_mgr->pool.alloc_size = 0;
    pool_mgr->pool.num_allocs = 0;
    pool_mgr->pool.num_gaps = 1;
    // link pool mgr to pool store
    pool_store[pool_store_size] = pool_mgr;
    pool_store_size++;
    // return the address of the mgr, cast to (pool_pt)
    return (pool_pt) pool_mgr;
}

alloc_status mem_pool_close(pool_pt pool) {
    // get mgr from pool by casting the pointer to (pool_mgr_pt)
    const pool_mgr_pt pool_mgr = (pool_mgr_pt) pool;
    // check if this pool is allocated
    if(pool_mgr == NULL) {
        return ALLOC_FAIL;
    }
    // check if pool has only one gap
    if(pool_mgr->pool.num_gaps != 1) {
        return ALLOC_NOT_FREED;
    }
    // check if it has zero allocations
    if(pool_mgr->pool.num_allocs != 0) {
        return ALLOC_NOT_FREED;
    }
    // free memory pool
    free(pool->mem);
    // free node heap
    free(pool_mgr->node_heap);
    pool_mgr->node_heap = NULL;
    // free gap index
    free(pool_mgr->gap_ix);
    pool_mgr->gap_ix = NULL;
    // find mgr in pool store and set to null
    for(int i = 0; i < pool_store_capacity; i++) {
        if(pool_store[i] == pool_mgr) {
            pool_store[i] = NULL;
            i = pool_store_capacity;
        }
    }
    // free mgr
    free(pool_mgr);
    return ALLOC_OK;
}

alloc_pt mem_new_alloc(pool_pt pool, size_t size) {
    // get mgr from pool by casting the pointer to (pool_mgr_pt)
    const pool_mgr_pt pool_mgr = (pool_mgr_pt) pool;
    // check if any gaps, return null if none
    if(pool_mgr->pool.num_gaps == 0) {
        return NULL;
    }
    // expand heap node, if necessary, quit on error
    _mem_resize_node_heap(pool_mgr);
    // check used nodes fewer than total nodes, quit on error
    // get a node for allocation:
    node_pt a_node = NULL;
    // if FIRST_FIT, then find the first sufficient node in the node heap
    if(pool_mgr->pool.policy == FIRST_FIT) {
        for(int i = 0; i < pool_mgr->total_nodes; i++) {
            a_node = &pool_mgr->node_heap[i];
            if(a_node->used && !a_node->allocated &&
               a_node->alloc_record.size >= size) {
                i = pool_mgr->total_nodes;
            }
        }
        if(a_node->alloc_record.size < size) {
            a_node = NULL;
        }
    }
        // BEST_FIT, then find the first sufficient node in the gap index
    else if(pool_mgr->pool.policy == BEST_FIT) {
        for(int j = 0; j < pool_mgr->gap_ix_capacity; j++) {
            if(pool_mgr->gap_ix[j].size >= size) {
                a_node = pool_mgr->gap_ix[j].node;
                j = pool_mgr->gap_ix_capacity;
            }
        }
    }
    // check if node found
    if(a_node == NULL) {
        return NULL;
    }
    // update metadata (num_allocs, alloc_size)
    pool_mgr->pool.num_allocs++;
    pool_mgr->pool.alloc_size += size;
    // calculate the size of the remaining gap, if any
    size_t remaining_gap_size = a_node->alloc_record.size - size;
    // remove node from gap index
    _mem_remove_from_gap_ix(pool_mgr, size, a_node);
    // convert gap_node to an allocation node of given size
    a_node->allocated = 1;
    a_node->alloc_record.size = size;
    // adjust node heap:
    // if remaining gap, need a new node
    if(remaining_gap_size != 0) {
        node_pt unusedNode = NULL;
        // find an unused one in the node heap
        for(int k = 0; k < pool_mgr->total_nodes; k++) {
            if(pool_mgr->node_heap[k].used == 0) {
                unusedNode = &pool_mgr->node_heap[k];
                k = pool_mgr->total_nodes;
            }
        }
        // make sure one was found
        if(unusedNode == NULL) {
            return NULL;
        }
        // initialize it to a gap node
        unusedNode->alloc_record.size = remaining_gap_size;
        unusedNode->alloc_record.mem = a_node->alloc_record.mem + size;
        unusedNode->allocated = 0;
        unusedNode->used = 1;
        // update metadata (used_nodes)
        pool_mgr->used_nodes++;
        // update linked list (new node right after the node for allocation)
        unusedNode->prev = a_node;
        unusedNode->next = a_node->next;
        if(a_node->next != NULL) {
            (*a_node->next).prev = unusedNode;
        }
        a_node->next = unusedNode;
        // add to gap index
        _mem_add_to_gap_ix(pool_mgr, remaining_gap_size, unusedNode);
        // check if successful
    }
    // return allocation record by casting the node to (alloc_pt)
    return (alloc_pt) a_node;
}

alloc_status mem_del_alloc(pool_pt pool, alloc_pt alloc)
{
    // get mgr from pool by casting the pointer to (pool_mgr_pt)
    pool_mgr_pt pool_mgr = (pool_mgr_pt) pool;
    // get node from alloc by casting the pointer to (node_pt)
    node_pt deleteNode = (node_pt) alloc;
    // convert to gap node
    deleteNode->allocated = 0;
    // update metadata (num_allocs, alloc_size)
    pool_mgr->pool.num_allocs--;
    pool_mgr->pool.alloc_size -= alloc->size;
    //the next node in the list is also a gap, merge into node-to-delete
    if(deleteNode->next != NULL && (*deleteNode->next).allocated == 0) {
        // remove the next node from gap index
        _mem_remove_from_gap_ix(pool_mgr, (*deleteNode->next).alloc_record.size, deleteNode->next);
        // add the size to the node-to-delete
        alloc->size += (*deleteNode->next).alloc_record.size;
        // update node as unused
        (*deleteNode->next).used = 0;
        (*deleteNode->next).alloc_record.size = 0;
        (*deleteNode->next).alloc_record.mem = NULL;
        // update metadata (used nodes)
        pool_mgr->used_nodes--;
        // update linked list:
        // there exists a node after the next node
        if((*deleteNode->next).next != NULL) {
            (*(*deleteNode->next).next).prev = deleteNode;
            deleteNode->next = (*deleteNode->next).next;
        }
            // the next node is the last node
        else {
            (*deleteNode->next).prev = NULL;
            deleteNode->next = NULL;
        }
    }
    // this merged node-to-delete might need to be added to the gap index
    // but one more thing to check...
    // the previous node in the list is also a gap, merge into previous!
    if(deleteNode->prev && (*deleteNode->prev).allocated == 0) {
        node_pt prev_node = deleteNode->prev;
        // remove the previous node from gap index
        alloc_status remove_status = _mem_remove_from_gap_ix(pool_mgr, prev_node->alloc_record.size, prev_node);
        // check success
        if(remove_status == ALLOC_FAIL) {
            return ALLOC_FAIL;
        }
        // add the size of node-to-delete to the previous
        prev_node->alloc_record.size += alloc->size;

        // update node-to-delete as unused
        deleteNode->used = 0;
        deleteNode->alloc_record.size = 0;
        deleteNode->alloc_record.mem = NULL;

        // update metadata (used_nodes)
        pool_mgr->used_nodes--;
        // update linked list
        // Delete around this node
        if(deleteNode->next != NULL) {
            prev_node->next = deleteNode->next;
            (*deleteNode->next).prev = prev_node;
        }
            // Delete this node
        else {
            prev_node->next = NULL;
        }
        deleteNode->next = NULL;
        deleteNode->prev = NULL;
        // change the node to add to the previous node!
        deleteNode = prev_node;
    }
    // add the resulting node to the gap index
    alloc_status add_status = _mem_add_to_gap_ix(pool_mgr, deleteNode->alloc_record.size, deleteNode);
    // check success
    return add_status;
}

void mem_inspect_pool(pool_pt pool, pool_segment_pt *segments, unsigned *num_segments) {
    // get the mgr from the pool
    pool_mgr_pt pool_mgr = (pool_mgr_pt) pool;
    // allocate the segments array with size == used_nodes
    pool_segment_pt sgmts = (pool_segment_pt)
            calloc(pool_mgr->used_nodes, sizeof(pool_segment_t));
    // check successful
    if(sgmts == NULL) {
        return;
    }
    node_pt thisNode = pool_mgr->node_heap;
    int i = 0;
    // loop through the node heap and the segments array
    while(thisNode != NULL) {
        // for each node, write the size and allocated in the segment
        sgmts[i].size = thisNode->alloc_record.size;
        sgmts[i].allocated = thisNode->allocated;
        i++;
        thisNode = thisNode->next;
    }
    // "return" the values:
    *segments = sgmts;
    *num_segments = pool_mgr->used_nodes;

}

/***********************************/
/*                                 */
/* Definitions of static functions */
/*                                 */
/***********************************/
static alloc_status _mem_resize_pool_store() {
    float used_size_percent = pool_store_size / pool_store_capacity;
    // pool_store is getting full and needs to expand
    if (used_size_percent > MEM_POOL_STORE_FILL_FACTOR) {
        unsigned new_cap = MEM_POOL_STORE_EXPAND_FACTOR*pool_store_capacity;
        pool_store = (pool_mgr_pt*) realloc(pool_store, new_cap * sizeof(pool_mgr_pt));
        pool_store_capacity = new_cap;
    }
    return ALLOC_OK;
}

static alloc_status _mem_resize_node_heap(pool_mgr_pt pool_mgr) {
    float used_nodes_percent = pool_mgr->used_nodes / pool_mgr->total_nodes;
    // node_heap is getting full and needs to expand
    if (used_nodes_percent > MEM_NODE_HEAP_FILL_FACTOR) {
        unsigned new_cap = MEM_NODE_HEAP_EXPAND_FACTOR*pool_mgr->total_nodes;
        pool_mgr->node_heap = (node_pt) realloc(pool_mgr->node_heap, new_cap * sizeof(node_t));
        pool_mgr->total_nodes = new_cap;
    }
    return ALLOC_OK;
}

static alloc_status _mem_resize_gap_ix(pool_mgr_pt pool_mgr) {
    float gaps_active_percent = pool_mgr->pool.num_gaps / pool_mgr->gap_ix_capacity;
    // gap_ix is getting full and needs to expand
    if (gaps_active_percent > MEM_GAP_IX_FILL_FACTOR) {
        unsigned new_cap = MEM_GAP_IX_EXPAND_FACTOR*pool_mgr->gap_ix_capacity;
        pool_mgr->gap_ix = (gap_pt) realloc(pool_mgr->gap_ix, new_cap * sizeof(gap_t));
        pool_mgr->gap_ix_capacity = new_cap;
    }
    return ALLOC_OK;
}

static alloc_status _mem_add_to_gap_ix(pool_mgr_pt pool_mgr, size_t size, node_pt node) {
    // expand the gap index, if necessary (call the function)
    _mem_resize_gap_ix(pool_mgr);
    // add the entry at the end
    gap_pt new_gap = &pool_mgr->gap_ix[pool_mgr->pool.num_gaps];
    new_gap->size = size;
    new_gap->node = node;
    // update metadata (num_gaps)
    pool_mgr->pool.num_gaps++;
    // sort the gap index (call the function)
    alloc_status stat = _mem_sort_gap_ix(pool_mgr);
    // check success
    if(stat == ALLOC_OK) {
        return ALLOC_OK;
    }
    return ALLOC_FAIL;
}

static alloc_status _mem_remove_from_gap_ix(pool_mgr_pt pool_mgr, size_t size, node_pt node) {
    int pos = -1;
    // find the position of the node in the gap index
    for(int i = 0; i < pool_mgr->gap_ix_capacity; i++) {
        if(pool_mgr->gap_ix[i].node == node) {
            pos = i;
            i = pool_mgr->gap_ix_capacity;
        }
    }
    // didn't find the node in the gap index
    if(pos < 0) {
        return ALLOC_FAIL;
    }
    // loop from there to the end of the array:
    // pull the entries (i.e. copy over) one position up
    // this effectively deletes the chosen node
    for(int j=pos; j < pool_mgr->gap_ix_capacity; j++) {
        pool_mgr->gap_ix[j] = pool_mgr->gap_ix[j + 1];
    }
    // update metadata (num_gaps)
    pool_mgr->pool.num_gaps--;
    // zero out the element at position num_gaps!
    gap_pt delete = &pool_mgr->gap_ix[pool_mgr->pool.num_gaps];
    delete->size = 0;
    delete->node = NULL;
    return ALLOC_OK;
}

// note: only called by _mem_add_to_gap_ix, which appends a single entry
static alloc_status _mem_sort_gap_ix(pool_mgr_pt pool_mgr) {
    // the new entry is at the end, so "bubble it up"
    // loop from num_gaps - 1 until but not including 0:
    for(int i = pool_mgr->pool.num_gaps - 1; i > 0; i--) {
        // if the size of the current entry is less than the previous (u - 1)
        // swap them (by copying)
        if(pool_mgr->gap_ix[i].size < pool_mgr->gap_ix[i - 1].size) {
            gap_t temp = pool_mgr->gap_ix[i];
            pool_mgr->gap_ix[i] = pool_mgr->gap_ix[i - 1];
            pool_mgr->gap_ix[i - 1] = temp;
        }
        // or if the sizes are the same
        // a node with a lower address of pool allocation address (mem)
        else if(pool_mgr->gap_ix[i].size == pool_mgr->gap_ix[i - 1].size) {
            // and the current entry points to a node with a lower address
            // of pool allocation address (mem)
            // swap them (by copying)
            if((*pool_mgr->gap_ix[i].node).alloc_record.mem < (*pool_mgr->gap_ix[i - 1].node).alloc_record.mem) {
                gap_t temp1 = pool_mgr->gap_ix[i];
                pool_mgr->gap_ix[i] = pool_mgr->gap_ix[i - 1];
                pool_mgr->gap_ix[i - 1] = temp1;
            }
        }
    }
    return ALLOC_OK;
}


