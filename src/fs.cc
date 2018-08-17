// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
* Ceph - scalable distributed file system
*
* Copyright (C) 2012 Inktank, Inc.
*
* This is free software; you can redistribute it and/or
* modify it under the terms of the GNU Lesser General Public
* License version 2.1, as published by the Free Software
* Foundation. See file COPYING.
*/
#include <map>
#include <set>
#include <string>

#include <boost/scoped_ptr.hpp>

#include "common/ceph_argparse.h"
#include "os/bluestore/Allocator.h"
#include "common/config.h"
#include "common/errno.h"
#include "common/strtol.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "include/stringify.h"
#include "include/utime.h"
#include "common/Clock.h"
#include "kv/KeyValueDB.h"
#include "common/url_escape.h"

#ifdef HAVE_LIBAIO
#include "os/bluestore/BlueStore.h"
#endif

#define bmap_test_assert(x) assert((x))

using namespace std;

void show_extents(int num, AllocExtentVector & extents)
{
    for (auto& p : extents) {
        cout << "file " << num << " extent: [ offset: " << p.offset << ", length: " << p.length << " ]" << std::endl;
    }
}

int allocate_space(uint64_t size, uint64_t min_alloc_size, Allocator *alloc, AllocExtentVector *extents)
{
    if (alloc->reserve(size) < 0) {
        cout << "reserve error" << std::endl;
        return -1;
    }
    if (alloc->allocate(size, min_alloc_size, size, 0, extents) != (int64_t)size) {
      cout << "allocate error" << std::endl;
      return -1;
    }
    return 0;
}

void free_space(Allocator *alloc, AllocExtentVector &extents)
{
    for (auto& p : extents) {
        alloc->release(p.offset, p.length);
    }
}

int save_space(KeyValueDB *db, const char *prefix, const char *key, AllocExtentVector &extents)
{
    KeyValueDB::Transaction t = db->get_transaction();
    bufferlist value;
    ::encode(extents.size(), value);
    for (auto& p : extents) {
        ::encode(p.offset, value);
        ::encode(p.length, value);
    }
    t->set(prefix, key, value);
    int ret = db->submit_transaction_sync(t);
    value.clear();
    return ret;
}

int load_space(KeyValueDB *db, const char *prefix, const char *key, AllocExtentVector &extents)
{
    bufferlist value;
    int ret = db->get(prefix, key, &value);
    if (ret) {
        cout << "get error" << std::endl;
        return ret;
    }
    size_t size;
    AllocExtent extent;
    bufferlist::iterator p = value.begin();
    ::decode(size, p);
    if (size == 0) {
        cout << "get error" << std::endl;
        return -1;
    }
    for (size_t m = 0; m < size; m++) {
        ::decode(extent.offset, p);
        ::decode(extent.length, p);
        extents.push_back(extent);
    }
    return 0;
}

KeyValueDB* init_db()
{
    stringstream err;
    KeyValueDB *db;
    db = KeyValueDB::create(g_ceph_context, "rocksdb", "/tmp/rocksdb");
    if (!db) {
        cout << "create error" << std::endl;
        return nullptr;
    }
    if (db->open(err)) {
        cout << "open error" << std::endl;
        delete db;
        return nullptr;
    }
    return db;
}

int main(int argc, const char *argv[])
{
    vector<const char*> args;
    argv_to_vec(argc, argv, args);
    env_to_vec(args);

    auto cct = global_init(
        NULL, args,
        CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY_NODOUT, 0);
    common_init_finish(g_ceph_context);

    uint64_t total = 6*1024;
    uint64_t need = 2*1024;
    uint64_t min_alloc_size = 2*1024;
    Allocator *alloc = Allocator::create(g_ceph_context, g_ceph_context->_conf->bluestore_allocator, (int64_t)total, (int64_t)min_alloc_size);
    if (!alloc) {
        cout << "allocator error" << std::endl;
        return -1;
    }
    alloc->init_add_free(0, total);
    cout << "free space: " << alloc->get_free() << std::endl;
    AllocExtentVector preallocate1, preallocate2, preallocate3;
    allocate_space(need, min_alloc_size, alloc, &preallocate1);
    show_extents(1, preallocate1);
    cout << "free space: " << alloc->get_free() << std::endl;
    allocate_space(need, min_alloc_size, alloc, &preallocate2);
    show_extents(2, preallocate2);
    cout << "free space: " << alloc->get_free() << std::endl;
    free_space(alloc, preallocate1);
    need = 4*1024;
    allocate_space(need, min_alloc_size, alloc, &preallocate3);
    show_extents(3, preallocate3);
    alloc->shutdown();
    KeyValueDB *db = init_db();
    save_space(db, "space", "f3", preallocate3);
    preallocate3.clear();
    load_space(db, "space", "f3", preallocate3);
    show_extents(3, preallocate3);
    delete db;
    /*
  const int max_iter = 3;

  for (int round = 0; round < 1; round++) {
    // Test zone of different sizes: 512, 1024, 2048
    int64_t zone_size = 512ull << round;
    ostringstream val;
    val << zone_size;
    g_conf->set_val("bluestore_bitmapallocator_blocks_per_zone", val.str());
    // choose randomized span_size
    int64_t span_size = 512ull << (rand() % 4);
    val.str("");
    val << span_size;
    g_conf->set_val("bluestore_bitmapallocator_span_size", val.str());
    g_ceph_context->_conf->apply_changes(NULL);

    int64_t total_blocks = zone_size * 4;
    int64_t allocated = 0;

    BitAllocator *alloc = new BitAllocator(g_ceph_context, total_blocks,
					   zone_size, CONCURRENT);
    int64_t alloc_size = 2;
    for (int64_t iter = 0; iter < max_iter; iter++) {
      for (int64_t j = 0; alloc_size <= total_blocks; j++) {
        int64_t blk_size = 1024;
        AllocExtentVector extents;
        ExtentList *block_list = new ExtentList(&extents, blk_size, alloc_size);
        for (int64_t i = 0; i < total_blocks; i += alloc_size) {
          bmap_test_assert(alloc->reserve_blocks(alloc_size) == true);
          allocated = alloc->alloc_blocks_dis_res(alloc_size, MIN(alloc_size, zone_size),
                                                  0, block_list);
          bmap_test_assert(alloc_size == allocated);
          bmap_test_assert(block_list->get_extent_count() == 
                           (alloc_size > zone_size? alloc_size / zone_size: 1));
          bmap_test_assert(extents[0].offset == (uint64_t) i * blk_size);
          bmap_test_assert((int64_t) extents[0].length == 
                           ((alloc_size > zone_size? zone_size: alloc_size) * blk_size));
          block_list->reset();
        }
        for (int64_t i = 0; i < total_blocks; i += alloc_size) {
          alloc->free_blocks(i, alloc_size);
        }
        alloc_size = 2 << j; 
      }
    }

    int64_t blk_size = 1024;
    AllocExtentVector extents;

    ExtentList *block_list = new ExtentList(&extents, blk_size);
  
    assert(alloc->reserve_blocks(alloc->size() / 2) == true);
    allocated = alloc->alloc_blocks_dis_res(alloc->size()/2, 1, 0, block_list);
    assert(alloc->size()/2 == allocated);

    block_list->reset();
    assert(alloc->reserve_blocks(1) == true);
    allocated = alloc->alloc_blocks_dis_res(1, 1, 0, block_list);
    assert(allocated == 1);

    alloc->free_blocks(alloc->size()/2, 1);

    block_list->reset();
    assert(alloc->reserve_blocks(1) == true);
    allocated = alloc->alloc_blocks_dis_res(1, 1, 0, block_list);
    bmap_test_assert(allocated == 1);

    bmap_test_assert((int64_t) extents[0].offset == alloc->size()/2 * blk_size);

    delete block_list;
    delete alloc;

  }

  // restore to typical value
  g_conf->set_val("bluestore_bitmapallocator_blocks_per_zone", "1024");
  g_conf->set_val("bluestore_bitmapallocator_span_size", "1024");
  g_ceph_context->_conf->apply_changes(NULL);*/

  cout << "success" << std::endl;

  return 0;
}
