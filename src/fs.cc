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
#include "os/bluestore/BitAllocator.h"
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

int main(int argc, const char *argv[])
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  auto cct = global_init(
      NULL, args,
      CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);


  const int max_iter = 3;

  for (int round = 0; round < 3; round++) {
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
  g_ceph_context->_conf->apply_changes(NULL);

  cout << "success" << std::endl;

  return 0;
}
