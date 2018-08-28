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
#include "os/bluestore/FreelistManager.h"
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
#include "os/kv.h"

#ifdef HAVE_LIBAIO
#include "os/bluestore/BlueStore.h"
#endif

#define bmap_test_assert(x) assert((x))
#define MB (1024*1024)
#define SUPER_RESERVED  8192
#define DISK_SIZE 8*MB
#define ALLOCATE_UNIT   2*MB

using namespace std;

const string PREFIX_SUPER = "S";   // field -> value
const string PREFIX_STAT = "T";    // field -> value(int64 array)
const string PREFIX_COLL = "C";    // collection name -> cnode_t
const string PREFIX_OBJ = "O";     // object name -> onode_t
const string PREFIX_OMAP = "M";    // u64 + keyname -> value
const string PREFIX_DEFERRED = "L";  // id -> deferred_transaction_t
const string PREFIX_ALLOC = "B";   // u64 offset -> u64 length (freelist)
const string PREFIX_SHARED_BLOB = "X"; // u64 offset -> shared_blob_t
const string PREFIX_METADATA = "BLOBMD";

typedef struct {
    std::string name;
    uint64_t size;
    AllocExtentVector extents;
} FileMetadata;

KeyValueDB* init_db();
KeyValueDB* create_db();
FreelistManager *create_fm(KeyValueDB *db);
Allocator *init_allocator(FreelistManager *fm);
FreelistManager *init_fm(KeyValueDB *db);
void close_allocator(Allocator *alloc);
void close_fm(FreelistManager *fm);
void close_db(KeyValueDB *db);

class MetadataService {
public:
    Allocator *alloc;
    KeyValueDB *db;
    FreelistManager *fm;

    void create()
    {
        db = create_db();
        fm = create_fm(db);
        alloc = init_allocator(fm);
    }

    void init()
    {
        db = init_db();
        fm = init_fm(db);
        alloc = init_allocator(fm);
    }

    void close()
    {
        close_allocator(alloc);
        close_fm(fm);
        close_db(db);
    }
};


void show_metadata(FileMetadata &meta)
{
    for (auto& p : meta.extents) {
        cout << "file: " << meta.name << " extent: [ offset: " << p.offset << ", length: " << p.length << " ]" << std::endl;
    }
}

int allocate_space(MetadataService &mds, FileMetadata &meta, uint64_t min_alloc_size = ALLOCATE_UNIT)
{
    if (mds.alloc->reserve(meta.size) < 0) {
        cout << "reserve error" << std::endl;
        return -1;
    }
    if (mds.alloc->allocate(meta.size, min_alloc_size, meta.size, 0, &meta.extents) != (int64_t)meta.size) {
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

int save_metadata(MetadataService &mds, FileMetadata &meta)
{
    KeyValueDB::Transaction t = mds.db->get_transaction();
    bufferlist value;
    ::encode(meta.size, value);
    ::encode(meta.extents.size(), value);
    for (auto& p : meta.extents) {
        ::encode(p.offset, value);
        ::encode(p.length, value);
        mds.fm->allocate(p.offset, p.length, t);
    }
    t->set(PREFIX_METADATA, meta.name, value);
    int ret = mds.db->submit_transaction_sync(t);
    value.clear();
    
    return ret;
}

int delete_metadata(MetadataService &mds, FileMetadata &meta)
{
    KeyValueDB::Transaction t = mds.db->get_transaction();
    t->rmkey(PREFIX_METADATA, meta.name);
    for (auto& p : meta.extents) {
        mds.alloc->release(p.offset, p.length);
        mds.fm->release(p.offset, p.length, t);
    }
    int ret = mds.db->submit_transaction_sync(t);
       
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

int load_metadata(MetadataService     &mds, FileMetadata &meta)
{
    bufferlist value;
    int ret = mds.db->get(PREFIX_METADATA, meta.name, &value);
    if (ret) {
        cout << "get error" << std::endl;
        return ret;
    }
    AllocExtent extent;
    size_t len;
    bufferlist::iterator p = value.begin();
    ::decode(meta.size, p);
    ::decode(len, p);
    if (len == 0) {
        cout << "get error" << std::endl;
        return -1;
    }
    for (size_t m = 0; m < len; m++) {
        ::decode(extent.offset, p);
        ::decode(extent.length, p);
        meta.extents.push_back(extent);
    }
    return 0;
}


struct _Int64ArrayMergeOperator : public KeyValueDB::MergeOperator {
  void merge_nonexistent(
    const char *rdata, size_t rlen, std::string *new_value) override {
    *new_value = std::string(rdata, rlen);
  }
  void merge(
    const char *ldata, size_t llen,
    const char *rdata, size_t rlen,
    std::string *new_value) override {
    assert(llen == rlen);
    assert((rlen % 8) == 0);
    new_value->resize(rlen);
    const __le64* lv = (const __le64*)ldata;
    const __le64* rv = (const __le64*)rdata;
    __le64* nv = &(__le64&)new_value->at(0);
    for (size_t i = 0; i < rlen >> 3; ++i) {
      nv[i] = lv[i] + rv[i];
    }
  }
  // We use each operator name and each prefix to construct the
  // overall RocksDB operator name for consistency check at open time.
  string name() const override {
    return "int64_array";
  }
};


KeyValueDB* create_db()
{
    stringstream err;
    KeyValueDB *db;
    db = KeyValueDB::create(g_ceph_context, "rocksdb", "/tmp/rocksdb");
    if (!db) {
        cout << "create error" << std::endl;
        return nullptr;
    }
    FreelistManager::setup_merge_operators(db);
    ceph::shared_ptr<_Int64ArrayMergeOperator> merge_op(new _Int64ArrayMergeOperator);
    db->set_merge_operator(PREFIX_STAT, merge_op);    
    db->init(g_ceph_context->_conf->bluestore_rocksdb_options);

    if (db->create_and_open(err)) {
        cout << "create error" << std::endl;
        delete db;
        return nullptr;
    }
    return db;
}

void close_db(KeyValueDB *db)
{
    delete db;
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
    FreelistManager::setup_merge_operators(db);
    ceph::shared_ptr<_Int64ArrayMergeOperator> merge_op(new _Int64ArrayMergeOperator);
    db->set_merge_operator(PREFIX_STAT, merge_op);    
    db->init(g_ceph_context->_conf->bluestore_rocksdb_options);

    if (db->open(err)) {
        cout << "open error" << std::endl;
        delete db;
        return nullptr;
    }
    return db;
}

void _make_offset_key(uint64_t offset, std::string *key)
{
  key->reserve(10);
  _key_encode_u64(offset, key);
}

FreelistManager *create_fm(KeyValueDB *db)
{
    FreelistManager *fm = FreelistManager::create(g_ceph_context, "bitmap", db, PREFIX_ALLOC);
    KeyValueDB::Transaction t = db->get_transaction();
    {
        bufferlist bl;
        bl.append("bitmap");
        t->set(PREFIX_SUPER, "freelist_type", bl);
    }
    fm->create(DISK_SIZE, ALLOCATE_UNIT, t);
    uint64_t reserved = ROUND_UP_TO(MAX(SUPER_RESERVED, ALLOCATE_UNIT),
                        ALLOCATE_UNIT);
    cout << "reserved" << reserved << std::endl;
    fm->allocate(0, reserved, t);
    if (db->submit_transaction_sync(t)) {
        cout << "db create error" << std::endl;
        return nullptr;
    }
    {
        uint64_t first_key = 0;
        string k;
        _make_offset_key(first_key, &k);
        bufferlist bl;
        int ret = db->get(std::string("b"), k, &bl);
        cout << "ret: " << ret << " length: " << bl.length() << std::endl;
        cout << " 0x" << std::hex << first_key << std::dec << ": ";
        bl.hexdump(cout, false);
        cout << std::endl;
    }
    return fm;
}

void close_fm(FreelistManager *fm)
{
    fm->shutdown();
    delete fm;
}

FreelistManager *init_fm(KeyValueDB *db)
{
    FreelistManager *fm = FreelistManager::create(g_ceph_context, "bitmap", db, PREFIX_ALLOC);
    int r = fm->init(DISK_SIZE);
     if (r < 0) {
       cout << "init fm error" << std::endl;
       delete fm;
       return nullptr;
     }
    return fm;
}

void test_fm()
{
    KeyValueDB *db;
    FreelistManager *fm; 

    db = init_db();
    fm = init_fm(db);
    {
        uint64_t first_key = 0;
        string k;
        _make_offset_key(first_key, &k);
        bufferlist bl;
        int ret = db->get(std::string("b"), k, &bl);
        cout << "ret: " << ret << " length: " << bl.length() << std::endl;
        cout << " 0x" << std::hex << first_key << std::dec << ": ";
        bl.hexdump(cout, false);
        cout << std::endl;
    }
    close_fm(fm);
    close_db(db);
}

Allocator *init_allocator(FreelistManager *fm)
{
    Allocator *alloc = Allocator::create(g_ceph_context, g_ceph_context->_conf->bluestore_allocator, DISK_SIZE, ALLOCATE_UNIT);
    if (!alloc) {
        cout << "allocator error" << std::endl;
        return nullptr;
    }

    uint64_t num = 0, bytes = 0;

    // initialize from freelist
    fm->enumerate_reset();
    uint64_t offset, length;
    while (fm->enumerate_next(&offset, &length)) {
        cout << "(" << offset << " , " << length << ")" << std::endl;
        alloc->init_add_free(offset, length);
        ++num;
        bytes += length;
    }
    fm->enumerate_reset();
    //cout << "free " << bytes/MB << " MB" << std::endl;
    return alloc;
}

void close_allocator(Allocator *alloc) 
{
    alloc->shutdown();
    delete alloc;
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

    MetadataService mds;

    if (argc == 2 && !strcmp(argv[1], "create")) {
        mds.create();
        mds.close();
        cout << "success" << std::endl;
        test_fm();
        return 0;
    }
    mds.init();
    cout << "free space: " << mds.alloc->get_free() << std::endl;
    FileMetadata meta;
    meta.name = "f1";
    meta.size = 2*MB;
    allocate_space(mds, meta);
    save_metadata(mds, meta);
    cout << "free space: " << mds.alloc->get_free() << std::endl;
    meta.name = "f2";
    meta.extents.clear();
    allocate_space(mds, meta);
    save_metadata(mds, meta);
    cout << "free space: " << mds.alloc->get_free() << std::endl;
    meta.name = "f1";
    meta.extents.clear();
    load_metadata(mds, meta);
    delete_metadata(mds, meta);
    cout << "free space: " << mds.alloc->get_free() << std::endl;
    meta.name = "f3";
    meta.size = 4*MB;
    meta.extents.clear();
    allocate_space(mds, meta);
    save_metadata(mds, meta);
    cout << "free space: " << mds.alloc->get_free() << std::endl;
    mds.close();
    meta.size = 0;
    meta.extents.clear();
    mds.init();
    load_metadata(mds, meta);
    cout << "size: " << meta.size << std::endl;
    show_metadata(meta);
    mds.close();
    
    cout << "success" << std::endl;

    return 0;
}
