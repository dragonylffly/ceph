// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank <info@inktank.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_OSD_HITSET_H
#define CEPH_OSD_HITSET_H

#include <boost/scoped_ptr.hpp>

#include "include/encoding.h"
#include "common/bloom_filter.hpp"
#include "common/hobject.h"
#include "common/Formatter.h"

/**
 * generic container for a HitSet
 *
 * Encapsulate a HitSetImpl of any type.  Expose a generic interface
 * to users and wrap the encoded object with a type so that it can be
 * safely decoded later.
 */

class HitSet {
public:
  typedef enum {
    TYPE_NONE = 0,
    TYPE_EXPLICIT_HASH = 1,
    TYPE_EXPLICIT_OBJECT = 2,
    TYPE_BLOOM = 3
  } impl_type_t;

  static const char *get_type_name(impl_type_t t) {
    switch (t) {
    case TYPE_NONE: return "none";
    case TYPE_EXPLICIT_HASH: return "explicit_hash";
    case TYPE_EXPLICIT_OBJECT: return "explicit_object";
    case TYPE_BLOOM: return "bloom";
    default: return "???";
    }
  }
  const char *get_type_name() const {
    if (impl)
      return get_type_name(impl->get_type());
    return get_type_name(TYPE_NONE);
  }

  /// abstract interface for a HitSet implementation
  class Impl {
  public:
    virtual impl_type_t get_type() const = 0;
    virtual void insert(const hobject_t& o) = 0;
    virtual bool contains(const hobject_t& o) const = 0;
    virtual unsigned insert_count() const = 0;
    virtual unsigned approx_unique_insert_count() const = 0;
    virtual void encode(bufferlist &bl) const = 0;
    virtual void decode(bufferlist::iterator& p) = 0;
    virtual void dump(Formatter *f) const = 0;
    /// optimize structure for a desired false positive probability
    virtual void optimize() {}
    virtual ~Impl() {}
  };

  boost::scoped_ptr<Impl> impl;

  class Params {
  protected:
    impl_type_t type; ///< Type of HitSet
  public:
    class ImplParams {
    public:
      virtual void encode(bufferlist& bl) const = 0;
      virtual void decode(bufferlist::iterator& bl) = 0;
      virtual void dump_stream(ostream& o) const {};
      virtual void dump(Formatter *f) const {}
      virtual ~ImplParams() {}
    };
    boost::scoped_ptr<ImplParams> params;
    Params() : type(TYPE_NONE) {}
    Params(impl_type_t t);
    Params(impl_type_t t, ImplParams *p) : type(t), params(p) {}
    Params(const Params& o);
    Params& operator=(const Params& o);
    void reset_to_type(impl_type_t type);
    impl_type_t get_type() const { return type; }
    void encode(bufferlist &bl) const;
    void decode(bufferlist::iterator &bl);
    void dump(Formatter *f) const;
    static void generate_test_instances(list<HitSet::Params*>& o);
  };
  WRITE_CLASS_ENCODER(Params);

  HitSet() : impl(NULL) {}
  HitSet(Impl *i) : impl(i) {}
  HitSet(HitSet::Params *params);

  HitSet(const HitSet& o) {
    // only allow copying empty instances... FIXME
    assert(!o.impl);
  }

  /// insert a hash into the set
  void insert(const hobject_t& o) {
    impl->insert(o);
  }

  /// query whether a hash is in the set
  bool contains(const hobject_t& o) const {
    return impl->contains(o);
  }

  unsigned insert_count() const {
    return impl->insert_count();
  }
  unsigned approx_unique_insert_count() const {
    return impl->approx_unique_insert_count();
  }
  void optimize() {
    impl->optimize();
  }

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<HitSet*>& o);

private:
  void reset_to_type(impl_type_t type);
};
WRITE_CLASS_ENCODER(HitSet);
WRITE_CLASS_ENCODER(HitSet::Params);
WRITE_CLASS_ENCODER(HitSet::Params::ImplParams);

ostream& operator<<(ostream& out, const HitSet::Params& p);
ostream& operator<<(ostream& out, const HitSet::Params::ImplParams& p);

/**
 * explicitly enumerate hash hits in the set
 */
class ExplicitHashHitSet : public HitSet::Impl {
  uint64_t count;
  hash_set<uint32_t> hits;
public:
  class Params : public HitSet::Params::ImplParams {
  public:
    Params() {}
    ~Params() {}
    void encode(bufferlist& bl) const {
      ENCODE_START(1, 1, bl);
      // empty!
      ENCODE_FINISH(bl);
    }
    void decode(bufferlist::iterator& bl) {
      DECODE_START(1, bl);
      // empty!
      DECODE_FINISH(bl);
    }
    static void generate_test_instances(list<Params*>& o) {
      o.push_back(new Params);
    }
  };

  ExplicitHashHitSet() : count(0) {}
  ExplicitHashHitSet(ExplicitHashHitSet::Params *p) : count(0) {}

  HitSet::impl_type_t get_type() const {
    return HitSet::TYPE_EXPLICIT_HASH;
  }
  void insert(const hobject_t& o) {
    hits.insert(o.hash);
    ++count;
  }
  bool contains(const hobject_t& o) const {
    return hits.count(o.hash);
  }
  unsigned insert_count() const {
    return count;
  }
  unsigned approx_unique_insert_count() const {
    return hits.size();
  }
  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(count, bl);
    ::encode(hits, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START(1, bl);
    ::decode(count, bl);
    ::decode(hits, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const {
    f->dump_unsigned("insert_count", count);
    f->open_array_section("hash_set");
    for (hash_set<uint32_t>::const_iterator p = hits.begin(); p != hits.end(); ++p)
      f->dump_unsigned("hash", *p);
    f->close_section();
  }
  static void generate_test_instances(list<ExplicitHashHitSet*>& o) {
    o.push_back(new ExplicitHashHitSet);
    o.push_back(new ExplicitHashHitSet);
    o.back()->insert(hobject_t());
    o.back()->insert(hobject_t("asdf", "", CEPH_NOSNAP, 123, 1, ""));
    o.back()->insert(hobject_t("qwer", "", CEPH_NOSNAP, 456, 1, ""));
  }
};
WRITE_CLASS_ENCODER(ExplicitHashHitSet)

/**
 * explicitly enumerate objects in the set
 */
class ExplicitObjectHitSet : public HitSet::Impl {
  uint64_t count;
  hash_set<hobject_t> hits;
public:
  class Params : public HitSet::Params::ImplParams {
  public:
    Params() {}
    ~Params() {}
    void encode(bufferlist& bl) const {
      ENCODE_START(1, 1, bl);
      // empty!
      ENCODE_FINISH(bl);
    }
    void decode(bufferlist::iterator& bl) {
      DECODE_START(1, bl);
      // empty!
      DECODE_FINISH(bl);
    }
    static void generate_test_instances(list<Params*>& o) {
      o.push_back(new Params);
    }
  };

  ExplicitObjectHitSet() : count(0) {}
  ExplicitObjectHitSet(ExplicitObjectHitSet::Params *p) : count(0) {}

  HitSet::impl_type_t get_type() const {
    return HitSet::TYPE_EXPLICIT_OBJECT;
  }
  void insert(const hobject_t& o) {
    hits.insert(o);
    ++count;
  }
  bool contains(const hobject_t& o) const {
    return hits.count(o);
  }
  unsigned insert_count() const {
    return count;
  }
  unsigned approx_unique_insert_count() const {
    return hits.size();
  }
  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(count, bl);
    ::encode(hits, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START(1, bl);
    ::decode(count, bl);
    ::decode(hits, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const {
    f->dump_unsigned("insert_count", count);
    f->open_array_section("set");
    for (hash_set<hobject_t>::const_iterator p = hits.begin(); p != hits.end(); ++p) {
      f->open_object_section("object");
      p->dump(f);
      f->close_section();
    }
    f->close_section();
  }
  static void generate_test_instances(list<ExplicitObjectHitSet*>& o) {
    o.push_back(new ExplicitObjectHitSet);
    o.push_back(new ExplicitObjectHitSet);
    o.back()->insert(hobject_t());
    o.back()->insert(hobject_t("asdf", "", CEPH_NOSNAP, 123, 1, ""));
    o.back()->insert(hobject_t("qwer", "", CEPH_NOSNAP, 456, 1, ""));
  }
};
WRITE_CLASS_ENCODER(ExplicitObjectHitSet)

/**
 * use a bloom_filter to track hits to the set
 */
class BloomHitSet : public HitSet::Impl {
  compressible_bloom_filter bloom;

public:
  HitSet::impl_type_t get_type() const {
    return HitSet::TYPE_BLOOM;
  }

  class Params : public HitSet::Params::ImplParams {
  public:
    double false_positive; ///< false positive probability
    uint64_t target_size; ///< number of unique insertions we expect to this HitSet
    uint64_t seed; ///< seed to use when initializing the bloom filter

    Params() : false_positive(0), target_size(0), seed(0) {}
    Params(double fpp, uint64_t t, uint64_t s) :
      false_positive(fpp), target_size(t), seed(s) {}
    ~Params() {}
    void encode(bufferlist& bl) const {
      ENCODE_START(1, 1, bl);
      uint16_t fpp_micro = static_cast<uint16_t>(false_positive * 1000000.0);
      ::encode(fpp_micro, bl);
      ::encode(target_size, bl);
      ::encode(seed, bl);
      ENCODE_FINISH(bl);
    }
    void decode(bufferlist::iterator& bl) {
      DECODE_START(1, bl);
      uint16_t fpp_micro;
      ::decode(fpp_micro, bl);
      false_positive = fpp_micro * 1000000.0;
      ::decode(target_size, bl);
      ::decode(seed, bl);
      DECODE_FINISH(bl);
    }
    void dump(Formatter *f) const {
      f->dump_int("false_positive_probability", false_positive);
      f->dump_int("target_size", target_size);
      f->dump_int("seed", seed);
    }
    void dump_stream(ostream& o) const {
      o << "false_positive_probability: "
	<< false_positive << ", target size: " << target_size
	<< ", seed: " << seed;
    }
    static void generate_test_instances(list<Params*>& o) {
      o.push_back(new Params);
      o.push_back(new Params);
      (*o.rbegin())->false_positive = 10;
      (*o.rbegin())->target_size = 300;
      (*o.rbegin())->seed = 99;
    }
  };

  BloomHitSet() {}
  BloomHitSet(unsigned inserts, double fpp, int seed)
    : bloom(inserts, fpp, seed)
  {}
  BloomHitSet(BloomHitSet::Params *p) : bloom(p->target_size,
                                              // convert to double non-micro
                                              p->false_positive,
                                              p->seed)
  {}

  void insert(const hobject_t& o) {
    bloom.insert(o.hash);
  }
  bool contains(const hobject_t& o) const {
    return bloom.contains(o.hash);
  }
  unsigned insert_count() const {
    return bloom.element_count();
  }
  unsigned approx_unique_insert_count() const {
    return bloom.approx_unique_element_count();
  }
  void optimize() {
    // aim for a density of .5 (50% of bit set)
    double pc = (double)bloom.density() * 2.0 * 100.0;
    bloom.compress(pc);
  }

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(bloom, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START(1, bl);
    ::decode(bloom, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const {
    f->open_object_section("bloom_filter");
    bloom.dump(f);
    f->close_section();
  }
  static void generate_test_instances(list<BloomHitSet*>& o) {
    o.push_back(new BloomHitSet);
    o.push_back(new BloomHitSet(10, .1, 1));
    o.back()->insert(hobject_t());
    o.back()->insert(hobject_t("asdf", "", CEPH_NOSNAP, 123, 1, ""));
    o.back()->insert(hobject_t("qwer", "", CEPH_NOSNAP, 456, 1, ""));
  }
};
WRITE_CLASS_ENCODER(BloomHitSet)

#endif
