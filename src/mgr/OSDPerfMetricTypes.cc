// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "mgr/OSDPerfMetricTypes.h"

#include <ostream>

std::ostream& operator<<(std::ostream& os,
                         const OSDPerfMetricSubKeyDescriptor &d) {
  switch(d.type) {
  case OSDPerfMetric::CLIENT_ID:
    os << "client_id";
    break;
  case OSDPerfMetric::CLIENT_ADDRESS:
    os << "client_address";
    break;
  case OSDPerfMetric::POOL_ID:
    os << "pool_id";
    break;
  case OSDPerfMetric::NAMESPACE:
    os << "namespace";
    break;
  case OSDPerfMetric::OSD_ID:
    os << "osd_id";
    break;
  case OSDPerfMetric::PG_ID:
    os << "pg_id";
    break;
  case OSDPerfMetric::OBJECT_NAME:
    os << "object_name";
    break;
  case OSDPerfMetric::SNAP_ID:
    os << "snap_id";
    break;
  default:
    os << "unknown (" << static_cast<int>(d.type) << ")";
  }
  return os << "~/" << d.regex_str << "/";
}

/*
void PerformanceCounterDescriptor::pack_counter(const PerformanceCounter &c,
                                                bufferlist *bl) const {
  //using ceph::encode;
  ::encode(c.first, *bl);
  switch(type) {
  case OSDPerfMetric::OPS:
  case OSDPerfMetric::WRITE_OPS:
  case OSDPerfMetric::READ_OPS:
  case OSDPerfMetric::BYTES:
  case OSDPerfMetric::WRITE_BYTES:
  case OSDPerfMetric::READ_BYTES:
    break;
  case OSDPerfMetric::LATENCY:
  case OSDPerfMetric::WRITE_LATENCY:
  case OSDPerfMetric::READ_LATENCY:
    ::encode(c.second, *bl);
    break;
  default:
    ceph_abort();
  }
}*/

void PerformanceCounterDescriptor::unpack_counter(
    //bufferlist::const_iterator& bl, PerformanceCounter *c) const {
    bufferlist::iterator& bl, PerformanceCounter *c) {
  //using ceph::decode;
  ::decode(c->first, bl);
  switch(type) {
  case OSDPerfMetric::OPS:
  case OSDPerfMetric::WRITE_OPS:
  case OSDPerfMetric::READ_OPS:
  case OSDPerfMetric::BYTES:
  case OSDPerfMetric::WRITE_BYTES:
  case OSDPerfMetric::READ_BYTES:
    break;
  case OSDPerfMetric::LATENCY:
  case OSDPerfMetric::WRITE_LATENCY:
  case OSDPerfMetric::READ_LATENCY:
    ::decode(c->second, bl);
    break;
  default:
    ceph_abort();
  }
}

std::ostream& operator<<(std::ostream& os,
                         const PerformanceCounterDescriptor &d) {
  switch(d.type) {
  case OSDPerfMetric::OPS:
    return os << "ops";
  case OSDPerfMetric::WRITE_OPS:
    return os << "write ops";
  case OSDPerfMetric::READ_OPS:
    return os << "read ops";
  case OSDPerfMetric::BYTES:
    return os << "bytes";
  case OSDPerfMetric::WRITE_BYTES:
    return os << "write bytes";
  case OSDPerfMetric::READ_BYTES:
    return os << "read bytes";
  case OSDPerfMetric::LATENCY:
    return os << "latency";
  case OSDPerfMetric::WRITE_LATENCY:
    return os << "write latency";
  case OSDPerfMetric::READ_LATENCY:
    return os << "read latency";
  default:
    return os << "unknown (" << static_cast<int>(d.type) << ")";
  }
}

std::ostream& operator<<(std::ostream& os, const OSDPerfMetricLimit &limit) {
  return os << "{order_by=" << limit.order_by << ", max_count="
            << limit.max_count << "}";
}

/*
void OSDPerfMetricQuery::pack_counters(const PerformanceCounters &counters,
//                                       bufferlist *bl) const {
                                       ceph::buffer::list *bl) const {
  auto it = counters.begin();
  for (auto &descriptor : performance_counter_descriptors) {
    if (it == counters.end()) {
      descriptor.pack_counter(PerformanceCounter(), bl);
    } else {
      descriptor.pack_counter(*it, bl);
      it++;
    }
  }
}*/

std::ostream& operator<<(std::ostream& os, const OSDPerfMetricQuery &query) {
  return os << "{key=" << query.key_descriptor << ", counters="
            << query.performance_counter_descriptors << "}";
}
