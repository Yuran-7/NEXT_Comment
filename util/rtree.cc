//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <vector>

#include "util/rtree.h"

namespace rocksdb {

    double GetMbrArea(Mbr aa) {
        double width = aa.first.max - aa.first.min;
        double length = aa.second.max - aa.second.min;
        double MbrArea = width * length;

        return MbrArea;
    }

    double GetOverlappingArea(Mbr aa, Mbr bb) {        
        if (!IntersectMbrExcludeIID(aa, bb)) {
            return 0.0;
        } else {
            double width;
            double length;

            width = std::min(aa.first.max, bb.first.max) - std::max(aa.first.min, bb.first.min);
            length = std::min(aa.second.max, bb.second.max) - std::max(aa.second.min, bb.second.min);
            double overlapArea = width * length; 

            return overlapArea;
        }
    }

    bool IntersectMbrExcludeIID(Mbr aa,
                      Mbr bb) {
        // If a bounding region is empty, return true
        // if aa or bb is empty, then may indicate full table scan
        if (aa.empty() || bb.empty()) {
            return true;
        }
        // If the bounding regions don't intersect in one dimension, they won't
        // intersect at all, hence we can return early
        if (aa.first.min > bb.first.max || bb.first.min > aa.first.max) {
            return false;
        }
        if (aa.second.min > bb.second.max || bb.second.min > aa.second.max) {
            return false;
        }
        return true;
    }

    bool IntersectMbr(Mbr aa,
                      Mbr bb) {
        // If a bounding region is empty, return true
        // if aa or bb is empty, then may indicate full table scan
        if (aa.empty() || bb.empty()) {
            return true;
        }

        // If the bounding regions don't intersect in one dimension, they won't
        // intersect at all, hence we can return early
        if (aa.iid.min > bb.iid.max || bb.iid.min > aa.iid.max) {
            return false;
        }
        if (aa.first.min > bb.first.max || bb.first.min > aa.first.max) {
            return false;
        }
        if (aa.second.min > bb.second.max || bb.second.min > aa.second.max) {
            return false;
        }
        return true;
    }

    bool IntersectValRangePoint(ValueRange aa,
                      double bb) {
        // If a bounding region is empty, return true
        // if aa or bb is empty, then may indicate full table scan
        if (aa.empty()) {
            return true;
        }

        // If the bounding regions don't intersect in one dimension, they won't
        // intersect at all, hence we can return early
        if (aa.range.min > bb || bb > aa.range.max) {
            return false;
        }

        return true;
    }

    bool IntersectValRange(ValueRange aa,
                          ValueRange bb) {
        // If a bounding region is empty, return true
        // if aa or bb is empty, then may indicate full table scan
        if (aa.empty() || bb.empty()) {
            return true;
        }

        // If the bounding regions don't intersect in one dimension, they won't
        // intersect at all, hence we can return early
        if (aa.range.min > bb.range.max || bb.range.min > aa.range.max) {
            return false;
        }

        return true;
    }

    void ReadMbrValues(Mbr& mbr, Slice& data) {
        double min = *reinterpret_cast<const double*>(data.data());
        double max = *reinterpret_cast<const double*>(data.data() + 8);
        mbr.set_first(min, max);
        min = *reinterpret_cast<const double*>(data.data() + 16);
        max = *reinterpret_cast<const double*>(data.data() + 24);
        mbr.set_second(min, max);
    }

    Mbr ReadKeyMbr(Slice data) {
        Mbr mbr;
        // In a key the first dimension is a single value only
        const uint64_t iid = *reinterpret_cast<const uint64_t*>(data.data());
        mbr.set_iid(iid, iid);
        data.remove_prefix(sizeof(uint64_t));
        ReadMbrValues(mbr, data);
        return mbr;
    }

    Mbr ReadValueMbr(Slice data) {
        Mbr mbr;
        // The value slice contains the coordinates only
        ReadMbrValues(mbr, data);
        return mbr;
    }

    Mbr ReadQueryMbr(Slice data) {
        Mbr mbr;
        // In a key the first dimension is a single value only
        const uint64_t iid_min = *reinterpret_cast<const uint64_t*>(data.data());
        const uint64_t iid_max =
                *reinterpret_cast<const uint64_t*>(data.data() + sizeof(uint64_t));
        mbr.set_iid(iid_min, iid_max);
        data.remove_prefix(2 * sizeof(uint64_t));
        ReadMbrValues(mbr, data);
        return mbr;
    }

     Mbr ReadSecQueryMbr(Slice data) {
        Mbr mbr;
        // For query on secondary attributes, only MBR is included, no iid
        ReadMbrValues(mbr, data);
        return mbr;
    }   

    ValueRange ReadValueRange(Slice data) {
        ValueRange valrange;
        double min = *reinterpret_cast<const double*>(data.data());
        double max = *reinterpret_cast<const double*>(data.data() + 8); 
        valrange.set_range(min, max);
        return valrange;      
    }

    std::string serializeValueRange(const ValueRange& valrange) {
        std::string serialized;
        serialized.append(reinterpret_cast<const char*>(&valrange.range.min),
                        sizeof(double));
        serialized.append(reinterpret_cast<const char*>(&valrange.range.max),
                        sizeof(double));
        return serialized;
    }

    std::string serializeMbrExcludeIID(const Mbr& mbr) {
        std::string serialized;
        serialized.append(reinterpret_cast<const char*>(&mbr.first.min),
                        sizeof(double));
        serialized.append(reinterpret_cast<const char*>(&mbr.first.max),
                        sizeof(double));
        serialized.append(reinterpret_cast<const char*>(&mbr.second.min),
                        sizeof(double));
        serialized.append(reinterpret_cast<const char*>(&mbr.second.max),
                        sizeof(double));
        return serialized;
    }

    std::string serializeMbr(const Mbr& mbr) {
        std::string serialized;
        serialized.append(reinterpret_cast<const char*>(&mbr.iid.min),
                        sizeof(uint64_t));
        serialized.append(reinterpret_cast<const char*>(&mbr.iid.max),
                        sizeof(uint64_t));
        serialized.append(reinterpret_cast<const char*>(&mbr.first.min),
                        sizeof(double));
        serialized.append(reinterpret_cast<const char*>(&mbr.first.max),
                        sizeof(double));
        serialized.append(reinterpret_cast<const char*>(&mbr.second.min),
                        sizeof(double));
        serialized.append(reinterpret_cast<const char*>(&mbr.second.max),
                        sizeof(double));
        return serialized;
    }

    void expandMbr(Mbr& to_expand, Mbr expander) {  // 把一个 MBR（to_expand）“扩张”为同时覆盖另一个 MBR（expander） 的并集
        if (to_expand.empty()) {
            to_expand = expander;
        } else {
            if (expander.iid.min < to_expand.iid.min) {
                to_expand.iid.min = expander.iid.min;
            }
            if (expander.iid.max > to_expand.iid.max) {
                to_expand.iid.max = expander.iid.max;
            }
            if (expander.first.min < to_expand.first.min) {
                to_expand.first.min = expander.first.min;
            }
            if (expander.first.max > to_expand.first.max) {
                to_expand.first.max = expander.first.max;
            }
            if (expander.second.min < to_expand.second.min) {
                to_expand.second.min = expander.second.min;
            }
            if (expander.second.max > to_expand.second.max) {
                to_expand.second.max = expander.second.max;
            }
        }
  }

    void expandMbrExcludeIID(Mbr& to_expand, Mbr expander) {
        if (to_expand.empty()) {
            to_expand = expander;
        } else {
            if (expander.first.min < to_expand.first.min) {
                to_expand.first.min = expander.first.min;
            }
            if (expander.first.max > to_expand.first.max) {
                to_expand.first.max = expander.first.max;
            }
            if (expander.second.min < to_expand.second.min) {
                to_expand.second.min = expander.second.min;
            }
            if (expander.second.max > to_expand.second.max) {
                to_expand.second.max = expander.second.max;
            }
        }
    }

    void expandSecValueRangeP(ValueRange& to_expand, double expander) {
        if (to_expand.empty()) {
            to_expand.set_range(expander, expander);
        } else {
            if (expander < to_expand.range.min) {
                to_expand.range.min = expander;
            }
            if (expander > to_expand.range.max) {
                to_expand.range.max = expander;
            }
        }
    }

    void expandSecValueRange(ValueRange& to_expand, ValueRange expander) {
        if (to_expand.empty()) {
            to_expand = expander;
        } else {
            if (expander.range.min < to_expand.range.min) {
                to_expand.range.min = expander.range.min;
            }
            if (expander.range.max > to_expand.range.max) {
                to_expand.range.max = expander.range.max;
            }
        }
    }

//   bool GlobalRTreeCallback(std::pair<uint64_t, BlockHandle> index_data) {
//     // nothing to do yet
//     // std::cout << "call back data" << std::endl;
//     (void) index_data;
//     return true;
//   }

}  // namespace rocksdb