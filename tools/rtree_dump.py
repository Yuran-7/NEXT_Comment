#!/usr/bin/env python3
"""
rtree_dump.py

用途:
  给出一个 RTree 的二进制保存文件 (由 C++ RTree_mem.h 的 Save / SaveRec 生成),
  解析并以可读形式打印树结构 (分层), value 按 GlobalSecIndexValue 解释。

RTree (模板参数) 在当前工程中实例化: RTree<GlobalSecIndexValue, double, 1 or 2, double>
我们只需要处理 NUMDIMS = 1 或 2 (可扩展)。脚本会自动根据文件头 NUMDIMS 读取。

二进制格式 (参考 RTree_mem.h 中 Save/SaveRec):
Header 顺序写入 7 个 int (32-bit little endian):
  1. magic: 'R'<<0 | 'T'<<8 | 'R'<<16 | 'E'<<24  (ASCII: RTRE -> 十六进制 0x45525452)
  2. sizeof(DATATYPE)
  3. NUMDIMS
  4. sizeof(ELEMTYPE)
  5. sizeof(ELEMTYPEREAL)
  6. TMAXNODES
  7. TMINNODES
随后递归保存节点:
  对每个节点: (int m_level) (int m_count)
    若内部节点: 对每个分支 i (0..m_count-1):
        写 m_rect.m_min[NUMDIMS] (ELEMTYPE array)
        写 m_rect.m_max[NUMDIMS]
        递归写子节点
    若叶子节点: 对每个分支 i:
        写 m_rect.m_min[]
        写 m_rect.m_max[]
        写 m_data (DATATYPE)

GlobalSecIndexValue 定义:
  int id;
  uint64_t filenum;
  BlockHandle blkhandle; // 结构为两个 uint64_t: offset_, size_ (见 format.h)
假设无额外 padding (GCC 默认按顺序 + 对齐; 可能存在填充, 我们根据文件中 dataSize 校验)。

安全策略:
  - 按头部 dataSize 读取整个 DATATYPE 原始字节, 再用 struct.unpack 拆字段, 允许 dataSize != 8*? 时给警告。
  - 如果 dataSize ==  (4 + 8 + 8 + 8) = 28，说明没有 padding; 若等于 32 可能有 4 字节 padding (常见 8 字节对齐) -> 我们兼容。

输出:
  - 打印 Header 信息
  - 按层打印: Level N: node_count=...  (限制 --max-nodes-per-level, --max-branches-per-node)
  - 每节点打印前若干分支: Bk: min=[..], max=[..], leaf => id=.. filenum=.. blk=(offset,size)

使用:
  python3 rtree_dump.py /path/to/global_rtree --max-nodes-per-level 10 --max-branches-per-node 10

返回码:
  0 成功
  非 0: 错误
"""
import argparse
import os
import struct
import sys
from typing import List, Tuple

MAGIC = (ord('R') << 0) | (ord('T') << 8) | (ord('R') << 16) | (ord('E') << 24)

class RTreeFileFormatError(Exception):
    pass

class GlobalSecIndexValue:
    __slots__ = ("id", "filenum", "offset", "size")
    def __init__(self, id_: int, filenum: int, offset: int, size: int):
        self.id = id_
        self.filenum = filenum
        self.offset = offset
        self.size = size
    def __repr__(self):
        return f"GlobalSecIndexValue(id={self.id}, filenum={self.filenum}, blk=({self.offset},{self.size}))"

class Branch:
    __slots__ = ("min","max","child","data")  # child = Node or None, data = GlobalSecIndexValue or None
    def __init__(self, min_vals, max_vals, child=None, data=None):
        self.min = min_vals
        self.max = max_vals
        self.child = child
        self.data = data

class Node:
    __slots__ = ("level","branches")
    def __init__(self, level:int):
        self.level = level
        self.branches: List[Branch] = []
    def is_leaf(self):
        return self.level == 0

class RTreeBinary:
    def __init__(self, path:str, max_nodes_per_level:int, max_branches_per_node:int):
        self.path = path
        self.max_nodes_per_level = max_nodes_per_level
        self.max_branches_per_node = max_branches_per_node
        self.dataSize = None
        self.NUMDIMS = None
        self.ELEMTYPE_size = None
        self.ELEMTYPEREAL_size = None
        self.TMAXNODES = None
        self.TMINNODES = None
        self.root: Node | None = None

    def parse(self):
        with open(self.path, 'rb') as f:
            self._parse_header(f)
            self.root = self._parse_node(f)

    def _read_exact(self, f, n: int) -> bytes:
        b = f.read(n)
        if len(b) != n:
            raise RTreeFileFormatError(f"Unexpected EOF (need {n} bytes, got {len(b)})")
        return b

    def _parse_header(self, f):
        header_fmt = '<7i'  # 7 * 4-byte little-endian ints
        raw = self._read_exact(f, struct.calcsize(header_fmt))
        (magic, dataSize, numDims, elemSize, elemRealSize, maxNodes, minNodes) = struct.unpack(header_fmt, raw)
        if magic != MAGIC:
            raise RTreeFileFormatError(f"Magic mismatch: {magic:#x} (expected {MAGIC:#x})")
        self.dataSize = dataSize
        self.NUMDIMS = numDims
        self.ELEMTYPE_size = elemSize
        self.ELEMTYPEREAL_size = elemRealSize
        self.TMAXNODES = maxNodes
        self.TMINNODES = minNodes

    def _parse_rect(self, f) -> Tuple[List[float], List[float]]:
        # ELEMTYPE assumed to be float/double; size determines format
        if self.ELEMTYPE_size == 8:
            fmt = '<' + 'd'*self.NUMDIMS
        elif self.ELEMTYPE_size == 4:
            fmt = '<' + 'f'*self.NUMDIMS
        else:
            raise RTreeFileFormatError(f"Unsupported ELEMTYPE size {self.ELEMTYPE_size}")
        size_needed = struct.calcsize(fmt)
        mmin = list(struct.unpack(fmt, self._read_exact(f, size_needed)))
        mmax = list(struct.unpack(fmt, self._read_exact(f, size_needed)))
        return mmin, mmax

    def _parse_node(self, f) -> Node:
        # Read level, count
        ints_raw = self._read_exact(f, 8)
        level, count = struct.unpack('<ii', ints_raw)
        node = Node(level)
        if count < 0 or count > self.TMAXNODES:
            raise RTreeFileFormatError(f"Invalid branch count {count} (TMAXNODES={self.TMAXNODES})")
        if level > 0:  # internal node
            for _ in range(count):
                mmin, mmax = self._parse_rect(f)
                child = self._parse_node(f)
                node.branches.append(Branch(mmin, mmax, child=child))
        else:  # leaf
            # Data size = dataSize (from header) per datum
            for _ in range(count):
                mmin, mmax = self._parse_rect(f)
                data_bytes = self._read_exact(f, self.dataSize)
                data = self._parse_global_sec_index_value(data_bytes)
                node.branches.append(Branch(mmin, mmax, data=data))
        return node

    def _parse_global_sec_index_value(self, b: bytes) -> GlobalSecIndexValue:
        # Try common layouts. First without padding (little endian): int, uint64, uint64, uint64
        # If dataSize == 28 -> 4 + 8 + 8 + 8 (no padding)
        # If dataSize == 32 -> maybe 4 + 4(pad) + 8 + 8 + 8
        if self.dataSize == 28:
            id_, filenum, off, size = struct.unpack('<IQQQ', b)
            return GlobalSecIndexValue(id_, filenum, off, size)
        elif self.dataSize == 32:
            # Try with potential 4-byte padding after int
            id_, pad, filenum, off, size = struct.unpack('<IIQQQ', b)
            return GlobalSecIndexValue(id_, filenum, off, size)
        else:
            # Fallback: if matches 24? or user changed struct; best effort parse
            if self.dataSize >= 28:
                # take first 4 + next three 8-byte chunks
                id_ = struct.unpack('<I', b[:4])[0]
                rest = b[4:]
                # pad if needed
                q_needed = 3*8
                if len(rest) < q_needed:
                    rest = rest + b'\x00'*(q_needed-len(rest))
                filenum, off, size = struct.unpack('<QQQ', rest[:24])
                return GlobalSecIndexValue(id_, filenum, off, size)
            else:
                raise RTreeFileFormatError(f"Unsupported GlobalSecIndexValue size {self.dataSize}")

    # Pretty print with sampling
    def dump(self):
        if not self.root:
            print("<Empty tree>")
            return
        print("Header:")
        print(f"  dataSize={self.dataSize} NUMDIMS={self.NUMDIMS} ELEMTYPE_size={self.ELEMTYPE_size} ELEMTYPEREAL_size={self.ELEMTYPEREAL_size}")
        print(f"  TMAXNODES={self.TMAXNODES} TMINNODES={self.TMINNODES}")
        # BFS gather nodes by tree depth (root depth=0) but we will print using RTree level numbers
        levels_by_depth: List[List[Node]] = []
        queue = [(self.root, 0)]
        while queue:
            n, depth = queue.pop(0)
            if len(levels_by_depth) <= depth:
                levels_by_depth.append([])
            levels_by_depth[depth].append(n)
            if not n.is_leaf():
                for br in n.branches:
                    queue.append((br.child, depth + 1))

        root_level = self.root.level  # Highest RTree level (root). Leaves have level 0.
        for depth, nodes in enumerate(levels_by_depth):
            rtree_level = root_level - depth
            # All nodes at this depth should share the same node.level == rtree_level
            print(f"Level {rtree_level} : nodes={len(nodes)}")
            for ni, node in enumerate(nodes[:self.max_nodes_per_level]):
                kind = 'leaf' if node.is_leaf() else 'internal'
                # node.level should equal rtree_level; omit duplicate depth info to avoid confusion.
                print(f"  Node#{ni+1} level={node.level} branches={len(node.branches)} {kind}")
                for bi, br in enumerate(node.branches[:self.max_branches_per_node]):
                    if self.NUMDIMS == 1:
                        rect_str = f"[{br.min[0]},{br.max[0]}]"
                    else:
                        rect_str = '[' + ', '.join(f"({br.min[d]},{br.max[d]})" for d in range(self.NUMDIMS)) + ']'
                    if node.is_leaf():
                        print(f"    B{bi}: {rect_str} => {br.data}")
                    else:
                        print(f"    B{bi}: {rect_str} -> child(level={br.child.level})")
                if len(node.branches) > self.max_branches_per_node:
                    print(f"    ... ({len(node.branches)-self.max_branches_per_node} more branches)")
            if len(nodes) > self.max_nodes_per_level:
                print(f"  ... ({len(nodes)-self.max_nodes_per_level} more nodes)")


def main():
    ap = argparse.ArgumentParser(description='Dump binary RTree (GlobalSecIndexValue) to human readable text.')
    ap.add_argument('file', help='RTree binary file path')
    ap.add_argument('--max-nodes-per-level', type=int, default=8)
    ap.add_argument('--max-branches-per-node', type=int, default=8)
    args = ap.parse_args()

    if not os.path.isfile(args.file):
        print(f"Error: file not found: {args.file}", file=sys.stderr)
        return 2
    try:
        tree = RTreeBinary(args.file, args.max_nodes_per_level, args.max_branches_per_node)
        tree.parse()
        tree.dump()
        return 0
    except RTreeFileFormatError as e:
        print(f"Format error: {e}", file=sys.stderr)
        return 3
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 4

if __name__ == '__main__':
    sys.exit(main())
