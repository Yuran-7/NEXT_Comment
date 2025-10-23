#!/usr/bin/env python3
"""
bplustree_dump.py

用途:
  给出一个 BPlusTree 的二进制保存文件 (由 C++ BPlusTree.h 的 Save / SaveRec 生成),
  解析并以可读形式打印树结构 (分层), 支持泛型 Key/Value 类型。

BPlusTree 模板参数: BPlusTree<Key, Value, MAX_KEYS, MIN_KEYS>
本脚本根据文件头自动识别 Key/Value 的大小，支持基本类型的解析。

二进制格式 (参考 BPlusTree.h 中 Save/SaveRec):
Header 顺序写入 5 个 uint32_t (32-bit little endian):
  1. magic: 'B'<<0 | 'P'<<8 | 'L'<<16 | 'S'<<24  (ASCII: BPLS -> 十六进制 0x534C5042)
  2. sizeof(Key)     - key_size
  3. sizeof(Value)   - val_size
  4. MAX_KEYS        - maxk
  5. MIN_KEYS        - mink

随后递归保存节点:
  对每个节点:
    uint8_t is_leaf    (1=叶子, 0=内部)
    uint32_t keys_count
    Key keys[keys_count]
    
    若内部节点:
      uint32_t children_count (= keys_count + 1)
      递归写每个子节点
    
    若叶子节点:
      对每个 key (共 keys_count 个):
        uint32_t values_count
        Value values[values_count]

输出:
  - 打印 Header 信息
  - 按树的层级打印节点
  - 每节点打印: 是否叶子, key 数量, 前若干 keys
  - 叶子节点打印每个 key 对应的 values
  - 限制输出量: --max-nodes-per-level, --max-keys-per-node, --max-values-per-key

使用:
  python examples/python/bplustree_dump.py testdb/bplustree_index --max-nodes-per-level 10 --max-keys-per-node 10

返回码:
  0 成功
  非 0: 错误
"""
import argparse
import os
import struct
import sys
from typing import List, Any, Tuple

MAGIC = (ord('B') << 0) | (ord('P') << 8) | (ord('L') << 16) | (ord('S') << 24)

class BPlusTreeFileFormatError(Exception):
    pass

class Node:
    __slots__ = ("is_leaf", "keys", "children", "values")
    def __init__(self, is_leaf: bool):
        self.is_leaf = is_leaf
        self.keys: List[Any] = []
        self.children: List['Node'] = []  # 仅内部节点
        self.values: List[List[Any]] = []  # 仅叶子节点，每个key对应一个value列表

class BPlusTreeBinary:
    def __init__(self, path: str, max_nodes_per_level: int, max_keys_per_node: int, max_values_per_key: int):
        self.path = path
        self.max_nodes_per_level = max_nodes_per_level
        self.max_keys_per_node = max_keys_per_node
        self.max_values_per_key = max_values_per_key
        self.key_size = None
        self.val_size = None
        self.max_keys = None
        self.min_keys = None
        self.root: Node | None = None

    def parse(self):
        with open(self.path, 'rb') as f:
            self._parse_header(f)
            self.root = self._parse_node(f)

    def _read_exact(self, f, n: int) -> bytes:
        b = f.read(n)
        if len(b) != n:
            raise BPlusTreeFileFormatError(f"Unexpected EOF (need {n} bytes, got {len(b)})")
        return b

    def _parse_header(self, f):
        header_fmt = '<5I'  # 5 * 4-byte little-endian unsigned ints
        raw = self._read_exact(f, struct.calcsize(header_fmt))
        (magic, key_size, val_size, maxk, mink) = struct.unpack(header_fmt, raw)
        if magic != MAGIC:
            raise BPlusTreeFileFormatError(f"Magic mismatch: {magic:#x} (expected {MAGIC:#x})")
        self.key_size = key_size
        self.val_size = val_size
        self.max_keys = maxk
        self.min_keys = mink

    def _parse_key(self, f) -> Any:
        """根据 key_size 解析一个 key"""
        raw = self._read_exact(f, self.key_size)
        return self._interpret_bytes(raw, self.key_size, "Key")

    def _parse_value(self, f) -> Any:
        """根据 val_size 解析一个 value"""
        raw = self._read_exact(f, self.val_size)
        return self._interpret_bytes(raw, self.val_size, "Value")

    def _interpret_bytes(self, raw: bytes, size: int, typename: str) -> Any:
        """尝试将字节解释为常见类型"""
        if size == 4:
            # 尝试 int32, uint32, float
            try:
                val_i = struct.unpack('<i', raw)[0]
                val_u = struct.unpack('<I', raw)[0]
                val_f = struct.unpack('<f', raw)[0]
                # 优先返回有符号整数，除非看起来像浮点数
                if abs(val_f) < 1e6 and val_f != int(val_f):
                    return val_f
                return val_i
            except:
                return raw.hex()
        elif size == 8:
            # 尝试 int64, uint64, double
            try:
                val_i = struct.unpack('<q', raw)[0]
                val_u = struct.unpack('<Q', raw)[0]
                val_d = struct.unpack('<d', raw)[0]
                # 优先返回有符号整数
                if abs(val_d) < 1e15 and val_d != int(val_d):
                    return val_d
                return val_i
            except:
                return raw.hex()
        elif size == 1:
            return struct.unpack('<B', raw)[0]
        elif size == 2:
            return struct.unpack('<H', raw)[0]
        else:
            # 对于复杂类型，返回十六进制字符串
            if size <= 32:
                return raw.hex()
            else:
                return f"<{size} bytes: {raw[:16].hex()}...>"

    def _parse_node(self, f) -> Node:
        # Read is_leaf (uint8_t), keys_count (uint32_t)
        header_raw = self._read_exact(f, 5)  # 1 + 4
        is_leaf, keys_count = struct.unpack('<BI', header_raw)
        
        node = Node(is_leaf != 0)
        
        if keys_count > self.max_keys + 1:  # 允许临时超出一点（分裂前）
            raise BPlusTreeFileFormatError(f"Invalid keys_count {keys_count} (MAX_KEYS={self.max_keys})")
        
        # 读取所有 keys
        for _ in range(keys_count):
            key = self._parse_key(f)
            node.keys.append(key)
        
        if not node.is_leaf:
            # 内部节点：读取 children_count 和所有子节点
            children_count_raw = self._read_exact(f, 4)
            children_count = struct.unpack('<I', children_count_raw)[0]
            
            if children_count != keys_count + 1:
                raise BPlusTreeFileFormatError(
                    f"Internal node: children_count {children_count} != keys_count + 1 ({keys_count + 1})")
            
            for _ in range(children_count):
                child = self._parse_node(f)
                node.children.append(child)
        else:
            # 叶子节点：读取每个 key 对应的 values
            for _ in range(keys_count):
                values_count_raw = self._read_exact(f, 4)
                values_count = struct.unpack('<I', values_count_raw)[0]
                
                values = []
                for _ in range(values_count):
                    val = self._parse_value(f)
                    values.append(val)
                node.values.append(values)
        
        return node

    def dump(self):
        if not self.root:
            print("<Empty tree>")
            return
        
        print("=" * 80)
        print("B+ Tree Dump")
        print("=" * 80)
        print("Header:")
        print(f"  Key size    : {self.key_size} bytes")
        print(f"  Value size  : {self.val_size} bytes")
        print(f"  MAX_KEYS    : {self.max_keys}")
        print(f"  MIN_KEYS    : {self.min_keys}")
        print()
        
        # BFS 遍历，按层打印
        levels: List[List[Node]] = []
        queue = [(self.root, 0)]
        
        while queue:
            node, depth = queue.pop(0)
            if len(levels) <= depth:
                levels.append([])
            levels[depth].append(node)
            
            if not node.is_leaf:
                for child in node.children:
                    queue.append((child, depth + 1))
        
        # 打印每一层
        total_keys = 0
        total_values = 0
        
        for depth, nodes in enumerate(levels):
            kind = "Leaf" if nodes[0].is_leaf else "Internal"
            print(f"{'=' * 80}")
            print(f"Level {depth} ({kind}): {len(nodes)} node(s)")
            print(f"{'=' * 80}")
            
            for ni, node in enumerate(nodes[:self.max_nodes_per_level]):
                keys_count = len(node.keys)
                total_keys += keys_count
                
                print(f"  Node #{ni + 1}:")
                print(f"    Type        : {'Leaf' if node.is_leaf else 'Internal'}")
                print(f"    Keys count  : {keys_count}")
                
                # 打印 keys
                display_keys = node.keys[:self.max_keys_per_node]
                if len(display_keys) > 0:
                    print(f"    Keys        : {display_keys}")
                    if len(node.keys) > self.max_keys_per_node:
                        print(f"                  ... ({len(node.keys) - self.max_keys_per_node} more keys)")
                
                if node.is_leaf:
                    # 打印 values
                    print(f"    Values:")
                    for ki, (key, vals) in enumerate(zip(node.keys[:self.max_keys_per_node], 
                                                          node.values[:self.max_keys_per_node])):
                        total_values += len(vals)
                        display_vals = vals[:self.max_values_per_key]
                        vals_str = ', '.join(str(v) for v in display_vals)
                        suffix = f" ... ({len(vals) - self.max_values_per_key} more)" if len(vals) > self.max_values_per_key else ""
                        print(f"      Key[{ki}]={key}: [{vals_str}]{suffix} (total: {len(vals)})")
                    
                    if len(node.keys) > self.max_keys_per_node:
                        remaining_values = sum(len(node.values[i]) for i in range(self.max_keys_per_node, len(node.values)))
                        print(f"      ... ({len(node.keys) - self.max_keys_per_node} more keys with {remaining_values} values)")
                else:
                    # 内部节点：显示子节点信息
                    print(f"    Children    : {len(node.children)} child node(s)")
                
                print()
            
            if len(nodes) > self.max_nodes_per_level:
                print(f"  ... ({len(nodes) - self.max_nodes_per_level} more nodes at this level)")
                print()
        
        print("=" * 80)
        print("Summary:")
        print(f"  Total levels: {len(levels)}")
        print(f"  Total keys  : {total_keys}")
        print(f"  Total values: {total_values}")
        print(f"  Leaf nodes  : {len(levels[-1]) if levels else 0}")
        print("=" * 80)


def main():
    ap = argparse.ArgumentParser(
        description='Dump binary B+ Tree file to human readable text.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python examples/python/bplustree_dump.py testdb/bplustree_index
  python examples/python/bplustree_dump.py testdb/bplustree_index --max-nodes-per-level 5 --max-keys-per-node 20
        """)
    ap.add_argument('file', help='B+ Tree binary file path')
    ap.add_argument('--max-nodes-per-level', type=int, default=10,
                    help='Maximum number of nodes to display per level (default: 10)')
    ap.add_argument('--max-keys-per-node', type=int, default=10,
                    help='Maximum number of keys to display per node (default: 10)')
    ap.add_argument('--max-values-per-key', type=int, default=5,
                    help='Maximum number of values to display per key (default: 5)')
    args = ap.parse_args()

    if not os.path.isfile(args.file):
        print(f"Error: file not found: {args.file}", file=sys.stderr)
        return 2
    
    try:
        tree = BPlusTreeBinary(args.file, args.max_nodes_per_level, 
                               args.max_keys_per_node, args.max_values_per_key)
        tree.parse()
        tree.dump()
        return 0
    except BPlusTreeFileFormatError as e:
        print(f"Format error: {e}", file=sys.stderr)
        return 3
    except Exception as e:
        import traceback
        print(f"Error: {e}", file=sys.stderr)
        traceback.print_exc()
        return 4

if __name__ == '__main__':
    sys.exit(main())

# 基本用法
# python python/bplustree_dump.py bplustree/global_bptree

# 限制输出
# python python/bplustree_dump.py testdb/bplustree_index --max-nodes-per-level 5 --max-keys-per-node 20 --max-values-per-key 3