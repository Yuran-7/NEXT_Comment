#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
一维R树辅助索引导出工具 - 纯文本版本

这个脚本用于解析并打印SST文件中OneDRtreeSecondaryIndexBuilder构建的二级索引内容。
不需要任何图形库，只输出文本信息。

使用方法：
    python dump_secondary_index.py --file <sst_file_path> --offset <block_offset> --size <block_size>

示例：
    python python/dump_secondary_index.py --file testdb/000016.sst --offset 67593921 --size 508 --debug
"""

import struct
import sys
import argparse
from typing import List, Tuple, Optional
from dataclasses import dataclass


@dataclass
class ValueRange:
    """值范围类，表示一维区间 [min, max]"""
    min: float  # 实际是double (8字节)
    max: float  # 实际是double (8字节)
    
    def __repr__(self):
        return f"[{self.min:.10f}, {self.max:.10f}]"
    
    def size(self):
        return self.max - self.min
    
    def center(self):
        return (self.min + self.max) / 2


@dataclass
class BlockHandle:
    """块句柄类"""
    offset: int
    size: int
    
    def __repr__(self):
        return f"BlockHandle(offset={self.offset}, size={self.size})"


@dataclass
class IndexEntry:
    """索引条目类"""
    value_range: ValueRange
    block_handle: BlockHandle
    sec_valranges: List[ValueRange] = None  # 全局索引的细粒度范围列表
    first_internal_key: Optional[bytes] = None


class VarintDecoder:
    """Varint编码解码器"""
    
    @staticmethod
    def decode_varint32(data: bytes, offset: int) -> Tuple[int, int]:
        result = 0
        shift = 0
        bytes_read = 0
        
        while bytes_read < 5:
            if offset + bytes_read >= len(data):
                raise ValueError("Varint32解码超出边界")
            
            byte = data[offset + bytes_read]
            bytes_read += 1
            result |= (byte & 0x7F) << shift
            
            if (byte & 0x80) == 0:
                return result, bytes_read
            
            shift += 7
        
        raise ValueError("Varint32解码失败")
    
    @staticmethod
    def decode_varint64(data: bytes, offset: int) -> Tuple[int, int]:
        result = 0
        shift = 0
        bytes_read = 0
        
        while bytes_read < 10:
            if offset + bytes_read >= len(data):
                raise ValueError("Varint64解码超出边界")
            
            byte = data[offset + bytes_read]
            bytes_read += 1
            result |= (byte & 0x7F) << shift
            
            if (byte & 0x80) == 0:
                return result, bytes_read
            
            shift += 7
        
        raise ValueError("Varint64解码失败")


class IndexBlockDumper:
    """索引块导出器"""
    
    def __init__(self, data: bytes):
        self.data = data
        self.decoder = VarintDecoder()
    
    def parse_and_dump(self, verbose: bool = True, debug: bool = False) -> List[IndexEntry]:
        """解析并打印索引块内容
        
        Args:
            verbose: 是否显示详细信息
            debug: 是否显示调试信息（包括原始hex数据）
        """
        
        if len(self.data) < 4:
            raise ValueError("数据太短，无法解析")
        
        # 读取restart points数量
        num_restarts = struct.unpack('<I', self.data[-4:])[0]
        restarts_offset = len(self.data) - 4 - (num_restarts * 4)
        
        # 读取restart points
        restart_points = []
        for i in range(num_restarts):
            offset = restarts_offset + i * 4
            restart_point = struct.unpack('<I', self.data[offset:offset+4])[0]
            restart_points.append(restart_point)
        
        if verbose:
            print("="*80)
            print("索引块基本信息")
            print("="*80)
            print(f"数据总大小: {len(self.data)} 字节")
            print(f"Restart Points数量: {num_restarts}")
            print(f"Restart Points位置: {restart_points}")
            print(f"数据区域大小: {restarts_offset} 字节")
            print()
        
        # 解析所有条目
        entries = []
        offset = 0
        entry_index = 0
        prev_key = b''
        
        while offset < restarts_offset:
            is_restart = offset in restart_points
            
            # 解析条目头部
            shared_len, n = self.decoder.decode_varint32(self.data, offset)
            offset += n
            
            unshared_len, n = self.decoder.decode_varint32(self.data, offset)
            offset += n
            
            value_len, n = self.decoder.decode_varint32(self.data, offset)
            offset += n
            
            # 读取key
            unshared_key = self.data[offset:offset + unshared_len]
            offset += unshared_len
            
            # 重建完整key
            if is_restart or entry_index == 0:
                full_key = unshared_key
            else:
                full_key = prev_key[:shared_len] + unshared_key
            
            # 解析ValueRange（16字节）
            # 先打印调试信息
            if debug and entry_index < 3:  # 只打印前3个条目的调试信息
                print(f"\n[调试] 条目 #{entry_index}:")
                print(f"  Is restart point: {is_restart}")
                print(f"  Shared/Unshared/Value len: {shared_len}/{unshared_len}/{value_len}")
                print(f"  Full key length: {len(full_key)}")
                print(f"  Full key hex: {full_key.hex()}")
                if len(full_key) >= 16:
                    print(f"  First 16 bytes hex: {full_key[:16].hex()}")
                    # 尝试不同的解析方式
                    try:
                        min_le = struct.unpack('<d', full_key[0:8])[0]
                        max_le = struct.unpack('<d', full_key[8:16])[0]
                        print(f"  解析为小端double: min={min_le}, max={max_le}")
                    except:
                        pass
                    try:
                        min_be = struct.unpack('>d', full_key[0:8])[0]
                        max_be = struct.unpack('>d', full_key[8:16])[0]
                        print(f"  解析为大端double: min={min_be}, max={max_be}")
                    except:
                        pass
            
            if len(full_key) >= 16:
                # 根据C++代码，RocksDB在x86上使用小端序
                min_val = struct.unpack('<d', full_key[0:8])[0]
                max_val = struct.unpack('<d', full_key[8:16])[0]
                value_range = ValueRange(min_val, max_val)
            else:
                raise ValueError(f"Key长度不足16字节: {len(full_key)}")
            
            # 解析value中的BlockHandle
            value_start = offset
            
            # 调试信息
            if debug and entry_index < 3:
                print(f"  Value length: {value_len}")
                print(f"  Value hex: {self.data[offset:offset+value_len].hex()}")
            
            block_offset, n = self.decoder.decode_varint64(self.data, offset)
            offset += n
            
            if debug and entry_index < 3:
                print(f"  Block offset decoded: {block_offset} (consumed {n} bytes)")
            
            block_size, n = self.decoder.decode_varint64(self.data, offset)
            offset += n
            
            if debug and entry_index < 3:
                print(f"  Block size decoded: {block_size} (consumed {n} bytes)")
            
            block_handle = BlockHandle(block_offset, block_size)
            
            # 跳过剩余的value数据（可能是sec_valranges或first_internal_key）
            remaining_value_len = value_len - (offset - value_start)
            sec_valranges = []
            first_key = None
            
            if debug and entry_index < 3:
                print(f"  Remaining value length: {remaining_value_len}")
            
            if remaining_value_len > 0:
                # 尝试解析为sec_valranges（多个ValueRange）
                # 每个ValueRange是16字节
                if remaining_value_len % 16 == 0:
                    num_ranges = remaining_value_len // 16
                    if debug and entry_index < 3:
                        print(f"  Parsing as {num_ranges} sec_valranges (16 bytes each)")
                    
                    for i in range(num_ranges):
                        range_offset = offset + i * 16
                        try:
                            r_min = struct.unpack('<d', self.data[range_offset:range_offset+8])[0]
                            r_max = struct.unpack('<d', self.data[range_offset+8:range_offset+16])[0]
                            sec_valranges.append(ValueRange(r_min, r_max))
                            if debug and entry_index < 3:
                                print(f"    Range #{i}: [{r_min}, {r_max}]")
                        except:
                            if debug:
                                print(f"    Failed to parse range #{i}")
                            break
                else:
                    # 不是16的倍数，可能是first_internal_key
                    first_key = self.data[offset:offset + remaining_value_len]
                    if debug and entry_index < 3:
                        print(f"  Parsed as first_internal_key (not multiple of 16)")
                
            offset = value_start + value_len
            
            # 创建条目
            entry = IndexEntry(value_range, block_handle, sec_valranges if sec_valranges else None, first_key)
            entries.append(entry)
            
            # 更新状态
            prev_key = full_key
            entry_index += 1
        
        # 打印条目详情
        if verbose:
            self.print_entries(entries)
        
        return entries
    
    def print_entries(self, entries: List[IndexEntry]):
        """打印所有条目"""
        print("="*80)
        print(f"索引条目详情 (共 {len(entries)} 个)")
        print("="*80)
        print()
        
        for i, entry in enumerate(entries):
            print(f"【条目 #{i}】")
            print(f"  Key (ValueRange):")
            print(f"    表示形式: {entry.value_range}")
            print(f"    Min值: {entry.value_range.min}")
            print(f"    Max值: {entry.value_range.max}")
            print(f"    范围大小: {entry.value_range.size():.10f}")
            print(f"    中心点: {entry.value_range.center():.10f}")
            print(f"  Value (BlockHandle):")
            print(f"    文件偏移量(offset): {entry.block_handle.offset}")
            print(f"    块大小(size): {entry.block_handle.size}")
            if entry.sec_valranges:
                print(f"  全局索引(GL) - 细粒度范围列表 (共{len(entry.sec_valranges)}个):")
                for j, vr in enumerate(entry.sec_valranges):
                    print(f"    Range #{j}: {vr} (size={vr.size():.10f})")
            if entry.first_internal_key:
                print(f"  First Internal Key ({len(entry.first_internal_key)} bytes): {entry.first_internal_key.hex()[:100]}...")
            print()
        
        print("="*80)
        print("统计信息")
        print("="*80)
        
        # 值范围统计
        total_range = ValueRange(
            min(e.value_range.min for e in entries),
            max(e.value_range.max for e in entries)
        )
        range_sizes = [e.value_range.size() for e in entries]
        
        print(f"值范围统计:")
        print(f"  全局范围: {total_range}")
        print(f"  全局大小: {total_range.size():.10f}")
        print(f"  平均区间大小: {sum(range_sizes)/len(range_sizes):.10f}")
        print(f"  最小区间大小: {min(range_sizes):.10f}")
        print(f"  最大区间大小: {max(range_sizes):.10f}")
        print()
        
        # 块统计
        block_sizes = [e.block_handle.size for e in entries]
        total_data = sum(block_sizes)
        
        print(f"块大小统计:")
        print(f"  平均块大小: {sum(block_sizes)/len(block_sizes):.0f} 字节")
        print(f"  最小块大小: {min(block_sizes)} 字节")
        print(f"  最大块大小: {max(block_sizes)} 字节")
        print(f"  总数据大小: {total_data} 字节 ({total_data/1024:.2f} KB)")
        print()
        
        # 覆盖分析
        overlaps = 0
        gaps = 0
        for i in range(len(entries) - 1):
            if entries[i].value_range.max > entries[i+1].value_range.min:
                overlaps += 1
            elif entries[i].value_range.max < entries[i+1].value_range.min:
                gaps += 1
        
        print(f"区间关系:")
        print(f"  重叠区间对: {overlaps}")
        print(f"  有间隙区间对: {gaps}")
        print(f"  连续区间对: {len(entries)-1-overlaps-gaps}")
        print()
        print("="*80)


def dump_from_file(sst_file: str, offset: int, size: int, verbose: bool = True, debug: bool = False):
    """从SST文件读取并导出索引块"""
    
    print("="*80)
    print("SST文件二级索引导出工具")
    print("="*80)
    print(f"文件: {sst_file}")
    print(f"偏移量: {offset}")
    print(f"大小: {size}")
    print()
    
    # 读取数据
    with open(sst_file, 'rb') as f:
        f.seek(offset)
        data = f.read(size)
    
    if len(data) != size:
        raise ValueError(f"读取数据不完整: 期望{size}字节, 实际{len(data)}字节")
    
    print(f"成功读取 {len(data)} 字节")
    print()
    
    # 解析并导出
    dumper = IndexBlockDumper(data)
    entries = dumper.parse_and_dump(verbose=verbose, debug=debug)
    
    return entries


def dump_from_hex(hex_data: str, verbose: bool = True, debug: bool = False):
    """从十六进制字符串导出索引块"""
    
    data = bytes.fromhex(hex_data.replace(' ', '').replace('\n', ''))
    
    print("="*80)
    print("从原始数据导出二级索引")
    print("="*80)
    print(f"数据大小: {len(data)} 字节")
    print()
    
    dumper = IndexBlockDumper(data)
    entries = dumper.parse_and_dump(verbose=verbose, debug=debug)
    
    return entries


def main():
    parser = argparse.ArgumentParser(
        description='导出RocksDB一维R树辅助索引内容（纯文本）',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:
  # 从SST文件导出
  python dump_secondary_index.py --file data.sst --offset 1024 --size 512
  
  # 简洁模式（只显示条目，不显示统计）
  python dump_secondary_index.py --file data.sst --offset 1024 --size 512 --quiet
  
  # 从十六进制数据导出
  python dump_secondary_index.py --hex "0000000000000000..."
        """
    )
    
    parser.add_argument('--file', '-f', type=str, help='SST文件路径')
    parser.add_argument('--offset', '-o', type=int, help='索引块偏移量')
    parser.add_argument('--size', '-s', type=int, help='索引块大小')
    parser.add_argument('--hex', type=str, help='十六进制格式的原始数据')
    parser.add_argument('--quiet', '-q', action='store_true', help='简洁模式')
    parser.add_argument('--debug', '-d', action='store_true', help='显示调试信息（包括hex数据）')
    
    args = parser.parse_args()
    
    try:
        if args.hex:
            # 从十六进制数据
            dump_from_hex(args.hex, verbose=not args.quiet, debug=args.debug)
        elif args.file and args.offset is not None and args.size is not None:
            # 从文件
            dump_from_file(args.file, args.offset, args.size, verbose=not args.quiet, debug=args.debug)
        else:
            parser.error("必须指定 --file --offset --size 或 --hex")
    
    except Exception as e:
        print(f"\n错误: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
