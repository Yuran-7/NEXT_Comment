import struct
import sys

def parse_ascii_to_double(s: str):
    """
    将形如 '\\330J\\350.\\211{+@' 的 ASCII 转义字符串解析成 double。
    """
    # 把类似 "\330" 的八进制转义转换为真实字节
    # encode('utf-8').decode('unicode_escape') 会解析转义序列
    try:
        raw_bytes = s.encode('utf-8').decode('unicode_escape').encode('latin1')
    except Exception as e:
        print(f"解析失败: {e}")
        return

    # 输出原始字节
    print("原始字节序列 (十六进制):", ' '.join(f"{b:02X}" for b in raw_bytes))

    # 根据长度决定解码个数
    if len(raw_bytes) % 8 != 0:
        print(f"⚠️ 字节长度 = {len(raw_bytes)} 不是 8 的倍数，可能不是完整的 double 数据")
    count = len(raw_bytes) // 8

    # 小端序 ('<d') 解码为 double
    doubles = struct.unpack('<' + 'd' * count, raw_bytes)
    for i, val in enumerate(doubles):
        print(f"第 {i+1} 个 double: {val}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("用法: python parse_double.py '\\330J\\350.\\211{+@'")
    else:
        parse_ascii_to_double(sys.argv[1])
# python python/parse_ascii_double.py '\351\327\326O\377\331\377?'