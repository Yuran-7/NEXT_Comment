#include <string>
#include <iostream>

std::string serialize_id(int iid) { // int类型的key，如果想让它按整数值大小排序，请确保 key 的序列化使用大端序（big-endian）
    std::string key;
    key.append(reinterpret_cast<const char*>(&iid), sizeof(int)); // 比如iid为1，
    return key;
}

int main() {
    int id = 1;
    std::string key = serialize_id(id);
    std::cout << "Serialized key size: " << key.size() << std::endl;
    
    // 以十六进制格式打印二进制数据
    std::cout << "Key (hex): ";
    for (unsigned char c : key) {
        printf("%02x ", c);
    }
    std::cout << std::endl;
    
    // 再测试一个更大的数字
    int id2 = 256;
    std::string key2 = serialize_id(id2);
    std::cout << "\nFor id = 256:" << std::endl;
    std::cout << "Key (hex): ";
    for (unsigned char c : key2) {
        printf("%02x ", c);
    }
    std::cout << std::endl;
    
    // 比较两个key的字典序
    if (key < key2) {
        std::cout << "\n1 < 256 (correct order)" << std::endl;
    } else {
        std::cout << "\n1 > 256 (WRONG order! 小端序导致)" << std::endl;
    }
    
    return 0;
}