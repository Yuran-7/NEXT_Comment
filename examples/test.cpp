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
    for (size_t i = 0; i < key.size(); ++i) {
        printf("Byte %zu: 0x%02x\n", i, static_cast<unsigned char>(key[i]));
    }
    return 0;
}