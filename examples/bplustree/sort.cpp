#include <iostream>
#include <vector>
#include <algorithm>
#include <random>
#include <chrono>

int main() {
    const size_t N = 1'000'000;  // 100万
    std::vector<double> v(N);

    // 初始化随机数生成器
    std::mt19937_64 rng(std::random_device{}());
    std::uniform_real_distribution<double> dist(0.0, 1.0e6);

    // 填充随机 double
    for (size_t i = 0; i < N; ++i) {
        v[i] = dist(rng);
    }

    std::cout << "开始排序 " << N << " 个 double...\n";

    auto start = std::chrono::high_resolution_clock::now();

    std::sort(v.begin(), v.end());

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> elapsed = end - start;

    std::cout << "排序完成，用时: " << elapsed.count() << " 毫秒\n";

    // 可选：检查前几个元素
    std::cout << "前5个元素: ";
    for (int i = 0; i < 5; ++i) std::cout << v[i] << " ";
    std::cout << "\n";

    return 0;
}
// g++ -O2 -std=c++17 ./bplustree/sort.cpp -o ./bplustree/sort
// ./bplustree/sort