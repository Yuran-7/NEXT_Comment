#ifndef BPLUSTREE_H
#define BPLUSTREE_H

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <functional>
#include <cstdio>
#include <limits>
#include <memory>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

// 轻量级 B+ 树实现（头文件-only），仿照 util/RTree_mem.h 的风格：
// - 模板参数化 Key/Handle 与节点容量
// - 仅支持基本功能：Insert / Search / RangeSearch（叶子链顺序扫描）
// - 重复 key 时，所有 (filenum, handle) 聚合存放在叶子节点对应 key 的一个 unordered_map<int, vector<BlockHandle>> 中

namespace ROCKSDB_NAMESPACE {

template <class Key, class BlockHandle, int MAX_KEYS = 64, int MIN_KEYS = MAX_KEYS / 2>
class BPlusTree {
  static_assert(MAX_KEYS >= 3, "MAX_KEYS must be >= 3");
  static_assert(MIN_KEYS >= 1, "MIN_KEYS must be >= 1");

 private:
  struct Node {
    bool is_leaf;
    std::vector<Key> keys;                                   // 大小 <= MAX_KEYS
    std::vector<Node*> children;                             // 内部节点 children.size() == keys.size() + 1
    // 仅叶子：与 keys 对齐；每个 key 下按 filenum 聚合多个 BlockHandle
    std::vector<std::unordered_map<int, std::vector<BlockHandle>>> vals;
    // 移除了 valid_counts：按需实时统计
    Node* next;                            // 叶子顺序链表
    Node* prev;                            // 叶子反向链

    Node(bool leaf) : is_leaf(leaf), next(nullptr), prev(nullptr) {
      keys.reserve(MAX_KEYS + 1);  // 预留一点空间，便于插入前检测是否需要 split
      if (leaf) {
        vals.reserve(MAX_KEYS + 1);
      } else {
        children.reserve(MAX_KEYS + 2);
      }
    }
  };

 public:
  BPlusTree() : root_(new Node(true)) {}

  ~BPlusTree() { FreeRec(root_); }

  BPlusTree(const BPlusTree&) = delete;
  BPlusTree& operator=(const BPlusTree&) = delete;

  // 插入：若 key 已存在，则将 (filenum, handle) 追加到该 key 的 vector；否则创建新的 key 槽
  void Insert(const Key& k, int filenum, const BlockHandle& handle) {
    Key sep{};
    Node* new_child = nullptr;
    bool split = InsertRec(root_, k, filenum, handle, &sep, &new_child);
    if (split) {  // 表示根节点发生了分裂
      // 根分裂：新建一层
      Node* new_root = new Node(false);
      new_root->keys.push_back(sep);
      new_root->children.push_back(root_);
      new_root->children.push_back(new_child);
      root_ = new_root;
    }
  }

  void InsertWithDelete(const Key& k, int filenum, const BlockHandle& handle, const std::vector<uint64_t>& delete_files, std::unordered_set<Key>& visited_keys) {
    Key sep{};
    Node* new_child = nullptr;
    bool split = InsertRecWithDelete(root_, k, filenum, handle, &sep, &new_child, delete_files, visited_keys);
    if (split) {  // 表示根节点发生了分裂
      // 根分裂：新建一层
      Node* new_root = new Node(false);
      new_root->keys.push_back(sep);
      new_root->children.push_back(root_);
      new_root->children.push_back(new_child);
      root_ = new_root;
    }
  }

  // 删除特定 key 下指定 filenum 的所有 handle（惰性删除，不改变树结构）
  // 返回：是否成功删除（找到了 key 和 filenum）
  bool Delete(const Key& k, int filenum) {
    Node* leaf = FindLeafMutable(root_, k);
    if (!leaf) return false;
    
    auto it = std::lower_bound(leaf->keys.begin(), leaf->keys.end(), k);
    if (it == leaf->keys.end()) {
      return false;  // key不存在
    }
    
    size_t idx = static_cast<size_t>(it - leaf->keys.begin());
    auto& values = leaf->vals[idx];

    size_t erased = values.erase(filenum);
    if (erased == 0) {
      return false; // 没有找到要删除的 filenum
    }
    return true;
  }

  // 精确查找：返回是否找到；若找到则 out_values 填充该 key 的所有 (filenum, handle)
  bool Search(const Key& k, std::unordered_multimap<int, BlockHandle>& out_values) const {
    const Node* leaf = FindLeaf(root_, k);
    if (!leaf) return false;
    auto it = std::lower_bound(leaf->keys.begin(), leaf->keys.end(), k);
    if (it != leaf->keys.end() && !(*it < k) && !(k < *it)) {
      size_t idx = static_cast<size_t>(it - leaf->keys.begin());
      out_values.clear();
      for (const auto& kv : leaf->vals[idx]) {
        int filenum = kv.first;
        const auto& vec = kv.second;
        for (const auto& handle : vec) {
          out_values.emplace(filenum, handle);
        }
      }
      return true;
    }
    return false;
  }

  // 返回某 key 的所有 (filenum, handle)；若不存在返回空 multimap
  std::unordered_multimap<int, BlockHandle> Get(const Key& k) const {
    std::unordered_multimap<int, BlockHandle> res;
    Search(k, res);
    return res;
  }

  // 区间查询 [l, r]，对每个匹配到的 (key, filenum, handle) 调用 callback(key, filenum, handle)
  // 若 callback 返回 false 则提前停止
  template <class Callback>
  void RangeSearch(const Key& l, const Key& r, Callback cb) const {
    if (Less(r, l)) return;
    const Node* leaf = FindLeafForLowerBound(root_, l);
    while (leaf) {
      for (size_t i = 0; i < leaf->keys.size(); ++i) {
        const Key& key = leaf->keys[i];
        if (Less(key, l)) continue;
        if (Less(r, key)) return;  // 超出上界，结束
        for (const auto& kv : leaf->vals[i]) {
          int filenum = kv.first;
          const auto& vec = kv.second;
          for (const auto& handle : vec) {
            if (!cb(key, filenum, handle)) return;
          }
        }
      }
      leaf = leaf->next;
    }
  }

  // --- 持久化（简单二进制格式，仿照 RTree_mem 的思路） ---
  // 格式：
  // header: magic(\"BPLS\"), key_size, val_size, max_keys, min_keys
  // 然后递归写节点：is_leaf(uint8_t), keys_count(uint32_t), keys[],
  // 如果内部节点：children_count(uint32_t)=keys_count+1，递归写子
  // 如果叶子：对每个 key 写 values_count(uint32_t) 与 (filenum, handle) 列表

  bool Save(const char* path) const {
    FILE* f = std::fopen(path, "wb");
    if (!f) return false;
    // 写头
    const uint32_t magic = ('B'<<0)|('P'<<8)|('L'<<16)|('S'<<24);
    uint32_t key_size = static_cast<uint32_t>(sizeof(Key));
    uint32_t val_size = static_cast<uint32_t>(sizeof(BlockHandle));
    uint32_t maxk = static_cast<uint32_t>(MAX_KEYS);
    uint32_t mink = static_cast<uint32_t>(MIN_KEYS);
    if (1 != std::fwrite(&magic, sizeof(magic), 1, f)) { std::fclose(f); return false; }
    if (1 != std::fwrite(&key_size, sizeof(key_size), 1, f)) { std::fclose(f); return false; }
    if (1 != std::fwrite(&val_size, sizeof(val_size), 1, f)) { std::fclose(f); return false; }
    if (1 != std::fwrite(&maxk, sizeof(maxk), 1, f)) { std::fclose(f); return false; }
    if (1 != std::fwrite(&mink, sizeof(mink), 1, f)) { std::fclose(f); return false; }
    bool ok = SaveRec(root_, f);
    std::fclose(f);
    return ok;
  }

  bool Load(const char* path) {
    FILE* f = std::fopen(path, "rb");
    if (!f) return false;
    uint32_t magic = 0, key_size = 0, val_size = 0, maxk = 0, mink = 0;
    auto rd = [&](void* p, size_t n){ return std::fread(p, 1, n, f) == n; };
    if (!rd(&magic, sizeof(magic)) || !rd(&key_size, sizeof(key_size)) ||
        !rd(&val_size, sizeof(val_size)) || !rd(&maxk, sizeof(maxk)) || !rd(&mink, sizeof(mink))) {
      std::fclose(f); return false;
    }
    const uint32_t expect = ('B'<<0)|('P'<<8)|('L'<<16)|('S'<<24);
    if (magic != expect || key_size != sizeof(Key) || val_size != sizeof(BlockHandle) ||
      maxk != MAX_KEYS || mink != MIN_KEYS) {
      std::fclose(f); return false;  // 不兼容
    }

    // 清理现有树
    FreeRec(root_);
    root_ = nullptr;

    // 递归读取
    root_ = LoadRec(f);
    std::fclose(f);

    // 重建叶子链：中序扫叶子并串起来
    RebuildLeafLinks();
    return root_ != nullptr;
  }

 private:
  static bool Less(const Key& a, const Key& b) { return a < b; }

  // 递归插入；若当前节点产生分裂，返回 true，并通过 sep/new_right 向父节点上推分裂信息
  bool InsertRec(Node* node, const Key& k, int filenum, const BlockHandle& handle, Key* sep, Node** new_right) {
    if (node->is_leaf) {
      // 在叶子插入或追加
      auto it = std::lower_bound(node->keys.begin(), node->keys.end(), k);  // 返回第一个大于等于 k 的位置
      size_t pos = static_cast<size_t>(it - node->keys.begin());
      if (it != node->keys.end() && *it == k) {
        // 命中：聚合到对应的 map<int, vector<BlockHandle>>
        auto& m = node->vals[pos];
        m[filenum].push_back(handle);
      } else {
        node->keys.insert(node->keys.begin() + static_cast<std::ptrdiff_t>(pos), k);
        node->vals.insert(node->vals.begin() + static_cast<std::ptrdiff_t>(pos), std::unordered_map<int, std::vector<BlockHandle>>());  // 初始化一个map
        auto& m = node->vals[pos];  // 获取刚刚初始化的map
        m[filenum].push_back(handle); // 添加
      }

      if ((int)node->keys.size() > MAX_KEYS) {
        SplitLeaf(node, sep, new_right);
        return true;
      }
      return false;
    }

    // 内部：找到子节点并下探
    size_t child_idx = UpperBoundChildIndex(node, k);
    Key child_sep{};
    Node* child_new_right = nullptr;
    bool child_split = InsertRec(node->children[child_idx], k, filenum, handle, &child_sep, &child_new_right);
    if (!child_split) return false;

    // 将子分裂上推的 sep 插入本节点
    node->keys.insert(node->keys.begin() + static_cast<std::ptrdiff_t>(child_idx), child_sep);
    node->children.insert(node->children.begin() + static_cast<std::ptrdiff_t>(child_idx + 1), child_new_right);

    if ((int)node->keys.size() > MAX_KEYS) {
      SplitInternal(node, sep, new_right);
      return true;
    }
    return false;
  }

  bool InsertRecWithDelete(Node* node, const Key& k, int filenum, const BlockHandle& handle, Key* sep, Node** new_right, const std::vector<uint64_t>& delete_files, std::unordered_set<Key>& visited_keys) {
    if (node->is_leaf) {
      // 在叶子插入或追加
      auto it = std::lower_bound(node->keys.begin(), node->keys.end(), k);  // 返回第一个大于等于 k 的位置
      size_t pos = static_cast<size_t>(it - node->keys.begin());
      if (it != node->keys.end() && *it == k) {
        // 命中：聚合到对应的 map<int, vector<BlockHandle>>
        auto& m = node->vals[pos];
        m[filenum].push_back(handle);
      } else {
        node->keys.insert(node->keys.begin() + static_cast<std::ptrdiff_t>(pos), k);
        node->vals.insert(node->vals.begin() + static_cast<std::ptrdiff_t>(pos), std::unordered_map<int, std::vector<BlockHandle>>());  // 初始化一个map
        auto& m = node->vals[pos];  // 获取刚刚初始化的map
        m[filenum].push_back(handle); // 添加
      }
      if(visited_keys.find(k) == visited_keys.end()) {
        visited_keys.insert(k);
        for(const auto& del_file : delete_files) {
          auto& m = node->vals[pos];
          m.erase(static_cast<int>(del_file));
        }
      }
      if ((int)node->keys.size() > MAX_KEYS) {
        SplitLeaf(node, sep, new_right);
        return true;
      }
      return false;
    }

    // 内部：找到子节点并下探
    size_t child_idx = UpperBoundChildIndex(node, k);
    Key child_sep{};
    Node* child_new_right = nullptr;
    bool child_split = InsertRecWithDelete(node->children[child_idx], k, filenum, handle, &child_sep, &child_new_right, delete_files, visited_keys);
    if (!child_split) return false;

    // 将子分裂上推的 sep 插入本节点
    node->keys.insert(node->keys.begin() + static_cast<std::ptrdiff_t>(child_idx), child_sep);
    node->children.insert(node->children.begin() + static_cast<std::ptrdiff_t>(child_idx + 1), child_new_right);

    if ((int)node->keys.size() > MAX_KEYS) {
      SplitInternal(node, sep, new_right);
      return true;
    }
    return false;
  }  

  // 叶子分裂：左保留 [0, mid)，右获得 [mid, n)，向上返回 sep = 右侧第 0 个 key
  void SplitLeaf(Node* left, Key* sep, Node** new_right) {
    const size_t n = left->keys.size();
    const size_t mid = n / 2;  // 平分
    Node* right = new Node(true);

    right->keys.assign(left->keys.begin() + static_cast<std::ptrdiff_t>(mid), left->keys.end());
    right->vals.assign(left->vals.begin() + static_cast<std::ptrdiff_t>(mid), left->vals.end());

    left->keys.resize(mid);
    left->vals.resize(mid);


    // 维护叶子链
    right->next = left->next;
    if (right->next) right->next->prev = right;
    left->next = right;
    right->prev = left;

    *sep = right->keys.front();
    *new_right = right;
  }

  // 内部分裂：提升中间 key，左保留 [0, mid)，右获得 (mid, n]，children 同步划分
  void SplitInternal(Node* left, Key* sep, Node** new_right) {
    const size_t n = left->keys.size();
    const size_t mid = n / 2;  // 提升 mid 位置 key

    Node* right = new Node(false);
    // 右边 keys = (mid, n)
    right->keys.assign(left->keys.begin() + static_cast<std::ptrdiff_t>(mid + 1), left->keys.end());
    // 右边 children = (mid, n]  共 right->keys.size() + 1 个
    right->children.assign(left->children.begin() + static_cast<std::ptrdiff_t>(mid + 1), left->children.end());

    // 上推分隔 key
    *sep = left->keys[mid];

    // 左边缩减为 [0, mid) 与 children [0, mid]
    left->keys.resize(mid);
    left->children.resize(mid + 1);

    *new_right = right;
  }

  // 返回第一个大于 k 的子节点下标
  size_t UpperBoundChildIndex(const Node* node, const Key& k) const {
    auto it = std::upper_bound(node->keys.begin(), node->keys.end(), k);
    return static_cast<size_t>(it - node->keys.begin()); // in [0, keys.size()]
  }

  const Node* FindLeaf(const Node* node, const Key& k) const {
    const Node* cur = node;
    while (cur && !cur->is_leaf) {
      size_t idx = UpperBoundChildIndex(cur, k);
      cur = cur->children[idx];
    }
    return cur;
  }

  const Node* FindLeafForLowerBound(const Node* node, const Key& k) const {
    const Node* cur = node;
    while (cur && !cur->is_leaf) {
      size_t idx = UpperBoundChildIndex(cur, k);
      cur = cur->children[idx];
    }
    if (!cur) return nullptr;
    // 叶子内也需要从第一个 >= k 的位置开始
    return cur;
  }

  // 可变版本的FindLeaf（用于Delete）
  Node* FindLeafMutable(Node* node, const Key& k) {
    Node* cur = node;
    while (cur && !cur->is_leaf) {
      size_t idx = UpperBoundChildIndex(cur, k);
      cur = cur->children[idx];
    }
    return cur;
  }

  void FreeRec(Node* x) {
    if (!x) return;
    if (!x->is_leaf) {
      for (Node* c : x->children) FreeRec(c);
    }
    delete x;
  }

  bool SaveRec(const Node* x, FILE* f) const {
    uint8_t leaf = x->is_leaf ? 1 : 0;
    uint32_t ksz = static_cast<uint32_t>(x->keys.size());
    if (1 != std::fwrite(&leaf, sizeof(leaf), 1, f)) return false;
    if (1 != std::fwrite(&ksz, sizeof(ksz), 1, f)) return false;
    if (ksz) {
      if (ksz != std::fwrite(x->keys.data(), sizeof(Key), ksz, f)) return false;
    }
    if (!x->is_leaf) {
      uint32_t csz = static_cast<uint32_t>(x->children.size());
      if (1 != std::fwrite(&csz, sizeof(csz), 1, f)) return false;
      for (const Node* c : x->children) {
        if (!SaveRec(c, f)) return false;
      }
    } else {
      // 叶子：为每个 key 写 (filenum, handle) 扁平化列表
      for (size_t i = 0; i < x->vals.size(); ++i) {
        const auto& m = x->vals[i];
        uint32_t vsz = 0; // total pairs
        for (const auto& kv : m) vsz += static_cast<uint32_t>(kv.second.size());
        if (1 != std::fwrite(&vsz, sizeof(vsz), 1, f)) return false;
        if (vsz) {
          for (const auto& kv : m) {
            int filenum = kv.first;
            const auto& vec = kv.second;
            for (const auto& handle : vec) {
              if (1 != std::fwrite(&filenum, sizeof(filenum), 1, f)) return false;
              if (1 != std::fwrite(&handle, sizeof(BlockHandle), 1, f)) return false;
            }
          }
        }
      }
    }
    return true;
  }

  Node* LoadRec(FILE* f) {
    uint8_t leaf = 0;
    uint32_t ksz = 0;
    auto rd = [&](void* p, size_t n){ return std::fread(p, 1, n, f) == n; };
    if (!rd(&leaf, sizeof(leaf)) || !rd(&ksz, sizeof(ksz))) return nullptr;
    Node* x = new Node(leaf != 0);
    x->keys.resize(ksz);
    if (ksz) {
      if (ksz != std::fread(x->keys.data(), sizeof(Key), ksz, f)) { delete x; return nullptr; }
    }
    if (!x->is_leaf) {
      uint32_t csz = 0;
      if (!rd(&csz, sizeof(csz))) { delete x; return nullptr; }
      x->children.resize(csz, nullptr);
      for (uint32_t i = 0; i < csz; ++i) {
        x->children[i] = LoadRec(f);
        if (!x->children[i]) { delete x; return nullptr; }
      }
    } else {
      x->vals.resize(ksz);
      for (uint32_t i = 0; i < ksz; ++i) {
        uint32_t vsz = 0;
        if (!rd(&vsz, sizeof(vsz))) { delete x; return nullptr; }
        auto& m = x->vals[i];
        for (uint32_t j = 0; j < vsz; ++j) {
          int filenum = 0;
          BlockHandle handle{};
          if (!rd(&filenum, sizeof(filenum)) || !rd(&handle, sizeof(handle))) { delete x; return nullptr; }
          m[filenum].push_back(handle);
        }
      }
    }
    return x;
  }

  void RebuildLeafLinks() {
    // 中序遍历，按从左到右的顺序把叶子串起来
    std::vector<Node*> leaves;
    CollectLeaves(root_, leaves);
    for (size_t i = 0; i < leaves.size(); ++i) {
      Node* cur = leaves[i];
      Node* nxt = (i + 1 < leaves.size()) ? leaves[i + 1] : nullptr;
      Node* prv = (i > 0) ? leaves[i - 1] : nullptr;
      cur->next = nxt;
      cur->prev = prv;
    }
  }

  void CollectLeaves(Node* x, std::vector<Node*>& out) {
    if (!x) return;
    if (x->is_leaf) { out.push_back(x); return; }
    for (Node* c : x->children) CollectLeaves(c, out);
  }

 private:
  Node* root_;
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // BPLUSTREE_H
