#ifndef BPLUSTREE_H
#define BPLUSTREE_H

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <functional>
#include <cstdio>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

// 轻量级 B+ 树实现（头文件-only），仿照 util/RTree_mem.h 的风格：
// - 模板参数化 Key/Value 与节点容量
// - 仅支持基本功能：Insert / Search / RangeSearch（叶子链顺序扫描）
// - 重复 key 时，所有 value 聚合存放在叶子节点对应 key 的一个 vector 中
// - 不实现删除（如需删除可后续扩展），不实现持久化

namespace ROCKSDB_NAMESPACE {

template <class Key, class Value, int MAX_KEYS = 64, int MIN_KEYS = MAX_KEYS / 2>
class BPlusTree {
  static_assert(MAX_KEYS >= 3, "MAX_KEYS must be >= 3");
  static_assert(MIN_KEYS >= 1, "MIN_KEYS must be >= 1");

 private:
  struct Node {
    bool is_leaf;
    std::vector<Key> keys;                 // 大小 <= MAX_KEYS
    std::vector<Node*> children;           // 内部节点 children.size() == keys.size() + 1
    std::vector<std::vector<Value>> vals;  // 仅叶子：与 keys 对齐，一个 key 一个 value 向量
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

  // 插入：若 key 已存在，则将 value 追加到该 key 的 value 向量尾部；否则创建新的 key 槽
  void Insert(const Key& k, const Value& v) {
    Key sep{};
    Node* new_child = nullptr;
    bool split = InsertRec(root_, k, v, &sep, &new_child);
    if (split) {
      // 根分裂：新建一层
      Node* new_root = new Node(false);
      new_root->keys.push_back(sep);
      new_root->children.push_back(root_);
      new_root->children.push_back(new_child);
      root_ = new_root;
    }
  }

  // 精确查找：返回是否找到；若找到则 out_values 填充该 key 的所有 values
  bool Search(const Key& k, std::vector<Value>& out_values) const {
    const Node* leaf = FindLeaf(root_, k);
    if (!leaf) return false;
    auto it = std::lower_bound(leaf->keys.begin(), leaf->keys.end(), k);
    if (it != leaf->keys.end() && !(*it < k) && !(k < *it)) {
      size_t idx = static_cast<size_t>(it - leaf->keys.begin());
      out_values = leaf->vals[idx];
      return true;
    }
    return false;
  }

  // 返回某 key 的所有 values；若不存在返回空 vector
  std::vector<Value> Get(const Key& k) const {
    std::vector<Value> res;
    Search(k, res);
    return res;
  }

  // 区间查询 [l, r]，对每个匹配到的 (key, value) 调用 callback(key, value)
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
        for (const auto& val : leaf->vals[i]) {
          if (!cb(key, val)) return;
        }
      }
      leaf = leaf->next;
    }
  }

  // 简单统计叶子（粗略示例）
  size_t LeafCount() const {
    const Node* x = root_;
    while (x && !x->is_leaf) x = x->children.front();
    size_t cnt = 0;
    while (x) {
      ++cnt;
      x = x->next;
    }
    return cnt;
  }

  // --- 持久化（简单二进制格式，仿照 RTree_mem 的思路） ---
  // 格式：
  // header: magic(\"BPLS\"), key_size, val_size, max_keys, min_keys
  // 然后递归写节点：is_leaf(uint8_t), keys_count(uint32_t), keys[],
  // 如果内部节点：children_count(uint32_t)=keys_count+1，递归写子
  // 如果叶子：对每个 key 写 values_count(uint32_t) 与 values[]

  bool Save(const char* path) const {
    FILE* f = std::fopen(path, "wb");
    if (!f) return false;
    auto close_guard = [](FILE* pf){ if (pf) std::fclose(pf); };
    // 写头
    const uint32_t magic = ('B'<<0)|('P'<<8)|('L'<<16)|('S'<<24);
    uint32_t key_size = static_cast<uint32_t>(sizeof(Key));
    uint32_t val_size = static_cast<uint32_t>(sizeof(Value));
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
    if (magic != expect || key_size != sizeof(Key) || val_size != sizeof(Value) ||
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

  // 递归插入；若当前节点产生分裂，返回 true，并通过 sep/new_right 向父节点上推分裂信息，Rec是Recursive的缩写
  bool InsertRec(Node* node, const Key& k, const Value& v, Key* sep, Node** new_right) {
    if (node->is_leaf) {
      // 在叶子插入或追加
      auto it = std::lower_bound(node->keys.begin(), node->keys.end(), k);
      size_t pos = static_cast<size_t>(it - node->keys.begin());
      if (it != node->keys.end() && !(*it < k) && !(k < *it)) {
        // 命中：聚合到对应的 value 向量
        node->vals[pos].push_back(v);
      } else {
        node->keys.insert(node->keys.begin() + static_cast<std::ptrdiff_t>(pos), k);
        node->vals.insert(node->vals.begin() + static_cast<std::ptrdiff_t>(pos), std::vector<Value>{v});
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
    bool child_split = InsertRec(node->children[child_idx], k, v, &child_sep, &child_new_right);
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

  // 在内部节点中找到应当下降的子下标：first i 使得 k < keys[i]，否则落到最后一个孩子
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
      // 叶子：为每个 key 写 value 列表
      for (const auto& vec : x->vals) {
        uint32_t vsz = static_cast<uint32_t>(vec.size());
        if (1 != std::fwrite(&vsz, sizeof(vsz), 1, f)) return false;
        if (vsz) {
          if (vsz != std::fwrite(vec.data(), sizeof(Value), vsz, f)) return false;
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
        x->vals[i].resize(vsz);
        if (vsz) {
          if (vsz != std::fread(x->vals[i].data(), sizeof(Value), vsz, f)) { delete x; return nullptr; }
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
