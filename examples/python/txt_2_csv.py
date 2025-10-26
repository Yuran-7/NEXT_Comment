import csv

def txt_to_csv(txt_path, csv_path):
    with open(txt_path, 'r', encoding='utf-8') as fin, \
         open(csv_path, 'w', newline='', encoding='utf-8') as fout:

        writer = csv.writer(fout)
        # 写入表头
        writer.writerow(["id", "area", "min_lon", "min_lat", "max_lon", "max_lat", "geom", "tags"])

        for line in fin:
            line = line.strip()
            if not line:
                continue

            # 按制表符切分
            parts = line.split('\t')

            if len(parts) < 8:
                print(f"⚠️ 跳过格式异常行: {line[:100]}...")
                continue

            # 直接写入一行
            writer.writerow(parts[:8])

    print(f"✅ 转换完成: {csv_path}")


if __name__ == "__main__":
    txt_to_csv("/NV1/ysh/dataset/buildings_10m.txt", "/NV1/ysh/dataset/buildings_10m.csv")
