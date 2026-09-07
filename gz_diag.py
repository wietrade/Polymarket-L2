"""gz_diag.py — 诊断 gzip 文件结构(成员数/损坏位置). 用法: python gz_diag.py <file>..."""
import re
import sys
import zlib

for p in sys.argv[1:]:
    data = open(p, "rb").read()
    idx = [m.start() for m in re.finditer(b"\x1f\x8b\x08", data)]
    print(f"{p.split('/')[-1]}: size={len(data)} 成员数={len(idx)} 偏移={idx[:8]}")
    for i, off in enumerate(idx):
        end = idx[i + 1] if i + 1 < len(idx) else len(data)
        member = data[off:end]
        try:
            d = zlib.decompress(member, 16 + zlib.MAX_WBITS)
            print(f"  成员{i} off={off} len={len(member)} 解压OK 行={len(d.splitlines())}")
        except Exception as e:
            print(f"  成员{i} off={off} len={len(member)} 失败: {type(e).__name__} {e}")
