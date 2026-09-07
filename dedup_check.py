import gzip, json, collections
p="/www/wwwroot/polymarket-l2/l2_data/2026-09-07/eth-updown-5m-1788801300.jsonl.gz"
n_book=n_lt=n_mark=n_other=0; lt_tx=[]; book_h=[]
with gzip.open(p,"rt",encoding="utf-8") as f:
    for line in f:
        try: m=json.loads(line)
        except Exception: continue
        if "mark" in m: n_mark+=1; continue
        et=m.get("event_type","")
        if et=="last_trade_price":
            n_lt+=1; lt_tx.append(m.get("transaction_hash") or m.get("hash") or m.get("txhash") or "(无)")
        elif "bids" in m or "asks" in m or et=="book":
            n_book+=1; book_h.append(m.get("hash"))
        else: n_other+=1
print(f"文件: {p}")
print(f"mark={n_mark} book={n_book} last_trade={n_lt} 其它={n_other}")
print(f"book 行={n_book} 唯一hash={len(set(book_h))} 重复={n_book-len(set(book_h))}")
print(f"last_trade 行={n_lt} 唯一tx={len(set(lt_tx))} 重复={n_lt-len(set(lt_tx))}")
c=collections.Counter([x for x in lt_tx if x!="(无)"])
dups=[(k,v) for k,v in c.items() if v>1]
print(f"last_trade 重复tx数={len(dups)} 示例={dups[:3]}")
c2=collections.Counter([x for x in book_h if x])
dups2=[(k,v) for k,v in c2.items() if v>1]
print(f"book 重复hash数={len(dups2)} 示例={dups2[:3]}")
