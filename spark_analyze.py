#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
spark .sparkprofile 分析器 v2

按 spark 官方 proto (spark_sampler.proto) 完整解析采样树，而不是按关键词扫字节。

  SamplerData     : 1 metadata, 2 threads, 3 class_sources,
                    4 method_sources, 5 line_sources, 6 time_windows,
                    7 statistics, 8 channel_info
  ThreadNode      : 1 name, 3 children(StackTraceNode), 4 times, 5 children_refs
  StackTraceNode  : 3 class, 4 method, 6 line, 7 desc, 8 times[], 9 refs[]
  children_refs   : 平铺数组里的**绝对下标**（自动校验，校验不过再试相对下标）

用法:
  python3 spark_analyze.py <file.sparkprofile> [选项]

  --top N           排行榜条数 (默认 25)
  --filter 文本      只看类名/方法名含该文本的条目（可重复给多次）
  --thread 名称      只看指定线程
  --no-windows      不打印每窗口表格
  --no-idle         不把 park/sleep 等空闲帧单独扣除
  --tree 文本        以匹配节点为根打印调用树
  --depth N          --tree 深度 (默认 3)
  --csv 路径         导出全部节点 (self/inclusive) 到 CSV
"""

import argparse
import collections
import gzip
import os
import struct
import sys

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

# ---------------------------------------------------------------- protobuf


def read_bytes(path):
    with open(path, "rb") as f:
        data = f.read()
    if data[:2] == b"\x1f\x8b":
        data = gzip.decompress(data)
    return data


def _varint(buf, i):
    out = 0
    shift = 0
    while True:
        b = buf[i]
        i += 1
        out |= (b & 0x7F) << shift
        if not b & 0x80:
            return out, i
        shift += 7


def parse_msg(buf):
    out = []
    i = 0
    n = len(buf)
    while i < n:
        key, i = _varint(buf, i)
        num, wt = key >> 3, key & 7
        if wt == 0:
            v, i = _varint(buf, i)
            out.append((num, wt, v))
        elif wt == 1:
            out.append((num, wt, struct.unpack("<d", buf[i:i + 8])[0]))
            i += 8
        elif wt == 2:
            ln, i = _varint(buf, i)
            out.append((num, wt, buf[i:i + ln]))
            i += ln
        elif wt == 5:
            out.append((num, wt, struct.unpack("<f", buf[i:i + 4])[0]))
            i += 4
        else:
            break
    return out


def packed_doubles(b):
    n = len(b) // 8
    return list(struct.unpack("<%dd" % n, b[:n * 8]))


def packed_ints(b):
    out = []
    i = 0
    while i < len(b):
        v, i = _varint(b, i)
        out.append(v)
    return out


def field(msg, num, default=None):
    for n, w, v in msg:
        if n == num:
            return v
    return default


def fields(msg, num):
    return [v for n, w, v in msg if n == num]


def s(v):
    return v.decode("utf-8", "replace") if isinstance(v, (bytes, bytearray)) else ""


# ---------------------------------------------------------------- 模型


class Node(object):
    __slots__ = ("cls", "method", "desc", "line", "times", "refs",
                 "total", "self", "kids", "parent", "depth", "idx", "idle")

    def __init__(self):
        self.cls = ""
        self.method = ""
        self.desc = ""
        self.line = 0
        self.times = []
        self.refs = []
        self.total = 0.0
        self.self = 0.0
        self.kids = []
        self.parent = -1
        self.depth = 0
        self.idx = 0
        self.idle = False

    @property
    def name(self):
        return self.cls.split(".")[-1] + "." + self.method

    @property
    def full(self):
        return self.cls + "." + self.method if self.method else self.cls


class Thread(object):
    def __init__(self, name):
        self.name = name
        self.nodes = []
        self.roots = []
        self.total = 0.0
        self.idle = 0.0


def load(path):
    data = read_bytes(path)
    top = parse_msg(data)

    meta = parse_msg(field(top, 1, b""))
    info = {
        "interval": field(meta, 3),
        "start": field(meta, 2),
        "end": field(meta, 11),
        "ticks": field(meta, 12),
        "comment": s(field(meta, 6, b"")),
        "agg_type": 0,
        "mode": field(meta, 15, 0) or 0,
        "engine": field(meta, 16, 0) or 0,
        "platform": "",
    }
    agg = field(meta, 5)
    if agg:
        info["agg_type"] = field(parse_msg(agg), 1, 0) or 0
    plat = field(meta, 7)
    if plat:
        p = parse_msg(plat)
        info["platform"] = " ".join(x for x in (s(field(p, 2, b"")), s(field(p, 3, b"")),
                                                s(field(p, 4, b""))) if x)
    windows = []
    for n, w, v in top:
        if n == 6 and w == 2:
            windows.extend(packed_ints(v))
        elif n == 6 and w == 0:
            windows.append(v)
    info["windows"] = windows

    threads = []
    for tv in fields(top, 2):
        tf = parse_msg(tv)
        th = Thread(s(field(tf, 1, b"")) or "?")
        for nv in [v for n, w, v in tf if n == 3]:
            nf = parse_msg(nv)
            nd = Node()
            for n, w, v in nf:
                if n == 3 and w == 2:
                    nd.cls = s(v)
                elif n == 4 and w == 2:
                    nd.method = s(v)
                elif n == 6 and w == 0:
                    nd.line = v
                elif n == 7 and w == 2:
                    nd.desc = s(v)
                elif n == 8 and w == 2:
                    nd.times = packed_doubles(v)
                elif n == 9 and w == 2:
                    nd.refs = packed_ints(v)
            nd.total = sum(nd.times)
            nd.idx = len(th.nodes)
            th.nodes.append(nd)
        _build_tree(th)
        threads.append(th)
    return info, threads


def _build_tree(th):
    nodes = th.nodes

    def attempt(absolute):
        kids = collections.defaultdict(list)
        parent = {}
        bad = 0
        for i, nd in enumerate(nodes):
            for r in nd.refs:
                ci = r if absolute else i + r + 1
                if ci < 0 or ci >= len(nodes) or ci == i or ci in parent:
                    bad += 1
                    continue
                parent[ci] = i
                kids[i].append(ci)
        for i in range(len(nodes)):
            seen = set()
            j = i
            while j in parent:
                if j in seen:
                    bad += 1
                    break
                seen.add(j)
                j = parent[j]
        for i, nd in enumerate(nodes):
            if sum(nodes[c].total for c in kids[i]) - nd.total > 1e-6:
                bad += 1
        return kids, parent, bad

    kids, parent, bad = attempt(True)
    if bad:
        k2, p2, bad2 = attempt(False)
        if bad2 < bad:
            kids, parent = k2, p2

    for i, nd in enumerate(nodes):
        nd.kids = kids.get(i, [])
        nd.parent = parent.get(i, -1)
        nd.self = nd.total - sum(nodes[c].total for c in nd.kids)
        if nd.self < 0:
            nd.self = 0.0
    roots = [i for i in range(len(nodes)) if nodes[i].parent < 0]
    stack = [(r, 0) for r in roots]
    while stack:
        i, d = stack.pop()
        nodes[i].depth = d
        for c in nodes[i].kids:
            stack.append((c, d + 1))
    th.roots = roots
    th.total = sum(nodes[r].total for r in roots)


# ---------------------------------------------------------------- 分类


IDLE_METHODS = {"park", "parkNanos", "parkUntil", "sleep", "sleep0", "waitForTasks"}
IDLE_CLASS = ("LockSupport", "Unsafe", "Object.wait", "Thread.sleep")


def is_idle(nd):
    return nd.method in IDLE_METHODS or any(h in nd.cls for h in IDLE_CLASS)


def mark_idle(th):
    """标出空闲子树（park/sleep 这些，主线程大部分时间都在这里等下一 tick）。"""
    nodes = th.nodes
    tops = [nd for nd in nodes
            if is_idle(nd) and (nd.parent < 0 or not is_idle(nodes[nd.parent]))]
    stack = list(tops)
    while stack:
        nd = stack.pop()
        nd.idle = True
        for c in nd.kids:
            stack.append(nodes[c])
    th.idle = sum(nd.total for nd in tops)


def owner_of(cls):
    if cls.startswith("native") or cls.startswith("libjvm") or cls.startswith("libc") or cls.startswith("0x000"):
        return "native/JVM"
    for pre, owner in (("net.minecraft.", "minecraft"), ("net.minecraftforge.", "forge"),
                       ("cpw.mods.", "forge"), ("java.", "java"), ("jdk.", "java"),
                       ("sun.", "java"), ("it.unimi.dsi.fastutil", "fastutil"),
                       ("io.netty.", "netty"), ("com.google.common", "guava"),
                       ("org.spongepowered.asm", "mixin"), ("org.embeddedt.modernfix", "modernfix"),
                       ("com.mojang.", "mojang")):
        if cls.startswith(pre):
            return owner
    parts = cls.split(".")
    return ".".join(parts[:3]) if len(parts) >= 3 else cls


def human(ms):
    return "%.2f s" % (ms / 1000.0) if ms >= 1000 else "%.0f ms" % ms


# ---------------------------------------------------------------- 报告


def report(info, threads, args, path):
    unit = "bytes" if info["mode"] == 1 else ("ns" if info["mode"] == 2 else "ms")
    total = sum(t.total for t in threads)
    dur = (info["end"] - info["start"]) / 1000.0 if info["start"] and info["end"] else None

    print("=" * 78)
    print("  spark profile : %s  (%.1f KB)" % (os.path.basename(path), os.path.getsize(path) / 1024.0))
    print("=" * 78)
    print("  平台        : %s" % (info["platform"] or "?"))
    print("  采样          : %s / %s / %s" % (
        ("执行", "内存分配", "锁")[info["mode"] if info["mode"] in (0, 1, 2) else 0],
        "Java采样器" if info["engine"] == 0 else "Async采样器",
        "简单平均" if info["agg_type"] == 0 else "只统计慢tick"))
    print("  采样间隔    : %s us" % info["interval"])
    if dur:
        print("  采样区间    : %.1f 秒" % dur)
    if info["ticks"] and dur:
        print("  覆盖 tick   : %d  (平均 %.2f TPS)" % (info["ticks"], info["ticks"] / dur))
    print("  时间窗口    : %d 个" % (len(info["windows"]) or 1))
    print("  采样总时间  : %s   (单位 %s，线程 %d 个)" % (human(total), unit, len(threads)))
    if info["comment"]:
        print("  备注        : %s" % info["comment"][:100])

    for th in threads:
        if args.thread and args.thread.lower() not in th.name.lower():
            continue
        mark_idle(th)
        nodes = th.nodes
        idle_t = 0.0 if args.no_idle else th.idle
        work = th.total - idle_t

        print("\n" + "-" * 78)
        print("  线程 %r   采样 %s   空闲 %s (%.1f%%)   工作 %s (%.1f%%)" % (
            th.name, human(th.total), human(idle_t),
            idle_t / th.total * 100 if th.total else 0, human(work),
            work / th.total * 100 if th.total else 0))

        tick_nodes = [nd for nd in nodes
                      if nd.method in ("m_5705_", "tickServer") and "MinecraftServer" in nd.cls]
        if not tick_nodes:
            tick_nodes = [nd for nd in nodes if nd.method in ("m_5705_", "tickServer")]
        tick_total = sum(nd.total for nd in tick_nodes
                         if nd.parent < 0 or nodes[nd.parent] not in tick_nodes)
        if tick_nodes and info["ticks"]:
            print("  tick 工作量 %s   平均 %.2f ms/tick   (tick 预算 50 ms)" % (
                human(tick_total), tick_total / info["ticks"]))

        # ---- 每窗口
        src = None
        for nd in tick_nodes:
            if len(nd.times) > 1:
                src = nd
                break
        if src is None and th.roots:
            src = nodes[th.roots[0]]
        if src is not None and len(src.times) > 1 and not args.no_windows:
            vals = list(src.times)
            med = sorted(vals)[len(vals) // 2]
            per_tick = (info["ticks"] / float(len(vals))) if info["ticks"] else 0
            print("  每窗口 tick 工作量:")
            for i, v in enumerate(vals):
                flag = ""
                if med and v > med * 2.5:
                    flag = "   <<< 尖峰 %.1fx" % (v / med)
                extra = ("   平均 %.2f ms/tick" % (v / per_tick)) if per_tick else ""
                print("     w%-2d %10s%s%s" % (i, human(v), extra, flag))

        # ---- 聚合排行
        sel = nodes
        if args.filter:
            lows = [f.lower() for f in args.filter]
            sel = [nd for nd in nodes if any(l in nd.full.lower() for l in lows)]

        self_agg = collections.Counter()
        incl_agg = collections.Counter()
        who = {}
        for nd in sel:
            if nd.total <= 0:
                continue
            key = (nd.cls, nd.method)
            incl_agg[key] += nd.total
            if not nd.idle and nd.self > 0:
                self_agg[key] += nd.self
                if nd.parent >= 0:
                    p = nodes[nd.parent]
                    pn = p.full
                    if self_agg[key] >= 0 and (key not in who or True):
                        who.setdefault(key, pn)

        def table(title, agg, denom, with_caller):
            print("\n  %s" % title)
            print("  %-52s %10s %7s" % ("class.method", "时间", "占比"))
            print("  " + "-" * 52 + " " + "-" * 10 + " " + "-" * 7)
            for (c, m), v in agg.most_common(args.top):
                tag = ""
                if with_caller and (c, m) in who:
                    tag = "  <- " + who[(c, m)].split(".")[-1]
                name = c.split(".")[-1] + "." + m
                print("  %-52s %10s %6.1f%%%s" % (name[:52], human(v),
                                                  v / denom * 100 if denom else 0, tag))

        denom = th.total or 1.0
        table("TOP %d  自耗 self time (已排除空闲帧)" % args.top, self_agg, denom, True)
        table("TOP %d  累计 inclusive" % args.top, incl_agg, denom, True)

        # ---- tick 拆解
        for nd in sorted(tick_nodes, key=lambda x: -x.total):
            if nd.parent >= 0 and nodes[nd.parent] in tick_nodes:
                continue
            print("\n  tick 内部拆解 (%s = %s)" % (nd.full, human(nd.total)))
            for c in sorted(nd.kids, key=lambda i: -nodes[i].total)[:15]:
                cn = nodes[c]
                print("    %-46s %10s %6.1f%%" % (cn.name[:46], human(cn.total),
                                                  cn.total / nd.total * 100 if nd.total else 0))
            break

        # ---- 按来源
        by_owner = collections.Counter()
        for (c, m), v in self_agg.items():
            by_owner[owner_of(c)] += v
        print("\n  按来源 (self time 聚合，前 15)")
        for k, v in by_owner.most_common(15):
            print("    %-46s %10s %6.1f%%" % (k[:46], human(v), v / denom * 100))

        # ---- 调用树
        if args.tree:
            cands = [nd for nd in sel if args.tree.lower() in nd.full.lower()]
            cands.sort(key=lambda x: -x.total)
            if cands:
                root = cands[0]
                print("\n  调用树: %s  (%s)" % (root.full, human(root.total)))

                def walk(i, d):
                    if d > args.depth:
                        return
                    nd = nodes[i]
                    print("    " + "  " * d + "%-44s %10s" % (nd.name[:44], human(nd.total)))
                    for c in sorted(nd.kids, key=lambda x: -nodes[x].total)[:8]:
                        walk(c, d + 1)
                walk(root.idx, 0)

    if args.csv:
        with open(args.csv, "w", encoding="utf-8") as f:
            f.write("thread,class,method,self_ms,inclusive_ms,depth,parent\n")
            for th in threads:
                for nd in th.nodes:
                    par = th.nodes[nd.parent].full if nd.parent >= 0 else ""
                    f.write("%s,%s,%s,%.3f,%.3f,%d,%s\n" % (
                        th.name, nd.cls, nd.method, nd.self, nd.total, nd.depth, par))
        print("\n  CSV -> %s" % args.csv)


def main():
    ap = argparse.ArgumentParser(description="spark .sparkprofile 分析器 v2")
    ap.add_argument("file")
    ap.add_argument("--top", type=int, default=25)
    ap.add_argument("--filter", action="append", default=[])
    ap.add_argument("--thread", default=None)
    ap.add_argument("--no-windows", action="store_true")
    ap.add_argument("--no-idle", action="store_true")
    ap.add_argument("--tree", default=None)
    ap.add_argument("--depth", type=int, default=3)
    ap.add_argument("--csv", default=None)
    args = ap.parse_args()
    if not os.path.isfile(args.file):
        sys.exit("文件不存在: %s" % args.file)
    info, threads = load(args.file)
    report(info, threads, args, args.file)


if __name__ == "__main__":
    main()
