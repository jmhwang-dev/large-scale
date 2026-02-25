"""
5억건 데이터 증강 — OOM 방어 및 작업 완료 후 멈춤(Hanging) 완벽 해결
══════════════════════════════════════════════════════════
[수정 내용]
 1. 청크 분할(Chunking): Zipf 가중치가 1000배가 넘더라도, 한 번에 10배수씩만 메모리에 올림.
 2. 미니 PC 최적화: 4코어 16GB 환경에 맞춰 MAX_WORKERS=2, MEM_PAUSE_PCT=75 설정.
 3. 강제 즉시 종료 (os._exit): 
    - 작업이 100% 완료되면 ProcessPoolExecutor의 무한 대기 버그를 무시하고 즉시 터미널로 복귀.
    - Ctrl+C 입력 시 진행 중인 자식 프로세스들을 psutil로 강제 사살하고 즉시 튕겨 나옴.
"""

import os, sys, gc, time, logging, signal, atexit, random, traceback
import concurrent.futures
from multiprocessing import shared_memory
from typing import Optional
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import psutil

# ══════════════════════════════════════════════════════
# ❶ 설정값 (4코어 16GB 미니 PC 최적화)
# ══════════════════════════════════════════════════════
DATASET_DIR      = 'downloads/olist'
DST_DIR          = 'downloads/olist_augmented'

TARGET_SCALE     = 5000
MAX_WORKERS      = 3    # 16GB 메모리 보호 및 컨텍스트 스위칭 최소화
BATCH_SIZE       = 50   # 큐에 과도하게 쌓이는 것 방지
MAX_QUEUE_DEPTH  = 4    # MAX_WORKERS * 2
COMPRESSION      = 'snappy'

MEM_PAUSE_PCT    = 75   # 85%는 위험. 75%에서 멈춰서 OOM Killer 회피
MIN_FREE_DISK_GB = 15
MAX_RETRY        = 4
RETRY_BASE_SEC   = 1.5

MAX_REPEAT_CHUNK = 5    # 단일 워커가 한 번에 너무 많은 데이터를 올리지 못하게 쪼갬

ZIPF_ALPHA       = 1.2
HOT_KEY_RATIO    = 0.40
HOT_KEY_TOP_N    = 10
MONTHLY_WEIGHTS  = [0.5, 0.5, 0.7, 0.7, 0.8, 0.9,
                    0.9, 1.0, 1.1, 1.3, 1.8, 2.0]
DATE_START       = pd.Timestamp('2018-01-01')
DATE_END         = pd.Timestamp('2025-12-31')

# ══════════════════════════════════════════════════════
# ❷ 로깅
# ══════════════════════════════════════════════════════
os.makedirs(DST_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            os.path.join(DST_DIR, '_augment.log'), mode='a', encoding='utf-8'),
    ]
)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════
# ❸ Skew 유틸
# ══════════════════════════════════════════════════════
def build_zipf_weights(n: int, alpha: float) -> np.ndarray:
    ranks = np.arange(1, n + 1, dtype=float)
    w     = 1.0 / (ranks ** alpha)
    raw   = w / w.sum() * n
    ints  = raw.astype(int).clip(min=1)
    diff  = n - ints.sum()
    if diff > 0:
        ints[np.argsort(-(raw - ints))[:diff]] += 1
    return ints


def build_day_weights() -> np.ndarray:
    days  = (DATE_END - DATE_START).days + 1
    day_w = np.zeros(days, dtype=float)
    for m in pd.date_range(DATE_START, DATE_END, freq='MS'):
        mw = MONTHLY_WEIGHTS[(m.month - 1) % 12]
        s  = (m - DATE_START).days
        e  = min(s + m.days_in_month, days)
        day_w[s:e] = mw
    day_w /= day_w.sum()
    return day_w


def apply_date_skew(df: pd.DataFrame, col_dtypes: dict,
                    seed: int, day_w: np.ndarray) -> pd.DataFrame:
    date_cols = [c for c in df.columns if 'date' in c.lower() or 'timestamp' in c.lower()]
    if not date_cols:
        return df
        
    rng   = np.random.default_rng(seed=seed)
    offs  = rng.choice(len(day_w), size=len(df), replace=True, p=day_w)
    target_dates = DATE_START + pd.to_timedelta(offs, unit='D')
    
    base_col = 'order_purchase_timestamp' if 'order_purchase_timestamp' in date_cols else date_cols[0]
    base_ts  = pd.to_datetime(df[base_col], errors='coerce')
    shift_delta = target_dates - base_ts
    
    for col in date_cols:
        orig_ts = pd.to_datetime(df[col], errors='coerce')
        new_ts  = orig_ts + shift_delta
        try:
            df[col] = new_ts.astype(col_dtypes.get(col, 'object')).to_numpy()
        except Exception:
            df[col] = new_ts.astype(str).to_numpy()
    return df


def apply_hot_key_skew(df: pd.DataFrame, hot_sellers: list, seed: int) -> pd.DataFrame:
    if 'seller_id' not in df.columns or len(hot_sellers) == 0:
        return df
        
    rng      = np.random.default_rng(seed=seed + 99999)
    hot_mask = rng.random(len(df)) < HOT_KEY_RATIO
    n_hot    = int(hot_mask.sum())
    
    if n_hot > 0:
        df.loc[hot_mask, 'seller_id'] = np.array(hot_sellers)[
            rng.integers(0, len(hot_sellers), size=n_hot)]
    return df


# ══════════════════════════════════════════════════════
# ❹ Shared Memory
# ══════════════════════════════════════════════════════
_shm_handles: dict[str, shared_memory.SharedMemory] = {}

def _cleanup_stale_shm():
    if not os.path.isdir('/dev/shm'): return
    for fname in os.listdir('/dev/shm'):
        if fname.startswith('olist_aug_'):
            try:
                s = shared_memory.SharedMemory(name=fname, create=False)
                s.close(); s.unlink()
            except Exception: pass

def df_to_shm(df: pd.DataFrame, key: str) -> dict:
    import pickle
    data = pickle.dumps(df, protocol=5)
    name = f"olist_aug_{key}_{os.getpid()}"
    shm  = shared_memory.SharedMemory(create=True, size=max(len(data), 1), name=name)
    shm.buf[:len(data)] = data
    _shm_handles[name]  = shm
    log.info(f"  SHM '{key}': {len(data)/1024/1024:.1f} MB")
    return {'name': name, 'size': len(data)}

def shm_to_df(meta: dict) -> pd.DataFrame:
    import pickle
    shm = shared_memory.SharedMemory(name=meta['name'], create=False)
    try: return pickle.loads(bytes(shm.buf[:meta['size']]))
    finally: shm.close()

def cleanup_shm():
    for shm in _shm_handles.values():
        try: shm.close()
        except Exception: pass
        try: shm.unlink()
        except Exception: pass
    _shm_handles.clear()


# ══════════════════════════════════════════════════════
# ❺ 워커 (Chunking으로 메모리 폭발 완벽 차단)
# ══════════════════════════════════════════════════════
_worker_shm_handles: list[shared_memory.SharedMemory] = []

def _worker_cleanup():
    for shm in _worker_shm_handles:
        try: shm.close()
        except Exception: pass
    _worker_shm_handles.clear()

def _worker_init(meta: dict):
    global _wmeta
    _wmeta = meta
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    atexit.register(_worker_cleanup)


def _concat_with_suffix(base_vals: np.ndarray, chunk: int, sfx: str, offset: int, total_repeat: int) -> np.ndarray:
    tiled = np.tile(base_vals, chunk)
    if total_repeat == 1:
        return np.char.add(tiled, sfx)
    sub = np.repeat(np.arange(offset, offset + chunk).astype(str), len(base_vals))
    return np.char.add(np.char.add(tiled, sfx + '_r'), sub)


def _build_batch(args: tuple) -> tuple[list[int], bool, Optional[str]]:
    batch_indices, zipf_weights, hot_sellers, day_w_list = args
    day_w = np.array(day_w_list)

    meta  = _wmeta
    dst   = meta['dst']
    rep   = batch_indices[0]
    end   = batch_indices[-1]
    fname = f"part-{rep:04d}-{end:04d}.parquet"
    paths: dict[str, str] = {}
    writers: dict[str, pq.ParquetWriter] = {}

    for attempt in range(MAX_RETRY):
        paths.clear()
        
        c_base = o_base = oi_base = op_base = None

        try:
            c_base  = shm_to_df(meta['c_shm'])
            o_base  = shm_to_df(meta['o_shm'])
            oi_base = shm_to_df(meta['oi_shm'])
            op_base = shm_to_df(meta['op_shm'])

            o_col_dtypes = {c: str(o_base[c].dtype) for c in o_base.columns}

            c_cid_vals  = c_base['customer_id'].to_numpy(dtype=str)
            o_oid_vals  = o_base['order_id'].to_numpy(dtype=str)
            o_cid_vals  = o_base['customer_id'].to_numpy(dtype=str)
            oi_oid_vals = oi_base['order_id'].to_numpy(dtype=str)
            op_oid_vals = op_base['order_id'].to_numpy(dtype=str)

            write_cfg = dict(compression=meta['compression'],
                             use_dictionary=True, write_statistics=False)

            for i, total_repeat in zip(batch_indices, zipf_weights):
                sfx = f"_{i:04d}"
                
                remaining = total_repeat
                offset = 0
                
                while remaining > 0:
                    chunk = min(MAX_REPEAT_CHUNK, remaining)
                    seed_val = i * 100000 + offset  

                    # ── 고객 ──
                    c = pd.concat([c_base] * chunk, ignore_index=True, copy=False)
                    c['customer_id'] = _concat_with_suffix(c_cid_vals, chunk, sfx, offset, total_repeat)

                    # ── 주문 ──
                    o = pd.concat([o_base] * chunk, ignore_index=True, copy=False)
                    o['order_id']    = _concat_with_suffix(o_oid_vals, chunk, sfx, offset, total_repeat)
                    o['customer_id'] = _concat_with_suffix(o_cid_vals, chunk, sfx, offset, total_repeat)
                    o = apply_date_skew(o, o_col_dtypes, seed_val, day_w)

                    # ── 주문 상품 ──
                    oi = pd.concat([oi_base] * chunk, ignore_index=True, copy=False)
                    oi_new_oids = _concat_with_suffix(oi_oid_vals, chunk, sfx, offset, total_repeat)
                    oi['order_id'] = oi_new_oids
                    oi = apply_hot_key_skew(oi, hot_sellers, seed_val)

                    # ── 결제 ──
                    op = pd.concat([op_base] * chunk, ignore_index=True, copy=False)
                    op['order_id'] = _concat_with_suffix(op_oid_vals, chunk, sfx, offset, total_repeat)

                    # ── 즉시 디스크 쓰기 및 RAM 반환 ──
                    for tname, df in [('customers', c), ('orders', o),
                                      ('order_items', oi), ('order_payments', op)]:
                        table = pa.Table.from_pandas(df, preserve_index=False)
                        if tname not in writers:
                            tdir = os.path.join(dst, tname)
                            os.makedirs(tdir, exist_ok=True)
                            path = os.path.join(tdir, fname)
                            paths[tname] = path
                            writers[tname] = pq.ParquetWriter(path, table.schema, **write_cfg)
                        writers[tname].write_table(table)

                    del c, o, oi, op, oi_new_oids, table
                    gc.collect()
                    
                    remaining -= chunk
                    offset += chunk

            for w in writers.values(): w.close()
            writers.clear()

            return batch_indices, True, None

        except Exception as e:
            for w in writers.values():
                try: w.close()
                except Exception: pass
            writers.clear()
            for p in paths.values():
                try:
                    if os.path.exists(p): os.remove(p)
                except Exception: pass
                
            wait = RETRY_BASE_SEC ** attempt
            log.warning(
                f"배치 {rep}~{end} 실패 (시도 {attempt+1}/{MAX_RETRY}), "
                f"{wait:.1f}s 후 재시도 | {e}")
            time.sleep(wait)

        finally:
            for obj in [c_base, o_base, oi_base, op_base]:
                del obj
            gc.collect()

    return batch_indices, False, f"MAX_RETRY({MAX_RETRY}) 초과"


# ══════════════════════════════════════════════════════
# ❻ 제출 상태 관리
# ══════════════════════════════════════════════════════
class _SubmitState:
    def __init__(self, pending: list, executor, meta: dict):
        self._it       = iter(pending)
        self._ex       = executor
        self.active: dict[concurrent.futures.Future, tuple] = {}
        self.exhausted = False

    def submit_next(self):
        if self.exhausted: return
        try:
            args = next(self._it)
            f    = self._ex.submit(_build_batch, args)
            self.active[f] = args
        except StopIteration:
            self.exhausted = True

    def fill_queue(self):
        if psutil.virtual_memory().percent >= MEM_PAUSE_PCT:
            log.warning(f"⏸ RAM {psutil.virtual_memory().percent:.0f}% → 제출 대기")
            return
        while len(self.active) < MAX_QUEUE_DEPTH and not self.exhausted:
            self.submit_next()


# ══════════════════════════════════════════════════════
# ❼ 사전 검사 & 이어하기
# ══════════════════════════════════════════════════════
def preflight_check():
    free_gb = psutil.disk_usage(DST_DIR).free / 1024**3
    ram_gb  = psutil.virtual_memory().available / 1024**3
    log.info(
        f"💾 디스크 여유: {free_gb:.1f}GB | RAM 여유: {ram_gb:.1f}GB "
        f"| 코어: {psutil.cpu_count(logical=False)}")
    if free_gb < MIN_FREE_DISK_GB:
        raise RuntimeError(f"디스크 여유 부족: {free_gb:.1f}GB")
    if ram_gb < 4.0:
        raise RuntimeError(f"RAM 여유 부족: {ram_gb:.1f}GB")

def batch_is_done(indices: list[int]) -> bool:
    rep, end = indices[0], indices[-1]
    return os.path.exists(
        os.path.join(DST_DIR, 'orders', f"part-{rep:04d}-{end:04d}.parquet"))


# ══════════════════════════════════════════════════════
# ❽ 메인
# ══════════════════════════════════════════════════════
if __name__ == '__main__':
    preflight_check()
    _cleanup_stale_shm()

    log.info("📦 원본 데이터 로드 중...")
    raw = {
        'customers':      pd.read_csv(f'{DATASET_DIR}/olist_customers_dataset.csv'),
        'orders':         pd.read_csv(f'{DATASET_DIR}/olist_orders_dataset.csv'),
        'order_items':    pd.read_csv(f'{DATASET_DIR}/olist_order_items_dataset.csv'),
        'order_payments': pd.read_csv(f'{DATASET_DIR}/olist_order_payments_dataset.csv'),
    }

    top_sellers = (raw['order_items']['seller_id']
                   .value_counts().head(HOT_KEY_TOP_N).index.tolist()
                   if 'seller_id' in raw['order_items'].columns else [])
    if top_sellers:
        log.info(f"🔥 Hot sellers {HOT_KEY_TOP_N}개: {top_sellers[:3]}...")

    zipf_w = build_zipf_weights(TARGET_SCALE, ZIPF_ALPHA)
    day_w  = build_day_weights()
    log.info(
        f"📊 Zipf: min={zipf_w.min()} max={zipf_w.max()} "
        f"상위20% 평균={zipf_w[zipf_w > np.percentile(zipf_w,80)].mean():.1f}배")

    for tname, fname in [('products', 'olist_products_dataset.csv'),
                         ('sellers',  'olist_sellers_dataset.csv')]:
        tdir = os.path.join(DST_DIR, tname)
        os.makedirs(tdir, exist_ok=True)
        pq.write_table(
            pa.Table.from_pandas(
                pd.read_csv(f'{DATASET_DIR}/{fname}'), preserve_index=False),
            os.path.join(tdir, 'part-0000.parquet'),
            compression=COMPRESSION)

    log.info("🔗 Shared Memory 적재 중...")
    shm_info = {
        'c_shm':  df_to_shm(raw['customers'],      'c'),
        'o_shm':  df_to_shm(raw['orders'],         'o'),
        'oi_shm': df_to_shm(raw['order_items'],    'oi'),
        'op_shm': df_to_shm(raw['order_payments'], 'op'),
    }
    del raw; gc.collect()

    worker_meta = {'dst': DST_DIR, 'compression': COMPRESSION, **shm_info}

    day_w_list = day_w.tolist()
    all_batches_args = [
        (list(range(i, min(i + BATCH_SIZE, TARGET_SCALE))),
         zipf_w[i: i + BATCH_SIZE].tolist(),
         top_sellers,
         day_w_list)
        for i in range(0, TARGET_SCALE, BATCH_SIZE)
    ]
    
    pending = [a for a in all_batches_args if not batch_is_done(a[0])]
    random.shuffle(pending)
    
    log.info(f"📊 전체 {len(all_batches_args)}배치 | 완료 {len(all_batches_args)-len(pending)} | 예정 {len(pending)}")

    if not pending:
        log.info("✅ 모든 배치 완료.")
        cleanup_shm(); sys.exit(0)

    log.info(
        f"🚀 시작 — 워커 {MAX_WORKERS}개 | 배치 크기 {BATCH_SIZE} | "
        f"압축 {COMPRESSION}\n"
        f"   Skew: Zipf α={ZIPF_ALPHA} | "
        f"Hot key top{HOT_KEY_TOP_N} {HOT_KEY_RATIO*100:.0f}% | "
        f"시계열 {DATE_START.date()}~{DATE_END.date()}")

    total_start = time.time()
    done_n = fail_n = 0
    failed_batches: list = []

    try:
        with concurrent.futures.ProcessPoolExecutor(
            max_workers=MAX_WORKERS,
            initializer=_worker_init,
            initargs=(worker_meta,),
        ) as ex:
            state = _SubmitState(pending, ex, worker_meta)
            state.fill_queue()

            completed = 0
            while state.active:
                try:
                    f = next(concurrent.futures.as_completed(list(state.active), timeout=2))
                except StopIteration:
                    break
                except concurrent.futures.TimeoutError:
                    state.fill_queue()
                    continue

                args = state.active.pop(f)
                try:
                    indices, success, err = f.result()
                except Exception as e:
                    indices, success, err = args[0], False, str(e)

                completed += 1
                if success: done_n += 1
                else:
                    fail_n += 1
                    failed_batches.append(indices)
                    log.error(f"❌ 배치 {indices[0]}~{indices[-1]} 최종 실패: {err}")

                state.fill_queue()

                if completed % 1 == 0:
                    elapsed = time.time() - total_start
                    rate    = completed / elapsed
                    eta     = (len(pending) - completed) / rate if rate > 0 else 0
                    log.info(
                        f"[{completed:>3}/{len(pending)}배치] "
                        f"✅{done_n} ❌{fail_n} | "
                        f"경과 {elapsed/60:.1f}분 | ETA {eta/60:.1f}분 | "
                        f"RAM {psutil.virtual_memory().percent:.0f}% | "
                        f"디스크 {psutil.disk_usage(DST_DIR).free/1024**3:.1f}GB")
                    if psutil.disk_usage(DST_DIR).free/1024**3 < MIN_FREE_DISK_GB:
                        raise RuntimeError("💀 디스크 여유 부족 → 중단")

            # 💡 [여기가 핵심] while 루프가 끝나면 무한 대기 버그를 무시하고 여기서 스크립트를 즉시 폭파시킵니다.
            elapsed = time.time() - total_start
            log.info(f"\n🎉 완료! 총 {elapsed/60:.1f}분 | 성공 {done_n}배치 | 실패 {fail_n}배치")
            if failed_batches:
                log.error(f"재실행 필요: {[b[0] for b in failed_batches]}")
                
            cleanup_shm()
            log.info("🧹 Shared Memory 정리 완료")
            log.info("👋 프로세스 풀 대기를 무시하고 즉시 터미널로 복귀합니다.")
            os._exit(0)  # 정상적 강제 즉시 종료

    except KeyboardInterrupt:
        log.warning("\n⚠️  Ctrl+C 감지됨 — 진행 중인 워커 자비 없이 강제 사살 중...")
        
        # 워커들 모조리 색출해서 사살
        current_process = psutil.Process()
        children = current_process.children(recursive=True)
        
        for child in children:
            try: child.terminate()
            except psutil.NoSuchProcess: pass
            
        _, alive = psutil.wait_procs(children, timeout=3)
        for child in alive:
            try: child.kill()
            except psutil.NoSuchProcess: pass
            
        log.warning("💀 모든 워커 사살 완료.")
        cleanup_shm()
        log.info("🧹 Shared Memory 정리 완료")
        os._exit(1) # Ctrl+C 강제 폭파

    finally:
        # 정상 종료 시에만 실행 (KeyboardInterrupt 시 위에서 os._exit 발생)
        cleanup_shm()
        log.info("🧹 Shared Memory 정리 완료")