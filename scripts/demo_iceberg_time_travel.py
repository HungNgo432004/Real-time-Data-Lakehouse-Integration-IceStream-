import sys
from pathlib import Path
from datetime import datetime

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from cdc_project.iceberg_warehouse import get_iceberg_catalog, get_or_create_iceberg_table

def format_timestamp(ms):
    return datetime.fromtimestamp(ms / 1000.0).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

def main():
    print("=" * 80)
    print("      APACHE ICEBERG - TIME TRAVEL DEMONSTRATION")
    print("=" * 80)

    catalog = get_iceberg_catalog()
    table = get_or_create_iceberg_table(catalog)
    
    # 1. Liet ke lich su Snapshots
    print("\n[1] DANH SACH CAC PHIEN BAN (SNAPSHOTS) HIEN CO:")
    print("-" * 80)
    print(f"{'Snapshot ID':<25} | {'Thoi gian tao':<25} | {'Operation'}")
    print("-" * 80)
    
    snapshots = list(table.snapshots())
    for s in snapshots[-10:]: # Show 10 gan nhat
        op = s.summary.get('operation', 'N/A')
        print(f"{s.snapshot_id:<25} | {format_timestamp(s.timestamp_ms):<25} | {op}")
    
    if not snapshots:
        print("Chua co snapshot nao.")
        return

    current_snapshot = snapshots[-1]
    
    # 2. Demo Time Travel theo Snapshot ID
    # Lay snapshot cach day 3 version (neu co)
    target_idx = max(0, len(snapshots) - 3)
    target_snapshot = snapshots[target_idx]
    
    print(f"\n[2] TRUY VAN NGUOC THOI GIAN (Tới Snapshot ID: {target_snapshot.snapshot_id})")
    print(f"    (Thoi diem: {format_timestamp(target_snapshot.timestamp_ms)})")
    print("-" * 80)
    
    # 3. Demo Time Travel theo Timestamp
    import time
    # Quay lai thoi diem cach day 10 phut
    target_ts_ms = int((time.time() - 600) * 1000)
    
    print(f"\n[3] TRUY VAN THEO THOI GIAN (Mốc thời điểm: {format_timestamp(target_ts_ms)})")
    print("-" * 80)
    try:
        # Tim snapshot phu hop nhat (Snapshot gan nhat nhung <= target_ts_ms)
        best_snapshot = None
        for s in snapshots:
            if s.timestamp_ms <= target_ts_ms:
                best_snapshot = s
            else:
                break
        
        if best_snapshot:
            print(f" -> Tim thay Snapshot phu hop: {best_snapshot.snapshot_id} (Tao luc: {format_timestamp(best_snapshot.timestamp_ms)})")
            ts_scan = table.scan(snapshot_id=best_snapshot.snapshot_id)
            df_ts = ts_scan.to_pandas()
            print(f" -> So luong records tai thoi diem do: {len(df_ts)}")
        else:
            print(" -> Khong tim thay snapshot nao truoc mốc thời gian nay.")
            
    except Exception as e:
        print(f"Loi: {e}")

    print("\n" + "=" * 80)
    print("      DEMO TIME TRAVEL HOAN TAT")
    print("=" * 80)

if __name__ == "__main__":
    main()
