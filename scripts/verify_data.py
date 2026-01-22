import sys
from pathlib import Path

# -----------------------------------------------------------
# 1. إضافة مجلد src إلى مسارات بايثون (الحل السحري)
# -----------------------------------------------------------
# نحدد مسار الجذر (خطوتين للوراء من scripts)
project_root = Path(__file__).resolve().parent.parent
# نضيف src للمسار
sys.path.append(str(project_root / "src"))

# -----------------------------------------------------------
# 2. الآن الاستيراد سيعمل بنجاح
# -----------------------------------------------------------
from loguru import logger
from MedGuard_AI.io_manager import IOManager

def main():
    logger.info(r'Hello from MedGuard-AI! Script: scripts/verify_data.py')
    
    # تهيئة المدير
    io = IOManager()
    
    # 1. تحميل أرقام الزيارات الموجودة في الوصفات
    print("Loading Prescriptions IDs...")
    try:
        df_presc = io.read_stream('prescriptions', usecols=['hadm_id'], chunk_size=50000)
        # نأخذ فقط أول Chunk للتجربة أو نجمع الكل (للديمو الحجم صغير)
        presc_ids = set()
        for chunk in df_presc:
            presc_ids.update(chunk['hadm_id'].unique())
    except Exception as e:
        logger.error(f"Error reading prescriptions: {e}")
        return

    # 2. تحميل أرقام الزيارات الموجودة في التشخيصات (من الكاش)
    print("Loading Diagnoses IDs...")
    try:
        df_diag = io.read_parquet("diagnoses_lookup.parquet", folder='cache')
        diag_ids = set(df_diag['hadm_id'].unique())
    except Exception as e:
        logger.error(f"Error reading diagnoses cache: {e}. Did you run ETL Phase 1?")
        return
    
    # 3. حساب التقاطع
    intersection = presc_ids.intersection(diag_ids)
    
    print(f"Total Prescriptions Visits: {len(presc_ids)}")
    print(f"Total Diagnoses Visits: {len(diag_ids)}")
    print(f"🔗 Overlapping Visits (Common IDs): {len(intersection)}")
    
    if len(intersection) == 0:
        print("🚨 Result: Zero Overlap! This confirms it's a Demo Data limitation.")
    else:
        print(f"✅ Result: Found {len(intersection)} matches. Check why they didn't appear in the sample.")
    
if __name__ == '__main__':
    main()