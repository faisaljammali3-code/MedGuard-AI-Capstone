import sys
import pandas as pd
from pathlib import Path

# إعداد المسارات
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root / "src"))

from MedGuard_AI.io_manager import IOManager

def inspect_merge_logic():
    print("🕵️‍♂️ Starting Merge Diagnosis...")
    io = IOManager()
    
    # 1. تحميل جدول التشخيصات (الطرف اليمين)
    print("\n1️⃣ Loading Diagnoses Lookup (Right Side)...")
    df_diag = io.read_parquet("diagnoses_lookup.parquet", folder='cache')
    
    # عرض معلومات hadm_id
    sample_id_diag = df_diag['hadm_id'].iloc[0]
    print(f"   -> Dtype: {df_diag['hadm_id'].dtype}")
    print(f"   -> Sample Value: {sample_id_diag} (Type: {type(sample_id_diag)})")
    
    # 2. تحميل قطعة من الوصفات (الطرف اليسار)
    print("\n2️⃣ Loading Prescriptions Chunk (Left Side)...")
    df_presc_iter = io.read_stream('prescriptions', usecols=['hadm_id'], chunk_size=5000)
    chunk = next(df_presc_iter)
    
    # تنظيف وتوحيد النوع كما نفعل في الكود الأصلي
    # سنطبق نفس التحويل الذي قمت به
    chunk['hadm_id'] = chunk['hadm_id'].astype('Int64') 
    
    sample_id_presc = chunk['hadm_id'].iloc[0]
    print(f"   -> Dtype: {chunk['hadm_id'].dtype}")
    print(f"   -> Sample Value: {sample_id_presc} (Type: {type(sample_id_presc)})")
    
    # 3. محاولة العثور على تطابق يدوي
    print("\n3️⃣ Trying to match IDs manually...")
    # نأخذ 5 أرقام زيارات من الوصفات ونبحث عنها في التشخيصات
    sample_ids = chunk['hadm_id'].dropna().head(5).tolist()
    
    print(f"   Searching for these IDs in Diagnoses: {sample_ids}")
    
    for pid in sample_ids:
        # البحث المباشر
        match = df_diag[df_diag['hadm_id'] == pid]
        if not match.empty:
            print(f"   ✅ MATCH FOUND for ID {pid}!")
            print(f"      Diagnosis Content: {match['diagnosis_list'].values[0]}")
        else:
            print(f"   ❌ No match for ID {pid}")
            # التحقق هل الرقم موجود أصلاً ولكن بنوع مختلف؟
            # نحول الرقم لقيمته الأصلية للبحث
            raw_match = df_diag[df_diag['hadm_id'].astype(str) == str(pid)]
            if not raw_match.empty:
                print(f"      ⚠️ WAIT! Found it if we cast to String. This is a TYPE MISMATCH issue.")

if __name__ == "__main__":
    inspect_merge_logic()