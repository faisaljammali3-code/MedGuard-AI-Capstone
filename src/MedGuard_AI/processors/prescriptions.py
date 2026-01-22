# src/MedGuard_AI/processors/prescriptions.py
import pandas as pd
import numpy as np # نحتاج numpy للتحقق
from loguru import logger

def process_prescription_chunk(
    chunk: pd.DataFrame, 
    df_patients: pd.DataFrame, 
    df_diagnoses: pd.DataFrame
) -> pd.DataFrame:
    
    # 1. تنظيف التواريخ
    chunk['starttime'] = pd.to_datetime(chunk['starttime'])
    
    # 2. توحيد أنواع مفاتيح الربط في الـ Chunk فقط
    # (الجداول المرجعية تم توحيدها مسبقاً في etl.py)
    if 'subject_id' in chunk.columns:
        chunk['subject_id'] = pd.to_numeric(chunk['subject_id'], errors='coerce').astype('Int64')

    if 'hadm_id' in chunk.columns:
        chunk['hadm_id'] = pd.to_numeric(chunk['hadm_id'], errors='coerce').astype('Int64')

    # 3. دمج معلومات المريض
    merged = chunk.merge(df_patients, on='subject_id', how='left')
    
    # 4. دمج التشخيصات
    merged = merged.merge(df_diagnoses, on='hadm_id', how='left')
    
    # 5. معالجة القيم المفقودة في القوائم
    if 'diagnosis_list' in merged.columns:
        # الطريقة الأكثر أماناً للتعامل مع NaN والقوائم
        # نحول NaN إلى قائمة فارغة []
        merged['diagnosis_list'] = merged['diagnosis_list'].apply(
            lambda x: x if isinstance(x, (list, np.ndarray)) else []
        )
    
    return merged