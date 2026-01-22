# src/MedGuard_AI/processors/prescriptions.py
import pandas as pd
import numpy as np
from loguru import logger

def process_prescription_chunk(
    chunk: pd.DataFrame, 
    df_patients: pd.DataFrame, 
    df_diagnoses: pd.DataFrame
) -> pd.DataFrame:
    """
    تحويل البيانات من مستوى "دواء" إلى مستوى "زيارة".
    الناتج: صف لكل زيارة مع قوائم للأدوية والجرعات والتشخيصات.
    """
    # 1. تنظيف التواريخ
    chunk['starttime'] = pd.to_datetime(chunk['starttime'])
    
    # 2. توحيد الأنواع (Strict Int64)
    if 'subject_id' in chunk.columns:
        chunk['subject_id'] = pd.to_numeric(chunk['subject_id'], errors='coerce').astype('Int64')
    if 'hadm_id' in chunk.columns:
        chunk['hadm_id'] = pd.to_numeric(chunk['hadm_id'], errors='coerce').astype('Int64')

    # 3. معالجة القيم المفقودة في الجرعات قبل التجميع
    # نستبدل NaN بكلمة 'Unknown' لكي لا تختفي عند التجميع
    chunk['dose_val_rx'] = chunk['dose_val_rx'].fillna('0')
    chunk['dose_unit_rx'] = chunk['dose_unit_rx'].fillna('-')

    # =========================================================
    # 💊 التجميع (Aggregation) - التعديل الجديد
    # =========================================================
    drugs_agg = chunk.groupby(['hadm_id', 'subject_id']).agg({
        'drug': list,           # قائمة الأدوية
        'dose_val_rx': list,    # قائمة الجرعات (جديد)
        'dose_unit_rx': list,   # قائمة الوحدات (جديد)
        'starttime': 'min'      # الوقت
    }).reset_index()
    
    # إعادة تسمية الأعمدة لتكون واضحة
    drugs_agg.rename(columns={
        'drug': 'medication_list',
        'dose_val_rx': 'dosage_list',
        'dose_unit_rx': 'unit_list'
    }, inplace=True)
    # =========================================================

    # 4. دمج معلومات المريض
    merged = drugs_agg.merge(df_patients, on='subject_id', how='left')
    
    # 5. دمج التشخيصات
    merged = merged.merge(df_diagnoses, on='hadm_id', how='left')
    
    # 6. تنظيف القوائم (Safety Check)
    list_cols = ['medication_list', 'dosage_list', 'unit_list', 'diagnosis_names']
    for col in list_cols:
        if col in merged.columns:
            merged[col] = merged[col].apply(
                lambda x: x if isinstance(x, (list, np.ndarray)) else []
            )

    # 7. اختيار الأعمدة النهائية (الإبقاء على hadm_id والجرعات)
    cols_to_keep = [
        'subject_id', 'hadm_id', 'starttime', 
        'gender', 'anchor_age', 'weight',
        'medication_list', 'dosage_list', 'unit_list', # الأعمدة الطبية
        'diagnosis_names'
    ]
    
    final_cols = [c for c in cols_to_keep if c in merged.columns]
    
    return merged[final_cols]