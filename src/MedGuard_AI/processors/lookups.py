# src/MedGuard_AI/processors/lookups.py
import pandas as pd
from loguru import logger

def transform_patients_weight(df_pat: pd.DataFrame, df_omr: pd.DataFrame = None) -> pd.DataFrame:
    """
    دمج بيانات المرضى مع متوسط أوزانهم.
    """
    # 1. تحسين الذاكرة
    df_pat = df_pat.copy()
    df_pat['gender'] = df_pat['gender'].astype('category')
    
    # 2. إذا لم يتوفر جدول الوزن، نرجع المرضى فقط
    if df_omr is None or df_omr.empty:
        # logger.warning("No weight data provided, returning patients only.") 
        # (علقت التحذير لتقليل الضجيج في اللوج إذا كان متوقعاً)
        return df_pat

    # 3. معالجة الوزن
    mask_weight = df_omr['result_name'].str.contains('Weight', case=False, na=False)
    df_weight = df_omr[mask_weight].copy()
    df_weight['result_value'] = pd.to_numeric(df_weight['result_value'], errors='coerce')
    
    # حساب المتوسط
    weight_agg = df_weight.groupby('subject_id')['result_value'].mean().reset_index()
    weight_agg.rename(columns={'result_value': 'weight'}, inplace=True)
    
    # 4. الدمج
    return df_pat.merge(weight_agg, on='subject_id', how='left')

def transform_diagnoses_with_names(df_diag: pd.DataFrame, df_dict: pd.DataFrame) -> pd.DataFrame:
    """
    تجميع أكواد التشخيص + أسمائها الكاملة في قوائم.
    """
    # 1. تنظيف القاموس
    df_dict = df_dict[['icd_code', 'icd_version', 'long_title']].copy()
    
    # =========================================================
    # 🛡️ Safety Patch: توحيد أنواع مفاتيح الدمج (String/Int)
    # لضمان عدم فشل الدمج بسبب اختلاف الأنواع
    # =========================================================
    # تحويل الأكواد إلى نصوص مع إزالة المسافات
    df_diag['icd_code'] = df_diag['icd_code'].astype(str).str.strip()
    df_dict['icd_code'] = df_dict['icd_code'].astype(str).str.strip()
    
    # تحويل النسخة إلى أرقام صحيحة
    df_diag['icd_version'] = pd.to_numeric(df_diag['icd_version'], errors='coerce').fillna(0).astype(int)
    df_dict['icd_version'] = pd.to_numeric(df_dict['icd_version'], errors='coerce').fillna(0).astype(int)
    # =========================================================

    # 2. دمج التشخيصات مع القاموس
    # ندمج على الكود والنسخة لضمان الدقة (ICD-9 vs ICD-10)
    merged = df_diag.merge(df_dict, on=['icd_code', 'icd_version'], how='left')
    
    # ملء الأسماء المفقودة بالكود نفسه كحل احتياطي
    merged['long_title'] = merged['long_title'].fillna(merged['icd_code'])
    
    # 3. التجميع (Aggregation)
    # نجمع الأكواد في قائمة، والأسماء في قائمة أخرى
    grouped = merged.groupby('hadm_id').agg({
        'icd_code': list,
        'long_title': list
    }).reset_index()
    
    # إعادة تسمية الأعمدة لتكون واضحة للمودل
    grouped.rename(columns={
        'icd_code': 'diagnosis_codes', 
        'long_title': 'diagnosis_names'
    }, inplace=True)
    
    return grouped