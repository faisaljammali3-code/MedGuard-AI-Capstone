# src/MedGuard_AI/processors/notes.py

import re
import pandas as pd
import numpy as np

def extract_meds_block_raw(text: str) -> str:
    """
    استخراج الفقرة النصية الكاملة الخاصة بأدوية ما قبل الدخول.
    Input: النص الكامل للملاحظة.
    Output: الفقرة النصية الخام (str) أو None.
    """
    if not isinstance(text, str):
        return None

    # البحث عن البداية والنهاية باستخدام Regex V8 (المحدث)
    # البداية: Medications on Admission أو ___ on Admission
    # النهاية: Discharge... أو Dictated By... أو نهاية الملف
    pattern = r"(?i)(?:Medications|___)\s+on\s+Admission:(.*?)(?=Discharge\s+Medications:|Discharge\s+Disposition:|Dictated\s+By:|$)"
    
    match = re.search(pattern, text, re.DOTALL)
    
    if match:
        return match.group(1).strip()
    return None

def determine_extraction_status(row: pd.Series) -> str:
    """
    تحديد حالة الاستخراج للمساعدة في الفحص (Audit).
    Input: صف من الداتا فريم (يجب أن يحتوي على full_note_text و home_meds_raw).
    Output: حالة الاستخراج (SUCCESS, NO_NOTE_FOUND, SECTION_MISSING).
    """
    # 1. هل الملاحظة الأصلية موجودة؟
    if pd.isna(row.get('full_note_text')) or row.get('full_note_text') == '':
        return 'NO_NOTE_FOUND'
    
    # 2. هل نجحنا في قص الفقرة؟
    if pd.notna(row.get('home_meds_raw')) and row.get('home_meds_raw') != '':
        return 'SUCCESS'
    
    # 3. الملاحظة موجودة لكن الفقرة مفقودة (لم نجد العنوان)
    return 'SECTION_MISSING'

def deduplicate_notes(df_notes: pd.DataFrame) -> pd.DataFrame:
    """
    تنظيف الملاحظات المكررة لنفس الزيارة.
    الاستراتيجية: الاحتفاظ بالملاحظة الأطول (الأكثر تفصيلاً).
    """
    if df_notes.empty:
        return df_notes

    # حساب طول النص
    df_notes['text_len'] = df_notes['text'].str.len()
    
    # الترتيب تنازلياً حسب الطول، ثم حذف التكرار بناءً على hadm_id
    # keep='first' ستحتفظ بالأطول لأننا رتبنا تنازلياً
    df_deduped = df_notes.sort_values('text_len', ascending=False).drop_duplicates(subset=['hadm_id'], keep='first')
    
    # تنظيف الأعمدة المؤقتة وإعادة التسمية
    return df_deduped[['hadm_id', 'text']].rename(columns={'text': 'full_note_text'})