# src/MedGuard_AI/pipelines/etl.py

from loguru import logger
from MedGuard_AI.io_manager import IOManager

from MedGuard_AI.processors.lookups import transform_patients_weight, transform_diagnoses_with_names
from MedGuard_AI.processors.prescriptions import process_prescription_chunk

class MedGuardETL:
    def __init__(self):
        # نهيئ مدير الإدخال والإخراج
        self.io = IOManager()

    def prepare_lookup_tables(self, force_rebuild: bool = False):
        """
        تجهيز الجداول المرجعية (المرضى + التشخيصات مع أسمائها).
        يتم حفظ النتيجة في مجلد Cache لسرعة الاستخدام لاحقاً.
        """
        logger.info("🚀 Phase 1: Preparing Lookup Tables (With Translations)...")
        
        # أسماء ملفات الكاش النهائية
        files_to_check = ["patients_lookup.parquet", "diagnoses_lookup.parquet"]
        
        # التحقق من وجود الكاش (إلا إذا طلبنا إعادة بناء قسري)
        if self.io.check_cache(files_to_check) and not force_rebuild:
            logger.success("✨ Lookup tables found in cache. Skipping rebuild.")
            return

        # ---------------------------
        # 1. معالجة المرضى والوزن
        # ---------------------------
        logger.info("Processing Patients & Weight...")
        df_pat = self.io.read_raw('patients', usecols=['subject_id', 'anchor_age', 'gender'])
        
        # محاولة تحميل الوزن (اختياري لأنه قد لا يتوفر دائماً)
        try:
            df_omr = self.io.read_raw('omr', usecols=['subject_id', 'result_name', 'result_value'])
        except Exception as e:
            logger.warning(f"Could not load OMR: {e}")
            df_omr = None
            
        # استدعاء المعالج
        df_pat_final = transform_patients_weight(df_pat, df_omr)
        
        # حفظ النتيجة
        self.io.save_parquet(df_pat_final, "patients_lookup.parquet", folder='cache')

        # ---------------------------
        # 2. معالجة التشخيصات + القاموس (MedGemma Logic)
        # ---------------------------
        logger.info("Processing Diagnoses with Dictionary...")
        
        # أ. تحميل جدول التشخيصات الخام
        df_diag = self.io.read_raw('diagnoses', usecols=['hadm_id', 'icd_code', 'icd_version'])
        
        # ب. تحميل قاموس الأمراض (لترجمة الأكواد إلى نصوص)
        try:
            df_dict = self.io.read_raw('d_icd', usecols=['icd_code', 'icd_version', 'long_title'])
        except Exception as e:
            logger.critical(f"❌ Could not load ICD Dictionary (d_icd). Check settings.yaml and file existence: {e}")
            raise
            
        # ج. استدعاء المعالج الجديد الذي يدمج الأسماء
        df_diag_final = transform_diagnoses_with_names(df_diag, df_dict)
        
        # د. ضمان نوع البيانات (Int64) لتجنب مشاكل الدمج لاحقاً
        df_diag_final['hadm_id'] = df_diag_final['hadm_id'].astype('Int64')
        
        # حفظ النتيجة
        self.io.save_parquet(df_diag_final, "diagnoses_lookup.parquet", folder='cache')
        
        logger.success("✅ Phase 1 Complete: Lookups Cached.")

    def process_main_events(self):
        """
        المرحلة 2: معالجة جدول الوصفات (Streaming) ودمجه مع الجداول المرجعية.
        """
        logger.info("🚀 Phase 2: Processing Prescriptions Stream...")
        
        try:
            # 1. تحميل الجداول المرجعية من الذاكرة (Cache)
            df_pat_lookup = self.io.read_parquet("patients_lookup.parquet", folder='cache')
            df_diag_lookup = self.io.read_parquet("diagnoses_lookup.parquet", folder='cache')
            
            # =========================================================
            # 🔥 FIX: توحيد الأنواع (Global Fix)
            # نضمن أن مفاتيح الربط هي Int64 قبل بدء أي عملية دمج
            # =========================================================
            logger.info("🔧 Enforcing Int64 types on lookup tables...")
            if 'subject_id' in df_pat_lookup.columns:
                df_pat_lookup['subject_id'] = df_pat_lookup['subject_id'].astype('Int64')
            
            if 'hadm_id' in df_diag_lookup.columns:
                df_diag_lookup['hadm_id'] = df_diag_lookup['hadm_id'].astype('Int64')
            # =========================================================

        except FileNotFoundError:
            logger.critical("❌ Lookup tables not found! Run 'prepare_lookup_tables' first.")
            return

        # 2. إعداد تدفق القراءة (Stream)
        cols = ['subject_id', 'hadm_id', 'drug', 'ndc', 'starttime', 'dose_val_rx', 'dose_unit_rx']
        chunks_iterator = self.io.read_stream('prescriptions', usecols=cols)
        
        part_idx = 0
        total_rows = 0
        
        # 3. حلقة المعالجة
        for chunk in chunks_iterator:
            part_idx += 1
            
            # استدعاء المعالج (Processor)
            # سيقوم هذا المعالج بدمج الوصفات مع (المرضى) و (التشخيصات المترجمة)
            processed_chunk = process_prescription_chunk(chunk, df_pat_lookup, df_diag_lookup)
            
            # حفظ الجزء (Partition)
            filename = f"part_{part_idx:04d}.parquet"
            self.io.save_parquet(processed_chunk, filename, folder='processed')
            
            rows_count = len(processed_chunk)
            total_rows += rows_count
            logger.info(f"   ✅ Processed Chunk #{part_idx} ({rows_count} rows)")

        logger.success(f"🎉 ETL Pipeline Finished! Total Rows Processed: {total_rows}")

if __name__ == "__main__":
    etl = MedGuardETL()
    
    # ⚠️ مهم جداً: نستخدم force_rebuild=True 
    # لأننا أضفنا أعمدة جديدة (diagnosis_names) ونريد إعادة بناء الكاش القديم
    etl.prepare_lookup_tables(force_rebuild=True) 
    
    # تشغيل المرحلة الثانية
    etl.process_main_events()