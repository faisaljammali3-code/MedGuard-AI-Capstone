# src/MedGuard_AI/pipelines/etl.py

import pandas as pd
from loguru import logger
from MedGuard_AI.io_manager import IOManager
from MedGuard_AI.processors.lookups import transform_patients_weight, transform_diagnoses_with_names
from MedGuard_AI.processors.prescriptions import process_prescription_chunk

class MedGuardETL:
    def __init__(self):
        self.io = IOManager()

    def prepare_lookup_tables(self, force_rebuild: bool = False):
        """ Phase 1: تجهيز الجداول المرجعية """
        logger.info("🚀 Phase 1: Preparing Lookup Tables...")
        files_to_check = ["patients_lookup.parquet", "diagnoses_lookup.parquet"]
        
        if self.io.check_cache(files_to_check) and not force_rebuild:
            logger.success("✨ Lookup tables found in cache.")
            return

        # Patients
        df_pat = self.io.read_raw('patients', usecols=['subject_id', 'anchor_age', 'gender'])
        try:
            df_omr = self.io.read_raw('omr', usecols=['subject_id', 'result_name', 'result_value'])
        except: df_omr = None
        df_pat_final = transform_patients_weight(df_pat, df_omr)
        self.io.save_parquet(df_pat_final, "patients_lookup.parquet", folder='cache')

        # Diagnoses + Dict
        df_diag = self.io.read_raw('diagnoses', usecols=['hadm_id', 'icd_code', 'icd_version'])
        try:
            df_dict = self.io.read_raw('d_icd', usecols=['icd_code', 'icd_version', 'long_title'])
        except Exception as e:
            logger.critical(f"❌ Missing Dictionary: {e}")
            raise
            
        df_diag_final = transform_diagnoses_with_names(df_diag, df_dict)
        df_diag_final['hadm_id'] = df_diag_final['hadm_id'].astype('Int64')
        self.io.save_parquet(df_diag_final, "diagnoses_lookup.parquet", folder='cache')
        logger.success("✅ Phase 1 Complete.")

    def process_main_events(self):
        """ Phase 2: معالجة الزيارات (Partial Aggregation) """
        logger.info("🚀 Phase 2: Processing Visits Stream...")
        
        try:
            df_pat_lookup = self.io.read_parquet("patients_lookup.parquet", folder='cache')
            df_diag_lookup = self.io.read_parquet("diagnoses_lookup.parquet", folder='cache')
            
            # Enforce Types
            if 'subject_id' in df_pat_lookup.columns:
                df_pat_lookup['subject_id'] = df_pat_lookup['subject_id'].astype('Int64')
            if 'hadm_id' in df_diag_lookup.columns:
                df_diag_lookup['hadm_id'] = df_diag_lookup['hadm_id'].astype('Int64')
        except FileNotFoundError:
            return

        # 🔥 التحديث: إضافة أعمدة الجرعة للقراءة
        cols = ['subject_id', 'hadm_id', 'drug', 'dose_val_rx', 'dose_unit_rx', 'starttime']
        chunks_iterator = self.io.read_stream('prescriptions', usecols=cols)
        
        part_idx = 0
        total_visits = 0
        
        for chunk in chunks_iterator:
            part_idx += 1
            processed_chunk = process_prescription_chunk(chunk, df_pat_lookup, df_diag_lookup)
            
            filename = f"visits_part_{part_idx:04d}.parquet"
            self.io.save_parquet(processed_chunk, filename, folder='processed')
            
            total_visits += len(processed_chunk)
            logger.info(f"   ✅ Processed Chunk #{part_idx} -> {len(processed_chunk)} visits")
            
        logger.success(f"🎉 Phase 2 Finished! Total Visits Generated: {total_visits}")

    def finalize_patient_data(self):
        """ Phase 3: تجميع الملف الذهبي """
        logger.info("🚀 Phase 3: Finalizing Patient Dataset...")
        
        try:
            df_all = pd.read_parquet(self.io.processed_path)
        except Exception as e:
            logger.error(f"No processed data found: {e}")
            return

        logger.info(f"   📚 Loaded {len(df_all)} visits. Filtering last visit...")

        # الترتيب والفلترة
        df_all['starttime'] = pd.to_datetime(df_all['starttime'])
        df_all.sort_values(by=['subject_id', 'starttime'], inplace=True)
        
        # اختيار آخر زيارة
        df_final = df_all.drop_duplicates(subset=['subject_id'], keep='last').copy()

        # 🔥 التحديث: حذفنا hadm_id من قائمة الحذف لنحتفظ به
        cols_to_drop = ['starttime'] 
        df_final.drop(columns=[c for c in cols_to_drop if c in df_final.columns], inplace=True)
        
        df_final.reset_index(drop=True, inplace=True)

        output_path = self.io.processed_path / "GOLD_patient_records.parquet"
        df_final.to_parquet(output_path, index=False)
        
        logger.success(f"🏆 GOLD Dataset Saved! Patients: {len(df_final)}")
        logger.success(f"   📍 Location: {output_path}")

if __name__ == "__main__":
    etl = MedGuardETL()
    # etl.prepare_lookup_tables() # الجداول المرجعية لا تحتاج تحديث
    etl.process_main_events()     # يجب تشغيلها لتجميع الجرعات الجديدة
    etl.finalize_patient_data()   # الحفظ النهائي