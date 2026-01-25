# src/MedGuard_AI/pipelines/etl.py

import pandas as pd
from loguru import logger
from MedGuard_AI.io_manager import IOManager

# استيراد المعالجات (Processors)
from MedGuard_AI.processors.lookups import transform_patients_weight, transform_diagnoses_with_names
from MedGuard_AI.processors.prescriptions import process_prescription_chunk
# استيراد دوال الملاحظات الجديدة
from MedGuard_AI.processors.notes import extract_meds_block_raw, determine_extraction_status, deduplicate_notes

class MedGuardETL:
    def __init__(self):
        self.io = IOManager()

    def prepare_lookup_tables(self, force_rebuild: bool = False):
        """ Phase 1: Lookup Tables Generation """
        logger.info("🚀 Phase 1: Preparing Lookup Tables...")
        files_to_check = ["patients_lookup.parquet", "diagnoses_lookup.parquet"]
        
        if self.io.check_cache(files_to_check) and not force_rebuild:
            logger.success("✨ Lookup tables found in cache.")
            return

        # Patients Processing
        df_pat = self.io.read_raw('patients', usecols=['subject_id', 'anchor_age', 'gender'])
        try:
            df_omr = self.io.read_raw('omr', usecols=['subject_id', 'result_name', 'result_value'])
        except: df_omr = None
        df_pat_final = transform_patients_weight(df_pat, df_omr)
        self.io.save_parquet(df_pat_final, "patients_lookup.parquet", folder='cache')

        # Diagnoses Processing
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
        """ Phase 2: Visits Processing (Prescriptions Aggregation) """
        logger.info("🚀 Phase 2: Processing Visits Stream...")
        
        try:
            df_pat_lookup = self.io.read_parquet("patients_lookup.parquet", folder='cache')
            df_diag_lookup = self.io.read_parquet("diagnoses_lookup.parquet", folder='cache')
            
            # Safety: Ensure ID Types
            if 'subject_id' in df_pat_lookup.columns:
                df_pat_lookup['subject_id'] = df_pat_lookup['subject_id'].astype('Int64')
            if 'hadm_id' in df_diag_lookup.columns:
                df_diag_lookup['hadm_id'] = df_diag_lookup['hadm_id'].astype('Int64')
        except FileNotFoundError:
            logger.error("❌ Lookup tables missing. Run Phase 1 first.")
            return

        # Read Prescriptions Stream
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
        """ Phase 3: Create Gold Dataset (Last Visit Logic) """
        logger.info("🚀 Phase 3: Finalizing Patient Dataset...")
        
        try:
            # Load all parts
            df_all = pd.read_parquet(self.io.processed_path)
        except Exception as e:
            logger.error(f"No processed data found: {e}")
            return

        logger.info(f"   📚 Loaded {len(df_all)} visits. Filtering last visit...")

        # Sort & Filter Last Visit
        df_all['starttime'] = pd.to_datetime(df_all['starttime'])
        df_all.sort_values(by=['subject_id', 'starttime'], inplace=True)
        
        df_final = df_all.drop_duplicates(subset=['subject_id'], keep='last').copy()

        # Cleanup
        cols_to_drop = ['starttime'] 
        df_final.drop(columns=[c for c in cols_to_drop if c in df_final.columns], inplace=True)
        df_final.reset_index(drop=True, inplace=True)

        # Save Gold File
        output_path = self.io.processed_path / "GOLD_patient_records.parquet"
        df_final.to_parquet(output_path, index=False)
        
        logger.success(f"🏆 GOLD Dataset Saved! Patients: {len(df_final)}")

    def integrate_clinical_notes(self):
        """
        Phase 4: Clinical Notes Enrichment (Clean Version).
        1. Load Gold Dataset.
        2. Filter Raw Notes based on Gold IDs.
        3. Merge & Extract Med Blocks.
        4. FILTER: Keep only SUCCESS rows (Clean Data).
        5. CLEANUP: Remove full text & status columns.
        6. Save the final optimized dataset.
        """
        logger.info("🚀 Phase 4: Integrating Clinical Notes (Clean Mode)...")
        
        # 1. Load Gold Data
        gold_path = self.io.processed_path / "GOLD_patient_records.parquet"
        if not gold_path.exists():
            logger.error("❌ GOLD dataset not found. Run previous phases first.")
            return

        df_gold = pd.read_parquet(gold_path)
        target_hadm_ids = set(df_gold['hadm_id'].unique())
        logger.info(f"   🎯 Target Visits (Gold): {len(target_hadm_ids)}")

        # 2. Smart Read & Filter Notes
        logger.info("   📖 Reading and filtering raw notes...")
        filtered_notes = []
        # نستخدم chunk_size (الاسم الصحيح الآن)
        chunk_iter = self.io.read_stream('notes', usecols=['hadm_id', 'text', 'charttime'], chunk_size=5000)
        
        for chunk in chunk_iter:
            if 'hadm_id' in chunk.columns:
                chunk['hadm_id'] = pd.to_numeric(chunk['hadm_id'], errors='coerce').astype('Int64')
            
            relevant_chunk = chunk[chunk['hadm_id'].isin(target_hadm_ids)].copy()
            if not relevant_chunk.empty:
                filtered_notes.append(relevant_chunk)

        if not filtered_notes:
            logger.warning("⚠️ No matching notes found!")
            return

        # 3. Deduplicate
        df_notes_raw = pd.concat(filtered_notes, ignore_index=True)
        df_notes_clean = deduplicate_notes(df_notes_raw)
        logger.info(f"   🧹 Notes ready for merge: {len(df_notes_clean)}")

        # 4. Merge
        logger.info("   🔗 Merging notes with GOLD dataset...")
        df_hybrid = df_gold.merge(df_notes_clean, on='hadm_id', how='left')

        # 5. Extract Block
        logger.info("   ✂️ Extracting medication blocks...")
        df_hybrid['home_meds_raw'] = df_hybrid['full_note_text'].apply(extract_meds_block_raw)
        df_hybrid['extraction_status'] = df_hybrid.apply(determine_extraction_status, axis=1)

        # =========================================================
        # 🔥 التعديلات الجديدة: الفلترة والتنظيف
        # =========================================================
        
        # أ) الاحتفاظ فقط بالناجحين
        count_before = len(df_hybrid)
        df_final = df_hybrid[df_hybrid['extraction_status'] == 'SUCCESS'].copy()
        count_after = len(df_final)
        logger.info(f"   📉 Filtering Success Only: {count_before} -> {count_after} patients")

        # ب) حذف الأعمدة غير الضرورية
        cols_to_drop = ['full_note_text', 'extraction_status', 'charttime', 'text_len']
        # نحذف فقط الأعمدة الموجودة فعلياً لتجنب الأخطاء
        cols_to_drop = [c for c in cols_to_drop if c in df_final.columns]
        
        df_final.drop(columns=cols_to_drop, inplace=True)
        logger.info(f"   🗑️ Dropped intermediate columns: {cols_to_drop}")

        # 6. Save Final
        output_path = self.io.processed_path / "GOLD_patient_records_enriched_without_full_text.parquet"
        df_final.to_parquet(output_path, index=False)

        logger.success(f"🏆 Final Clean Dataset Saved!")
        logger.success(f"   👥 Final Patient Count: {len(df_final)}")
        logger.success(f"   📍 Location: {output_path}")

if __name__ == "__main__":
    etl = MedGuardETL()
    # يمكنك تفعيل المراحل حسب الحاجة:
    # etl.prepare_lookup_tables()
    # etl.process_main_events()
    # etl.finalize_patient_data()
    
    # تشغيل المرحلة الجديدة
    etl.integrate_clinical_notes()