# src/MedGuard_AI/io_manager.py
import pandas as pd
from pathlib import Path
from loguru import logger
from MedGuard_AI.config import load_config, PROJECT_ROOT

class IOManager:
    def __init__(self):
        self.conf = load_config()
        self.raw_path = PROJECT_ROOT / self.conf['paths']['raw']
        self.cache_path = PROJECT_ROOT / self.conf['paths']['cache']
        self.processed_path = PROJECT_ROOT / self.conf['paths']['processed']
        self.files_map = self.conf['files']
        
        # إنشاء المجلدات
        self.cache_path.mkdir(parents=True, exist_ok=True)
        self.processed_path.mkdir(parents=True, exist_ok=True)

    def read_raw(self, key: str, usecols: list) -> pd.DataFrame:
        """قراءة ملف خام بناءً على المفتاح في الإعدادات"""
        filename = self.files_map.get(key)
        if not filename:
            raise ValueError(f"File key '{key}' not found in config.")
            
        path = self.raw_path / filename
        if not path.exists():
            logger.error(f"File not found: {path}")
            raise FileNotFoundError(f"Missing {filename}")
            
        logger.debug(f"📖 Reading {key} from {path.name}...")
        return pd.read_csv(path, compression='gzip', usecols=usecols)

    def save_parquet(self, df: pd.DataFrame, filename: str, folder: str = 'cache'):
        """حفظ الداتا فريم كملف Parquet"""
        if folder == 'cache':
            path = self.cache_path / filename
        else:
            path = self.processed_path / filename
            
        logger.debug(f"💾 Saving to {path.name}...")
        df.to_parquet(path, index=False)
        return path

    def check_cache(self, filenames: list) -> bool:
        """التحقق هل الملفات موجودة في الكاش أم لا"""
        return all((self.cache_path / f).exists() for f in filenames)
    
    def read_stream(self, key: str, usecols: list, chunk_size: int = None):
        """
        قراءة ملف CSV كتدفق (Stream) من الكتل (Chunks).
        إذا لم نحدد chunk_size، نأخذه من الإعدادات.
        """
        filename = self.files_map.get(key)
        if not filename:
            raise ValueError(f"File key '{key}' not found in config.")
            
        path = self.raw_path / filename
        if not path.exists():
            raise FileNotFoundError(f"Missing {filename}")
            
        # تحديد حجم الدفعة من الإعدادات إذا لم يرسل
        size = chunk_size or self.conf['processing']['chunk_size']
        
        logger.debug(f"🌊 Streaming {key} from {path.name} (Chunk Size: {size})...")
        
        # إرجاع Iterator
        return pd.read_csv(
            path, 
            compression='gzip', 
            usecols=usecols, 
            chunksize=size
        )

    def read_parquet(self, filename: str, folder: str = 'cache') -> pd.DataFrame:
        """قراءة ملف Parquet جاهز (سنحتاجه لقراءة الجداول المرجعية)"""
        if folder == 'cache':
            path = self.cache_path / filename
        else:
            path = self.processed_path / filename
            
        if not path.exists():
            raise FileNotFoundError(f"Parquet file not found: {path}")
            
        return pd.read_parquet(path)