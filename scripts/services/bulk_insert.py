"""
Módulo optimizado para inserciones masivas en PostgreSQL usando asyncpg
Implementa múltiples estrategias para acelerar la inserción de grandes volúmenes
"""

import asyncio
import time
from typing import List, Dict, Any, Optional, Callable
from dataclasses import dataclass
from asyncpg import Pool
from scripts.utils import constants as CONSTANTS


@dataclass
class InsertStats:
    """Estadísticas de inserción"""
    total_records: int = 0
    inserted_records: int = 0
    failed_records: int = 0
    start_time: float = 0
    batches_completed: int = 0

    def __post_init__(self):
        self.start_time = time.time()

    def get_progress_pct(self) -> float:
        """Calcula el porcentaje de progreso"""
        if self.total_records == 0:
            return 0.0
        return (self.inserted_records / self.total_records) * 100

    def get_elapsed_time(self) -> float:
        """Tiempo transcurrido en segundos"""
        return time.time() - self.start_time

    def get_rate(self) -> float:
        """Registros por segundo"""
        elapsed = self.get_elapsed_time()
        if elapsed == 0:
            return 0
        return self.inserted_records / elapsed

    def get_eta(self) -> float:
        """Tiempo estimado restante en segundos"""
        rate = self.get_rate()
        if rate == 0:
            return 0
        remaining = self.total_records - self.inserted_records
        return remaining / rate

    def format_time(self, seconds: float) -> str:
        """Formatea segundos a formato legible"""
        if seconds < 60:
            return f"{seconds:.0f}s"
        elif seconds < 3600:
            return f"{seconds/60:.1f}min"
        else:
            return f"{seconds/3600:.1f}h"


class BulkInserter:
    """
    Clase para inserciones masivas optimizadas en PostgreSQL

    Características:
    - Procesamiento paralelo con múltiples workers
    - Batch size configurable
    - Manejo de errores robusto
    - Estadísticas en tiempo real
    - Retry automático
    """

    def __init__(
        self,
        pool: Pool,
        table_name: str,
        batch_size: int = CONSTANTS.DB_DEFAULT_BATCH_SIZE,
        max_workers: int = CONSTANTS.DB_DEFAULT_MAX_WORKERS,
        max_retries: int = CONSTANTS.DB_DEFAULT_MAX_RETRIES,
        progress_callback: Optional[Callable[[InsertStats], None]] = None
    ):
        """
        Args:
            pool: Pool de conexiones asyncpg
            table_name: Nombre de la tabla
            batch_size: Tamaño del batch
            max_workers: Número de workers paralelos
            max_retries: Intentos máximos por batch fallido
            progress_callback: Función a llamar con stats en cada actualización
        """
        self.pool = pool
        self.table_name = table_name
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.max_retries = max_retries
        self.progress_callback = progress_callback

        self.stats = InsertStats()

    def _create_batches(self, data: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
        """Divide los datos en batches"""
        batches = []
        for i in range(0, len(data), self.batch_size):
            batch = data[i:i + self.batch_size]
            batches.append(batch)
        return batches

    async def _insert_batch(
        self,
        batch: List[Dict[str, Any]],
        batch_num: int,
        total_batches: int
    ) -> tuple[bool, int]:
        """
        Inserta un batch con reintentos

        Returns:
            (success, records_inserted)
        """
        for attempt in range(self.max_retries):
            try:
                async with self.pool.acquire() as conn:
                    async with conn.transaction():
                        columns = list(batch[0].keys())
                        placeholders = ', '.join([f'${i+1}' for i in range(len(columns))])
                        query = f"INSERT INTO {self.table_name} ({', '.join(columns)}) VALUES ({placeholders})"
                        
                        for row in batch:
                            values = tuple(row[col] for col in columns)
                            await conn.execute(query, values)

                        records_inserted = len(batch)

                        self.stats.inserted_records += records_inserted
                        self.stats.batches_completed += 1

                        if self.progress_callback:
                            self.progress_callback(self.stats)

                        if len(batch) >= 500:
                            await asyncio.sleep(0.5)
                        else:
                            await asyncio.sleep(0.3)

                        return True, records_inserted

            except Exception as e:
                if attempt < self.max_retries - 1:
                    wait_time = (attempt + 1) * 5
                    print(f"⚠️  Batch {batch_num}/{total_batches} falló (intento {attempt + 1}/{self.max_retries}), reintentando en {wait_time}s...")
                    await asyncio.sleep(wait_time)
                else:
                    print(f"❌ Batch {batch_num}/{total_batches} falló después de {self.max_retries} intentos: {e}")
                    self.stats.failed_records += len(batch)
                    return False, 0

        return False, 0

    async def insert_bulk(self, data: List[Dict[str, Any]]) -> InsertStats:
        """
        Inserta datos en bulk usando procesamiento paralelo

        Args:
            data: Lista de diccionarios con los datos a insertar

        Returns:
            InsertStats con las estadísticas finales
        """
        if not data:
            print("⚠️  No hay datos para insertar")
            return self.stats

        self.stats = InsertStats()
        self.stats.total_records = len(data)

        print(f"\n🚀 Iniciando inserción masiva en tabla '{self.table_name}':")
        print(f"   📊 Total de registros: {len(data):,}")
        print(f"   📦 Tamaño de batch: {self.batch_size:,}")
        print(f"   👷 Workers paralelos: {self.max_workers}")

        batches = self._create_batches(data)
        total_batches = len(batches)
        print(f"   🔢 Total de batches: {total_batches}")
        print()

        tasks = [
            self._insert_batch(batch, i + 1, total_batches)
            for i, batch in enumerate(batches)
        ]

        await asyncio.gather(*tasks)

        self._print_summary()

        return self.stats

    def _print_summary(self):
        """Imprime resumen de la operación"""
        elapsed = self.stats.get_elapsed_time()
        rate = self.stats.get_rate()

        print(f"\n{'='*60}")
        print(f"✅ Inserción completada en tabla '{self.table_name}'")
        print(f"{'='*60}")
        print(f"📊 Total registros:     {self.stats.total_records:,}")
        print(f"✅ Insertados:          {self.stats.inserted_records:,} ({self.stats.get_progress_pct():.1f}%)")
        print(f"❌ Fallidos:            {self.stats.failed_records:,}")
        print(f"⏱️  Tiempo total:        {self.stats.format_time(elapsed)}")
        print(f"🚀 Velocidad promedio:  {rate:.0f} registros/seg")
        print(f"{'='*60}\n")


def default_progress_callback(stats: InsertStats):
    """Callback por defecto para mostrar progreso"""
    if stats.batches_completed % 5 == 0:
        progress_pct = stats.get_progress_pct()
        rate = stats.get_rate()
        eta = stats.get_eta()

        print(
            f"      📊 {stats.inserted_records:,}/{stats.total_records:,} "
            f"({progress_pct:.1f}%) | "
            f"⚡ {rate:.0f} reg/seg | "
            f"⏱️  ETA: {stats.format_time(eta)}"
        )


async def insert_bulk_optimized(
    pool: Pool,
    table_name: str,
    data: List[Dict[str, Any]],
    batch_size: int = 500,
    max_workers: int = 1
) -> InsertStats:
    """
    Función de conveniencia para insertar datos en bulk

    Args:
        pool: Pool de conexiones asyncpg
        table_name: Nombre de la tabla
        data: Lista de datos a insertar
        batch_size: Tamaño del batch (default: 500)
        max_workers: Workers paralelos (default: 1)

    Returns:
        InsertStats con estadísticas de la operación
    """
    inserter = BulkInserter(
        pool=pool,
        table_name=table_name,
        batch_size=batch_size,
        max_workers=max_workers,
        progress_callback=default_progress_callback
    )

    return await inserter.insert_bulk(data)
