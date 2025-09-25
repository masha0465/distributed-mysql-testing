"""
네이버 MySQL 분산 일관성 테스트 모듈
Master-Slave 복제 환경의 데이터 일관성 검증
"""
import asyncio
import time
import hashlib
import random
from typing import Dict, List
from datetime import datetime

import aiomysql
from colorama import Fore, Style

class ConsistencyTestSuite:
    """분산 일관성 테스트 스위트"""
    
    def __init__(self):
        self.master_config = {
            'host': 'localhost',
            'port': 3306, 
            'user': 'root',
            'password': 'testpass',
            'db': 'testdb'
        }
        
        self.slave_configs = [
            {
                'host': 'localhost',
                'port': 3307,
                'user': 'root', 
                'password': 'testpass',
                'db': 'testdb'
            }
        ]
    
    async def run(self) -> Dict:
        """일관성 테스트 실행"""
        print(f"{Fore.CYAN}🔄 분산 일관성 테스트 시작{Style.RESET_ALL}")
        
        results = {}
        
        # 1. Read-after-Write 일관성 테스트
        print("\n📝 Read-after-Write 일관성 테스트...")
        raw_result = await self._test_read_after_write_consistency()
        results['read_after_write'] = raw_result
        
        # 2. 복제 지연 측정
        print("\n⏱️  복제 지연 측정 테스트...")
        lag_result = await self._test_replication_lag()
        results['replication_lag'] = lag_result
        
        return results
    
    async def _test_read_after_write_consistency(self) -> Dict:
        """Write 후 즉시 Read 일관성 검증"""
        consistency_results = []
        test_cases = 20  # 20회 테스트
        
        for i in range(test_cases):
            test_id = f"consistency_test_{int(time.time())}_{i}"
            test_data = f"네이버_테스트_데이터_{random.randint(1000, 9999)}"
            
            # Master에 데이터 쓰기
            write_success = await self._write_to_master(test_id, test_data)
            
            if not write_success:
                continue
            
            # 다양한 지연 시간으로 테스트
            for delay in [0.1, 0.5, 1.0, 2.0]:
                await asyncio.sleep(delay)
                
                # 모든 Slave에서 읽기 테스트
                for slave_idx, slave_config in enumerate(self.slave_configs):
                    consistent = await self._read_and_verify(
                        slave_config, test_id, test_data
                    )
                    
                    consistency_results.append({
                        'test_case': i,
                        'slave_index': slave_idx,
                        'delay_seconds': delay,
                        'consistent': consistent,
                        'timestamp': datetime.now().isoformat()
                    })
            
            # 테스트 데이터 정리
            await self._cleanup_test_data(test_id)
        
        # 결과 분석
        total_tests = len(consistency_results)
        consistent_tests = sum(1 for r in consistency_results if r['consistent'])
        consistency_rate = consistent_tests / total_tests if total_tests > 0 else 0
        
        print(f"   ✅ 일관성 달성률: {consistency_rate:.1%}")
        print(f"   📊 총 테스트: {total_tests}회, 일관성 유지: {consistent_tests}회")
        
        return {
            'total_tests': total_tests,
            'consistent_tests': consistent_tests,
            'consistency_rate': consistency_rate,
            'detailed_results': consistency_results
        }
    
    async def _test_replication_lag(self) -> Dict:
        """복제 지연 시간 측정"""
        lag_measurements = []
        
        for i in range(10):  # 10회 측정
            test_id = f"lag_test_{int(time.time())}_{i}"
            test_data = f"지연_측정_{i}"
            
            # Master에 쓰기 시작 시간
            write_start = time.time()
            await self._write_to_master(test_id, test_data)
            write_end = time.time()
            
            # Slave에서 데이터 확인될 때까지 대기
            for slave_idx, slave_config in enumerate(self.slave_configs):
                read_start = time.time()
                
                # 최대 5초 대기
                while (time.time() - read_start) < 5.0:
                    if await self._read_and_verify(slave_config, test_id, test_data):
                        lag_time = time.time() - write_end
                        lag_measurements.append({
                            'slave_index': slave_idx,
                            'lag_seconds': lag_time,
                            'test_case': i
                        })
                        break
                    
                    await asyncio.sleep(0.1)
            
            await self._cleanup_test_data(test_id)
            await asyncio.sleep(0.5)  # 테스트 간 간격
        
        # 통계 계산
        if lag_measurements:
            lag_times = [m['lag_seconds'] for m in lag_measurements]
            avg_lag = sum(lag_times) / len(lag_times)
            max_lag = max(lag_times)
            min_lag = min(lag_times)
        else:
            avg_lag = max_lag = min_lag = 0
        
        print(f"   ⚡ 평균 복제 지연: {avg_lag:.3f}초")
        print(f"   📊 최소/최대 지연: {min_lag:.3f}초 / {max_lag:.3f}초")
        
        return {
            'avg_lag_seconds': avg_lag,
            'max_lag_seconds': max_lag, 
            'min_lag_seconds': min_lag,
            'measurements': lag_measurements
        }
    
    async def _write_to_master(self, test_id: str, test_data: str) -> bool:
        """Master에 테스트 데이터 쓰기"""
        try:
            async with aiomysql.connect(**self.master_config) as conn:
                async with conn.cursor() as cursor:
                    # 테스트 테이블 생성 (없는 경우)
                    await cursor.execute("""
                        CREATE TABLE IF NOT EXISTS consistency_test (
                            test_id VARCHAR(100) PRIMARY KEY,
                            test_data VARCHAR(500),
                            checksum VARCHAR(64),
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    
                    # 체크섬 생성
                    checksum = hashlib.md5((test_id + test_data).encode()).hexdigest()
                    
                    # 데이터 삽입
                    await cursor.execute("""
                        INSERT INTO consistency_test (test_id, test_data, checksum)
                        VALUES (%s, %s, %s)
                    """, (test_id, test_data, checksum))
                    
                    await conn.commit()
            
            return True
            
        except Exception as e:
            print(f"Master 쓰기 실패: {e}")
            return False
    
    async def _read_and_verify(self, slave_config: Dict, test_id: str, expected_data: str) -> bool:
        """Slave에서 데이터 읽기 및 검증"""
        try:
            async with aiomysql.connect(**slave_config) as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                        SELECT test_data, checksum FROM consistency_test
                        WHERE test_id = %s
                    """, (test_id,))
                    
                    result = await cursor.fetchone()
                    
                    if not result:
                        return False
                    
                    stored_data, stored_checksum = result
                    expected_checksum = hashlib.md5((test_id + expected_data).encode()).hexdigest()
                    
                    return (stored_data == expected_data and 
                           stored_checksum == expected_checksum)
            
        except Exception as e:
            return False
    
    async def _cleanup_test_data(self, test_id: str):
        """테스트 데이터 정리"""
        try:
            async with aiomysql.connect(**self.master_config) as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(
                        "DELETE FROM consistency_test WHERE test_id = %s", 
                        (test_id,)
                    )
                    await conn.commit()
        except Exception:
            pass  # 정리 실패해도 테스트는 계속