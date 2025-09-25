"""
네이버 MySQL 데이터베이스 연결 관리자
분산 MySQL 환경의 연결 풀 및 로드 밸런싱 관리
"""
import asyncio
import random
import time
from typing import List, Dict, Optional
from contextlib import asynccontextmanager
import logging

import mysql.connector
import aiomysql
from mysql.connector import pooling

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseConnectionManager:
    """데이터베이스 연결 관리자"""
    
    def __init__(self, db_configs: List[Dict]):
        """
        Args:
            db_configs: 데이터베이스 설정 리스트 [Master, Slave1, Slave2, ...]
        """
        self.db_configs = db_configs
        self.master_config = db_configs[0] if db_configs else None
        self.slave_configs = db_configs[1:] if len(db_configs) > 1 else []
        
        # 연결 풀 설정
        self.connection_pools = {}
        self.pool_size = 20
        self.max_overflow = 10
        
        # 헬스 체크 상태
        self.server_health = {}
        self.last_health_check = 0
        self.health_check_interval = 30  # 30초
        
        # 로드 밸런싱 가중치
        self.slave_weights = [1.0] * len(self.slave_configs)
        
    async def initialize_pools(self):
        """연결 풀 초기화"""
        logger.info("🔗 데이터베이스 연결 풀 초기화 중...")
        
        for i, config in enumerate(self.db_configs):
            server_name = "master" if i == 0 else f"slave_{i}"
            
            try:
                # aiomysql 연결 풀 생성
                pool = await aiomysql.create_pool(
                    host=config['host'],
                    port=config.get('port', 3306),
                    user=config['user'],
                    password=config['password'],
                    db=config.get('db', config.get('database', '')),
                    minsize=5,
                    maxsize=self.pool_size,
                    autocommit=True,
                    charset='utf8mb4'
                )
                
                self.connection_pools[server_name] = pool
                self.server_health[server_name] = True
                
                logger.info(f"✅ {server_name} 연결 풀 생성 완료 ({config['host']}:{config.get('port', 3306)})")
                
            except Exception as e:
                logger.error(f"❌ {server_name} 연결 풀 생성 실패: {e}")
                self.server_health[server_name] = False
    
    @asynccontextmanager
    async def get_master_connection(self):
        """Master 연결 획득 (쓰기 작업용)"""
        if "master" not in self.connection_pools:
            raise Exception("Master 연결 풀이 초기화되지 않음")
        
        pool = self.connection_pools["master"]
        async with pool.acquire() as conn:
            yield conn
    
    @asynccontextmanager 
    async def get_slave_connection(self, prefer_server: Optional[str] = None):
        """Slave 연결 획득 (읽기 작업용, 로드 밸런싱)"""
        # 건강한 Slave 서버 선택
        available_slaves = []
        
        for i, slave_config in enumerate(self.slave_configs):
            slave_name = f"slave_{i+1}"
            if (slave_name in self.connection_pools and 
                self.server_health.get(slave_name, False)):
                available_slaves.append((slave_name, self.slave_weights[i]))
        
        # 사용 가능한 Slave가 없으면 Master 사용
        if not available_slaves:
            logger.warning("⚠️ 사용 가능한 Slave가 없음, Master 사용")
            async with self.get_master_connection() as conn:
                yield conn
            return
        
        # 선호 서버가 지정되고 사용 가능하면 해당 서버 사용
        if prefer_server and prefer_server in [slave[0] for slave in available_slaves]:
            selected_slave = prefer_server
        else:
            # 가중치 기반 랜덤 선택
            selected_slave = self._weighted_random_selection(available_slaves)
        
        pool = self.connection_pools[selected_slave]
        async with pool.acquire() as conn:
            yield conn
    
    @asynccontextmanager
    async def get_any_connection(self, prefer_read: bool = True):
        """읽기/쓰기 구분 없이 사용 가능한 연결 획득"""
        if prefer_read and self.slave_configs:
            try:
                async with self.get_slave_connection() as conn:
                    yield conn
                return
            except Exception as e:
                logger.warning(f"Slave 연결 실패, Master로 폴백: {e}")
        
        async with self.get_master_connection() as conn:
            yield conn
    
    def _weighted_random_selection(self, available_slaves: List[tuple]) -> str:
        """가중치 기반 서버 선택"""
        total_weight = sum(weight for _, weight in available_slaves)
        
        if total_weight == 0:
            return available_slaves[0][0]  # 첫 번째 서버 선택
        
        rand_val = random.uniform(0, total_weight)
        cumulative = 0
        
        for server_name, weight in available_slaves:
            cumulative += weight
            if rand_val <= cumulative:
                return server_name
        
        return available_slaves[-1][0]  # 마지막 서버 선택
    
    async def health_check(self, force: bool = False):
        """서버 헬스 체크"""
        current_time = time.time()
        
        if not force and (current_time - self.last_health_check) < self.health_check_interval:
            return self.server_health
        
        logger.info("🏥 서버 헬스 체크 실행 중...")
        
        for server_name, pool in self.connection_pools.items():
            try:
                async with pool.acquire() as conn:
                    async with conn.cursor() as cursor:
                        await cursor.execute("SELECT 1")
                        await cursor.fetchone()
                
                # 응답 시간 측정
                start_time = time.time()
                async with pool.acquire() as conn:
                    async with conn.cursor() as cursor:
                        await cursor.execute("SELECT NOW()")
                        await cursor.fetchone()
                response_time = time.time() - start_time
                
                self.server_health[server_name] = True
                
                # 응답 시간에 따른 가중치 조정 (Slave만)
                if server_name.startswith("slave_"):
                    slave_index = int(server_name.split("_")[1]) - 1
                    if response_time < 0.05:  # 50ms 이하
                        self.slave_weights[slave_index] = 1.2
                    elif response_time < 0.1:  # 100ms 이하
                        self.slave_weights[slave_index] = 1.0
                    else:  # 100ms 초과
                        self.slave_weights[slave_index] = 0.8
                
                logger.info(f"✅ {server_name} 건강 상태: OK (응답시간: {response_time*1000:.1f}ms)")
                
            except Exception as e:
                self.server_health[server_name] = False
                logger.error(f"❌ {server_name} 건강 상태: FAIL ({e})")
                
                # 실패한 Slave의 가중치 0으로 설정
                if server_name.startswith("slave_"):
                    slave_index = int(server_name.split("_")[1]) - 1
                    self.slave_weights[slave_index] = 0
        
        self.last_health_check = current_time
        return self.server_health
    
    async def execute_read_query(self, query: str, params: tuple = None):
        """읽기 쿼리 실행 (Slave 우선 사용)"""
        async with self.get_slave_connection() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, params)
                return await cursor.fetchall()
    
    async def execute_write_query(self, query: str, params: tuple = None):
        """쓰기 쿼리 실행 (Master 사용)"""
        async with self.get_master_connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query, params)
                return cursor.rowcount
    
    async def execute_transaction(self, queries: List[tuple]):
        """트랜잭션 실행 (Master 사용)"""
        async with self.get_master_connection() as conn:
            try:
                await conn.begin()
                
                async with conn.cursor() as cursor:
                    for query, params in queries:
                        await cursor.execute(query, params)
                
                await conn.commit()
                return True
                
            except Exception as e:
                await conn.rollback()
                logger.error(f"트랜잭션 실행 실패: {e}")
                raise
    
    def get_connection_stats(self) -> Dict:
        """연결 풀 통계 조회"""
        stats = {}
        
        for server_name, pool in self.connection_pools.items():
            stats[server_name] = {
                "pool_size": pool.size,
                "pool_freesize": pool.freesize,
                "health_status": self.server_health.get(server_name, False)
            }
            
            # Slave 가중치 정보 추가
            if server_name.startswith("slave_"):
                slave_index = int(server_name.split("_")[1]) - 1
                stats[server_name]["weight"] = self.slave_weights[slave_index]
        
        return stats
    
    async def close_all_pools(self):
        """모든 연결 풀 종료"""
        logger.info("🔒 모든 데이터베이스 연결 풀 종료 중...")
        
        for server_name, pool in self.connection_pools.items():
            try:
                pool.close()
                await pool.wait_closed()
                logger.info(f"✅ {server_name} 연결 풀 종료 완료")
            except Exception as e:
                logger.error(f"❌ {server_name} 연결 풀 종료 실패: {e}")
        
        self.connection_pools.clear()
        self.server_health.clear()

# 간편 사용을 위한 유틸리티 함수들
class QuickDBConnection:
    """간편한 DB 연결 사용을 위한 헬퍼 클래스"""
    
    def __init__(self, db_configs: List[Dict]):
        self.manager = DatabaseConnectionManager(db_configs)
        self.initialized = False
    
    async def __aenter__(self):
        if not self.initialized:
            await self.manager.initialize_pools()
            self.initialized = True
        return self.manager
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.manager.close_all_pools()

# 사용 예시
async def example_usage():
    """연결 관리자 사용 예시"""
    db_configs = [
        {  # Master
            'host': 'localhost',
            'port': 3306,
            'user': 'root',
            'password': 'testpass',
            'db': 'testdb'
        },
        {  # Slave
            'host': 'localhost',
            'port': 3307, 
            'user': 'root',
            'password': 'testpass',
            'db': 'testdb'
        }
    ]
    
    async with QuickDBConnection(db_configs) as db_manager:
        # 헬스 체크
        health_status = await db_manager.health_check()
        print(f"서버 상태: {health_status}")
        
        # 읽기 쿼리 (Slave 사용)
        results = await db_manager.execute_read_query("SELECT NOW() as current_time")
        print(f"읽기 결과: {results}")
        
        # 쓰기 쿼리 (Master 사용)
        affected_rows = await db_manager.execute_write_query(
            "INSERT INTO test_table (name) VALUES (%s)", 
            ("test_data",)
        )
        print(f"영향받은 행 수: {affected_rows}")
        
        # 연결 통계
        stats = db_manager.get_connection_stats()
        print(f"연결 통계: {stats}")

if __name__ == "__main__":
    asyncio.run(example_usage())