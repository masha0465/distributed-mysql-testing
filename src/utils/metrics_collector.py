"""
네이버 MySQL 메트릭 수집기
실시간 성능 지표 수집 및 분석
"""
import asyncio
import time
import json
from typing import Dict, List, Optional
from datetime import datetime
from dataclasses import dataclass, asdict

import aiomysql
import psutil

@dataclass
class SystemMetrics:
    """시스템 메트릭 데이터 클래스"""
    timestamp: float
    cpu_percent: float
    memory_mb: float
    disk_usage_percent: float
    network_sent_mb: float
    network_recv_mb: float

@dataclass 
class DatabaseMetrics:
    """데이터베이스 메트릭 데이터 클래스"""
    timestamp: float
    server_type: str  # 'master' or 'slave'
    connections_active: int
    connections_max: int
    queries_per_second: float
    slow_queries: int
    innodb_buffer_hit_rate: float
    replication_lag_seconds: Optional[float] = None

class MetricsCollector:
    """종합 메트릭 수집기"""
    
    def __init__(self, db_configs: List[Dict]):
        self.db_configs = db_configs
        self.system_metrics_history = []
        self.database_metrics_history = []
        self.is_collecting = False
        
        # 이전 네트워크 통계 (변화량 계산용)
        self.prev_network_stats = psutil.net_io_counters()
        
    async def start_collection(self, interval_seconds: int = 10):
        """메트릭 수집 시작"""
        print("📊 실시간 메트릭 수집 시작...")
        self.is_collecting = True
        
        while self.is_collecting:
            # 시스템 메트릭 수집
            system_metrics = self._collect_system_metrics()
            self.system_metrics_history.append(system_metrics)
            
            # 데이터베이스 메트릭 수집
            db_metrics = await self._collect_database_metrics()
            self.database_metrics_history.extend(db_metrics)
            
            # 최대 1000개 항목만 유지 (메모리 절약)
            if len(self.system_metrics_history) > 1000:
                self.system_metrics_history = self.system_metrics_history[-1000:]
            if len(self.database_metrics_history) > 1000:
                self.database_metrics_history = self.database_metrics_history[-1000:]
            
            await asyncio.sleep(interval_seconds)
    
    def stop_collection(self):
        """메트릭 수집 중지"""
        self.is_collecting = False
        print("📊 메트릭 수집 중지됨")
    
    def _collect_system_metrics(self) -> SystemMetrics:
        """시스템 리소스 메트릭 수집"""
        try:
            # CPU 사용률
            cpu_percent = psutil.cpu_percent(interval=1)
            
            # 메모리 사용량
            memory = psutil.virtual_memory()
            memory_mb = memory.used / 1024 / 1024
            
            # 디스크 사용률
            disk = psutil.disk_usage('/')
            disk_usage_percent = (disk.used / disk.total) * 100
            
            # 네트워크 사용량 (변화량)
            current_network = psutil.net_io_counters()
            network_sent_mb = (current_network.bytes_sent - self.prev_network_stats.bytes_sent) / 1024 / 1024
            network_recv_mb = (current_network.bytes_recv - self.prev_network_stats.bytes_recv) / 1024 / 1024
            self.prev_network_stats = current_network
            
            return SystemMetrics(
                timestamp=time.time(),
                cpu_percent=cpu_percent,
                memory_mb=memory_mb,
                disk_usage_percent=disk_usage_percent,
                network_sent_mb=max(0, network_sent_mb),  # 음수 방지
                network_recv_mb=max(0, network_recv_mb)
            )
            
        except Exception as e:
            print(f"시스템 메트릭 수집 오류: {e}")
            return SystemMetrics(
                timestamp=time.time(),
                cpu_percent=0, memory_mb=0, disk_usage_percent=0,
                network_sent_mb=0, network_recv_mb=0
            )
    
    async def _collect_database_metrics(self) -> List[DatabaseMetrics]:
        """데이터베이스 메트릭 수집"""
        metrics = []
        
        for i, config in enumerate(self.db_configs):
            server_type = "master" if i == 0 else f"slave_{i}"
            
            try:
                async with aiomysql.connect(**config) as conn:
                    async with conn.cursor(aiomysql.DictCursor) as cursor:
                        db_metrics = await self._collect_single_db_metrics(cursor, server_type)
                        metrics.append(db_metrics)
                        
            except Exception as e:
                print(f"DB 메트릭 수집 오류 ({server_type}): {e}")
                # 오류 시 기본값으로 메트릭 생성
                metrics.append(DatabaseMetrics(
                    timestamp=time.time(),
                    server_type=server_type,
                    connections_active=0,
                    connections_max=0,
                    queries_per_second=0,
                    slow_queries=0,
                    innodb_buffer_hit_rate=0
                ))
        
        return metrics
    
    async def _collect_single_db_metrics(self, cursor, server_type: str) -> DatabaseMetrics:
        """단일 DB 서버 메트릭 수집"""
        timestamp = time.time()
        
        # 활성 연결 수
        await cursor.execute("SHOW STATUS LIKE 'Threads_connected'")
        connections_active = int((await cursor.fetchone())['Value'])
        
        # 최대 연결 수
        await cursor.execute("SHOW VARIABLES LIKE 'max_connections'")
        connections_max = int((await cursor.fetchone())['Value'])
        
        # QPS 계산
        await cursor.execute("SHOW STATUS LIKE 'Questions'")
        questions = int((await cursor.fetchone())['Value'])
        await cursor.execute("SHOW STATUS LIKE 'Uptime'")
        uptime = int((await cursor.fetchone())['Value'])
        qps = questions / uptime if uptime > 0 else 0
        
        # Slow Queries
        await cursor.execute("SHOW STATUS LIKE 'Slow_queries'")
        slow_queries = int((await cursor.fetchone())['Value'])
        
        # InnoDB Buffer Pool Hit Rate
        await cursor.execute("SHOW STATUS LIKE 'Innodb_buffer_pool_read_requests'")
        read_requests = int((await cursor.fetchone())['Value'])
        await cursor.execute("SHOW STATUS LIKE 'Innodb_buffer_pool_reads'")
        reads = int((await cursor.fetchone())['Value'])
        
        hit_rate = (1 - reads/read_requests) * 100 if read_requests > 0 else 0
        
        # 복제 지연 (Slave인 경우)
        replication_lag = None
        if server_type != "master":
            try:
                await cursor.execute("SHOW SLAVE STATUS")
                slave_status = await cursor.fetchone()
                if slave_status and slave_status.get('Seconds_Behind_Master') is not None:
                    replication_lag = float(slave_status['Seconds_Behind_Master'])
            except Exception:
                pass  # 복제 정보를 가져올 수 없는 경우 무시
        
        return DatabaseMetrics(
            timestamp=timestamp,
            server_type=server_type,
            connections_active=connections_active,
            connections_max=connections_max,
            queries_per_second=qps,
            slow_queries=slow_queries,
            innodb_buffer_hit_rate=hit_rate,
            replication_lag_seconds=replication_lag
        )
    
    def get_latest_metrics(self) -> Dict:
        """최신 메트릭 조회"""
        latest_system = self.system_metrics_history[-1] if self.system_metrics_history else None
        latest_db = self.database_metrics_history[-len(self.db_configs):] if self.database_metrics_history else []
        
        return {
            'timestamp': datetime.now().isoformat(),
            'system': asdict(latest_system) if latest_system else None,
            'databases': [asdict(db) for db in latest_db]
        }
    
    def get_metrics_summary(self, last_minutes: int = 10) -> Dict:
        """지정 시간 동안의 메트릭 요약"""
        cutoff_time = time.time() - (last_minutes * 60)
        
        # 해당 시간 범위의 메트릭만 필터링
        recent_system = [m for m in self.system_metrics_history if m.timestamp >= cutoff_time]
        recent_db = [m for m in self.database_metrics_history if m.timestamp >= cutoff_time]
        
        if not recent_system or not recent_db:
            return {"error": "충분한 메트릭 데이터가 없습니다"}
        
        # 시스템 메트릭 요약
        system_summary = {
            'avg_cpu_percent': sum(m.cpu_percent for m in recent_system) / len(recent_system),
            'max_cpu_percent': max(m.cpu_percent for m in recent_system),
            'avg_memory_mb': sum(m.memory_mb for m in recent_system) / len(recent_system),
            'max_memory_mb': max(m.memory_mb for m in recent_system)
        }
        
        # 데이터베이스 메트릭 요약 (서버별)
        db_summary = {}
        for server_type in set(m.server_type for m in recent_db):
            server_metrics = [m for m in recent_db if m.server_type == server_type]
            
            db_summary[server_type] = {
                'avg_qps': sum(m.queries_per_second for m in server_metrics) / len(server_metrics),
                'max_connections': max(m.connections_active for m in server_metrics),
                'avg_buffer_hit_rate': sum(m.innodb_buffer_hit_rate for m in server_metrics) / len(server_metrics),
                'total_slow_queries': max(m.slow_queries for m in server_metrics) - min(m.slow_queries for m in server_metrics)
            }
            
            # 복제 지연 (Slave인 경우)
            replication_lags = [m.replication_lag_seconds for m in server_metrics if m.replication_lag_seconds is not None]
            if replication_lags:
                db_summary[server_type]['avg_replication_lag'] = sum(replication_lags) / len(replication_lags)
        
        return {
            'time_range_minutes': last_minutes,
            'data_points': len(recent_system),
            'system': system_summary,
            'databases': db_summary
        }
    
    def export_metrics(self, filename: str):
        """메트릭 데이터를 JSON 파일로 내보내기"""
        export_data = {
            'export_timestamp': datetime.now().isoformat(),
            'collection_duration_minutes': len(self.system_metrics_history) * 10 / 60,  # 10초 간격 가정
            'system_metrics': [asdict(m) for m in self.system_metrics_history],
            'database_metrics': [asdict(m) for m in self.database_metrics_history]
        }
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, indent=2, ensure_ascii=False)
        
        print(f"📁 메트릭 데이터 내보내기 완료: {filename}")
        print(f"   시스템 메트릭: {len(self.system_metrics_history)}개")
        print(f"   DB 메트릭: {len(self.database_metrics_history)}개")