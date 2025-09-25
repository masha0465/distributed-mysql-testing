"""
네이버 MySQL 성능 테스트 모듈
대용량 트래픽 처리 능력 검증
"""
import asyncio
import time
import random
from typing import List, Dict
from dataclasses import dataclass
from datetime import datetime

import aiomysql
from tabulate import tabulate
from colorama import Fore, Style, init

init()  # colorama 초기화

@dataclass
class PerformanceMetrics:
    """성능 메트릭 데이터 클래스"""
    test_name: str
    duration_seconds: float
    total_queries: int
    successful_queries: int
    failed_queries: int
    queries_per_second: float
    avg_response_time_ms: float
    p95_response_time_ms: float
    p99_response_time_ms: float

class PerformanceTestSuite:
    """성능 테스트 스위트"""
    
    def __init__(self):
        # Docker 환경 설정 (실제 NCP 환경으로 쉽게 변경 가능)
        self.db_configs = [
            {  # Master
                'host': 'localhost',
                'port': 3306,
                'user': 'root',
                'password': 'testpass',
                'db': 'testdb'
            },
            {  # Slave 1
                'host': 'localhost', 
                'port': 3307,
                'user': 'root',
                'password': 'testpass',
                'db': 'testdb'
            }
        ]
        
        self.response_times = []
        self.error_count = 0
        
    async def run(self) -> Dict:
        """성능 테스트 실행"""
        print(f"{Fore.CYAN}📊 성능 테스트 시작{Style.RESET_ALL}")
        
        # 테스트 시나리오
        test_scenarios = [
            {"name": "기본_SELECT_테스트", "duration": 60, "qps": 50},
            {"name": "고부하_혼합_쿼리", "duration": 120, "qps": 100},
            {"name": "최대_처리량_테스트", "duration": 60, "qps": 200}
        ]
        
        results = {}
        
        for scenario in test_scenarios:
            print(f"\n🔄 {scenario['name']} 실행 중...")
            
            metrics = await self._run_load_test(
                scenario['name'],
                scenario['duration'], 
                scenario['qps']
            )
            
            results[scenario['name']] = metrics
            self._print_performance_results(metrics)
            
            # 테스트 간 휴식
            await asyncio.sleep(10)
        
        return results
    
    async def _run_load_test(self, test_name: str, duration: int, target_qps: int) -> PerformanceMetrics:
        """부하 테스트 실행"""
        start_time = time.time()
        self.response_times = []
        self.error_count = 0
        
        # QPS에 맞춰 태스크 생성
        tasks = []
        total_queries = duration * target_qps
        
        for i in range(total_queries):
            delay = i / target_qps  # QPS 조절을 위한 지연
            config = random.choice(self.db_configs)  # 부하 분산
            
            task = asyncio.create_task(
                self._execute_query_with_delay(config, delay)
            )
            tasks.append(task)
        
        # 모든 쿼리 실행
        await asyncio.gather(*tasks, return_exceptions=True)
        
        end_time = time.time()
        actual_duration = end_time - start_time
        
        return self._calculate_metrics(test_name, actual_duration, total_queries)
    
    async def _execute_query_with_delay(self, config: Dict, delay: float):
        """지연 시간 후 쿼리 실행"""
        await asyncio.sleep(delay)
        
        query_start = time.time()
        
        try:
            async with aiomysql.connect(**config) as conn:
                async with conn.cursor() as cursor:
                    # 네이버 서비스 유형의 쿼리들
                    queries = [
                        "SELECT COUNT(*) FROM information_schema.tables",
                        "SELECT NOW(), VERSION()", 
                        "SHOW STATUS LIKE 'Threads_connected'",
                        "SELECT SLEEP(0.01)"  # 10ms 시뮬레이션
                    ]
                    
                    query = random.choice(queries)
                    await cursor.execute(query)
                    await cursor.fetchall()
            
            response_time = (time.time() - query_start) * 1000  # ms 변환
            self.response_times.append(response_time)
            
        except Exception as e:
            self.error_count += 1
            print(f"Query error: {e}")
    
    def _calculate_metrics(self, test_name: str, duration: float, total_queries: int) -> PerformanceMetrics:
        """성능 메트릭 계산"""
        successful_queries = len(self.response_times)
        failed_queries = self.error_count
        qps = successful_queries / duration if duration > 0 else 0
        
        if self.response_times:
            avg_response = sum(self.response_times) / len(self.response_times)
            sorted_times = sorted(self.response_times)
            p95_response = sorted_times[int(len(sorted_times) * 0.95)]
            p99_response = sorted_times[int(len(sorted_times) * 0.99)]
        else:
            avg_response = p95_response = p99_response = 0
        
        return PerformanceMetrics(
            test_name=test_name,
            duration_seconds=duration,
            total_queries=total_queries,
            successful_queries=successful_queries,
            failed_queries=failed_queries,
            queries_per_second=qps,
            avg_response_time_ms=avg_response,
            p95_response_time_ms=p95_response,
            p99_response_time_ms=p99_response
        )
    
    def _print_performance_results(self, metrics: PerformanceMetrics):
        """성능 결과 출력"""
        data = [
            ["QPS", f"{metrics.queries_per_second:.1f}"],
            ["평균 응답시간", f"{metrics.avg_response_time_ms:.1f}ms"],
            ["P95 응답시간", f"{metrics.p95_response_time_ms:.1f}ms"], 
            ["P99 응답시간", f"{metrics.p99_response_time_ms:.1f}ms"],
            ["성공률", f"{metrics.successful_queries/metrics.total_queries*100:.1f}%"]
        ]
        
        print(f"\n{Fore.GREEN}📈 {metrics.test_name} 결과:{Style.RESET_ALL}")
        print(tabulate(data, headers=["메트릭", "값"], tablefmt="grid"))
        
        # 성능 평가
        if metrics.queries_per_second >= 150:
            print(f"{Fore.GREEN}🎉 EXCELLENT 성능!{Style.RESET_ALL}")
        elif metrics.queries_per_second >= 100:
            print(f"{Fore.YELLOW}✅ GOOD 성능{Style.RESET_ALL}")
        else:
            print(f"{Fore.RED}⚠️  성능 개선 필요{Style.RESET_ALL}")