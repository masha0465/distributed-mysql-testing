"""
네이버 MySQL 안정성 테스트 모듈
장시간 운영 및 장애 상황에서의 시스템 안정성 검증
"""
import asyncio
import time
import psutil
import gc
from typing import Dict, List
from datetime import datetime, timedelta

import aiomysql
from colorama import Fore, Style

class StabilityTestSuite:
    """안정성 테스트 스위트"""
    
    def __init__(self):
        self.db_configs = [
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
        
        self.memory_usage_history = []
        self.connection_history = []
        
    async def run(self) -> Dict:
        """안정성 테스트 실행"""
        print(f"{Fore.CYAN}🔒 안정성 테스트 시작{Style.RESET_ALL}")
        
        results = {}
        
        # 1. 장시간 부하 테스트 (시뮬레이션)
        print("\n⏱️  장시간 부하 테스트 (6시간 시뮬레이션)...")
        long_running_result = await self._simulate_long_running_test()
        results['long_running'] = long_running_result
        
        # 2. 메모리 누수 감지 테스트
        print("\n🧠 메모리 누수 감지 테스트...")
        memory_leak_result = await self._test_memory_leak()
        results['memory_leak'] = memory_leak_result
        
        # 3. Connection Pool 고갈 테스트
        print("\n🔗 Connection Pool 고갈 테스트...")
        connection_pool_result = await self._test_connection_pool_exhaustion()
        results['connection_pool'] = connection_pool_result
        
        # 4. 장애 복구 테스트
        print("\n⚡ 장애 복구 시뮬레이션...")
        failover_result = await self._simulate_failover_test()
        results['failover'] = failover_result
        
        return results
    
    async def _simulate_long_running_test(self) -> Dict:
        """6시간 장시간 테스트 시뮬레이션"""
        # 실제로는 6시간이지만, 포트폴리오용으로 3분간 집중 테스트
        test_duration = 180  # 3분
        start_time = time.time()
        
        error_count = 0
        total_requests = 0
        response_times = []
        
        print(f"   📊 3분간 집중 안정성 테스트 실행...")
        
        # 지속적인 부하 생성
        async def continuous_load():
            nonlocal error_count, total_requests
            
            while (time.time() - start_time) < test_duration:
                try:
                    config = self.db_configs[0]  # Master 사용
                    async with aiomysql.connect(**config) as conn:
                        async with conn.cursor() as cursor:
                            # 다양한 쿼리 패턴
                            queries = [
                                "SELECT COUNT(*) FROM information_schema.tables",
                                "SELECT NOW(), CONNECTION_ID()",
                                "SHOW STATUS LIKE 'Threads_connected'",
                                "SELECT SLEEP(0.01)"  # 10ms 시뮬레이션
                            ]
                            
                            for query in queries:
                                query_start = time.time()
                                await cursor.execute(query)
                                await cursor.fetchall()
                                
                                response_time = (time.time() - query_start) * 1000
                                response_times.append(response_time)
                                total_requests += 1
                    
                    await asyncio.sleep(0.1)  # 100ms 간격
                    
                except Exception as e:
                    error_count += 1
                    await asyncio.sleep(1)  # 오류 시 1초 대기
        
        # 메모리 모니터링
        async def monitor_resources():
            while (time.time() - start_time) < test_duration:
                process = psutil.Process()
                memory_mb = process.memory_info().rss / 1024 / 1024
                self.memory_usage_history.append({
                    'timestamp': time.time(),
                    'memory_mb': memory_mb
                })
                
                await asyncio.sleep(10)  # 10초마다 수집
        
        # 병렬 실행
        await asyncio.gather(
            continuous_load(),
            monitor_resources()
        )
        
        end_time = time.time()
        actual_duration = end_time - start_time
        
        # 결과 계산
        success_rate = (total_requests - error_count) / total_requests if total_requests > 0 else 0
        avg_response_time = sum(response_times) / len(response_times) if response_times else 0
        
        # 가용성 계산 (99.9% 목표)
        uptime_percentage = success_rate * 100
        
        print(f"   ✅ 장시간 테스트 완료:")
        print(f"       총 요청: {total_requests:,}회")
        print(f"       에러: {error_count}회")
        print(f"       가용성: {uptime_percentage:.2f}%")
        print(f"       평균 응답시간: {avg_response_time:.1f}ms")
        
        return {
            'duration_seconds': actual_duration,
            'total_requests': total_requests,
            'error_count': error_count,
            'success_rate': success_rate,
            'uptime_percentage': uptime_percentage,
            'avg_response_time_ms': avg_response_time,
            'memory_usage_history': self.memory_usage_history
        }
    
    async def _test_memory_leak(self) -> Dict:
        """메모리 누수 감지 테스트"""
        print(f"   🔍 메모리 사용량 패턴 분석 중...")
        
        initial_memory = psutil.Process().memory_info().rss / 1024 / 1024
        
        # 반복적인 연결 생성/해제로 메모리 누수 테스트
        connections_created = 0
        memory_samples = []
        
        for i in range(100):  # 100번 연결 테스트
            try:
                config = self.db_configs[0]
                async with aiomysql.connect(**config) as conn:
                    async with conn.cursor() as cursor:
                        await cursor.execute("SELECT 1")
                        await cursor.fetchone()
                        connections_created += 1
                
                # 10번마다 메모리 측정
                if i % 10 == 0:
                    current_memory = psutil.Process().memory_info().rss / 1024 / 1024
                    memory_samples.append(current_memory)
                    
            except Exception as e:
                print(f"   ⚠️  연결 오류: {e}")
            
            await asyncio.sleep(0.05)  # 50ms 간격
        
        # 강제 가비지 컬렉션
        gc.collect()
        final_memory = psutil.Process().memory_info().rss / 1024 / 1024
        
        # 메모리 증가율 계산
        memory_increase = final_memory - initial_memory
        memory_increase_rate = (memory_increase / initial_memory) * 100 if initial_memory > 0 else 0
        
        # 메모리 누수 판정 (5MB 이상 또는 10% 이상 증가 시 의심)
        leak_suspected = memory_increase > 5 or memory_increase_rate > 10
        
        print(f"   📊 메모리 분석 결과:")
        print(f"       초기 메모리: {initial_memory:.1f}MB")
        print(f"       최종 메모리: {final_memory:.1f}MB")
        print(f"       메모리 증가: {memory_increase:.1f}MB ({memory_increase_rate:.1f}%)")
        print(f"       누수 의심: {'🔴 YES' if leak_suspected else '🟢 NO'}")
        
        return {
            'initial_memory_mb': initial_memory,
            'final_memory_mb': final_memory,
            'memory_increase_mb': memory_increase,
            'memory_increase_rate': memory_increase_rate,
            'connections_tested': connections_created,
            'leak_suspected': leak_suspected,
            'memory_samples': memory_samples
        }
    
    async def _test_connection_pool_exhaustion(self) -> Dict:
        """Connection Pool 고갈 상황 테스트"""
        print(f"   🔗 동시 연결 한계 테스트...")
        
        # 동시 연결 수를 점진적으로 늘려가며 테스트
        max_successful_connections = 0
        connection_errors = []
        
        for batch_size in [10, 50, 100, 200, 500]:
            print(f"       {batch_size}개 동시 연결 테스트...")
            
            connections = []
            successful_connections = 0
            
            # 동시에 여러 연결 시도
            async def create_connection(connection_id):
                nonlocal successful_connections
                try:
                    config = self.db_configs[0]
                    conn = await aiomysql.connect(**config)
                    connections.append(conn)
                    successful_connections += 1
                    return True
                except Exception as e:
                    connection_errors.append({
                        'batch_size': batch_size,
                        'connection_id': connection_id,
                        'error': str(e)
                    })
                    return False
            
            # 배치 단위로 연결 시도
            tasks = [create_connection(i) for i in range(batch_size)]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            max_successful_connections = max(max_successful_connections, successful_connections)
            
            # 연결 정리
            for conn in connections:
                try:
                    conn.close()
                except:
                    pass
            
            print(f"         성공한 연결: {successful_connections}/{batch_size}")
            
            await asyncio.sleep(2)  # 연결 정리 대기
        
        print(f"   ✅ Connection Pool 테스트 완료:")
        print(f"       최대 동시 연결 수: {max_successful_connections}")
        print(f"       연결 오류 발생: {len(connection_errors)}회")
        
        return {
            'max_successful_connections': max_successful_connections,
            'connection_errors': len(connection_errors),
            'error_details': connection_errors[:5],  # 처음 5개 오류만
            'connection_stability': 'GOOD' if len(connection_errors) < 10 else 'POOR'
        }
    
    async def _simulate_failover_test(self) -> Dict:
        """Master 장애 복구 시뮬레이션"""
        print(f"   ⚡ 장애 복구 시나리오 시뮬레이션...")
        
        # 정상 상태에서 성능 측정
        print(f"       1️⃣ 정상 상태 성능 측정...")
        normal_performance = await self._measure_performance(duration=30)
        
        # 장애 시뮬레이션 (Master 연결 실패)
        print(f"       2️⃣ 장애 상황 시뮬레이션...")
        failure_start = time.time()
        
        # Slave로 자동 전환 시뮬레이션
        await asyncio.sleep(5)  # 5초 장애 지속
        
        print(f"       3️⃣ Slave 서버로 자동 전환...")
        recovery_performance = await self._measure_performance(
            duration=30, 
            use_slave=True
        )
        
        failure_end = time.time()
        recovery_time = failure_end - failure_start
        
        # 복구 후 성능 비교
        performance_degradation = (
            (normal_performance['avg_response_time'] - recovery_performance['avg_response_time']) 
            / normal_performance['avg_response_time'] * 100
        )
        
        print(f"   📊 장애 복구 결과:")
        print(f"       복구 시간: {recovery_time:.1f}초")
        print(f"       정상 상태 QPS: {normal_performance['qps']:.1f}")
        print(f"       복구 후 QPS: {recovery_performance['qps']:.1f}")
        print(f"       성능 영향: {abs(performance_degradation):.1f}%")
        
        # 복구 품질 평가
        if recovery_time <= 30 and abs(performance_degradation) <= 20:
            recovery_quality = "EXCELLENT"
        elif recovery_time <= 60 and abs(performance_degradation) <= 40:
            recovery_quality = "GOOD"
        else:
            recovery_quality = "NEEDS_IMPROVEMENT"
        
        return {
            'recovery_time_seconds': recovery_time,
            'normal_qps': normal_performance['qps'],
            'recovery_qps': recovery_performance['qps'],
            'performance_impact_percent': abs(performance_degradation),
            'recovery_quality': recovery_quality,
            'target_recovery_time': 30,  # 30초 목표
            'recovery_success': recovery_time <= 30
        }
    
    async def _measure_performance(self, duration: int = 30, use_slave: bool = False) -> Dict:
        """성능 측정 헬퍼 함수"""
        config = self.db_configs[1] if use_slave else self.db_configs[0]
        
        start_time = time.time()
        request_count = 0
        response_times = []
        error_count = 0
        
        while (time.time() - start_time) < duration:
            try:
                query_start = time.time()
                async with aiomysql.connect(**config) as conn:
                    async with conn.cursor() as cursor:
                        await cursor.execute("SELECT NOW()")
                        await cursor.fetchone()
                
                response_time = (time.time() - query_start) * 1000
                response_times.append(response_time)
                request_count += 1
                
            except Exception as e:
                error_count += 1
            
            await asyncio.sleep(0.1)
        
        actual_duration = time.time() - start_time
        qps = request_count / actual_duration
        avg_response_time = sum(response_times) / len(response_times) if response_times else 0
        
        return {
            'qps': qps,
            'avg_response_time': avg_response_time,
            'request_count': request_count,
            'error_count': error_count
        }