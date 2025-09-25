"""
네이버 분산 MySQL 테스트 프레임워크
포트폴리오용 메인 실행기
"""
import asyncio
import argparse
import sys
from pathlib import Path
from datetime import datetime

# 프로젝트 루트를 Python path에 추가
sys.path.append(str(Path(__file__).parent))

from src.tests.performance_test import PerformanceTestSuite
from src.tests.consistency_test import ConsistencyTestSuite
from src.tests.stability_test import StabilityTestSuite
from src.utils.report_generator import ReportGenerator

class NaverMySQLTestRunner:
    """네이버 MySQL 테스트 메인 러너"""
    
    def __init__(self):
        self.test_suites = {
            'performance': PerformanceTestSuite(),
            'consistency': ConsistencyTestSuite(), 
            'stability': StabilityTestSuite()
        }
        self.report_generator = ReportGenerator()
        
    async def run_all_tests(self):
        """전체 테스트 수행"""
        print("🚀 네이버 MySQL 분산 테스트 프레임워크 실행")
        print("=" * 60)
        
        start_time = datetime.now()
        results = {}
        
        for test_name, test_suite in self.test_suites.items():
            print(f"\n🔄 {test_name.upper()} 테스트 시작...")
            
            try:
                result = await test_suite.run()
                results[test_name] = result
                print(f"✅ {test_name.upper()} 테스트 완료")
                
            except Exception as e:
                print(f"❌ {test_name.upper()} 테스트 실패: {e}")
                results[test_name] = {"error": str(e)}
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # 종합 리포트 생성
        await self.report_generator.create_comprehensive_report(
            results, start_time, end_time, duration
        )
        
        print(f"\n🎉 전체 테스트 완료 (소요시간: {duration:.1f}초)")
        return results
    
    async def run_specific_test(self, test_type: str):
        """특정 테스트만 실행"""
        if test_type not in self.test_suites:
            print(f"❌ 지원하지 않는 테스트 유형: {test_type}")
            return
            
        print(f"🔄 {test_type.upper()} 테스트 실행 중...")
        
        try:
            result = await self.test_suites[test_type].run()
            print(f"✅ {test_type.upper()} 테스트 완료")
            return result
            
        except Exception as e:
            print(f"❌ {test_type.upper()} 테스트 실패: {e}")
            return {"error": str(e)}

async def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(
        description="네이버 MySQL 분산 테스트 프레임워크"
    )
    parser.add_argument(
        "--test", 
        choices=['all', 'performance', 'consistency', 'stability'],
        default='all',
        help="실행할 테스트 유형"
    )
    
    args = parser.parse_args()
    runner = NaverMySQLTestRunner()
    
    if args.test == 'all':
        await runner.run_all_tests()
    else:
        await runner.run_specific_test(args.test)

if __name__ == "__main__":
    # VSCode에서 F5로 실행 가능
    asyncio.run(main())