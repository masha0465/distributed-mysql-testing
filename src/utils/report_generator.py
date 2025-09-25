"""
네이버 MySQL 테스트 리포트 생성기 (수정됨)
"""
import json
from datetime import datetime
from typing import Dict, Any
from pathlib import Path
from dataclasses import asdict

class ReportGenerator:
    """테스트 리포트 생성기"""
    
    def __init__(self):
        pass
    
    async def create_comprehensive_report(self, results: Dict, start_time: datetime, end_time: datetime, duration: float):
        """종합 리포트 생성"""
        print("📊 종합 리포트 생성 중...")
        
        # JSON 직렬화 가능한 형태로 변환
        serializable_results = self._make_serializable(results)
        
        # JSON 리포트 생성
        json_report = self._create_json_report(serializable_results, start_time, end_time, duration)
        
        # 파일로 저장
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_filename = f"test_report_{timestamp}.json"
        
        with open(json_filename, 'w', encoding='utf-8') as f:
            json.dump(json_report, f, indent=2, ensure_ascii=False)
        
        print(f"✅ 리포트 생성 완료: {json_filename}")
        
        # 결과 요약 출력
        self._print_summary(json_report)
        
        return json_filename
    
    def _make_serializable(self, obj):
        """객체를 JSON 직렬화 가능한 형태로 변환"""
        if hasattr(obj, '__dict__'):
            # dataclass나 일반 객체는 딕셔너리로 변환
            return {k: self._make_serializable(v) for k, v in obj.__dict__.items()}
        elif isinstance(obj, dict):
            return {k: self._make_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._make_serializable(item) for item in obj]
        elif isinstance(obj, (str, int, float, bool)) or obj is None:
            return obj
        elif isinstance(obj, datetime):
            return obj.isoformat()
        else:
            # 기타 객체는 문자열로 변환
            return str(obj)
    
    def _create_json_report(self, results: Dict, start_time: datetime, end_time: datetime, duration: float) -> Dict:
        """JSON 형태 리포트 생성"""
        return {
            "report_metadata": {
                "generated_at": datetime.now().isoformat(),
                "test_start_time": start_time.isoformat(),
                "test_end_time": end_time.isoformat(),
                "total_duration_seconds": duration,
                "test_framework": "네이버 MySQL 분산 테스트 프레임워크",
                "version": "1.0.0"
            },
            "test_results": results,
            "summary": self._generate_summary(results),
            "recommendations": self._generate_recommendations(results)
        }
    
    def _generate_summary(self, results: Dict) -> Dict:
        """테스트 결과 요약 생성"""
        summary = {
            "total_test_suites": len(results),
            "successful_suites": 0,
            "failed_suites": 0,
            "overall_status": "UNKNOWN"
        }
        
        for suite_name, suite_results in results.items():
            if isinstance(suite_results, dict) and "error" not in suite_results:
                summary["successful_suites"] += 1
            else:
                summary["failed_suites"] += 1
        
        if summary["failed_suites"] == 0:
            summary["overall_status"] = "PASS"
        elif summary["successful_suites"] > summary["failed_suites"]:
            summary["overall_status"] = "PARTIAL_PASS"
        else:
            summary["overall_status"] = "FAIL"
        
        return summary
    
    def _generate_recommendations(self, results: Dict) -> list:
        """개선 권장사항 생성"""
        recommendations = []
        
        # 성능 테스트 결과 분석
        if "performance" in results:
            perf_results = results["performance"]
            if isinstance(perf_results, dict):
                for test_name, test_result in perf_results.items():
                    if isinstance(test_result, dict):
                        qps = test_result.get('queries_per_second', 0)
                        if qps < 100:
                            recommendations.append(f"성능 개선 필요: {test_name} QPS가 {qps:.1f}로 낮음")
                        
                        response_time = test_result.get('p95_response_time_ms', 0)
                        if response_time > 100:
                            recommendations.append(f"응답시간 개선 필요: {test_name} P95 응답시간이 {response_time:.1f}ms")
        
        # 일관성 테스트 결과 분석  
        if "consistency" in results:
            cons_results = results["consistency"]
            if isinstance(cons_results, dict) and "read_after_write" in cons_results:
                consistency_data = cons_results["read_after_write"]
                if isinstance(consistency_data, dict):
                    consistency_rate = consistency_data.get("consistency_rate", 0)
                    if consistency_rate < 0.99:
                        recommendations.append(f"데이터 일관성 개선 필요: {consistency_rate:.1%} (목표: 99% 이상)")
        
        # 안정성 테스트 결과 분석
        if "stability" in results:
            stab_results = results["stability"]
            if isinstance(stab_results, dict):
                if "memory_leak" in stab_results:
                    memory_data = stab_results["memory_leak"]
                    if isinstance(memory_data, dict) and memory_data.get("leak_suspected", False):
                        recommendations.append("메모리 누수 의심 - 메모리 사용량 모니터링 강화 필요")
                
                if "recovery" in stab_results:
                    recovery_data = stab_results["recovery"]
                    if isinstance(recovery_data, dict):
                        recovery_time = recovery_data.get("recovery_time_seconds", 0)
                        if recovery_time > 30:
                            recommendations.append(f"장애 복구 시간 개선 필요: {recovery_time:.1f}초 (목표: 30초 이하)")
        
        if not recommendations:
            recommendations.append("모든 테스트가 기준을 충족합니다. 현재 설정을 유지하세요.")
        
        return recommendations
    
    def _print_summary(self, report: Dict):
        """리포트 요약 출력"""
        print("\n" + "="*60)
        print("📋 네이버 MySQL 테스트 결과 요약")
        print("="*60)
        
        summary = report["summary"]
        print(f"🎯 전체 상태: {summary['overall_status']}")
        print(f"✅ 성공한 테스트: {summary['successful_suites']}")
        print(f"❌ 실패한 테스트: {summary['failed_suites']}")
        
        print(f"\n💡 주요 성과:")
        
        # 성능 테스트 요약
        if "performance" in report["test_results"]:
            perf_data = report["test_results"]["performance"]
            if isinstance(perf_data, dict):
                max_qps = 0
                for test_result in perf_data.values():
                    if isinstance(test_result, dict):
                        qps = test_result.get('queries_per_second', 0)
                        max_qps = max(max_qps, qps)
                print(f"   📊 최대 달성 QPS: {max_qps:.1f}")
        
        # 안정성 테스트 요약
        if "stability" in report["test_results"]:
            stab_data = report["test_results"]["stability"]
            if isinstance(stab_data, dict) and "long_running" in stab_data:
                long_running = stab_data["long_running"]
                if isinstance(long_running, dict):
                    uptime = long_running.get('uptime_percentage', 0)
                    print(f"   ⏱️  시스템 가용성: {uptime:.1f}%")
        
        print(f"\n💡 권장사항:")
        for i, rec in enumerate(report["recommendations"], 1):
            print(f"   {i}. {rec}")
        
        print("="*60)