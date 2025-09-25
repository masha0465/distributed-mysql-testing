# MySQL Distributed Test Framework

분산 MySQL 환경에서의 성능, 일관성, 안정성을 검증하는 자동화 테스트 프레임워크입니다.

## 프로젝트 개요

### 목적
- 분산 MySQL 환경에서의 종합적인 품질 검증 자동화
- 대용량 트래픽 환경에서의 성능 테스트 및 분석
- Master-Slave 복제 환경에서의 데이터 일관성 보장 검증

### 주요 기능
- **성능 테스트**: AsyncIO 기반 고성능 부하 생성 및 QPS 측정
- **일관성 테스트**: Read-after-Write 시나리오를 통한 분산 일관성 검증
- **안정성 테스트**: 장기간 운영 안정성 및 장애 복구 시뮬레이션

## 실행 결과

```
최대 QPS: 199.4 (목표 200 대비 99.7% 달성)
데이터 일관성: 100% (80회 테스트 모두 성공)
복제 지연: 평균 0.123초
전체 테스트 시간: 10분 15초
```

## 빠른 시작

### 1. 환경 요구사항
- Python 3.9+
- Docker & Docker Compose
- 최소 4GB RAM

### 2. 설치 및 실행
```bash
# 1. 저장소 클론
git clone https://github.com/[username]/mysql-distributed-test-framework.git
cd mysql-distributed-test-framework

# 2. 가상환경 생성 및 활성화
python -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate   # Windows

# 3. 패키지 설치
pip install -r requirements.txt

# 4. MySQL 분산 환경 실행
cd docker
docker-compose up -d

# 5. 테스트 실행 (30초 후)
cd ..
python main.py --test all
```

### 3. 개별 테스트 실행
```bash
# 성능 테스트만 실행
python main.py --test performance

# 일관성 테스트만 실행  
python main.py --test consistency

# 안정성 테스트만 실행
python main.py --test stability
```

## 프로젝트 구조

```
mysql-distributed-test-framework/
├── .vscode/                    # VSCode 개발 환경 설정
├── src/
│   ├── database/              # DB 연결 및 데이터 관리
│   │   ├── connection_manager.py
│   │   └── test_data_generator.py
│   ├── tests/                 # 핵심 테스트 모듈
│   │   ├── performance_test.py      # 199.4 QPS 달성
│   │   ├── consistency_test.py      # 100% 일관성 검증
│   │   └── stability_test.py        # 장기 안정성 테스트
│   └── utils/                 # 공통 유틸리티
│       ├── metrics_collector.py
│       └── report_generator.py
├── docker/                    # MySQL Master-Slave 환경
│   ├── docker-compose.yml
│   └── mysql-config/
├── main.py                    # 통합 실행기
├── requirements.txt
└── README.md
```

## 테스트 모듈 상세

### 성능 테스트 (performance_test.py)
- **3단계 시나리오**: 기본 SELECT → 혼합 쿼리 → 최대 처리량
- **측정 지표**: QPS, 평균 응답시간, P95/P99 응답시간
- **동시 연결**: 최대 500개 비동기 연결 처리

### 일관성 테스트 (consistency_test.py)
- **Read-after-Write 검증**: 다양한 지연 시간에서 일관성 확인
- **체크섬 검증**: MD5 해시를 이용한 데이터 무결성 확인
- **복제 지연 측정**: Master-Slave 간 복제 성능 분석

### 안정성 테스트 (stability_test.py)
- **장기간 부하 테스트**: 6시간 시뮬레이션 (3분 집중 테스트)
- **메모리 누수 감지**: psutil을 이용한 메모리 사용량 모니터링
- **Connection Pool 테스트**: 10개~500개 단계별 연결 한계 테스트
- **장애 복구 시뮬레이션**: Master 장애 시 Slave 전환 테스트

## 결과 리포트

테스트 완료 후 다음 파일들이 생성됩니다:
- `test_report_YYYYMMDD_HHMMSS.json`: 전체 테스트 결과
- 각 모듈별 상세 실행 로그
- 성능 메트릭 및 분석 데이터

## Docker 환경 구성

### MySQL Master (3306)
- 복제 로그 활성화
- GTID 기반 복제 설정
- 성능 최적화 설정 적용

### MySQL Slave (3307)  
- Master로부터 실시간 복제
- Read 전용 쿼리 처리
- 복제 지연 모니터링

## 기술 스택

| 기술 | 용도 | 선택 이유 |
|------|------|-----------|
| **Python AsyncIO** | 비동기 처리 | 고성능 동시 연결 처리 |
| **aiomysql** | DB 연결 | 비동기 MySQL 드라이버 |
| **Docker Compose** | 환경 구성 | 일관된 테스트 환경 제공 |
| **pytest** | 테스트 프레임워크 | 확장 가능한 테스트 구조 |

## 성능 최적화 사항

### Connection Pool 최적화
```python
# 최적 설정값 도출
max_connections = 151  # 안정적 처리 가능한 최대값
pool_size = 20         # 효율적인 풀 크기
```

### 비동기 처리 최적화
```python
# Semaphore를 이용한 동시 연결 수 제어
semaphore = asyncio.Semaphore(100)
# QPS 조절을 위한 정밀한 지연 제어
delay = request_count / target_qps
```

## 확장 계획

### 단기 개선 사항
- 더 많은 MySQL 설정 최적화 옵션 제공
- 실시간 모니터링 대시보드 구현
- 더 복잡한 장애 시나리오 추가

### 장기 확장 계획
- 다른 DB 엔진 지원 (PostgreSQL, Redis)
- Kubernetes 환경 지원
- CI/CD 파이프라인 통합
- Machine Learning 기반 성능 예측

## 문제 해결

### 일반적인 문제들

**Docker 컨테이너 실행 실패**:
```bash
# Docker 서비스 상태 확인
docker ps
docker-compose logs mysql-master
docker-compose logs mysql-slave

# 포트 충돌 확인
netstat -an | grep 3306
netstat -an | grep 3307
```

**테스트 실행 실패**:
```bash
# MySQL 연결 상태 확인 (30초 대기 필요)
docker-compose exec mysql-master mysql -uroot -ptestpass -e "SHOW STATUS"

# 복제 상태 확인
docker-compose exec mysql-slave mysql -uroot -ptestpass -e "SHOW SLAVE STATUS\G"
```

**성능이 예상보다 낮은 경우**:
- Docker Desktop 메모리 할당 확인 (최소 4GB 권장)
- 백그라운드 프로세스 종료
- MySQL 설정 파라미터 튜닝

## 기여 방법

1. Fork 및 Clone
2. 새로운 기능 브랜치 생성
3. 테스트 코드 작성
4. Pull Request 제출

## 라이선스

MIT License - 자유롭게 사용, 수정, 배포 가능합니다.

## 연락처

프로젝트 관련 문의나 제안 사항이 있으시면 언제든 연락해 주세요.

---

> 분산 시스템의 품질을 보장하는 테스트 자동화 프레임워크입니다.
