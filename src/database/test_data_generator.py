"""
네이버 MySQL 테스트 데이터 생성기
실제 네이버 서비스와 유사한 대용량 테스트 데이터 생성
"""
import asyncio
import random
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from dataclasses import dataclass

import mysql.connector
import aiomysql
from faker import Faker

@dataclass
class GenerationStats:
    """데이터 생성 통계"""
    table_name: str
    target_count: int
    generated_count: int
    start_time: datetime
    end_time: datetime
    duration_seconds: float
    generation_rate: float

class NaverStyleDataGenerator:
    """네이버 스타일 테스트 데이터 생성기"""
    
    def __init__(self, db_config: Dict):
        self.db_config = db_config
        self.fake_kr = Faker(['ko_KR'])
        self.fake_en = Faker(['en_US'])
        
        self.naver_domains = ['naver.com', 'hanmail.net', 'daum.net', 'gmail.com']
        self.blog_categories = [
            '일상', '여행', '맛집', '패션', '뷰티', 'IT', '게임', '영화', '음악', '스포츠',
            '요리', '육아', '펜펫', '자동차', '부동산', '주식', '취업', '학습', '건강', '연애'
        ]
        self.search_keywords = [
            '날씨', '뉴스', '코로나', '주식', '맛집', '여행', '쇼핑', '게임', '영화', '드라마',
            '음악', '스포츠', 'BTS', '손흥민', '삼성', 'LG', '현대', '카카오', '네이버', 'IT',
            '코로나19', '백신', '부동산', '아파트', '전세', '대출', '금리', '비트코인', '주식투자',
            '삼성전자', 'SK하이닉스', '카카오뱅크', '배달음식', '치킨', '피자', '족발', '보쌈',
            '떡볶이', '라면', '넷플릭스', '유튜브', '인스타그램', '틱톡', '카카오톡', '아이폰',
            '갤럭시', '맥북', 'LG그램', '에어팟', 'TV'
        ]
        
    async def create_naver_service_tables(self):
        """네이버 서비스 스타일 테이블 생성"""
        print("🏗️ 네이버 서비스 테이블 생성 중...")
        
        async with aiomysql.connect(**self.db_config) as conn:
            async with conn.cursor() as cursor:
                
                await cursor.execute("""
                    CREATE TABLE IF NOT EXISTS naver_users (
                        user_id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        naver_id VARCHAR(50) NOT NULL UNIQUE,
                        email VARCHAR(100) NOT NULL,
                        phone VARCHAR(20),
                        name VARCHAR(50),
                        birth_date DATE,
                        gender ENUM('M', 'F', 'N') DEFAULT 'N',
                        join_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_login TIMESTAMP,
                        login_count INT DEFAULT 0,
                        status ENUM('active', 'dormant', 'withdrawn') DEFAULT 'active',
                        region VARCHAR(20),
                        
                        INDEX idx_naver_id (naver_id),
                        INDEX idx_email (email),
                        INDEX idx_join_date (join_date),
                        INDEX idx_last_login (last_login),
                        INDEX idx_status (status)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """)
                
                await cursor.execute("""
                    CREATE TABLE IF NOT EXISTS blog_posts (
                        post_id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        title VARCHAR(200) NOT NULL,
                        content LONGTEXT,
                        category VARCHAR(50),
                        view_count INT DEFAULT 0,
                        like_count INT DEFAULT 0,
                        comment_count INT DEFAULT 0,
                        share_count INT DEFAULT 0,
                        is_public BOOLEAN DEFAULT true,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        
                        INDEX idx_user_id (user_id),
                        INDEX idx_created_at (created_at),
                        INDEX idx_category (category),
                        INDEX idx_view_count (view_count),
                        FOREIGN KEY (user_id) REFERENCES naver_users(user_id) ON DELETE CASCADE
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """)
                
                await cursor.execute("""
                    CREATE TABLE IF NOT EXISTS search_logs (
                        log_id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        user_id BIGINT NULL,
                        search_query VARCHAR(500) NOT NULL,
                        search_type ENUM('web', 'image', 'news', 'shopping', 'blog', 'cafe', 'video') DEFAULT 'web',
                        results_count INT DEFAULT 0,
                        click_position INT DEFAULT 0,
                        search_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        ip_address VARCHAR(45),
                        user_agent TEXT,
                        session_id VARCHAR(100),
                        
                        INDEX idx_user_id (user_id),
                        INDEX idx_search_time (search_time),
                        INDEX idx_search_type (search_type),
                        INDEX idx_search_query (search_query(100)),
                        FOREIGN KEY (user_id) REFERENCES naver_users(user_id) ON DELETE SET NULL
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """)
                
                await cursor.execute("""
                    CREATE TABLE IF NOT EXISTS shopping_orders (
                        order_id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        product_name VARCHAR(200) NOT NULL,
                        product_category VARCHAR(50),
                        product_price DECIMAL(12,2) NOT NULL,
                        quantity INT DEFAULT 1,
                        total_amount DECIMAL(12,2) NOT NULL,
                        order_status ENUM('pending', 'paid', 'shipped', 'delivered', 'cancelled') DEFAULT 'pending',
                        payment_method ENUM('card', 'bank', 'naver_pay', 'payco') DEFAULT 'card',
                        order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        shipped_date TIMESTAMP NULL,
                        delivered_date TIMESTAMP NULL,
                        seller_id VARCHAR(50),
                        
                        INDEX idx_user_id (user_id),
                        INDEX idx_order_date (order_date),
                        INDEX idx_order_status (order_status),
                        INDEX idx_total_amount (total_amount),
                        FOREIGN KEY (user_id) REFERENCES naver_users(user_id) ON DELETE CASCADE
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """)
                
                await conn.commit()
        
        print("✅ 네이버 서비스 테이블 생성 완료")
    
    async def generate_naver_users(self, count: int = 10000) -> GenerationStats:
        """네이버 사용자 데이터 생성"""
        print(f"👥 {count:,}명의 네이버 사용자 생성 중...")
        
        start_time = datetime.now()
        generated_count = 0
        batch_size = 1000
        
        async with aiomysql.connect(**self.db_config) as conn:
            async with conn.cursor() as cursor:
                
                for i in range(0, count, batch_size):
                    batch_data = []
                    current_batch_size = min(batch_size, count - i)
                    
                    for j in range(current_batch_size):
                        naver_id = f"{self.fake_en.user_name()}{random.randint(1000, 9999)}"
                        email_domain = random.choice(self.naver_domains)
                        email = f"{naver_id}@{email_domain}"
                        
                        if random.random() < 0.7:
                            name = self.fake_kr.name()
                        else:
                            name = self.fake_en.name()
                        
                        birth_date = self.fake_kr.date_of_birth(minimum_age=14, maximum_age=80)
                        gender = random.choices(['M', 'F', 'N'], weights=[45, 45, 10])[0]
                        join_date = self.fake_kr.date_time_between(start_date='-5y', end_date='now')
                        
                        if random.random() < 0.95:
                            last_login = self.fake_kr.date_time_between(start_date=join_date, end_date='now')
                            login_count = random.randint(1, 1000)
                        else:
                            last_login = None
                            login_count = 0
                        
                        status = random.choices(['active', 'dormant', 'withdrawn'], weights=[80, 15, 5])[0]
                        regions = ['서울', '부산', '인천', '대구', '대전', '광주', '울산', '경기', '강원', '제주']
                        region = random.choice(regions)
                        
                        batch_data.append((
                            naver_id, email, self.fake_kr.phone_number(), name, birth_date, gender, 
                            join_date, last_login, login_count, status, region
                        ))
                    
                    await cursor.executemany("""
                        INSERT INTO naver_users 
                        (naver_id, email, phone, name, birth_date, gender, 
                         join_date, last_login, login_count, status, region)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, batch_data)
                    
                    generated_count += current_batch_size
                    
                    if generated_count % 5000 == 0:
                        progress = (generated_count / count) * 100
                        print(f"   진행률: {generated_count:,}/{count:,} ({progress:.0f}%)")
                
                await conn.commit()
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        rate = generated_count / duration if duration > 0 else 0
        
        print(f"✅ 사용자 데이터 생성 완료: {generated_count:,}명 ({rate:.1f}/초)")
        
        return GenerationStats(
            table_name="naver_users", target_count=count, generated_count=generated_count,
            start_time=start_time, end_time=end_time, duration_seconds=duration, generation_rate=rate
        )
    
    async def generate_blog_posts(self, count: int = 50000) -> GenerationStats:
        """네이버 블로그 포스트 생성"""
        print(f"📝 {count:,}개의 블로그 포스트 생성 중...")
        
        async with aiomysql.connect(**self.db_config) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT MIN(user_id), MAX(user_id) FROM naver_users WHERE status = 'active'")
                result = await cursor.fetchone()
                
                if result and result[0]:
                    min_user_id, max_user_id = result
                else:
                    raise Exception("활성 사용자가 없습니다.")
        
        start_time = datetime.now()
        generated_count = 0
        batch_size = 2000
        
        async with aiomysql.connect(**self.db_config) as conn:
            async with conn.cursor() as cursor:
                
                for i in range(0, count, batch_size):
                    batch_data = []
                    current_batch_size = min(batch_size, count - i)
                    
                    for j in range(current_batch_size):
                        user_id = random.randint(min_user_id, max_user_id)
                        
                        if random.random() < 0.7:
                            title = self.fake_kr.sentence(nb_words=random.randint(3, 12))[:-1]
                        else:
                            title = self.fake_en.sentence(nb_words=random.randint(3, 10))[:-1]
                        
                        category = random.choice(self.blog_categories)
                        
                        paragraph_count = random.randint(1, 5)
                        if random.random() < 0.7:
                            content = '\n\n'.join([self.fake_kr.paragraph() for _ in range(paragraph_count)])
                        else:
                            content = '\n\n'.join([self.fake_en.paragraph() for _ in range(paragraph_count)])
                        
                        popularity = random.choices([1, 2, 3, 4, 5], weights=[50, 25, 15, 8, 2])[0]
                        
                        if popularity == 1:
                            view_count = random.randint(0, 500)
                            like_count = random.randint(0, view_count // 20)
                        elif popularity == 2:
                            view_count = random.randint(500, 2000)
                            like_count = random.randint(view_count // 20, view_count // 10)
                        elif popularity == 3:
                            view_count = random.randint(2000, 10000)
                            like_count = random.randint(view_count // 15, view_count // 8)
                        elif popularity == 4:
                            view_count = random.randint(10000, 50000)
                            like_count = random.randint(view_count // 12, view_count // 6)
                        else:
                            view_count = random.randint(50000, 500000)
                            like_count = random.randint(view_count // 10, view_count // 5)
                        
                        comment_count = random.randint(0, like_count // 2)
                        share_count = random.randint(0, comment_count * 2)
                        is_public = random.choice([True, True, True, False])
                        created_at = self.fake_kr.date_time_between(start_date='-2y', end_date='now')
                        
                        batch_data.append((
                            user_id, title, content, category, view_count, like_count,
                            comment_count, share_count, is_public, created_at
                        ))
                    
                    await cursor.executemany("""
                        INSERT INTO blog_posts 
                        (user_id, title, content, category, view_count, like_count,
                         comment_count, share_count, is_public, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, batch_data)
                    
                    generated_count += current_batch_size
                    
                    if generated_count % 10000 == 0:
                        progress = (generated_count / count) * 100
                        print(f"   진행률: {generated_count:,}/{count:,} ({progress:.0f}%)")
                
                await conn.commit()
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        rate = generated_count / duration if duration > 0 else 0
        
        print(f"✅ 블로그 포스트 생성 완료: {generated_count:,}개 ({rate:.1f}/초)")
        
        return GenerationStats(
            table_name="blog_posts", target_count=count, generated_count=generated_count,
            start_time=start_time, end_time=end_time, duration_seconds=duration, generation_rate=rate
        )
    
    async def generate_search_logs(self, count: int = 200000) -> GenerationStats:
        """네이버 검색 로그 생성"""
        print(f"🔍 {count:,}개의 검색 로그 생성 중...")
        
        async with aiomysql.connect(**self.db_config) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT MIN(user_id), MAX(user_id) FROM naver_users")
                result = await cursor.fetchone()
                min_user_id, max_user_id = result if result else (1, 1000)
        
        start_time = datetime.now()
        generated_count = 0
        batch_size = 5000
        
        async with aiomysql.connect(**self.db_config) as conn:
            async with conn.cursor() as cursor:
                
                for i in range(0, count, batch_size):
                    batch_data = []
                    current_batch_size = min(batch_size, count - i)
                    
                    for j in range(current_batch_size):
                        user_id = random.randint(min_user_id, max_user_id) if random.random() < 0.4 else None
                        
                        if random.random() < 0.7:
                            keyword_count = random.choices([1, 2, 3], weights=[30, 50, 20])[0]
                            selected_keywords = random.sample(self.search_keywords, min(keyword_count, len(self.search_keywords)))
                            search_query = ' '.join(selected_keywords)
                        else:
                            search_query = random.choice(self.search_keywords)
                        
                        if random.random() < 0.1:
                            regions = ['강남', '홍대', '명동', '이태원', '신촌', '부산', '대구']
                            search_query += f' {random.choice(regions)}'
                        
                        search_type = random.choices(
                            ['web', 'image', 'news', 'shopping', 'blog', 'cafe', 'video'],
                            weights=[60, 15, 8, 7, 4, 3, 3]
                        )[0]
                        
                        if search_type == 'web':
                            results_count = random.randint(100, 50000)
                        elif search_type == 'shopping':
                            results_count = random.randint(10, 5000)
                        else:
                            results_count = random.randint(0, 10000)
                        
                        if results_count > 0 and random.random() < 0.8:
                            click_weights = [40, 20, 15, 10, 8, 4, 2, 1]
                            max_position = min(8, results_count)
                            click_position = random.choices(
                                range(1, max_position + 1), 
                                weights=click_weights[:max_position]
                            )[0]
                        else:
                            click_position = 0
                        
                        search_time = self.fake_kr.date_time_between(start_date='-1y', end_date='now')
                        ip_address = self.fake_kr.ipv4()
                        user_agent = self.fake_en.user_agent()
                        session_id = f"sess_{random.randint(100000, 999999)}_{int(time.time())}"
                        
                        batch_data.append((
                            user_id, search_query, search_type, results_count,
                            click_position, search_time, ip_address, user_agent, session_id
                        ))
                    
                    await cursor.executemany("""
                        INSERT INTO search_logs 
                        (user_id, search_query, search_type, results_count,
                         click_position, search_time, ip_address, user_agent, session_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, batch_data)
                    
                    generated_count += current_batch_size
                    
                    if generated_count % 50000 == 0:
                        progress = (generated_count / count) * 100
                        elapsed = (datetime.now() - start_time).total_seconds()
                        current_rate = generated_count / elapsed if elapsed > 0 else 0
                        print(f"   진행률: {generated_count:,}/{count:,} ({progress:.0f}%) - {current_rate:.0f}/초")
                
                await conn.commit()
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        rate = generated_count / duration if duration > 0 else 0
        
        print(f"✅ 검색 로그 생성 완료: {generated_count:,}개 ({rate:.1f}/초)")
        
        return GenerationStats(
            table_name="search_logs", target_count=count, generated_count=generated_count,
            start_time=start_time, end_time=end_time, duration_seconds=duration, generation_rate=rate
        )
    
    async def generate_shopping_orders(self, count: int = 80000) -> GenerationStats:
        """네이버 쇼핑 주문 데이터 생성"""
        print(f"🛒 {count:,}개의 쇼핑 주문 생성 중...")
        
        product_categories = {
            '패션의류': (10000, 200000),
            '화장품': (5000, 150000), 
            '식품': (3000, 50000),
            '생활용품': (2000, 100000),
            '가전제품': (50000, 500000),
            '도서': (5000, 50000),
            '스포츠': (20000, 300000),
            '디지털': (10000, 1000000)
        }
        
        async with aiomysql.connect(**self.db_config) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT MIN(user_id), MAX(user_id) FROM naver_users WHERE status = 'active'")
                result = await cursor.fetchone()
                min_user_id, max_user_id = result if result and result[0] else (1, 1000)
        
        start_time = datetime.now()
        generated_count = 0
        batch_size = 3000
        
        async with aiomysql.connect(**self.db_config) as conn:
            async with conn.cursor() as cursor:
                
                for i in range(0, count, batch_size):
                    batch_data = []
                    current_batch_size = min(batch_size, count - i)
                    
                    for j in range(current_batch_size):
                        user_id = random.randint(min_user_id, max_user_id)
                        
                        category = random.choice(list(product_categories.keys()))
                        price_range = product_categories[category]
                        
                        if category == '패션의류':
                            products = ['후드티', '청바지', '원피스', '코트', '스니커즈']
                            brands = ['나이키', '아디다스', '유니클로', 'H&M', '자라']
                        elif category == '화장품':
                            products = ['립스틱', '파운데이션', '아이섀도', '마스카라']
                            brands = ['이니스프리', '에뛰드', 'MAC', '랑콤']
                        elif category == '가전제품':
                            products = ['에어프라이어', '믹서기', '청소기', '공기청정기']
                            brands = ['삼성', 'LG', '다이슨', '필립스']
                        else:
                            products = ['상품A', '상품B', '상품C']
                            brands = ['브랜드A', '브랜드B', '브랜드C']
                        
                        product_name = f"{random.choice(brands)} {random.choice(products)}"
                        min_price, max_price = price_range
                        product_price = random.randint(min_price, max_price)
                        quantity = random.choices([1, 2, 3, 4, 5], weights=[60, 25, 8, 4, 3])[0]
                        total_amount = product_price * quantity
                        
                        order_status = random.choices(
                            ['delivered', 'shipped', 'paid', 'pending', 'cancelled'],
                            weights=[70, 15, 8, 4, 3]
                        )[0]
                        
                        payment_method = random.choices(
                            ['card', 'naver_pay', 'bank', 'payco'],
                            weights=[50, 25, 15, 10]
                        )[0]
                        
                        order_date = self.fake_kr.date_time_between(start_date='-1y', end_date='now')
                        
                        shipped_date = None
                        delivered_date = None
                        
                        if order_status in ['shipped', 'delivered']:
                            shipped_date = order_date + timedelta(days=random.randint(1, 3))
                            
                        if order_status == 'delivered':
                            delivered_date = shipped_date + timedelta(days=random.randint(1, 5))
                        
                        seller_id = f"seller_{random.randint(1000, 9999)}"
                        
                        batch_data.append((
                            user_id, product_name, category, product_price, quantity,
                            total_amount, order_status, payment_method, order_date,
                            shipped_date, delivered_date, seller_id
                        ))
                    
                    await cursor.executemany("""
                        INSERT INTO shopping_orders 
                        (user_id, product_name, product_category, product_price, quantity,
                         total_amount, order_status, payment_method, order_date,
                         shipped_date, delivered_date, seller_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, batch_data)
                    
                    generated_count += current_batch_size
                    
                    if generated_count % 20000 == 0:
                        progress = (generated_count / count) * 100
                        print(f"   진행률: {generated_count:,}/{count:,} ({progress:.0f}%)")
                
                await conn.commit()
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        rate = generated_count / duration if duration > 0 else 0
        
        print(f"✅ 쇼핑 주문 생성 완료: {generated_count:,}개 ({rate:.1f}/초)")
        
        return GenerationStats(
            table_name="shopping_orders", target_count=count, generated_count=generated_count,
            start_time=start_time, end_time=end_time, duration_seconds=duration, generation_rate=rate
        )
    
    async def generate_all_test_data(self, scale: str = 'small') -> Dict[str, GenerationStats]:
        """전체 테스트 데이터 생성"""
        
        scales = {
            'small': {'users': 10000, 'blog_posts': 50000, 'search_logs': 200000, 'shopping_orders': 80000},
            'medium': {'users': 100000, 'blog_posts': 500000, 'search_logs': 2000000, 'shopping_orders': 800000},
            'large': {'users': 1000000, 'blog_posts': 5000000, 'search_logs': 20000000, 'shopping_orders': 8000000}
        }
        
        if scale not in scales:
            scale = 'small'
        
        target_counts = scales[scale]
        print(f"🎯 {scale.upper()} 스케일 테스트 데이터 생성 시작")
        for key, value in target_counts.items():
            print(f"   {key}: {value:,}")
        
        total_start_time = datetime.now()
        results = {}
        
        try:
            await self.create_naver_service_tables()
            results['users'] = await self.generate_naver_users(target_counts['users'])
            results['blog_posts'] = await self.generate_blog_posts(target_counts['blog_posts'])
            results['search_logs'] = await self.generate_search_logs(target_counts['search_logs'])
            results['shopping_orders'] = await self.generate_shopping_orders(target_counts['shopping_orders'])
            
        except Exception as e:
            print(f"❌ 데이터 생성 중 오류 발생: {e}")
            return results
        
        total_end_time = datetime.now()
        total_duration = (total_end_time - total_start_time).total_seconds()
        total_records = sum(stats.generated_count for stats in results.values())
        total_rate = total_records / total_duration if total_duration > 0 else 0
        
        print(f"\n🎉 전체 데이터 생성 완료!")
        print(f"   총 레코드: {total_records:,}개")
        print(f"   총 소요 시간: {total_duration:.1f}초")
        print(f"   평균 생성률: {total_rate:.1f}개/초")
        
        print(f"\n📊 테이블별 생성 통계:")
        for table_name, stats in results.items():
            success_rate = (stats.generated_count / stats.target_count) * 100
            print(f"   {table_name}: {stats.generated_count:,}/{stats.target_count:,} ({success_rate:.1f}%) - {stats.generation_rate:.1f}/초")
        
        return results

async def quick_generate_test_data(db_config: Dict, scale: str = 'small'):
    """빠른 테스트 데이터 생성"""
    generator = NaverStyleDataGenerator(db_config)
    return await generator.generate_all_test_data(scale)

async def example_usage():
    """사용 예시"""
    db_config = {
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': 'testpass',
        'db': 'testdb'
    }
    
    print("=" * 60)
    print("네이버 스타일 테스트 데이터 생성기 예시")
    print("=" * 60)
    
    generator = NaverStyleDataGenerator(db_config)
    
    print(f"\n🎯 사용 가능한 스케일:")
    print(f"   small: 사용자 1만명 + 관련 데이터")
    print(f"   medium: 사용자 10만명 + 관련 데이터")  
    print(f"   large: 사용자 100만명 + 관련 데이터")
    
    # 샘플 데이터 생성
    results = await generator.generate_all_test_data(scale='small')
    
    print(f"\n✅ 예시 실행 완료!")
    return results

if __name__ == "__main__":
    asyncio.run(example_usage())