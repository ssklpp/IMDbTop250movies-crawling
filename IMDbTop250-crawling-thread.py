"""
IMDb Top 250 영화 병렬 크롤링 프로그램
동시성 처리를 통해 성능 개선한 버전
"""

# 필수 라이브러리 임포트
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager  # 크롬 드라이버 자동 관리
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from concurrent.futures import ThreadPoolExecutor, as_completed  # 병렬 처리용
import csv
import time


# ---------------------------
# 크롬 드라이버 생성 함수 (각 스레드별 독립 인스턴스)
# ---------------------------
def create_driver():
    """개별 스레드 전용 크롬 드라이버 생성"""
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")  # 헤드리스 모드 활성화
    options.add_argument("--window-size=1920,1080")  # 가상 화면 크기 설정
    options.add_argument(
        "--disable-blink-features=AutomationControlled"
    )  # 자동화 탐지 방지
    options.add_experimental_option(
        "excludeSwitches", ["enable-automation"]
    )  # 자동화 메시지 제거
    options.add_experimental_option(
        "useAutomationExtension", False
    )  # 확장 프로그램 비활성화
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    )  # 실제 유저 에이전트 설정

    service = Service(ChromeDriverManager().install())  # 크롬 드라이버 자동 설치
    return webdriver.Chrome(service=service, options=options)


# ---------------------------
# 영화 상세 정보 추출 함수
# ---------------------------
def extract_movie_details(movie_link, rank):
    """개별 영화 상세 페이지 크롤링 함수"""
    driver = create_driver()  # 새 드라이버 인스턴스 생성
    try:
        driver.get(movie_link)

        # 평점 요소가 로드될 때까지 최대 10초 대기
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (
                    By.CSS_SELECTOR,
                    'div[data-testid="hero-rating-bar__aggregate-rating__score"]',
                )
            )
        )

        # 기본 정보 추출
        title = driver.find_element(By.TAG_NAME, "h1").text  # 영화 제목
        score = driver.find_element(
            By.CSS_SELECTOR,
            'div[data-testid="hero-rating-bar__aggregate-rating__score"] span',
        ).text  # 평점

        # 제작진 정보 초기화
        director, writers, stars = "N/A", "N/A", "N/A"

        # 제작진 정보가 포함된 섹션 탐색
        credit_sections = driver.find_elements(
            By.CSS_SELECTOR, "li.ipc-metadata-list__item"
        )
        for section in credit_sections:
            try:
                label = section.find_element(
                    By.CSS_SELECTOR, ".ipc-metadata-list-item__label"
                ).text
                people = [
                    p.text
                    for p in section.find_elements(
                        By.CSS_SELECTOR,
                        "a.ipc-metadata-list-item__list-content-item--link",
                    )[
                        :3
                    ]  # 최대 3명만 추출
                ]

                # 라벨에 따른 정보 분류
                if "Director" in label:
                    director = ", ".join(people)
                elif "Writer" in label:
                    writers = ", ".join(people)
                elif "Stars" in label:
                    stars = ", ".join(people)
            except:
                continue  # 요소 찾기 실패 시 계속 진행

        return {
            "Rank": rank,
            "Title": title,
            "Score": score,
            "Director": director,
            "Writers": writers,
            "Stars": stars,
        }

    except Exception as e:
        print(f"[{rank}] 크롤링 실패: {str(e)}")
        return None
    finally:
        driver.quit()  # 리소스 정리


# ---------------------------
# 메인 크롤링 함수
# ---------------------------
def main():
    """메인 실행 함수"""
    # 1. 메인 페이지에서 영화 링크 수집
    main_driver = create_driver()
    main_driver.get("https://www.imdb.com/chart/top/")

    # 영화 리스트 로딩 대기
    WebDriverWait(main_driver, 5).until(
        EC.presence_of_element_located(
            (By.CSS_SELECTOR, "ul.ipc-metadata-list li.ipc-metadata-list-summary-item")
        )
    )

    # 상위 250개 영화 선택
    movies = main_driver.find_elements(
        By.CSS_SELECTOR, "li.ipc-metadata-list-summary-item"
    )[:250]

    # (링크, 순위) 튜플 리스트 생성
    movie_links = [
        (
            movie.find_element(
                By.CSS_SELECTOR, "a.ipc-title-link-wrapper"
            ).get_attribute("href"),
            idx + 1,
        )
        for idx, movie in enumerate(movies)
    ]

    main_driver.quit()  # 메인 드라이버 종료
    print(f"총 {len(movie_links)}개 영화 링크 수집 완료")

    # 2. 병렬 처리 실행 (최대 5개 스레드)
    results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        # 모든 영화에 대해 비동기 작업 제출
        futures = [
            executor.submit(extract_movie_details, link, rank)
            for link, rank in movie_links
        ]

        # 완료된 작업부터 결과 수집
        for future in as_completed(futures):
            result = future.result()
            if result:
                results.append(result)
                print(f"[{result['Rank']}] {result['Title']} - 처리 완료")

    # 3. CSV 파일 저장
    with open("imdb_top250_parallel_1.csv", "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f, fieldnames=["Rank", "Title", "Director", "Writers", "Stars", "Score"]
        )
        writer.writeheader()  # 헤더 작성
        writer.writerows(
            sorted(results, key=lambda x: x["Rank"])
        )  # 순위 기준 정렬 후 저장
    print("CSV 저장 완료")


# ---------------------------
# 프로그램 실행
# ---------------------------
if __name__ == "__main__":
    start_time = time.time()  # 실행 시간 측정 시작
    main()
    print(f"총 소요 시간: {time.time() - start_time:.2f}초")  # 전체 실행 시간 출력
