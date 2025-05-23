"""
IMDb Top 250 영화 크롤링 프로그램
Selenium을 이용한 동적 웹 크롤링 예제
"""

# 필요한 라이브러리 임포트
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager  # 크롬 드라이버 자동 관리
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
import time
import csv
import random  # 랜덤 대기 시간 생성용

# 크롬 브라우저 옵션 설정 (헤드리스 모드 최적화)
options = webdriver.ChromeOptions()
options.add_argument("--headless=new")  # 새로운 헤드리스 모드 활성화
options.add_argument("--window-size=1920,1080")  # 가상 화면 크기 설정
options.add_argument(
    "--disable-blink-features=AutomationControlled"
)  # 자동화 탐지 방지
options.add_experimental_option(
    "excludeSwitches", ["enable-automation"]
)  # 자동화 메시지 제거
options.add_experimental_option(
    "useAutomationExtension", False
)  # 확장 프로그램 사용 안함
user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
options.add_argument(f"user-agent={user_agent}")  # 실제 브라우저 같은 User-Agent 설정

# 웹드라이버 설정 및 실행
service = Service(ChromeDriverManager().install())  # 크롬 드라이버 자동 설치
browser = webdriver.Chrome(service=service, options=options)


def get_imdb_top250():
    """IMDb Top 250 영화 정보 크롤링 메인 함수"""
    browser.get("https://www.imdb.com/chart/top/")

    # 1. 초기 페이지 로딩 대기 (메인 컨텐츠가 로드될 때까지 최대 5초 대기)
    WebDriverWait(browser, 5).until(
        EC.presence_of_element_located((By.TAG_NAME, "main"))
    )

    # 2. 동적 콘텐츠 로딩을 위한 스크롤 시뮬레이션 (3회 수행)
    for _ in range(3):
        # 페이지 하단으로 스크롤
        browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1)
        # 페이지 상단으로 스크롤 (스크롤 위치 초기화)
        browser.execute_script("window.scrollTo(0, 0);")
        time.sleep(0.5)

    # 3. 영화 리스트 요소 수집 (CSS 선택자로 최신 요소 구조 사용)
    movies = WebDriverWait(browser, 3).until(
        EC.presence_of_all_elements_located(
            (By.CSS_SELECTOR, "ul.ipc-metadata-list li.ipc-metadata-list-summary-item")
        )
    )
    print(f"■ 발견된 영화 수: {len(movies)}")

    movie_data = []  # 영화 정보 저장 리스트

    # 4. 각 영화 상세 정보 수집 (최대 250개 처리)
    for index, movie in enumerate(movies[:250]):
        rank = index + 1  # 순위 계산

        try:
            # 5. 영화 상세 페이지 링크 추출 및 새 탭에서 열기
            movie_link = movie.find_element(
                By.CSS_SELECTOR, "a.ipc-title-link-wrapper"
            ).get_attribute("href")
            browser.execute_script(f"window.open('{movie_link}');")
            browser.switch_to.window(browser.window_handles[1])  # 새 탭으로 전환

            # 6. 상세 페이지 요소 로딩 대기 (평점 요소가 나타날 때까지 최대 3초 대기)
            WebDriverWait(browser, 3).until(
                EC.presence_of_element_located(
                    (
                        By.CSS_SELECTOR,
                        'div[data-testid="hero-rating-bar__aggregate-rating__score"]',
                    )
                )
            )

            # 7. 기본 정보 추출
            title = browser.find_element(By.TAG_NAME, "h1").text  # 영화 제목
            score = browser.find_element(
                By.CSS_SELECTOR,
                'div[data-testid="hero-rating-bar__aggregate-rating__score"] span',
            ).text  # 평점

            # 8. 부가 정보 추출 (감독/작가/주연)
            director, writers, stars = extract_credits(browser)

            # 9. 데이터 저장
            movie_data.append(
                {
                    "Rank": rank,
                    "Title": title,
                    "Director": director,
                    "Writers": writers,
                    "Stars": stars,
                    "Score": score,
                }
            )
            print(f"[{rank}] {title} - 크롤링 성공")

        except Exception as e:
            print(f"[{rank}] 오류 발생: {str(e)}")
        finally:
            # 10. 리소스 정리 및 대기
            browser.close()  # 현재 탭 닫기
            browser.switch_to.window(browser.window_handles[0])  # 원래 탭으로 복귀
            time.sleep(random.uniform(1, 3))  # 랜덤 대기 시간 (1~3초)

    # 11. CSV 파일 저장 및 브라우저 종료
    save_to_csv(movie_data)
    browser.quit()  # 브라우저 완전 종료


def extract_credits(driver):
    """영화 제작진 정보 추출 함수"""
    director = "N/A"
    writers = "N/A"
    stars = "N/A"

    try:
        # 1. 정보가 포함된 섹션 요소 찾기
        sections = driver.find_elements(By.CSS_SELECTOR, "li.ipc-metadata-list__item")

        for section in sections:
            try:
                # 2. 라벨 확인 (감독/작가/주연 구분)
                label = section.find_element(
                    By.CSS_SELECTOR, ".ipc-metadata-list-item__label"
                ).text.strip()
                # 3. 관련 인물 이름 추출 (최대 3명만 처리)
                people = section.find_elements(
                    By.CSS_SELECTOR, "a.ipc-metadata-list-item__list-content-item--link"
                )
                names = [p.text.strip() for p in people[:3]]  # 3명만 추출

                # 4. 정보 분류
                if "Director" in label:
                    director = ", ".join(names)
                elif "Writer" in label:
                    writers = ", ".join(names)
                elif "Stars" in label:
                    stars = ", ".join(names)
            except:
                continue  # 요소 찾기 실패 시 다음 섹션으로
    except:
        pass  # 전체 섹션 찾기 실패 시 무시

    return director, writers, stars


def save_to_csv(data):
    """CSV 파일 저장 함수"""
    with open("imdb_top250_crawling.csv", "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f, fieldnames=["Rank", "Title", "Director", "Writers", "Stars", "Score"]
        )
        writer.writeheader()  # 헤더 작성
        writer.writerows(data)  # 데이터 일괄 저장
    print("CSV 저장 완료!")


if __name__ == "__main__":
    get_imdb_top250()
