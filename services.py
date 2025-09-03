# @author: MembaCo.

import logging
import json
import os
import re
import signal
import subprocess
import sys
import threading
from multiprocessing import Process
import time

from bs4 import BeautifulSoup
import undetected_chromedriver as uc
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import config
from database import get_db, get_setting
from worker import process_video

logger = logging.getLogger(__name__)


def get_page_source_with_selenium(url):
    options = uc.ChromeOptions()
    options.add_argument("--window-size=1280,720")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    # --- EKLENEN SATIR ---
    options.add_argument("--ignore-certificate-errors")

    driver = None
    try:
        driver = uc.Chrome(options=options)
        driver.get(url)
        wait = WebDriverWait(driver, 60)
        logger.info(
            "Ana içerik konteynerinin (icerikcat) HTML'de var olması bekleniyor..."
        )
        wait.until(EC.presence_of_element_located((By.ID, "icerikcat")))
        logger.info("Ana içerik konteyneri başarıyla bulundu. Sayfa kaynağı alınıyor.")
        html = driver.page_source
        return html, None
    except TimeoutException:
        error_message = (
            f"Zaman aşımı: Ana içerik 60 saniye içinde bulunamadı. URL: {url}"
        )
        logger.error(error_message)
        if driver:
            try:
                html_source = driver.page_source
                error_filename = "error_page_source.html"
                with open(error_filename, "w", encoding="utf-8") as f:
                    f.write(html_source)
                logger.info(
                    f"Sayfanın mevcut durumu '{error_filename}' dosyasına kaydedildi."
                )
            except Exception as e:
                logger.error(f"Hata sayfası kaydedilirken bir sorun oluştu: {e}")
        return None, error_message
    except Exception as e:
        error_message = f"Undetected Chromedriver ile sayfa kaynağı alınırken hata: {e}"
        logger.error(error_message, exc_info=True)
        return None, error_message
    finally:
        if driver:
            driver.quit()


# ... (dosyanın geri kalanını değiştirmeyin) ...
def scrape_series_data(series_url):
    logger.info(f"Dizi verisi çekiliyor (Undetected Chrome ile): {series_url}")
    html_content, error = get_page_source_with_selenium(series_url)
    if error or not html_content:
        return None
    try:
        soup = BeautifulSoup(html_content, "html.parser")

        og_title = soup.find("meta", property="og:title")
        title = (
            og_title["content"].split("İzle")[0].strip()
            if og_title
            else "Başlık Bulunamadı"
        )

        poster_element = soup.select_one("div.category_image img")
        description_element = soup.select_one("div.category_desc")

        if not all([title, poster_element, description_element]):
            logger.error(
                f"HTML ayrıştırılırken dizi ana bilgileri bulunamadı. URL: {series_url}"
            )
            return None

        series_info = {
            "title": title,
            "poster_url": poster_element.get("src"),
            "description": description_element.text.strip(),
            "source_url": series_url,
            "seasons": [],
        }

        season_buttons = soup.select("div#butonlar button.btn")
        if not season_buttons:
            logger.warning(f"Dizi için sezon bilgisi bulunamadı. URL: {series_url}")
        else:
            season_text = season_buttons[0].text.strip()
            season_number_match = re.search(r"(\d+)", season_text)
            season_number = (
                int(season_number_match.group(1)) if season_number_match else 1
            )

            season_data = {"season_number": season_number, "episodes": []}

            episode_list = soup.select("div.bolumust")
            for item in episode_list:
                link_tag = item.find("a")
                if not link_tag:
                    continue

                episode_url = link_tag.get("href")
                baslik_div = item.select_one("div.baslik")
                full_title_text = (
                    " ".join(baslik_div.text.split()) if baslik_div else ""
                )

                match = re.search(r"(\d+)\.\s*Sezon\s*(\d+)\.\s*Bölüm", full_title_text)
                if match:
                    episode_number = int(match.group(2))
                    episode_title_raw = baslik_div.select_one("div.bolumismi")
                    episode_title = (
                        episode_title_raw.text.strip("()")
                        if episode_title_raw
                        else f"{episode_number}. Bölüm"
                    )
                else:
                    continue

                season_data["episodes"].append(
                    {
                        "episode_number": episode_number,
                        "title": episode_title,
                        "url": episode_url,
                    }
                )

            season_data["episodes"].sort(key=lambda x: x["episode_number"])
            series_info["seasons"].append(season_data)

        logger.info(
            f"'{series_info['title']}' dizisi için {len(series_info['seasons'])} sezon ve {sum(len(s['episodes']) for s in series_info['seasons'])} bölüm bulundu."
        )
        return series_info
    except Exception as e:
        logger.error(f"HTML ayrıştırılırken hata: {e}", exc_info=True)
        return None


def add_series_to_queue(series_url):
    db = get_db()
    series_data = scrape_series_data(series_url)
    if not series_data:
        return (
            False,
            "Dizi bilgileri çekilemedi. Linki kontrol edin veya site yapısı değişmiş olabilir.",
        )
    cursor = db.cursor()
    cursor.execute(
        "SELECT id FROM series WHERE source_url = ?", (series_data["source_url"],)
    )
    series_row = cursor.fetchone()
    if not series_row:
        cursor.execute(
            "INSERT INTO series (title, poster_url, description, source_url) VALUES (?, ?, ?, ?)",
            (
                series_data["title"],
                series_data["poster_url"],
                series_data["description"],
                series_data["source_url"],
            ),
        )
        series_id = cursor.lastrowid
    else:
        series_id = series_row["id"]
    added_count = 0
    for season in series_data["seasons"]:
        cursor.execute(
            "SELECT id FROM seasons WHERE series_id = ? AND season_number = ?",
            (series_id, season["season_number"]),
        )
        season_row = cursor.fetchone()
        if not season_row:
            cursor.execute(
                "INSERT INTO seasons (series_id, season_number) VALUES (?, ?)",
                (series_id, season["season_number"]),
            )
            season_id = cursor.lastrowid
        else:
            season_id = season_row["id"]
        for episode in season["episodes"]:
            res = cursor.execute(
                "INSERT OR IGNORE INTO episodes (season_id, episode_number, title, url) VALUES (?, ?, ?, ?)",
                (
                    season_id,
                    episode["episode_number"],
                    episode["title"],
                    episode["url"],
                ),
            )
            if res.rowcount > 0:
                added_count += 1
    db.commit()
    return (
        True,
        f'"{series_data["title"]}" dizisi için {added_count} yeni bölüm sıraya eklendi.',
    )


def add_series_to_queue_async(app, series_url):
    with app.app_context():
        success, message = add_series_to_queue(series_url)
        if not success:
            logger.error(f"Dizi ekleme hatası ({series_url}): {message}")
        else:
            logger.info(message)


def start_download(item_id, item_type, active_processes):
    db = get_db()
    table = "episodes"
    item = db.execute(f"SELECT * FROM {table} WHERE id = ?", (item_id,)).fetchone()
    if not item:
        return False, "Kayıt bulunamadı."
    if item["status"] in ["Kaynak aranıyor...", "İndiriliyor"]:
        return False, "Bu indirme zaten devam ediyor."
    p = Process(target=process_video, args=(item_id, item_type))
    p.start()
    pid = p.pid
    active_processes[pid] = p
    db.execute(
        f"UPDATE {table} SET status = ?, pid = ?, progress = 0, filepath = NULL WHERE id = ?",
        ("Kaynak aranıyor...", pid, item_id),
    )
    db.commit()
    title = item["title"] if item["title"] else f"Bölüm {item['episode_number']}"
    logger.info(f"ID {item_id} ('{title}') için indirme başlatıldı. PID: {pid}")
    return True, f'"{title}" için indirme başlatıldı.'


def stop_download(item_id, item_type):
    db = get_db()
    table = "episodes"
    item = db.execute(f"SELECT * FROM {table} WHERE id = ?", (item_id,)).fetchone()
    if not (item and item["pid"]):
        return False, "Durdurulacak bir işlem bulunamadı."
    pid = item["pid"]
    try:
        if sys.platform != "win32":
            os.killpg(os.getpgid(pid), signal.SIGTERM)
        else:
            subprocess.run(
                ["taskkill", "/F", "/T", "/PID", str(pid)],
                check=True,
                capture_output=True,
            )
        message = "İndirme durdurma isteği gönderildi."
    except (ProcessLookupError, subprocess.CalledProcessError):
        message = "İşlem zaten sonlanmış."
    except OSError as e:
        message = f"İşlem durdurulurken bir hata oluştu: {e}"
    db.execute(
        f"UPDATE {table} SET status = 'Duraklatıldı', pid = NULL WHERE id = ?",
        (item_id,),
    )
    db.commit()
    return True, message


def delete_record(item_id, item_type, active_processes):
    db = get_db()
    table = "episodes"
    item = db.execute(f"SELECT * FROM {table} WHERE id = ?", (item_id,)).fetchone()
    if item and item["pid"]:
        pid = item["pid"]
        stop_download(item_id, item_type)
        if pid in active_processes:
            del active_processes[pid]
    db.execute(f"DELETE FROM {table} WHERE id = ?", (item_id,))
    db.commit()
    return True, "Kayıt başarıyla silindi."


def delete_series_record(series_id, active_processes):
    db = get_db()
    series = db.execute(
        "SELECT title FROM series WHERE id = ?", (series_id,)
    ).fetchone()
    if not series:
        return False, "Silinecek dizi bulunamadı."

    episodes_to_stop = db.execute(
        "SELECT e.id, e.pid FROM episodes e JOIN seasons s ON e.season_id = s.id WHERE s.series_id = ? AND e.pid IS NOT NULL",
        (series_id,),
    ).fetchall()

    for episode in episodes_to_stop:
        pid = episode["pid"]
        stop_download(episode["id"], "episode")
        if pid in active_processes:
            del active_processes[pid]

    cursor = db.cursor()
    cursor.execute("DELETE FROM series WHERE id = ?", (series_id,))
    db.commit()
    logger.info(f"'{series['title']}' dizisi ve tüm bölümleri başarıyla silindi.")
    return True, f"'{series['title']}' dizisi başarıyla silindi."


def start_all_episodes_for_series(series_id):
    db = get_db()
    episodes_to_queue = db.execute(
        "SELECT e.id FROM episodes e JOIN seasons s ON e.season_id = s.id WHERE s.series_id = ? AND e.status NOT IN ('Tamamlandı', 'İndiriliyor', 'Kaynak aranıyor...')",
        (series_id,),
    ).fetchall()
    if not episodes_to_queue:
        return False, "Sıraya eklenecek yeni bölüm bulunamadı."
    count = 0
    for episode in episodes_to_queue:
        db.execute(
            "UPDATE episodes SET status = 'Sırada' WHERE id = ?", (episode["id"],)
        )
        count += 1
    db.commit()
    series_title = db.execute(
        "SELECT title FROM series WHERE id = ?", (series_id,)
    ).fetchone()["title"]
    logger.info(f"'{series_title}' dizisi için {count} bölüm indirme sırasına alındı.")
    return True, f"'{series_title}' dizisi için {count} bölüm indirme sırasına alındı."


def delete_item_file(item_id, item_type):
    db = get_db()
    table = "episodes"
    item = db.execute(f"SELECT * FROM {table} WHERE id = ?", (item_id,)).fetchone()
    if not item:
        return False, "Kayıt bulunamadı."
    filepath = item["filepath"]
    if filepath and os.path.exists(filepath):
        try:
            os.remove(filepath)
            db.execute(f"UPDATE {table} SET filepath = NULL WHERE id = ?", (item_id,))
            db.commit()
            logger.info(f"Dosya diskten silindi: {filepath}")
            return True, f'"{os.path.basename(filepath)}" diskten başarıyla silindi.'
        except OSError:
            logger.error(f"Dosya silinemedi: {filepath}", exc_info=True)
            return False, "Dosya silinirken bir hata oluştu."
    else:
        db.execute(f"UPDATE {table} SET filepath = NULL WHERE id = ?", (item_id,))
        db.commit()
        return False, "Silinecek dosya bulunamadı veya zaten silinmiş."


def run_auto_download_cycle(active_processes):
    db = get_db()
    try:
        concurrent_limit = int(get_setting("CONCURRENT_DOWNLOADS", db))
    except (ValueError, TypeError):
        concurrent_limit = 1
    for pid, process in list(active_processes.items()):
        if not process.is_alive():
            del active_processes[pid]
            logger.info(
                f"Otomatik yönetici: Tamamlanmış proses (PID: {pid}) temizlendi."
            )
    while len(active_processes) < concurrent_limit:
        next_item = db.execute(
            "SELECT id, 'episode' as type, created_at FROM episodes WHERE status = 'Sırada' ORDER BY created_at ASC LIMIT 1"
        ).fetchone()
        if not next_item:
            break
        item_id = next_item["id"]
        item_type = next_item["type"]
        logger.info(
            f"[Auto-Download] Sırada bekleyen bulundu ({item_type} ID: {item_id}). İndirme başlatılıyor."
        )
        start_download(item_id, item_type, active_processes)


def get_all_movies_status():
    return {}


def get_all_series_status():
    db = get_db()
    series_list = db.execute("SELECT * FROM series ORDER BY title ASC").fetchall()
    series_data = []
    for s in series_list:
        series_dict = dict(s)
        seasons = db.execute(
            "SELECT * FROM seasons WHERE series_id = ? ORDER BY season_number ASC",
            (s["id"],),
        ).fetchall()
        series_dict["seasons"] = []
        for season in seasons:
            season_dict = dict(season)
            episodes = db.execute(
                "SELECT * FROM episodes WHERE season_id = ? ORDER BY episode_number ASC",
                (season["id"],),
            ).fetchall()
            season_dict["episodes"] = [dict(ep) for ep in episodes]
            series_dict["seasons"].append(season_dict)
        series_data.append(series_dict)
    return series_data
