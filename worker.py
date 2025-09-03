# @author: MembaCo.

import sqlite3
import time
import subprocess
import re
import os
import sys
import logging
import glob
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

import config
from logging_config import setup_logging
from database import get_all_settings as get_all_settings_from_db

logger = logging.getLogger(__name__)


def _update_status_worker(
    conn, item_id, item_type, status=None, source_url=None, progress=None, filepath=None
):
    table = "episodes"
    try:
        cursor = conn.cursor()
        if status:
            cursor.execute(
                f"UPDATE {table} SET status = ? WHERE id = ?", (status, item_id)
            )
        if source_url:
            cursor.execute(
                f"UPDATE {table} SET source_url = ? WHERE id = ?", (source_url, item_id)
            )
        if progress is not None:
            cursor.execute(
                f"UPDATE {table} SET progress = ? WHERE id = ?", (progress, item_id)
            )
        if filepath is not None:
            cursor.execute(
                f"UPDATE {table} SET filepath = ? WHERE id = ?", (filepath, item_id)
            )
        conn.commit()
    except sqlite3.Error as e:
        logger.error(
            f"ID {item_id} ({item_type}) için worker DB güncellemesinde hata: {e}",
            exc_info=True,
        )


def find_manifest_url(target_url):
    """Tespit edilemeyen chromedriver ile manifest URL'sini bulur."""
    options = uc.ChromeOptions()
    # options.add_argument("--headless")
    options.add_argument(f"user-agent={config.USER_AGENT}")
    options.add_argument("--window-size=1280,720")

    driver = None
    try:
        # --- KİLİT DEĞİŞİKLİK ---
        # version_main parametresini kaldırarak kütüphanenin
        # doğru sürücüyü otomatik bulmasını sağlıyoruz.
        driver = uc.Chrome(options=options)

        wait = WebDriverWait(driver, 30)
        logger.info(f"Bölüm sayfasına gidiliyor: {target_url}")
        driver.get(target_url)

        logger.info("İlk iframe'in var olması bekleniyor...")
        iframe_locator_1 = (By.CSS_SELECTOR, "div.player-content > iframe")
        wait.until(EC.presence_of_element_located(iframe_locator_1))
        driver.switch_to.frame(driver.find_element(*iframe_locator_1))

        logger.info("İkinci iframe'in var olması bekleniyor...")
        iframe_locator_2 = (By.TAG_NAME, "iframe")
        wait.until(EC.presence_of_element_located(iframe_locator_2))
        driver.switch_to.frame(driver.find_element(*iframe_locator_2))

        time.sleep(5)

        page_source = driver.page_source
        manifest_match = re.search(
            r'file:"(https?://[^\s"]+\.m3u8[^\s"]*)"', page_source
        )

        if not manifest_match:
            logger.warning("Manifest URL'si sayfa kaynağında bulunamadı.")
            raise TimeoutException

        manifest_url = manifest_match.group(1).replace("\\", "")
        logger.info(f"Manifest URL'si bulundu: {manifest_url}")

        return manifest_url, {}, []

    except TimeoutException:
        logger.warning(f"Manifest URL'si bulunamadı. URL: {target_url}")
        return None, None, None
    finally:
        if driver:
            driver.quit()


def download_with_yt_dlp(
    conn,
    item_id,
    item_type,
    manifest_url,
    headers,
    cookie_filepath,
    output_template,
    speed_limit,
):
    command = [
        "yt-dlp",
        "--newline",
        "--no-check-certificates",
        "--no-color",
        "--progress",
        "--verbose",
        "--hls-use-mpegts",
        "-o",
        f"{output_template}.%(ext)s",
    ]
    if speed_limit:
        command.extend(["--limit-rate", speed_limit])
    command.append(manifest_url)

    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        preexec_fn=os.setsid if sys.platform != "win32" else None,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP
        if sys.platform == "win32"
        else 0,
    )
    full_output = ""
    for line_bytes in iter(process.stdout.readline, b""):
        line = line_bytes.decode("utf-8", errors="ignore")
        full_output += line
        progress_match = re.search(r"\[download\]\s+([0-9\.]+)%", line)
        if progress_match:
            try:
                progress = float(progress_match.group(1))
                _update_status_worker(conn, item_id, item_type, progress=progress)
            except (ValueError, IndexError):
                continue
    process.wait()

    if process.returncode == 0:
        return True, "İndirme tamamlandı."
    else:
        if "403 Forbidden" in full_output:
            error_message = "Hata: Sunucu erişimi reddetti (403)."
        elif "No space left on device" in full_output:
            error_message = "Hata: Diskte yeterli alan yok."
        elif "HTTP Error 404" in full_output:
            error_message = "Hata: Video kaynağı bulunamadı (404)."
        else:
            last_lines = "\n".join(full_output.strip().split("\n")[-5:])
            error_message = f"Hata: İndirme başarısız oldu. Detay: ...{last_lines}"
        return False, error_message


def to_ascii_safe(text):
    text = (
        str(text)
        .replace("ı", "i")
        .replace("İ", "I")
        .replace("ğ", "g")
        .replace("Ğ", "G")
        .replace("ü", "u")
        .replace("Ü", "U")
        .replace("ş", "s")
        .replace("Ş", "S")
        .replace("ö", "o")
        .replace("Ö", "O")
        .replace("ç", "c")
        .replace("Ç", "C")
    )
    text = re.sub(r'[<>:"/\\|?*]', "_", text).strip()
    text = re.sub(r"[^\x00-\x7F]+", "", text)
    return text


def process_video(item_id, item_type):
    global logger
    logger = setup_logging()
    conn = None
    cookie_filepath = f"cookies_{item_id}_{item_type}.txt"
    try:
        conn = sqlite3.connect(config.DATABASE)
        conn.row_factory = sqlite3.Row
        settings = get_all_settings_from_db(conn)
        base_download_folder = settings.get("DOWNLOADS_FOLDER", "downloads")
        item = conn.execute(
            "SELECT e.*, s.season_number, ser.title as series_title FROM episodes e JOIN seasons s ON e.season_id = s.id JOIN series ser ON s.series_id = ser.id WHERE e.id = ?",
            (item_id,),
        ).fetchone()
        if not item:
            return
        url_to_fetch = item["url"]
        filename_template = settings.get(
            "SERIES_FILENAME_TEMPLATE",
            "{series_title}/S{season_number:02d}E{episode_number:02d} - {episode_title}",
        )
        path_string = filename_template.format(
            series_title=to_ascii_safe(item["series_title"]),
            season_number=item["season_number"],
            episode_number=item["episode_number"],
            episode_title=to_ascii_safe(item["title"] or ""),
        )
        final_path = os.path.join(base_download_folder, *path_string.split(os.path.sep))
        final_dir = os.path.dirname(final_path)
        safe_filename = os.path.basename(final_path)
        os.makedirs(final_dir, exist_ok=True)
        output_template = os.path.join(final_dir, safe_filename)

        _update_status_worker(conn, item_id, item_type, status="Kaynak aranıyor...")
        manifest_url, headers, cookies = find_manifest_url(url_to_fetch)

        if manifest_url:
            _update_status_worker(conn, item_id, item_type, status="İndiriliyor")
            success, message = download_with_yt_dlp(
                conn,
                item_id,
                item_type,
                manifest_url,
                headers,
                cookie_filepath,
                output_template,
                settings.get("SPEED_LIMIT"),
            )
            if success:
                files = glob.glob(f"{output_template}.*")
                if files:
                    final_filepath = files[0]
                    _update_status_worker(
                        conn,
                        item_id,
                        item_type,
                        status="Tamamlandı",
                        progress=100,
                        filepath=final_filepath,
                    )
                    logger.info(
                        f"ID {item_id} ({item_type}): İndirme başarıyla tamamlandı. Dosya: {final_filepath}"
                    )
                else:
                    _update_status_worker(
                        conn,
                        item_id,
                        item_type,
                        status="Hata: İndirilen dosya bulunamadı",
                    )
            else:
                _update_status_worker(conn, item_id, item_type, status=message)
                logger.error(f"ID {item_id} ({item_type}): İndirme hatası - {message}")
        else:
            _update_status_worker(
                conn, item_id, item_type, status="Hata: Video kaynağı bulunamadı"
            )
            logger.warning(f"ID {item_id} ({item_type}): Manifest URL bulunamadı.")
    except Exception as e:
        logger.exception(
            f"ID {item_id} ({item_type}): process_video içinde beklenmedik hata: {e}"
        )
        if conn:
            _update_status_worker(
                conn, item_id, item_type, status="Hata: Beklenmedik Sistem Hatası"
            )
    finally:
        if conn:
            conn.close()
        if os.path.exists(cookie_filepath):
            os.remove(cookie_filepath)
