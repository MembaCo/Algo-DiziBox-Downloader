# @author: MembaCo.

import sqlite3
import time
import subprocess
import re
import os
import sys
import logging
import glob
import shutil
from seleniumwire import undetected_chromedriver as uc
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


def find_manifest_url(target_url, user_data_dir):
    """
    Manifest URL'sini bulur. Tarayıcıyı başlatırken belirtilen profil dizinini kullanır.
    """
    options = uc.ChromeOptions()
    options.add_argument("--window-size=1280,720")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--mute-audio")
    options.add_argument(f"user-agent={config.USER_AGENT}")
    options.add_argument("--disable-blink-features=AutomationControlled")

    driver = None
    try:
        logger.info(f"Belirtilen tarayıcı profili kullanılıyor: {user_data_dir}")
        driver = uc.Chrome(user_data_dir=user_data_dir, options=options)

        driver.execute_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )

        logger.info(f"Ana bölüm sayfasına gidiliyor: {target_url}")
        driver.get(target_url)
        wait = WebDriverWait(driver, 20)

        logger.info("Video oynatıcı iframe'i aranıyor...")
        player_iframe_element = wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "div.video-container iframe")
            )
        )
        iframe_url = player_iframe_element.get_attribute("src")

        if not iframe_url:
            logger.error("Video oynatıcı iframe'inin URL'si alınamadı.")
            return None

        driver.header_overrides = {"Referer": target_url}
        driver.get(iframe_url)

        logger.info(
            "Reklam engelleyici ekranının yüklenmesi için 5 saniye bekleniyor..."
        )
        time.sleep(5)

        try:
            driver.execute_script("""
                var deblockerWrapper = document.querySelector('.deblocker-wrapper');
                var deblockerBlackout = document.querySelector('.deblocker-blackout');
                if (deblockerWrapper) { deblockerWrapper.remove(); }
                if (deblockerBlackout) { deblockerBlackout.remove(); }
            """)
            logger.info("Deblocker ekranı başarıyla kaldırıldı.")
        except Exception as js_error:
            logger.warning(f"Deblocker ekranı kaldırılırken hata oluştu: {js_error}")

        logger.info(
            "Reklamın bitmesi ve asıl videonun başlaması için 25 saniye bekleniyor..."
        )
        time.sleep(25)

        requests = driver.requests
        ad_servers = ["video.twimg.com", "1king-dizibox.pages.dev"]
        manifest_url = None
        manifest_keywords = [".m3u8", "master.txt"]

        for req in reversed(requests):
            if any(keyword in req.url for keyword in manifest_keywords) and not any(
                server in req.url for server in ad_servers
            ):
                manifest_url = req.url
                logger.info(
                    f"Asıl video manifest/playlist'i başarıyla bulundu: {manifest_url}"
                )
                break

        if not manifest_url:
            raise TimeoutException("Asıl video manifest'i bulunamadı.")

        return manifest_url

    except TimeoutException:
        logger.warning(
            f"Manifest URL'si 30 saniye içinde ağ trafiğinde bulunamadı. URL: {target_url}"
        )
        return None
    except Exception as e:
        logger.error(
            f"Manifest URL'si aranırken beklenmedik bir hata oluştu: {e}", exc_info=True
        )
        return None
    finally:
        if driver:
            try:
                driver.quit()
            except OSError as e:
                logger.warning(f"Driver kapatılırken beklenen bir hata oluştu: {e}")


def download_with_yt_dlp(
    conn,
    item_id,
    item_type,
    manifest_url,
    user_data_dir,
    output_template,
    speed_limit,
):
    final_output_template = f"{output_template}.mp4"
    command = [
        "yt-dlp",
        "--newline",
        "--no-check-certificates",
        "--no-color",
        "--progress",
        "--verbose",
        "--hls-use-mpegts",
        "--merge-output-format",
        "mp4",
        f"--referer={manifest_url}",
        "-o",
        final_output_template,
    ]

    if speed_limit:
        command.extend(["--limit-rate", speed_limit])

    if user_data_dir:
        logger.info(f"yt-dlp için tarayıcı profili kullanılıyor: {user_data_dir}")
        # --- GÜNCELLEME: --user-data-dir ve yolu ayrı argümanlar olarak ekle ---
        command.extend(
            ["--cookies-from-browser", "chrome", "--user-data-dir", user_data_dir]
        )
        # --- GÜNCELLEME SONU ---

    command.append(manifest_url)

    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        encoding="utf-8",
        errors="ignore",
        preexec_fn=os.setsid if sys.platform != "win32" else None,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP
        if sys.platform == "win32"
        else 0,
    )
    full_output = ""
    for line in iter(process.stdout.readline, ""):
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
    if not logger.handlers:
        logger = setup_logging()
    conn = None
    profile_dir = None
    try:
        profile_dir = os.path.abspath(
            os.path.join("chrome_profiles", f"user_{item_id}")
        )
        if os.path.exists(profile_dir):
            shutil.rmtree(profile_dir)
        os.makedirs(profile_dir, exist_ok=True)

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
            "{series_title}/Season {season_number:02d}/{series_title} - S{season_number:02d}E{episode_number:02d} - {episode_title}",
        )

        path_string = filename_template.format(
            series_title=to_ascii_safe(item["series_title"]),
            season_number=item["season_number"],
            season_num=item["season_number"],
            episode_number=item["episode_number"],
            episode_num=item["episode_number"],
            episode_title=to_ascii_safe(item["title"] or ""),
        )

        final_path = os.path.join(base_download_folder, *path_string.split(os.path.sep))
        final_dir = os.path.dirname(final_path)
        safe_filename = os.path.basename(final_path)
        os.makedirs(final_dir, exist_ok=True)
        output_template = os.path.join(final_dir, safe_filename)

        _update_status_worker(conn, item_id, item_type, status="Kaynak aranıyor...")

        manifest_url = find_manifest_url(url_to_fetch, profile_dir)

        if manifest_url:
            _update_status_worker(conn, item_id, item_type, status="İndiriliyor")
            success, message = download_with_yt_dlp(
                conn,
                item_id,
                item_type,
                manifest_url,
                profile_dir,
                output_template,
                settings.get("SPEED_LIMIT"),
            )
            if success:
                final_filepath = f"{output_template}.mp4"
                if os.path.exists(final_filepath):
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
        if profile_dir and os.path.exists(profile_dir):
            try:
                shutil.rmtree(profile_dir)
                logger.info(f"Geçici profil '{profile_dir}' başarıyla silindi.")
            except Exception as e:
                logger.error(f"Geçici profil '{profile_dir}' silinirken hata: {e}")
