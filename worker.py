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
import base64
from hashlib import md5

from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad

from seleniumwire import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

import config
from logging_config import setup_logging
from database import get_all_settings as get_all_settings_from_db

logger = logging.getLogger(__name__)


# --- ŞİFRE ÇÖZME FONKSİYONLARI (dizibox.py'dan adapte edildi) ---
def bytes_to_key(data, salt, output=48):
    data += salt
    key = md5(data).digest()
    final_key = key
    while len(final_key) < output:
        key = md5(key + data).digest()
        final_key += key
    return final_key[:output]


def decrypt_aes(encrypted_data, password):
    """AES şifreli veriyi çözer."""
    try:
        encrypted_data_bytes = base64.b64decode(encrypted_data)
        salt = encrypted_data_bytes[8:16]
        ciphertext = encrypted_data_bytes[16:]
        key_iv = bytes_to_key(password.encode(), salt, 48)
        key = key_iv[:32]
        iv = key_iv[32:]
        cipher = AES.new(key, AES.MODE_CBC, iv)
        decrypted_padded = cipher.decrypt(ciphertext)
        return unpad(decrypted_padded, AES.block_size).decode()
    except Exception as e:
        logger.error(f"AES şifresi çözülürken hata: {e}")
        return None


# --- ŞİFRE ÇÖZME SONU ---


def _update_status_worker(
    conn, item_id, item_type, status=None, progress=None, filepath=None
):
    """Veritabanındaki indirme durumunu günceller."""
    table = "episodes"
    try:
        cursor = conn.cursor()
        if status:
            cursor.execute(
                f"UPDATE {table} SET status = ? WHERE id = ?", (status, item_id)
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
        logger.error(f"ID {item_id} için DB güncellemesinde hata: {e}", exc_info=True)


def find_video_source(target_url, user_data_dir):
    """Selenium ile iframe zincirini takip ederek video kaynağını ve şifresini bulur."""
    options = uc.ChromeOptions()
    options.add_argument("--window-size=1280,720")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--mute-audio")
    options.add_argument(f"--user-agent={config.USER_AGENT}")

    driver = None
    try:
        logger.info(f"Chrome başlatılıyor...")
        driver = uc.Chrome(user_data_dir=user_data_dir, options=options)

        # 1. Adım: Ana dizi sayfasına git
        logger.info(f"Ana sayfa yükleniyor: {target_url}")
        driver.get(target_url)
        wait = WebDriverWait(driver, 30)

        # 2. Adım: İlk iframe'i bul ve URL'sini al (örneğin king.php)
        logger.info("İlk video iframe'i aranıyor...")
        iframe1 = wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "iframe[src*='king.php'], iframe[src*='stream']")
            )
        )
        iframe1_url = iframe1.get_attribute("src")

        # 3. Adım: İlk iframe'in sayfasına git
        logger.info(f"İlk iframe'e gidiliyor: {iframe1_url}")
        driver.get(iframe1_url)

        # 4. Adım: İkinci iframe'i bul (molystream/cehennemstream)
        logger.info("İkinci video iframe'i aranıyor...")
        iframe2 = wait.until(
            EC.presence_of_element_located(
                (
                    By.CSS_SELECTOR,
                    "iframe[src*='molystream'], iframe[src*='cehennemstream']",
                )
            )
        )
        iframe2_url = iframe2.get_attribute("src")

        # 5. Adım: İkinci ve son iframe'in sayfasına git
        logger.info(f"Son video iframe'ine gidiliyor: {iframe2_url}")
        driver.get(iframe2_url)

        # 6. Adım: Şifreleme verisini ara
        logger.info("Şifreleme verisi aranıyor...")
        time.sleep(5)  # Sayfanın tam yüklenmesi için kısa bir bekleme
        page_source = driver.page_source
        match = re.search(
            r'CryptoJS\.AES\.decrypt\("([^"]+)",\s*"([^"]+)"\)', page_source, re.DOTALL
        )

        if not match:
            # --- HATA AYIKLAMA ÖZELLİĞİ ---
            debug_folder = "debug_logs"
            os.makedirs(debug_folder, exist_ok=True)
            filename = f"error_page_source_{int(time.time())}.html"
            filepath = os.path.join(debug_folder, filename)
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(page_source)
            logger.error(
                f"Sayfa kaynağında şifreleme verisi bulunamadı. Sayfa içeriği şuraya kaydedildi: {filepath}"
            )
            # --- HATA AYIKLAMA SONU ---
            return None, None

        encrypted_data = match.group(1)
        password = match.group(2)
        logger.info("Şifreleme verisi ve parola başarıyla bulundu.")

        decrypted_html = decrypt_aes(encrypted_data, password)
        if not decrypted_html:
            return None, None

        # 7. Adım: Şifresi çözülmüş HTML'den asıl video linkini çıkar
        source_match = re.search(r'src="([^"]+\.(?:m3u8|mp4))"', decrypted_html)
        if not source_match:
            logger.error("Çözülmüş HTML içinde video linki bulunamadı.")
            return None, None

        final_video_url = source_match.group(1)
        referer_url = iframe2_url

        logger.info(f"Asıl video linki başarıyla çözüldü: {final_video_url}")
        return final_video_url, referer_url

    except Exception as e:
        logger.error(
            f"Video kaynağı aranırken genel bir hata oluştu: {e}", exc_info=True
        )
        return None, None
    finally:
        if driver:
            driver.quit()


def download_with_yt_dlp(
    conn, item_id, item_type, video_url, referer, output_template, speed_limit
):
    """Verilen video linkini yt-dlp ile indirir."""
    final_output = f"{output_template}.%(ext)s"
    cmd = [
        "yt-dlp",
        "--newline",
        "--no-check-certificates",
        "--progress",
        "--verbose",
        "--hls-use-mpegts",
        "--merge-output-format",
        "mp4",
        "--format",
        "best",
        "-o",
        final_output,
    ]

    if speed_limit:
        cmd.extend(["--limit-rate", speed_limit])

    if referer:
        cmd.extend(["--referer", referer])

    cmd.append(video_url)

    logger.info(f"yt-dlp ile indirme başlatılıyor...")
    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            encoding="utf-8",
            errors="ignore",
        )

        output, last_progress = "", 0
        for line in iter(process.stdout.readline, ""):
            if not line:
                break
            output += line
            if "[download]" in line:
                progress_match = re.search(r"([0-9\.]+)%", line)
                if progress_match:
                    try:
                        progress = float(progress_match.group(1))
                        if progress > last_progress:
                            _update_status_worker(
                                conn, item_id, item_type, progress=progress
                            )
                            last_progress = progress
                    except:
                        pass
        process.wait()

        if process.returncode == 0:
            possible_files = glob.glob(f"{output_template}.*")
            if possible_files and os.path.getsize(possible_files[0]) > 1024 * 1024:
                return True, possible_files[0]
            else:
                file_size = os.path.getsize(possible_files[0]) if possible_files else 0
                return False, f"Hata: İndirilen dosya çok küçük ({file_size} bytes)"
        else:
            logger.error(f"yt-dlp hatası (kod: {process.returncode}): {output[-500:]}")
            return False, f"İndirme hatası (kod: {process.returncode})"

    except Exception as e:
        return False, f"Process hatası: {str(e)}"


def to_ascii_safe(text):
    """Dosya adları için güvenli karakter dönüşümü."""
    if not text:
        return ""
    replacements = {
        "ı": "i",
        "İ": "I",
        "ğ": "g",
        "Ğ": "G",
        "ü": "u",
        "Ü": "U",
        "ş": "s",
        "Ş": "S",
        "ö": "o",
        "Ö": "O",
        "ç": "c",
        "Ç": "C",
    }
    for tr, en in replacements.items():
        text = text.replace(tr, en)
    text = re.sub(r'[<>:"/\\|?*]', "_", text)
    text = re.sub(r"[^\x00-\x7F]+", "", text)
    return text.strip()


def process_video(item_id, item_type):
    """Ana video işleme süreci."""
    global logger
    if not logger.handlers:
        logger = setup_logging()

    conn = None
    profile_dir = os.path.abspath(os.path.join("chrome_profiles", f"user_{item_id}"))
    if os.path.exists(profile_dir):
        shutil.rmtree(profile_dir)
    os.makedirs(profile_dir, exist_ok=True)

    try:
        conn = sqlite3.connect(config.DATABASE)
        conn.row_factory = sqlite3.Row
        settings = get_all_settings_from_db(conn)
        item = conn.execute(
            "SELECT e.*, s.season_number, ser.title as series_title FROM episodes e JOIN seasons s ON e.season_id = s.id JOIN series ser ON s.series_id = ser.id WHERE e.id = ?",
            (item_id,),
        ).fetchone()
        if not item:
            logger.error(f"Episode ID {item_id} bulunamadı")
            return

        base_folder = settings.get("DOWNLOADS_FOLDER", "downloads")
        filename_template = settings.get("SERIES_FILENAME_TEMPLATE")
        file_path = filename_template.format(
            series_title=to_ascii_safe(item["series_title"]),
            season_number=item["season_number"],
            episode_number=item["episode_number"],
            episode_title=to_ascii_safe(
                item["title"] or f"Episode_{item['episode_number']}"
            ),
        )
        full_path = os.path.join(base_folder, *file_path.split(os.path.sep))
        final_dir = os.path.dirname(full_path)
        os.makedirs(final_dir, exist_ok=True)
        output_template = os.path.join(final_dir, os.path.basename(full_path))

        _update_status_worker(conn, item_id, item_type, status="Kaynak aranıyor...")
        video_url, referer = find_video_source(item["url"], profile_dir)

        if not video_url:
            _update_status_worker(
                conn, item_id, item_type, status="Hata: Video kaynağı bulunamadı"
            )
            return

        _update_status_worker(conn, item_id, item_type, status="İndiriliyor")
        success, result = download_with_yt_dlp(
            conn,
            item_id,
            item_type,
            video_url,
            referer,
            output_template,
            settings.get("SPEED_LIMIT"),
        )

        if success:
            _update_status_worker(
                conn,
                item_id,
                item_type,
                status="Tamamlandı",
                progress=100,
                filepath=result,
            )
            logger.info(
                f"ID {item_id} tamamlandı: {result} ({os.path.getsize(result) / 1024 / 1024:.1f}MB)"
            )
        else:
            _update_status_worker(conn, item_id, item_type, status=result)
            logger.error(f"ID {item_id} hata: {result}")

    except Exception as e:
        logger.exception(f"ID {item_id} genel hata: {e}")
        if conn:
            _update_status_worker(
                conn, item_id, item_type, status="Hata: Sistem hatası"
            )
    finally:
        if conn:
            conn.close()
        if profile_dir and os.path.exists(profile_dir):
            try:
                shutil.rmtree(profile_dir)
            except:
                pass
